//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
	"os"
	"reflect"
	"strings"
	"sync"
)

// 锁序：外层原resource flock → 本stage mutex → 固定dirfd文件/同步fsync。
// 不能从Persist重入withRecord。这里不改共享record，不创建正式Birth或normal许可。
type nativeBirthStage struct {
	mu       sync.Mutex
	state    *lockedState
	snapshot nativeBirthStageSnapshot
	previous string
	failed   bool
}

func nativeBirthStageDirectory(s *lockedState, p nativeBirthPending) error {
	if s == nil || s.dir == nil || s.lock == nil {
		return fmt.Errorf("native stage requires original locked state")
	}
	if err := privateRegular(int(s.lock.Fd()), unix.S_IFREG); err != nil {
		return err
	}
	if err := privateRegular(int(s.dir.Fd()), unix.S_IFDIR); err != nil {
		return err
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(s.dir.Fd()), &st); err != nil {
		return err
	}
	if control.Uint64(st.Dev) != p.StateDevice || control.Uint64(st.Ino) != p.StateInode {
		return fmt.Errorf("native original state directory changed")
	}
	return nil
}
func nativeBirthStageRecord(r *record, p nativeBirthPending, source nativeBirthSource) error {
	if p.validate() != nil || source.resource.Validate() != nil || p.Source != source.binding || r == nil ||
		r.validate(p.Original.Identity) != nil || r.Bootstrap == nil || !r.Bootstrap.Applied ||
		r.Original != nil && !r.Applied || sameBootstrapSource(source.resource, r.Bootstrap.Original) != nil ||
		r.Floor != p.ResourceFloor || p.Original.AddressSpaceMaxBytes != r.addressSpaceLimit() {
		return fmt.Errorf("native pending differs from retained resource source")
	}
	if r.Birth != nil && *r.Birth != p.Original {
		return fmt.Errorf("native pending cannot replace another original birth")
	}
	return nil
}

var nativeBirthConsumerOnce sync.Once
var nativeBirthConsumerInstance string
var nativeBirthConsumerInstanceError error

// 每进程私有随机实例不从WAL恢复。PID/start ticks可能同tick复用，不能把它们
// 当绝对唯一标识；原listener仅向可信launcher提供本实例元数据。
func nativeBirthCurrentConsumerIdentity() (nativeBirthConsumer, error) {
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return nativeBirthConsumer{}, fmt.Errorf("native consumer requires real root")
	}
	nativeBirthConsumerOnce.Do(func() { nativeBirthConsumerInstance, nativeBirthConsumerInstanceError = newNativeBirthNonce() })
	if nativeBirthConsumerInstanceError != nil {
		return nativeBirthConsumer{}, nativeBirthConsumerInstanceError
	}
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return nativeBirthConsumer{}, err
	}
	ticks, err := postmasterTicks(string(stat), os.Getpid())
	if err != nil {
		return nativeBirthConsumer{}, err
	}
	return nativeBirthConsumer{PID: os.Getpid(), StartTicks: control.Uint64(ticks), InstanceNonce: nativeBirthConsumerInstance}, nil
}
func nativeBirthCurrentConsumer(p nativeBirthPending) error {
	current, err := nativeBirthCurrentConsumerIdentity()
	if err != nil || current != p.Consumer {
		return fmt.Errorf("native original Root consumer incarnation changed")
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(string(boot)) != p.Original.Identity.GuestBootID {
		return fmt.Errorf("native original consumer Guest changed")
	}
	return nil
}
func nativeBirthStageFiles(s *lockedState) (map[string]bool, error) {
	fd, err := unix.Openat(int(s.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "native-original-state-scan")
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, n := range names {
		if strings.HasPrefix(n, "native-birth.") {
			out[n] = true
		}
	}
	return out, nil
}

// 只能受控launcher在任何PG exec之前创建。Consumer来自原固定listener的真实
// Root身份，不接受HTTP自选PID；Source来自只读ISO reader。此层仍不替代内核核验。
func createNativeBirthPendingLocked(s *lockedState, r *record, source nativeBirthSource, p nativeBirthPending) (NativeBirthRef, error) {
	var zero NativeBirthRef
	if err := nativeBirthStageRecord(r, p, source); err != nil {
		return zero, err
	}
	if err := nativeBirthStageDirectory(s, p); err != nil {
		return zero, err
	}
	files, err := nativeBirthStageFiles(s)
	if err != nil {
		return zero, err
	}
	if len(files) != 0 {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("native pending/history already present; explicit original recovery required"))
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return zero, err
	}
	if err := s.createPhysicalFile(nativeBirthStageName(p.Nonce, "pending"), raw); err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	return nativeBirthRef(raw, p), nil
}

// Observe不会返回可写stage，不同步或修复文件；partial/orphan/未知阶段均Unknown。
// 即使released文件有效，也只是历史non-authorizing内容，不凭重启后读取发新资格。
func observeNativeBirthStageLocked(s *lockedState, ref NativeBirthRef) (nativeBirthStageSnapshot, error) {
	var out nativeBirthStageSnapshot
	unknown := func(err error) (nativeBirthStageSnapshot, error) {
		return nativeBirthStageSnapshot{}, errors.Join(ErrUnknown, err)
	}
	if ref.validate() != nil || s == nil || s.dir == nil || s.lock == nil {
		return unknown(fmt.Errorf("native original reference or lock missing"))
	}
	raw, found, err := s.readPhysicalFile(nativeBirthStageName(ref.Nonce, "pending"))
	if err != nil || !found {
		return unknown(fmt.Errorf("native original pending unreadable: %w", err))
	}
	var p nativeBirthPending
	if control.UnmarshalObject(raw, &p) != nil || p.validate() != nil || nativeBirthRef(raw, p) != ref {
		return unknown(fmt.Errorf("native original pending bytes differ"))
	}
	if err := nativeBirthStageDirectory(s, p); err != nil {
		return unknown(err)
	}
	files, err := nativeBirthStageFiles(s)
	if err != nil {
		return unknown(err)
	}
	delete(files, nativeBirthStageName(ref.Nonce, "pending"))
	out = nativeBirthStageSnapshot{Ref: ref, Pending: p, Phase: nativeBirthStagePending}
	previous := ref.PendingSHA256
	missing := false
	for _, phase := range []nativeBirthStagePhase{nativeBirthStageBase, nativeBirthStageFinal, nativeBirthStageReleased} {
		name := nativeBirthStageName(ref.Nonce, nativeBirthStagePhaseName(phase))
		exists := files[name]
		delete(files, name)
		if !exists {
			missing = true
			continue
		}
		if missing {
			return unknown(fmt.Errorf("native stage has a phase gap"))
		}
		raw, found, err := s.readPhysicalFile(name)
		if err != nil || !found {
			return unknown(fmt.Errorf("native phase unreadable"))
		}
		var e nativeBirthStageEntry
		if err := control.UnmarshalObject(raw, &e); err != nil {
			return unknown(err)
		}
		var prior *nativeBirthStageEntry
		if len(out.Entries) != 0 {
			prior = &out.Entries[len(out.Entries)-1]
		}
		if e.Phase != phase {
			return unknown(fmt.Errorf("native stage filename differs"))
		}
		if err := e.validate(p, ref, previous, prior); err != nil {
			return unknown(err)
		}
		out.Phase = phase
		out.Entries = append(out.Entries, e)
		previous = physicalDigest(raw)
	}
	if len(files) != 0 {
		return unknown(fmt.Errorf("native foreign/orphan stage family present"))
	}
	return out, nil
}

// 只有原Root实例能接收尚未消费的Pending；新Root只Observe。已有base/final/released
// 永不重开writer，避免重复ACK、重复迁组或把verified-pending-release猜成成功。
func openNativeBirthPendingLocked(s *lockedState, r *record, source nativeBirthSource, ref NativeBirthRef) (*nativeBirthStage, error) {
	snap, err := observeNativeBirthStageLocked(s, ref)
	if err != nil {
		return nil, err
	}
	if snap.Phase != nativeBirthStagePending {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("native stage already consumed"))
	}
	if err := nativeBirthStageRecord(r, snap.Pending, source); err != nil {
		return nil, err
	}
	if r.Birth == nil || *r.Birth != snap.Pending.Original {
		return nil, fmt.Errorf("native original launch Birth not paired")
	}
	if err := nativeBirthCurrentConsumer(snap.Pending); err != nil {
		return nil, errors.Join(ErrUnknown, err)
	}
	return &nativeBirthStage{state: s, snapshot: snap, previous: ref.PendingSHA256}, nil
}
func (s *nativeBirthStage) persist(ctx context.Context, phase nativeFilterPhase, tr nativeBirthTraceTranscript) error {
	p := nativeBirthStagePending
	switch phase {
	case nativeFilterPMBase:
		p = nativeBirthStageBase
	case nativeFilterPMFinal:
		p = nativeBirthStageFinal
	default:
		return fmt.Errorf("unsupported native callback phase")
	}
	return s.append(ctx, p, tr)
}
func (s *nativeBirthStage) released(ctx context.Context, tr nativeBirthTraceTranscript) error {
	return s.append(ctx, nativeBirthStageReleased, tr)
}
func (s *nativeBirthStage) append(ctx context.Context, phase nativeBirthStagePhase, tr nativeBirthTraceTranscript) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return errors.Join(ErrUnknown, fmt.Errorf("native stage failure cannot replay callback"))
	}
	s.failed = true // 任何未知I/O后不能复用同一writer；也不能将取消包装成NoEffect。
	if ctx == nil {
		return fmt.Errorf("native stage context required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := nativeBirthCurrentConsumer(s.snapshot.Pending); err != nil {
		return errors.Join(ErrUnknown, err)
	}
	if phase != s.snapshot.Phase+1 {
		return fmt.Errorf("native stage out of order")
	}
	current, err := observeNativeBirthStageLocked(s.state, s.snapshot.Ref)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, s.snapshot) {
		return errors.Join(ErrUnknown, fmt.Errorf("native stage changed while locked"))
	}
	entry := nativeBirthStageEntry{ProtocolVersion: 1, Scope: nativeBirthStageScope, Ref: s.snapshot.Ref, Phase: phase, PreviousSHA256: s.previous, Transcript: tr}
	var prior *nativeBirthStageEntry
	if len(s.snapshot.Entries) != 0 {
		prior = &s.snapshot.Entries[len(s.snapshot.Entries)-1]
	}
	if err := entry.validate(s.snapshot.Pending, s.snapshot.Ref, s.previous, prior); err != nil {
		return err
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if err := s.state.createPhysicalFile(nativeBirthStageName(s.snapshot.Ref.Nonce, nativeBirthStagePhaseName(phase)), raw); err != nil {
		return errors.Join(ErrUnknown, err)
	}
	s.snapshot.Phase = phase
	s.snapshot.Entries = append(s.snapshot.Entries, entry)
	s.previous = physicalDigest(raw)
	s.failed = false
	return nil
}
