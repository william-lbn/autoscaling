//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"golang.org/x/sys/unix"
)

const normalMemoryGuestScope = "normal-memory-origin-guest-source-v1"
const normalMemoryGuestIntentFile = "normal-memory-origin.intent.json"
const normalMemoryGuestCompleteFile = "normal-memory-origin.complete.json"

// 来源由固定channel实际消费，而非HTTP传入bool。原Host引用仍是非授权线值；
// 完整Host生命周期/未来Root控制写者必须在后继authority层另外持有其opaque来源。
type normalMemoryOriginIntent struct {
	ProtocolVersion uint32                                       `json:"protocol_version"`
	Scope           string                                       `json:"scope"`
	Host            bootprotocol.NormalMemoryOriginHostReference `json:"host"`
	InitialWAL      initialPrebootGuestWitnessKey                `json:"original_initial_wal"`
	Physical        PhysicalKernelObservation                    `json:"physical"`
	Region          guestMemoryMaximumObservation                `json:"region"`
	OriginalRoot    nativeBirthConsumer                          `json:"original_root"`
	MaximumBytes    uint64                                       `json:"maximum_bytes"`
}
type normalMemoryOriginCompletion struct {
	ProtocolVersion uint32                                  `json:"protocol_version"`
	Scope           string                                  `json:"scope"`
	IntentSHA256    string                                  `json:"intent_sha256"`
	Ack             bootprotocol.NormalMemoryOriginGuestAck `json:"ack"`
}

// 仅本Root真实capture或显式recover后持有。字段不导出、不可JSON反序列化成授权。
// 此对象只封原最大几何/历史，不封System/FS/所有角色的未来writer，不签normal。
type normalMemoryOriginSource struct {
	intent      normalMemoryOriginIntent
	complete    normalMemoryOriginCompletion
	currentRoot nativeBirthConsumer
	intentSHA   string
	completeSHA string
	journal     [2][3]uint64
}

// 前后真实读取原8WAL与固定ISO。运行期不调用initialPrebootFactsLocked的PGEmpty门，
// 不重放Kernel/Origin/PreSys准备，仅复用原KernelPrep的只读Observe和Physical.Match。
func (g *Guard) normalMemoryOriginCurrentLocked(ctx context.Context, s *lockedState, v normalMemoryOriginIntent) (guestMemoryMaximumObservation, error) {
	var zero guestMemoryMaximumObservation
	if g == nil || s == nil || s.dir == nil || s.lock == nil || ctx == nil || ctx.Err() != nil || v.OriginalRoot.PID <= 1 || v.OriginalRoot.StartTicks == 0 || !physicalDigestValid(v.OriginalRoot.InstanceNonce) || v.ProtocolVersion != 1 || v.Scope != normalMemoryGuestScope || v.Host.Validate() != nil || v.Physical.Validate() != nil || v.Region.validate() != nil {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("original memory Guest source malformed"))
	}
	in, e := g.initialPrebootInputs(v.Host.Prepared.Challenge)
	if e != nil {
		return zero, e
	}
	if e = normalMemoryGuestSelection(in); e != nil {
		return zero, e
	}
	if in.requirements.ProfileID() != NormalSplitV1PhysicalKernelProfileID || v.Host.ProfileID != NormalSplitV1PhysicalKernelProfileID || in.requirements.KernelBuildReference().KernelArtifactSHA256 != v.Host.Prepared.Challenge.KernelBuildSHA256 {
		return zero, fmt.Errorf("memory origin differs from original reviewed build")
	}
	before, e := initialPrebootGuestWitnessLocked(s, v.Host.Prepared.Challenge)
	if e != nil || before != v.InitialWAL {
		return zero, errors.Join(ErrUnknown, e, fmt.Errorf("original memory eight-WAL identity changed"))
	}
	var intent initialPrebootGuestIntent
	var done initialPrebootGuestComplete
	_, exists, e := initialPrebootReadRecord(s, initialPrebootGuestIntentName, &intent)
	if e != nil || !exists {
		return zero, errors.Join(ErrUnknown, e)
	}
	_, exists, e = initialPrebootReadRecord(s, initialPrebootGuestCompleteName, &done)
	if e != nil || !exists {
		return zero, errors.Join(ErrUnknown, e)
	}
	receipt, e := initialPrebootOriginalReceipt(intent, done)
	if e != nil || receipt != v.Host.Prepared {
		return zero, errors.Join(ErrUnknown, e, fmt.Errorf("original memory prepared receipt differs"))
	}
	ki, e := g.normalKernelBootstrapInputs()
	if e != nil {
		return zero, e
	}
	kp, e := g.normalKernelPreparationLocked(ctx, ki, kernelPrepObserve, s)
	if e != nil {
		return zero, e
	}
	sha, e := kp.ReferenceSHA256()
	if e != nil || sha != receipt.KernelPreparationSHA256 {
		return zero, errors.Join(ErrUnknown, e, fmt.Errorf("original kernel preparation reference changed"))
	}
	physical, ok := g.kernel.(physicalOriginKernel)
	if !ok {
		return zero, fmt.Errorf("original memory physical reader unavailable")
	}
	current, e := physical.physicalOriginRead()
	if e != nil {
		return zero, e
	}
	if e = v.Physical.Match(current); e != nil {
		return zero, e
	}
	if current.Identity.GuestBootID != v.Host.Prepared.GuestBootID {
		return zero, fmt.Errorf("memory Guest boot drifted")
	}
	one, e := readGuestMemoryMaximumComponent(current.Identity.GuestBootID)
	if e != nil {
		return zero, e
	}
	if e = v.Region.match(one); e != nil {
		return zero, e
	}
	two, e := readGuestMemoryMaximumComponent(current.Identity.GuestBootID)
	if e != nil {
		return zero, e
	}
	if e = one.match(two); e != nil {
		return zero, e
	}
	geometry, e := bootprotocol.DecodeNormalMemoryGeometry(v.Host.Prepared.Challenge.Identity, v.Host.Prepared.Challenge.OriginalBase, v.Host.QMP)
	if e != nil {
		return zero, e
	}
	maximum, e := compareGuestMaximumGeometry(one, uint64(geometry.BaseBytes), uint64(geometry.MaximumBytes), uint64(geometry.DeviceAddress), uint64(geometry.BackendBytes))
	if e != nil || maximum != v.MaximumBytes {
		return zero, errors.Join(ErrUnknown, e, fmt.Errorf("original Host/Guest maximum region changed"))
	}
	after, e := initialPrebootGuestWitnessLocked(s, v.Host.Prepared.Challenge)
	if e != nil || after != before {
		return zero, errors.Join(ErrUnknown, e, fmt.Errorf("original memory prefix changed while reading"))
	}
	return one, nil
}

func captureNormalMemoryOriginLocked(ctx context.Context, s *lockedState, g *Guard, prepared bootprotocol.InitialPrebootReceipt, moved bootprotocol.InitialMovableReceipt, done bootprotocol.InitialCompletion, host bootprotocol.NormalMemoryOriginHostReference) (*normalMemoryOriginSource, error) {
	if ctx == nil || ctx.Err() != nil || s == nil || s.dir == nil || s.lock == nil || g == nil || host.Validate() != nil || host.Prepared != prepared || host.Movable != moved || host.Completion != done || done.MatchReceipt(moved) != nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("memory source changed original initial channel"))
	}
	in, e := g.initialPrebootInputs(prepared.Challenge)
	if e != nil {
		return nil, e
	}
	if e = normalMemoryGuestSelection(in); e != nil {
		return nil, e
	}
	actual, e := g.initialPrebootReceiptLocked(ctx, s, in, false)
	if e != nil || actual != prepared {
		return nil, errors.Join(ErrUnknown, e, fmt.Errorf("memory capture needs original durable preparation"))
	}
	if _, exists, e := s.readPhysicalFile(normalMemoryGuestIntentFile); e != nil || exists {
		return nil, errors.Join(ErrUnknown, e, fmt.Errorf("memory origin exists; explicit original recovery required"))
	}
	if _, exists, e := s.readPhysicalFile(normalMemoryGuestCompleteFile); e != nil || exists {
		return nil, errors.Join(ErrUnknown, e, fmt.Errorf("orphan memory source completion"))
	}
	w, e := initialPrebootGuestWitnessLocked(s, prepared.Challenge)
	if e != nil {
		return nil, e
	}
	root, e := nativeBirthCurrentConsumerIdentity()
	if e != nil {
		return nil, e
	}
	if root.InstanceNonce != moved.RootInstanceNonce {
		return nil, fmt.Errorf("memory capture current Root changed after original Movable receipt")
	}
	k, ok := g.kernel.(physicalOriginKernel)
	if !ok {
		return nil, fmt.Errorf("original memory physical backend absent")
	}
	physical, e := k.physicalOriginRead()
	if e != nil {
		return nil, e
	}
	region, e := readGuestMemoryMaximumComponent(prepared.GuestBootID)
	if e != nil {
		return nil, e
	}
	geom, e := bootprotocol.DecodeNormalMemoryGeometry(prepared.Challenge.Identity, prepared.Challenge.OriginalBase, host.QMP)
	if e != nil {
		return nil, e
	}
	maximum, e := compareGuestMaximumGeometry(region, uint64(geom.BaseBytes), uint64(geom.MaximumBytes), uint64(geom.DeviceAddress), uint64(geom.BackendBytes))
	if e != nil {
		return nil, e
	}
	v := normalMemoryOriginIntent{1, normalMemoryGuestScope, host, w, physical, region, root, maximum}
	if _, e = g.normalMemoryOriginCurrentLocked(ctx, s, v); e != nil {
		return nil, e
	}
	raw, e := json.Marshal(v)
	if e != nil || len(raw) > 64<<10 {
		return nil, errors.Join(ErrUnknown, e, fmt.Errorf("memory Guest origin exceeds bound"))
	}
	if e = s.createPhysicalFile(normalMemoryGuestIntentFile, raw); e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	hostSHA, e := host.SHA256()
	if e != nil {
		return nil, e
	}
	ack := bootprotocol.NormalMemoryOriginGuestAck{ProtocolVersion: 1, Scope: bootprotocol.NormalMemoryOriginAckScope, HostReferenceSHA256: hostSHA, GuestSourceSHA256: physicalDigest(raw), GuestBootID: prepared.GuestBootID, RootInstanceNonce: root.InstanceNonce}
	if e = ack.MatchReference(host); e != nil {
		return nil, e
	}
	complete := normalMemoryOriginCompletion{1, normalMemoryGuestScope, physicalDigest(raw), ack}
	cr, e := json.Marshal(complete)
	if e != nil {
		return nil, e
	}
	if e = s.createPhysicalFile(normalMemoryGuestCompleteFile, cr); e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	if e = g.normalMemorySyncSourceLocked(ctx, s, v, complete); e != nil {
		return nil, e
	}
	journal, e := normalMemoryGuestJournalIdentity(s)
	if e != nil {
		return nil, e
	}
	return &normalMemoryOriginSource{v, complete, root, physicalDigest(raw), physicalDigest(cr), journal}, nil
}

// 首次完成后来源nonce/Host引用不变。恢复只同步原8WAL及原source，不造新Origin；
// 即使PG已出现也按原历史/当前固定控制验证，不再要求初始PGEmpty。
func (g *Guard) recoverNormalMemoryOriginLocked(ctx context.Context, s *lockedState) (*normalMemoryOriginSource, error) {
	if g == nil || s == nil || s.dir == nil || s.lock == nil || ctx == nil || ctx.Err() != nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original memory recovery lock/context missing"))
	}
	var v normalMemoryOriginIntent
	var c normalMemoryOriginCompletion
	raw, exists, e := initialPrebootReadRecord(s, normalMemoryGuestIntentFile, &v)
	if e != nil || !exists {
		return nil, errors.Join(ErrUnknown, e)
	}
	cr, exists, e := initialPrebootReadRecord(s, normalMemoryGuestCompleteFile, &c)
	if e != nil || !exists {
		return nil, errors.Join(ErrUnknown, e)
	}
	if c.ProtocolVersion != 1 || c.Scope != normalMemoryGuestScope || c.IntentSHA256 != physicalDigest(raw) || c.Ack.GuestSourceSHA256 != c.IntentSHA256 || c.Ack.MatchReference(v.Host) != nil || c.Ack.RootInstanceNonce != v.OriginalRoot.InstanceNonce {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original memory source completion differs"))
	}
	if e = g.normalMemorySyncSourceLocked(ctx, s, v, c); e != nil {
		return nil, e
	}
	root, e := nativeBirthCurrentConsumerIdentity()
	if e != nil {
		return nil, e
	}
	journal, e := normalMemoryGuestJournalIdentity(s)
	if e != nil {
		return nil, e
	}
	return &normalMemoryOriginSource{v, c, root, physicalDigest(raw), physicalDigest(cr), journal}, nil
}
func (g *Guard) normalMemorySyncSourceLocked(ctx context.Context, s *lockedState, v normalMemoryOriginIntent, c normalMemoryOriginCompletion) error {
	if _, e := g.normalMemoryOriginCurrentLocked(ctx, s, v); e != nil {
		return e
	}
	for _, n := range append(append([]string(nil), initialPrebootGuestFiles[:]...), normalMemoryGuestIntentFile, normalMemoryGuestCompleteFile) {
		if e := ctx.Err(); e != nil {
			return errors.Join(ErrUnknown, e)
		}
		if e := s.syncPhysicalFile(n); e != nil {
			return errors.Join(ErrUnknown, e)
		}
	}
	if e := s.dir.Sync(); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if _, e := g.normalMemoryOriginCurrentLocked(ctx, s, v); e != nil {
		return e
	}
	raw, e := json.Marshal(v)
	if e != nil {
		return e
	}
	cr, e := json.Marshal(c)
	if e != nil {
		return e
	}
	a, exists, e := s.readPhysicalFile(normalMemoryGuestIntentFile)
	if e != nil || !exists || !bytes.Equal(a, raw) {
		return errors.Join(ErrUnknown, e, fmt.Errorf("memory source intent changed during sync"))
	}
	b, exists, e := s.readPhysicalFile(normalMemoryGuestCompleteFile)
	if e != nil || !exists || !bytes.Equal(b, cr) {
		return errors.Join(ErrUnknown, e, fmt.Errorf("memory source complete changed during sync"))
	}
	return nil
}
func (o *normalMemoryOriginSource) recheckLocked(ctx context.Context, s *lockedState, g *Guard) error {
	if o == nil || g == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("opaque memory source absent"))
	}
	journal, e := normalMemoryGuestJournalIdentity(s)
	if e != nil || journal != o.journal {
		return errors.Join(ErrUnknown, e, fmt.Errorf("memory source durable file identity changed"))
	}
	root, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || root != o.currentRoot {
		return errors.Join(ErrUnknown, e, fmt.Errorf("memory source requires explicit current Root recovery"))
	}
	raw, exists, e := s.readPhysicalFile(normalMemoryGuestIntentFile)
	if e != nil || !exists || physicalDigest(raw) != o.intentSHA {
		return errors.Join(ErrUnknown, e, fmt.Errorf("memory source original intent drift"))
	}
	cr, exists, e := s.readPhysicalFile(normalMemoryGuestCompleteFile)
	if e != nil || !exists || physicalDigest(cr) != o.completeSHA {
		return errors.Join(ErrUnknown, e, fmt.Errorf("memory source original completion drift"))
	}
	_, e = g.normalMemoryOriginCurrentLocked(ctx, s, o.intent)
	return e
}

// 仅被固定initialGuestExchange的新协议分支调用。收到kind6才做本Root真实捕获，
// 完整持久后发kind7；读取错误/取消/短帧不重发，不调用正式Provision或签normal。
func normalMemoryGuestExchange(ctx context.Context, port io.ReadWriter, g *Guard, prepared bootprotocol.InitialPrebootReceipt, moved bootprotocol.InitialMovableReceipt, done bootprotocol.InitialCompletion) error {
	if ctx == nil || g == nil || port == nil || done.MatchReceipt(moved) != nil {
		return fmt.Errorf("fixed original memory Guest channel required")
	}
	host, e := bootprotocol.ReadNormalMemoryOriginHostReference(port)
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(host.Prepared, prepared) || !reflect.DeepEqual(host.Movable, moved) || !reflect.DeepEqual(host.Completion, done) {
		return fmt.Errorf("memory Host reference changed initial exchange")
	}
	s, e := lockState(ctx, g.config.StateDir)
	if e != nil {
		return e
	}
	defer s.close()
	source, e := captureNormalMemoryOriginLocked(ctx, s, g, prepared, moved, done, host)
	if e != nil {
		return e
	}
	if e = source.recheckLocked(ctx, s, g); e != nil {
		return e
	}
	return bootprotocol.WriteNormalMemoryOriginGuestAck(port, source.complete.Ack)
}

// 本Root成功sync后的两source inode也必须固定；同字节替换不能继承耐久见证。
func normalMemoryGuestJournalIdentity(s *lockedState) ([2][3]uint64, error) {
	var out [2][3]uint64
	if s == nil || s.dir == nil || s.lock == nil {
		return out, ErrUnknown
	}
	for i, n := range []string{normalMemoryGuestIntentFile, normalMemoryGuestCompleteFile} {
		var st unix.Stat_t
		if e := unix.Fstatat(int(s.dir.Fd()), n, &st, unix.AT_SYMLINK_NOFOLLOW); e != nil {
			return out, e
		}
		if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0077 != 0 || st.Uid != 0 || st.Size <= 0 || st.Size > 64<<10 {
			return out, ErrUnknown
		}
		out[i] = [3]uint64{st.Dev, st.Ino, uint64(st.Size)}
	}
	return out, nil
}

func normalMemoryGuestSelection(in initialPrebootGuestInputs) error {
	if !in.requirements.Enabled() || in.requirements.ProfileID() != NormalSplitV1PhysicalKernelProfileID || in.requirements.MemoryOriginProtocolVersion() != 1 || in.requirements.RuntimeDocumentSHA256() != physicalDigest(in.runtime) {
		return errors.Join(ErrUnknown, fmt.Errorf("original ISO did not explicitly select normal memory origin wire v1"))
	}
	return nil
}
