//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const normalSplitQuotaPrefix = "normal-split-quota."
const normalSplitQuotaScope = "original-normal-split-quota-v1"

var normalSplitQuotaStages = [...]string{"pending", "first-applied", "second-applied", "complete"}

// 全部字段是Root完整来源检查后固定的原意图，不能直接由HTTP/PG JSON构造授权。
// P固定；缩容先A再N，扩容先N再A。F与counter是连续来源上界，不是current/RSS。
type normalSplitQuotaOriginal struct {
	Command               Command                     `json:"command"`
	SourceSHA256          string                      `json:"source_sha256"`
	SplitIntentSHA256     string                      `json:"split_intent_sha256"`
	SplitCompletionSHA256 string                      `json:"split_completion_sha256"`
	PGRequestSHA256       string                      `json:"pg_request_sha256"`
	MappingGeneration     control.Uint64              `json:"mapping_generation"`
	Original              NormalSplitTopologySnapshot `json:"original"`
	TargetArraysBytes     control.Uint64              `json:"target_arrays_bytes"`
	ForceUpperBytes       control.Uint64              `json:"force_upper_bytes"`
	CounterGapBytes       control.Uint64              `json:"counter_gap_bytes"`
}

func (v normalSplitQuotaOriginal) validate() error {
	if v.Command.Validate() != nil || splitBasic(v.Original) != nil || v.Command.Identity != v.Original.Identity || v.MappingGeneration == 0 || v.Original.Private == nil || v.Original.Arrays == nil {
		return fmt.Errorf("quota original source or topology missing")
	}
	for _, h := range []string{v.SourceSHA256, v.SplitIntentSHA256, v.SplitCompletionSHA256, v.PGRequestSHA256} {
		if !physicalDigestValid(h) {
			return fmt.Errorf("quota original source digest invalid")
		}
	}
	a, p, n := v.Original.Arrays, v.Original.Private, v.Original.Parent
	if !splitSameNode(*p, *p) || !splitSameNode(*a, *a) || p.Device != n.Device || a.Device != n.Device || p.Inode == 0 || a.Inode == 0 || p.Inode == a.Inode || p.Inode == n.Inode || a.Inode == n.Inode || n.Controllers != "memory pids" || p.Controllers != "" || a.Controllers != "" || a.PidsMaxTasks != 1 || !a.Empty || p.PidsMaxTasks == 0 || p.PidsMaxTasks >= control.Uint64(math.MaxInt64) {
		return fmt.Errorf("quota exact two-leaf source differs")
	}
	for _, node := range []normalSplitNode{n, *p, *a} {
		if node.MemoryMaxBytes == 0 || uint64(node.MemoryMaxBytes) >= math.MaxInt64 || uint64(node.MemoryMaxBytes)%(64<<10) != 0 || node.SwapMaxBytes != 0 || node.SwapCurrentBytes != 0 || !node.MemoryHighUnlimited {
			return fmt.Errorf("quota original caps are not finite fixed profile")
		}
	}
	if v.TargetArraysBytes == 0 || uint64(v.TargetArraysBytes) >= math.MaxInt64 || uint64(v.TargetArraysBytes)%(64<<10) != 0 || uint64(v.Command.MemoryMaxBytes)%(64<<10) != 0 || v.Command.PidsMaxTasks != n.PidsMaxTasks {
		return fmt.Errorf("quota target changes original task profile or alignment")
	}
	sum, e := normalInflightSum(uint64(p.MemoryMaxBytes), uint64(v.TargetArraysBytes), uint64(v.ForceUpperBytes), uint64(v.CounterGapBytes))
	if e != nil || sum > uint64(v.Command.MemoryMaxBytes) {
		return fmt.Errorf("quota target does not cover fixed P plus A and continuous F/counter")
	}
	if v.Command.MemoryMaxBytes < n.MemoryMaxBytes {
		if v.TargetArraysBytes >= a.MemoryMaxBytes {
			return fmt.Errorf("quota shrink must lower actual array domain")
		}
	} else if v.Command.MemoryMaxBytes > n.MemoryMaxBytes {
		if v.TargetArraysBytes <= a.MemoryMaxBytes {
			return fmt.Errorf("quota grow must enlarge actual array domain")
		}
	} else {
		return fmt.Errorf("quota transaction must change actual normal parent")
	}
	return nil
}
func (v normalSplitQuotaOriginal) growing() bool {
	return v.Command.MemoryMaxBytes > v.Original.Parent.MemoryMaxBytes
}
func normalSplitQuotaKey(v normalSplitQuotaOriginal) string {
	b, _ := json.Marshal(v.Command)
	return physicalDigest(b)
}
func normalSplitQuotaName(key, stage string) string {
	return normalSplitQuotaPrefix + key + "." + stage + ".json"
}

// 此opaque只由normalQuotaAuthorizeLocked在同原flock中消费原Birth/pin、PG原
// 完成或pre-grow计划、固定P/Kernel/System/F/I预算后创建；nil绝不放行。
// A_target包含旧代真实收费与helper预留；未来helper仍由同锁和持续A硬cap约束。
// 单有N等式、结构合法、来源SHA或一次current采样均不能代替完整检查。
type normalSplitQuotaAuthorization struct {
	original      normalSplitQuotaOriginal
	check         func(context.Context, *lockedState, *record, normalSplitQuotaOriginal) error
	budgetHandoff *normalQuotaBudgetHandoff
}
type normalSplitQuotaKernel interface {
	normalSplitRead() (NormalSplitTopologySnapshot, error)
	normalSplitQuotaWrite(string, normalSplitNode, control.Uint64) error
}
type normalSplitQuotaEntry struct {
	ProtocolVersion uint32                   `json:"protocol_version"`
	Scope           string                   `json:"scope"`
	Stage           string                   `json:"stage"`
	Original        normalSplitQuotaOriginal `json:"original"`
	PreviousSHA256  string                   `json:"previous_sha256"`
}
type normalSplitQuotaWitness struct {
	Directory [4]uint64
	FileIDs   [4][3]uint64
	SHA       [4]string
}
type normalSplitQuotaJournal struct{ durable sync.Map }

var normalSplitQuotaJournals sync.Map

func (g *Guard) normalSplitQuotaJournal() *normalSplitQuotaJournal {
	v, _ := normalSplitQuotaJournals.LoadOrStore(g, &normalSplitQuotaJournal{})
	return v.(*normalSplitQuotaJournal)
}

type normalSplitQuotaIO struct {
	create   func(*lockedState, string, []byte) error
	syncFile func(*lockedState, string) error
	syncDir  func(*lockedState) error
}

func normalSplitQuotaDiskIO() normalSplitQuotaIO {
	return normalSplitQuotaIO{func(s *lockedState, n string, b []byte) error { return s.createPhysicalFile(n, b) }, func(s *lockedState, n string) error { return s.syncPhysicalFile(n) }, func(s *lockedState) error { return s.dir.Sync() }}
}

func normalSplitQuotaRead(s *lockedState, key string) (normalSplitQuotaOriginal, int, normalSplitQuotaWitness, error) {
	var original normalSplitQuotaOriginal
	var w normalSplitQuotaWitness
	fail := func(e error) (normalSplitQuotaOriginal, int, normalSplitQuotaWitness, error) {
		return original, 0, w, errors.Join(ErrUnknown, e)
	}
	if s == nil || s.dir == nil || s.lock == nil || !physicalDigestValid(key) {
		return fail(fmt.Errorf("quota original state missing"))
	}
	var dir, lock unix.Stat_t
	if unix.Fstat(int(s.dir.Fd()), &dir) != nil || unix.Fstat(int(s.lock.Fd()), &lock) != nil || privateRegular(int(s.dir.Fd()), unix.S_IFDIR) != nil || privateRegular(int(s.lock.Fd()), unix.S_IFREG) != nil || dir.Dev != lock.Dev {
		return fail(fmt.Errorf("quota original state identity invalid"))
	}
	w.Directory = [4]uint64{dir.Dev, dir.Ino, lock.Dev, lock.Ino}
	count := 0
	prev := ""
	gap := false
	for i, stage := range normalSplitQuotaStages {
		name := normalSplitQuotaName(key, stage)
		raw, found, e := s.readPhysicalFile(name)
		if e != nil {
			return fail(e)
		}
		if !found {
			gap = true
			continue
		}
		if gap {
			return fail(fmt.Errorf("quota phase has missing original prefix"))
		}
		var entry normalSplitQuotaEntry
		if control.UnmarshalObjectBounded(raw, &entry, 256<<10, 65536) != nil || entry.ProtocolVersion != 1 || entry.Scope != normalSplitQuotaScope || entry.Stage != stage || entry.PreviousSHA256 != prev || entry.Original.validate() != nil || normalSplitQuotaKey(entry.Original) != key {
			return fail(fmt.Errorf("quota phase binding invalid"))
		}
		canonical, e := json.Marshal(entry)
		if e != nil || !bytes.Equal(raw, canonical) {
			return fail(fmt.Errorf("quota phase bytes not canonical"))
		}
		if i == 0 {
			original = entry.Original
		} else if !reflect.DeepEqual(original, entry.Original) {
			return fail(fmt.Errorf("quota phase changed original intent"))
		}
		var st unix.Stat_t
		if unix.Fstatat(int(s.dir.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != 0 || st.Mode&0077 != 0 {
			return fail(fmt.Errorf("quota phase inode is not private"))
		}
		w.FileIDs[i] = [3]uint64{st.Dev, st.Ino, uint64(st.Size)}
		w.SHA[i] = physicalDigest(raw)
		prev = w.SHA[i]
		count++
	}
	return original, count, w, nil
}
func normalSplitQuotaKeys(s *lockedState) ([]string, error) {
	if s == nil || s.dir == nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("quota state missing"))
	}
	fd, e := unix.Openat(int(s.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), "quota-prefix-scan")
	defer f.Close()
	names, e := f.Readdirnames(4097)
	if e != nil && e != io.EOF {
		return nil, e
	}
	if len(names) > 4096 {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("quota journal inventory exceeds bound"))
	}
	seen := map[string]bool{}
	var keys []string
	for _, n := range names {
		if !strings.HasPrefix(n, normalSplitQuotaPrefix) {
			continue
		}
		rest := strings.TrimPrefix(n, normalSplitQuotaPrefix)
		parts := strings.Split(rest, ".")
		if len(parts) != 3 || !physicalDigestValid(parts[0]) || parts[2] != "json" {
			return nil, errors.Join(ErrUnknown, fmt.Errorf("foreign quota journal"))
		}
		valid := false
		for _, stage := range normalSplitQuotaStages {
			valid = valid || stage == parts[1]
		}
		if !valid {
			return nil, errors.Join(ErrUnknown, fmt.Errorf("unknown quota journal phase"))
		}
		if !seen[parts[0]] {
			seen[parts[0]] = true
			keys = append(keys, parts[0])
		}
	}
	return keys, nil
}

// 仅零写核原quota族；完整可见但无本Guard成功fsync见证时仍拒，不能从SHA补造。
func (g *Guard) rejectNormalSplitQuotaPendingLocked(s *lockedState) error {
	keys, e := normalSplitQuotaKeys(s)
	if e != nil {
		return e
	}
	j := g.normalSplitQuotaJournal()
	for _, key := range keys {
		_, count, w, e := normalSplitQuotaRead(s, key)
		if e != nil {
			return e
		}
		if count != 4 {
			return errors.Join(ErrUnknown, fmt.Errorf("original quota is pending"))
		}
		if _, ok := j.durable.Load(w); !ok {
			return errors.Join(ErrUnknown, fmt.Errorf("quota completion lacks this Guard durability witness"))
		}
	}
	return nil
}

func normalSplitQuotaMatches(v normalSplitQuotaOriginal, now NormalSplitTopologySnapshot, firstDone, secondDone bool) error {
	if splitStable(v.Original, now) != nil || now.Private == nil || now.Arrays == nil || !splitSameNode(*v.Original.Private, *now.Private) || !splitSameNode(*v.Original.Arrays, *now.Arrays) {
		return fmt.Errorf("quota original cgroup inode drift")
	}
	before := v.Original
	wantN, wantA := before.Parent.MemoryMaxBytes, before.Arrays.MemoryMaxBytes
	if firstDone {
		if v.growing() {
			wantN = v.Command.MemoryMaxBytes
		} else {
			wantA = v.TargetArraysBytes
		}
	}
	if secondDone {
		wantN, wantA = v.Command.MemoryMaxBytes, v.TargetArraysBytes
	}
	for i, pair := range [][2]normalSplitNode{{before.Parent, now.Parent}, {*before.Private, *now.Private}, {*before.Arrays, *now.Arrays}} {
		a, b := pair[0], pair[1]
		want := a.MemoryMaxBytes
		if i == 0 {
			want = wantN
		}
		if i == 2 {
			want = wantA
		}
		if b.MemoryMaxBytes != want || b.PidsMaxTasks != a.PidsMaxTasks || b.SwapMaxBytes != 0 || b.SwapCurrentBytes != 0 || !b.MemoryHighUnlimited || b.Controllers != a.Controllers {
			return fmt.Errorf("quota original immutable control or exact phase cap drift")
		}
	}
	if !now.Arrays.Empty {
		return fmt.Errorf("quota helper is still executing")
	}
	return nil
}

// 私有有界事务，只由完整normal生产信封消费者交入opaque授权，不输出VM许可。
// 普通Apply的原降限拒绝保持；本函数不改state.json/Held，交接由独立handoff完成。
func (g *Guard) normalSplitQuotaLocked(ctx context.Context, s *lockedState, r *record, auth normalSplitQuotaAuthorization, mode string, disk normalSplitQuotaIO) (NormalSplitTopologySnapshot, error) {
	var zero NormalSplitTopologySnapshot
	unknown := func(e error) (NormalSplitTopologySnapshot, error) { return zero, errors.Join(ErrUnknown, e) }
	v := auth.original
	if ctx == nil || auth.check == nil || v.validate() != nil || mode != "apply" && mode != "observe" && mode != "replay" {
		return unknown(fmt.Errorf("quota original authorization or action missing"))
	}
	k, ok := g.kernel.(normalSplitQuotaKernel)
	if !ok {
		return unknown(fmt.Errorf("quota original kernel unavailable"))
	}
	if disk.create == nil || disk.syncFile == nil || disk.syncDir == nil {
		return unknown(fmt.Errorf("quota durability backend missing"))
	}
	if e := auth.check(ctx, s, r, v); e != nil {
		return unknown(e)
	}
	if e := g.rejectArrayPendingLocked(s, r); e != nil {
		return unknown(e)
	}
	key := normalSplitQuotaKey(v)
	j := g.normalSplitQuotaJournal()
	keys, e := normalSplitQuotaKeys(s)
	if e != nil {
		return unknown(e)
	}
	exists := false
	for _, other := range keys {
		if other == key {
			exists = true
			continue
		}
		prior, count, w, e := normalSplitQuotaRead(s, other)
		if e != nil || count != 4 {
			return unknown(fmt.Errorf("another quota intent is pending: %v", e))
		}
		if _, ok := j.durable.Load(w); !ok {
			return unknown(fmt.Errorf("historical quota lacks original durability"))
		}
		if prior.Command.Identity != v.Command.Identity || prior.Command.IssuerEpoch > v.Command.IssuerEpoch || prior.Command.IssuerEpoch == v.Command.IssuerEpoch && prior.Command.Revision >= v.Command.Revision {
			return unknown(fmt.Errorf("quota original revision/floor regressed"))
		}
	}
	_, count, w, e := normalSplitQuotaRead(s, key)
	if e != nil {
		return unknown(e)
	}
	if exists {
		old, _, _, e := normalSplitQuotaRead(s, key)
		if e != nil || !reflect.DeepEqual(old, v) {
			return unknown(fmt.Errorf("quota replay changed original intent"))
		}
		if count < 4 && mode != "replay" {
			return unknown(fmt.Errorf("pending quota requires exact explicit replay"))
		}
		if count == 4 {
			if _, ok := j.durable.Load(w); ok {
				now, e := k.normalSplitRead()
				if e != nil {
					return unknown(e)
				}
				if e = normalSplitQuotaMatches(v, now, true, true); e != nil {
					return unknown(e)
				}
				return now, nil
			}
			if mode != "replay" {
				return unknown(fmt.Errorf("visible quota complete is not durability"))
			}
		}
	} else if mode != "apply" {
		return unknown(fmt.Errorf("unrecorded quota cannot observe or replay"))
	}
	if mode == "observe" {
		return unknown(fmt.Errorf("quota observation is not completed"))
	}
	appendStage := func(index int) error {
		prev := ""
		if index > 0 {
			_, n, b, e := normalSplitQuotaRead(s, key)
			if e != nil || n != index {
				return errors.Join(e, fmt.Errorf("quota original prefix changed"))
			}
			prev = b.SHA[index-1]
		}
		b, e := json.Marshal(normalSplitQuotaEntry{1, normalSplitQuotaScope, normalSplitQuotaStages[index], v, prev})
		if e != nil {
			return e
		}
		return disk.create(s, normalSplitQuotaName(key, normalSplitQuotaStages[index]), b)
	}
	if !exists {
		now, e := k.normalSplitRead()
		if e != nil {
			return unknown(e)
		}
		if e = normalSplitQuotaMatches(v, now, false, false); e != nil {
			return unknown(e)
		}
		if e = ctx.Err(); e != nil {
			return unknown(e)
		}
		if e = appendStage(0); e != nil {
			return unknown(e)
		}
		count = 1
	}
	// 每次写入同步保持原flock。重放只接受此原步骤的准确写前/写后状态，
	// 不得先降低再恢复一份外来上限。
	for step := 0; step < 2; step++ {
		if count >= step+2 {
			continue
		}
		if e = ctx.Err(); e != nil {
			return unknown(e)
		}
		if e = auth.check(ctx, s, r, v); e != nil {
			return unknown(e)
		}
		if e = g.rejectArrayPendingLocked(s, r); e != nil {
			return unknown(e)
		}
		for i := 0; i < count; i++ {
			if e = disk.syncFile(s, normalSplitQuotaName(key, normalSplitQuotaStages[i])); e != nil {
				return unknown(e)
			}
		}
		if e = disk.syncDir(s); e != nil {
			return unknown(e)
		}
		now, e := k.normalSplitRead()
		if e != nil {
			return unknown(e)
		}
		beforeErr := normalSplitQuotaMatches(v, now, step > 0, false)
		afterErr := normalSplitQuotaMatches(v, now, true, step > 0)
		if beforeErr != nil && afterErr != nil {
			return unknown(fmt.Errorf("quota actual partial step is neither original before nor exact after"))
		}
		if beforeErr == nil {
			name, node, target := "arrays", *v.Original.Arrays, v.TargetArraysBytes
			if (v.growing() && step == 0) || (!v.growing() && step == 1) {
				name, node, target = "normal", v.Original.Parent, v.Command.MemoryMaxBytes
			}
			// 当前收费只作额外拒绝条件，绝不是连续安全包络来源。
			current := now.Arrays.MemoryCurrentBytes
			if name == "normal" {
				current = now.Parent.MemoryCurrentBytes
			}
			if current > target {
				return unknown(fmt.Errorf("quota target below current charged bytes"))
			}
			if e = k.normalSplitQuotaWrite(name, node, target); e != nil {
				return unknown(e)
			}
		}
		now, e = k.normalSplitRead()
		if e != nil {
			return unknown(e)
		}
		if e = normalSplitQuotaMatches(v, now, true, step > 0); e != nil {
			return unknown(e)
		}
		if e = appendStage(step + 1); e != nil {
			return unknown(e)
		}
		count = step + 2
	}
	if e = ctx.Err(); e != nil {
		return unknown(e)
	}
	if e = auth.check(ctx, s, r, v); e != nil {
		return unknown(e)
	}
	if e = g.rejectArrayPendingLocked(s, r); e != nil {
		return unknown(e)
	}
	now, e := k.normalSplitRead()
	if e != nil {
		return unknown(e)
	}
	if e = normalSplitQuotaMatches(v, now, true, true); e != nil {
		return unknown(e)
	}
	if count == 3 {
		if e = appendStage(3); e != nil {
			return unknown(e)
		}
	}
	_, count, before, e := normalSplitQuotaRead(s, key)
	if e != nil || count != 4 {
		return unknown(fmt.Errorf("quota completed prefix unavailable: %v", e))
	}
	j.durable.Delete(before)
	for _, stage := range normalSplitQuotaStages {
		if e = disk.syncFile(s, normalSplitQuotaName(key, stage)); e != nil {
			return unknown(e)
		}
	}
	if e = disk.syncDir(s); e != nil {
		return unknown(e)
	}
	_, _, after, e := normalSplitQuotaRead(s, key)
	if e != nil || before != after {
		return unknown(fmt.Errorf("quota prefix changed during fsync"))
	}
	j.durable.Store(after, struct{}{})
	return now, nil
}
