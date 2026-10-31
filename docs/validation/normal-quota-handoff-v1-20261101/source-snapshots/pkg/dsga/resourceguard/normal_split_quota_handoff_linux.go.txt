//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"sync"

	"golang.org/x/sys/unix"
)

// quota与state.json是同原flock下的两个持久前缀。只有两者均确认，新的
// Original/Held才可供既有View读取；不新增按PG版本分叉的上层额度协议。
type normalQuotaResourceWitness struct {
	Directory [4]uint64
	File      [3]uint64
	SHA256    string
}
type normalQuotaHandoffJournal struct{ durable sync.Map }

var normalQuotaHandoffJournals sync.Map

func (g *Guard) normalQuotaHandoffJournal() *normalQuotaHandoffJournal {
	v, _ := normalQuotaHandoffJournals.LoadOrStore(g, &normalQuotaHandoffJournal{})
	return v.(*normalQuotaHandoffJournal)
}

type normalQuotaHandoffIO struct {
	quota     normalSplitQuotaIO
	save      func(*lockedState, *record) error
	syncState func(*lockedState) error
}

func normalQuotaHandoffDiskIO() normalQuotaHandoffIO {
	return normalQuotaHandoffIO{normalSplitQuotaDiskIO(), func(s *lockedState, r *record) error { return s.save(r) }, normalQuotaSyncState}
}

func normalQuotaSyncState(s *lockedState) error {
	fd, e := unix.Openat(int(s.dir.Fd()), "state.json", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(fd), "quota-resource-state-sync")
	defer f.Close()
	if e = privateRegular(fd, unix.S_IFREG); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	return s.dir.Sync()
}
func normalQuotaReadStateWitness(s *lockedState, r *record) (normalQuotaResourceWitness, error) {
	var w normalQuotaResourceWitness
	if s == nil || s.dir == nil || s.lock == nil || r == nil {
		return w, ErrUnknown
	}
	var dir, lock unix.Stat_t
	if unix.Fstat(int(s.dir.Fd()), &dir) != nil || unix.Fstat(int(s.lock.Fd()), &lock) != nil || privateRegular(int(s.dir.Fd()), unix.S_IFDIR) != nil || privateRegular(int(s.lock.Fd()), unix.S_IFREG) != nil || dir.Dev != lock.Dev {
		return w, ErrUnknown
	}
	w.Directory = [4]uint64{dir.Dev, dir.Ino, lock.Dev, lock.Ino}
	fd, e := unix.Openat(int(s.dir.Fd()), "state.json", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return w, e
	}
	f := os.NewFile(uintptr(fd), "quota-resource-original-state")
	defer f.Close()
	if e = privateRegular(fd, unix.S_IFREG); e != nil {
		return w, e
	}
	var st unix.Stat_t
	if e = unix.Fstat(fd, &st); e != nil {
		return w, e
	}
	raw, e := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if e != nil || len(raw) > 64<<10 {
		return w, errors.Join(e, ErrUnknown)
	}
	canonical, e := json.Marshal(r)
	if e != nil || !bytes.Equal(canonical, raw) {
		return w, errors.Join(e, fmt.Errorf("quota resource state bytes differ"))
	}
	w.File = [3]uint64{st.Dev, st.Ino, uint64(st.Size)}
	w.SHA256 = physicalDigest(raw)
	return w, nil
}

// 持resource flock的capwriter/原数组入口使用这个零写门禁。没有v3标记的
// legacy路径不升级；标记存在但完成/本实例耐久见证缺失时不能借state可见性放行。
func (g *Guard) checkNormalQuotaResourceLocked(s *lockedState, r *record) error {
	if r == nil || r.NormalSplitQuota == nil {
		return nil
	}
	if r.validate(r.Identity) != nil {
		return errors.Join(ErrUnknown, fmt.Errorf("quota resource original is unconfirmed"))
	}
	ref := r.NormalSplitQuota
	original, count, w, e := normalSplitQuotaRead(s, normalSplitQuotaCommandKey(ref.Command))
	if e != nil || count != 4 || w.SHA[0] != ref.IntentSHA256 || !reflect.DeepEqual(original.Command, ref.Command) {
		return errors.Join(ErrUnknown, e, fmt.Errorf("quota resource reference lacks complete original prefix"))
	}
	if e = g.rejectNormalSplitQuotaPendingLocked(s); e != nil {
		return e
	}
	// generic预算/取消同样改变state inode；全部Applied=true的v3状态都要
	// 本Root成功持久见证，不因Original不是quota而跳过最终交接。
	if !r.Applied {
		if reflect.DeepEqual(*r.Original, ref.Command) {
			return errors.Join(ErrUnknown, fmt.Errorf("quota handoff is pending"))
		}
		return nil
	}
	{
		state, e := normalQuotaReadStateWitness(s, r)
		if e != nil {
			return errors.Join(ErrUnknown, e)
		}
		if _, ok := g.normalQuotaHandoffJournal().durable.Load(state); !ok {
			return errors.Join(ErrUnknown, fmt.Errorf("visible quota Held is not this Guard durable handoff"))
		}
	}
	return nil
}

// InspectState可以展示Applied=false的原Unknown，供现有消费者精确对账；
// 但Applied=true不能仅凭rename后的可见文件变为ConfirmedLimits。
func (g *Guard) checkNormalQuotaObservationLocked(s *lockedState, r *record) error {
	if r != nil && r.NormalSplitQuota != nil && r.Applied {
		return g.checkNormalQuotaResourceLocked(s, r)
	}
	return nil
}

// 显式恢复只同步完整旧前缀，不重做任何旧cap写；普通Observe不调用。
// 当前Source检查由调用者先完成，历史只取同原VM/两inode/P的已完成Root日志。
// 当前key可仍pending（仅供同原Replay）；其它key必须完整，不能跳过未知历史。
func (g *Guard) recoverNormalQuotaAncestorsLocked(ctx context.Context, s *lockedState, active normalSplitQuotaOriginal, disk normalSplitQuotaIO) error {
	keys, e := normalSplitQuotaKeys(s)
	if e != nil {
		return e
	}
	type item struct {
		original normalSplitQuotaOriginal
		count    int
		w        normalSplitQuotaWitness
		key      string
	}
	all := make([]item, 0, len(keys))
	activeKey := normalSplitQuotaKey(active)
	for _, key := range keys {
		o, count, w, e := normalSplitQuotaRead(s, key)
		if e != nil {
			return e
		}
		if key != activeKey && count != 4 {
			return errors.Join(ErrUnknown, fmt.Errorf("another quota history remains partial"))
		}
		if count == 0 {
			return ErrUnknown
		}
		if splitStable(o.Original, active.Original) != nil || o.SourceSHA256 != active.SourceSHA256 || o.SplitIntentSHA256 != active.SplitIntentSHA256 || o.SplitCompletionSHA256 != active.SplitCompletionSHA256 || !splitSameNode(*o.Original.Private, *active.Original.Private) || !splitSameNode(*o.Original.Arrays, *active.Original.Arrays) || o.Original.Private.MemoryMaxBytes != active.Original.Private.MemoryMaxBytes {
			return errors.Join(ErrUnknown, fmt.Errorf("quota ancestors changed original source or leaves"))
		}
		all = append(all, item{o, count, w, key})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].original.Command.Revision < all[j].original.Command.Revision })
	for i, x := range all {
		if x.original.Command.Revision > active.Command.Revision || x.original.Command.IssuerEpoch > active.Command.IssuerEpoch {
			return errors.Join(ErrUnknown, fmt.Errorf("quota recovery has a newer foreign original"))
		}
		if i > 0 {
			prev := all[i-1].original
			if prev.Command.Revision >= x.original.Command.Revision || prev.Command.IssuerEpoch > x.original.Command.IssuerEpoch || x.original.Original.Parent.MemoryMaxBytes < prev.Command.MemoryMaxBytes || x.original.Original.Arrays.MemoryMaxBytes != prev.TargetArraysBytes {
				return errors.Join(ErrUnknown, fmt.Errorf("quota ancestor ordering/cap continuity differs"))
			}
		}
	}
	for _, x := range all {
		if x.count != 4 {
			continue
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		g.normalSplitQuotaJournal().durable.Delete(x.w)
		for _, stage := range normalSplitQuotaStages {
			if e = disk.syncFile(s, normalSplitQuotaName(x.key, stage)); e != nil {
				return errors.Join(ErrUnknown, e)
			}
		}
		if e = disk.syncDir(s); e != nil {
			return errors.Join(ErrUnknown, e)
		}
		_, count, after, e := normalSplitQuotaRead(s, x.key)
		if e != nil || count != 4 || after != x.w {
			return errors.Join(ErrUnknown, e, fmt.Errorf("quota ancestor prefix changed during recovery"))
		}
		g.normalSplitQuotaJournal().durable.Store(after, struct{}{})
	}
	return nil
}

func normalQuotaReference(v normalSplitQuotaOriginal) (*normalSplitQuotaStateReference, error) {
	raw, e := json.Marshal(normalSplitQuotaEntry{1, normalSplitQuotaScope, normalSplitQuotaStages[0], v, ""})
	if e != nil {
		return nil, e
	}
	return &normalSplitQuotaStateReference{1, v.Command, physicalDigest(raw)}, nil
}
func normalQuotaCloneRecord(r *record) (*record, error) {
	raw, e := json.Marshal(r)
	if e != nil {
		return nil, e
	}
	var out record
	if e = json.Unmarshal(raw, &out); e != nil {
		return nil, e
	}
	return &out, nil
}

// 仅供后继完整Source producer的私有交接，未接public/HTTP正常降限入口。
// apply先持久原Unknown(v3)，再A/N事务，最后同命令Held；失败保旧Held。
// replay只补同一原未确认动作。recover只处理已经Applied且全部quota完成的
// 原命令，允许当前更高floor重新同步旧事实，但绝不在旧epoch重写内核cap。
func (g *Guard) normalQuotaHandoffLocked(ctx context.Context, s *lockedState, r *record, auth normalSplitQuotaAuthorization, mode string, disk normalQuotaHandoffIO) (Evidence, error) {
	var zero Evidence
	unknown := func(e error) (Evidence, error) { return zero, errors.Join(ErrUnknown, e) }
	if ctx == nil || r == nil || auth.check == nil || auth.original.validate() != nil || disk.save == nil || disk.syncState == nil || disk.quota.create == nil || disk.quota.syncFile == nil || disk.quota.syncDir == nil {
		return unknown(fmt.Errorf("quota handoff original source/backend missing"))
	}
	if mode != "apply" && mode != "observe" && mode != "replay" && mode != "recover" {
		return unknown(fmt.Errorf("quota handoff action unsupported"))
	}
	if e := ctx.Err(); e != nil {
		return unknown(e)
	}
	c := auth.original.Command
	ref, e := normalQuotaReference(auth.original)
	if e != nil {
		return unknown(e)
	}
	if r.validate(c.Identity) != nil || r.Bootstrap == nil || !r.Bootstrap.Applied || r.Birth == nil || mode != "recover" && !r.Birth.matches(c) || c.PidsMaxTasks != r.taskLimit() || c.SystemMemoryMaxBytes != r.systemLimit() || c.AddressSpaceMaxBytes != r.addressSpaceLimit() {
		return unknown(fmt.Errorf("quota handoff original Birth/bootstrap or immutable bounds changed"))
	}
	if c.IssuerEpoch != r.Floor && (mode != "recover" || c.IssuerEpoch > r.Floor) {
		return unknown(fmt.Errorf("quota handoff owner is fenced"))
	}
	if e = g.kernel.VerifyProcess(*r.Birth); e != nil {
		return unknown(e)
	}
	if e = auth.check(ctx, s, r, auth.original); e != nil {
		return unknown(e)
	}
	if e = g.rejectArrayPendingLocked(s, r); e != nil {
		return unknown(e)
	}

	if mode == "recover" {
		if r.NormalSplitQuota == nil || !reflect.DeepEqual(r.NormalSplitQuota, ref) || r.Original == nil {
			return unknown(fmt.Errorf("recovery lacks exact retained quota reference"))
		}
		_, count, _, err := normalSplitQuotaRead(s, normalSplitQuotaKey(auth.original))
		if err != nil || count != 4 {
			return unknown(fmt.Errorf("recovery cannot replay a partial cap: %v", err))
		}
		k, ok := g.kernel.(normalSplitQuotaKernel)
		if !ok {
			return unknown(fmt.Errorf("quota backend unavailable"))
		}
		now, err := k.normalSplitRead()
		if err != nil {
			return unknown(err)
		}
		if err = normalSplitQuotaMatches(auth.original, now, true, true); err != nil {
			return unknown(err)
		}
		if err = g.recoverNormalQuotaAncestorsLocked(ctx, s, auth.original, disk.quota); err != nil {
			return unknown(err)
		}
		// 当前Original可为同N预算续接；只确认当前原limits，不把旧Q cap重写一次。
		evidence, err := g.kernel.ReadBounds(r.Identity, r.Original.limits())
		if err != nil {
			return unknown(err)
		}
		if err = evidence.matches(*r.Original); err != nil {
			return unknown(err)
		}
		if err = auth.check(ctx, s, r, auth.original); err != nil {
			return unknown(err)
		}
		if !r.Applied {
			complete, err := normalQuotaCloneRecord(r)
			if err != nil {
				return unknown(err)
			}
			complete.Applied = true
			complete.Held = complete.Original
			if err = complete.validate(r.Identity); err != nil {
				return unknown(err)
			}
			if err = disk.save(s, complete); err != nil {
				return unknown(err)
			}
			*r = *complete
		}
		if err = g.confirmNormalQuotaResourceLocked(ctx, s, r, disk); err != nil {
			return unknown(err)
		}
		return evidence, nil
	}
	same := r.Original != nil && reflect.DeepEqual(*r.Original, c)
	if same && r.Applied {
		if r.NormalSplitQuota == nil || !reflect.DeepEqual(r.NormalSplitQuota, ref) || !reflect.DeepEqual(*r.Held, c) {
			return unknown(fmt.Errorf("completed resource is not this original quota"))
		}
		if e = g.checkNormalQuotaResourceLocked(s, r); e != nil {
			return unknown(e)
		}
		evidence, e := g.kernel.ReadBounds(r.Identity, c.limits())
		if e != nil {
			return unknown(e)
		}
		if e = evidence.matches(c); e != nil {
			return unknown(e)
		}
		return evidence, nil
	}
	if mode == "observe" || mode == "recover" {
		return unknown(fmt.Errorf("quota original is not confirmed"))
	}
	if same {
		if mode != "replay" || r.NormalSplitQuota == nil || !reflect.DeepEqual(r.NormalSplitQuota, ref) {
			return unknown(fmt.Errorf("pending quota needs same exact explicit replay"))
		}
	} else {
		if mode != "apply" || r.Original != nil && !r.Applied {
			return unknown(fmt.Errorf("another original resource is unconfirmed"))
		}
		next, e := r.nextRevision()
		if e != nil || c.Revision != next {
			return unknown(fmt.Errorf("quota resource revision is not consecutive"))
		}
		if e = g.checkNormalQuotaResourceLocked(s, r); e != nil {
			return unknown(e)
		}
		held, budget := r.Bootstrap.Original.Limits, r.Bootstrap.Original.BudgetCommitmentID
		if r.Held != nil {
			held, budget = r.Held.limits(), r.Held.BudgetCommitmentID
		}
		if auth.original.Original.Parent.MemoryMaxBytes != held.MemoryMaxBytes || c.BudgetCommitmentID != budget || c.OutsideUpperBytes != held.OutsideUpperBytes || !reflect.DeepEqual(c.HugePools, held.HugePools) {
			return unknown(fmt.Errorf("quota changes original held budget/pools/outside envelope"))
		}
		before, e := g.kernel.ReadBounds(r.Identity, held)
		if e != nil {
			return unknown(e)
		}
		if e = before.matchesLimits(r.Identity, held); e != nil {
			return unknown(e)
		}
		pending, e := normalQuotaCloneRecord(r)
		if e != nil {
			return unknown(e)
		}
		copy := c
		copy.HugePools = append([]HugePool(nil), c.HugePools...)
		pending.Version = 3
		pending.NormalSplitQuota = ref
		pending.Original = &copy
		pending.Applied = false
		if e = pending.validate(r.Identity); e != nil {
			return unknown(e)
		}
		if e = disk.save(s, pending); e != nil {
			return unknown(e)
		}
		*r = *pending
	}
	if mode == "replay" {
		if e = g.recoverNormalQuotaAncestorsLocked(ctx, s, auth.original, disk.quota); e != nil {
			return unknown(e)
		}
	}
	// quota自身只用array-only门禁，不能经过通用quotaPending把自己的Replay拒死。
	privateMode := "apply"
	if same && mode == "replay" {
		_, count, _, readErr := normalSplitQuotaRead(s, normalSplitQuotaKey(auth.original))
		if readErr != nil {
			return unknown(readErr)
		}
		if count > 0 {
			privateMode = "replay"
		}
	}
	_, e = g.normalSplitQuotaLocked(ctx, s, r, auth, privateMode, disk.quota)
	if e != nil {
		return unknown(e)
	}
	if e = ctx.Err(); e != nil {
		return unknown(e)
	}
	if e = g.kernel.VerifyProcess(*r.Birth); e != nil {
		return unknown(e)
	}
	if e = auth.check(ctx, s, r, auth.original); e != nil {
		return unknown(e)
	}
	evidence, e := g.kernel.ReadBounds(r.Identity, c.limits())
	if e != nil {
		return unknown(e)
	}
	if e = evidence.matches(c); e != nil {
		return unknown(e)
	}
	complete, e := normalQuotaCloneRecord(r)
	if e != nil {
		return unknown(e)
	}
	complete.Applied = true
	complete.Held = complete.Original
	if e = complete.validate(r.Identity); e != nil {
		return unknown(e)
	}
	if e = disk.save(s, complete); e != nil {
		return unknown(e)
	}
	*r = *complete
	if e = g.confirmNormalQuotaResourceLocked(ctx, s, r, disk); e != nil {
		return unknown(e)
	}
	return evidence, nil
}

func (g *Guard) confirmNormalQuotaResourceLocked(ctx context.Context, s *lockedState, r *record, disk normalQuotaHandoffIO) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	before, e := normalQuotaReadStateWitness(s, r)
	if e != nil {
		return e
	}
	g.normalQuotaHandoffJournal().durable.Delete(before)
	if e = disk.syncState(s); e != nil {
		return e
	}
	after, e := normalQuotaReadStateWitness(s, r)
	if e != nil || before != after {
		return errors.Join(ErrUnknown, e, fmt.Errorf("quota state changed during fsync"))
	}
	g.normalQuotaHandoffJournal().durable.Store(after, struct{}{})
	return nil
}

// Fence/取消仅在调用前已有本Root确认时刷新成功写后的元数据见证。
// 不用于Observe，不给新Root/原Unknown补签；调用者须先核原确认状态。
func (g *Guard) confirmNormalQuotaMetadataLocked(ctx context.Context, s *lockedState, r *record) error {
	if r == nil || r.NormalSplitQuota == nil {
		return nil
	}
	if r.validate(r.Identity) != nil || !r.Applied {
		return errors.Join(ErrUnknown, fmt.Errorf("quota metadata cannot confirm original Unknown"))
	}
	if e := g.rejectNormalSplitQuotaPendingLocked(s); e != nil {
		return e
	}
	return g.confirmNormalQuotaResourceLocked(ctx, s, r, normalQuotaHandoffDiskIO())
}
