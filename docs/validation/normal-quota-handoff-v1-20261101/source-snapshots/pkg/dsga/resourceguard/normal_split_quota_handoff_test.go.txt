//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

// 仅内核/进程/完整Source是假；原state原子rename、flock、四阶段WAL/fsync真实。
// 本批关闭账本交接缺口，不认证生产normal或任何PG版本的安全资格。
type handoffFixtureKernel struct {
	*quotaFixtureKernel
	dir      string
	expected Command
}

func (k *handoffFixtureKernel) normalSplitQuotaWrite(name string, node normalSplitNode, target control.Uint64) error {
	raw, e := os.ReadFile(filepath.Join(k.dir, "state.json"))
	if e != nil {
		return e
	}
	var r record
	if e = json.Unmarshal(raw, &r); e != nil {
		return e
	}
	if r.validate(k.snapshot.Identity) != nil || r.Version != 3 || r.NormalSplitQuota == nil || r.Applied || r.Original == nil || !reflect.DeepEqual(*r.Original, k.expected) {
		return fmt.Errorf("quota cap effect preceded original v3 resource Unknown")
	}
	return k.quotaFixtureKernel.normalSplitQuotaWrite(name, node, target)
}
func (k *handoffFixtureKernel) ReadBounds(id Identity, l Limits) (Evidence, error) {
	e, err := k.fixtureKernel.Read(Command{})
	if err != nil {
		return e, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	e.MemoryMaxBytes = k.snapshot.Parent.MemoryMaxBytes
	e.MemoryCurrentBytes = k.snapshot.Parent.MemoryCurrentBytes
	e.SwapMaxBytes = 0
	e.SwapCurrentBytes = 0
	return e, nil
}

// 公共Read与ReadBounds必须看同一当前split，不能用旧fixture的初始N制造假绿。
func (k *handoffFixtureKernel) Read(c Command) (Evidence, error) {
	return k.ReadBounds(c.Identity, c.limits())
}
func (k *handoffFixtureKernel) Apply(c Command) error {
	raw, err := os.ReadFile(filepath.Join(k.dir, "state.json"))
	if err != nil {
		return err
	}
	var r record
	if err = json.Unmarshal(raw, &r); err != nil {
		return err
	}
	if r.Applied || r.Original == nil || !reflect.DeepEqual(*r.Original, c) || r.NormalSplitQuota == nil {
		return fmt.Errorf("generic metadata preceded original v3 Unknown")
	}
	actual, err := k.Read(c)
	if err != nil {
		return err
	}
	if err = actual.matches(c); err != nil {
		return err
	}
	// 该夹具只允许同额度预算元数据。任何真实cap变化必须由quota模型记录。
	k.mu.Lock()
	k.fixtureKernel.writes++
	k.mu.Unlock()
	if k.fixtureKernel.fail {
		return fmt.Errorf("injected metadata completion loss")
	}
	return nil
}

type handoffFixture struct {
	q *quotaFixture
	k *handoffFixtureKernel
}

func newHandoffFixture(t *testing.T) *handoffFixture {
	t.Helper()
	q := newQuotaFixture(t)
	c := q.auth.original.Command
	k := &handoffFixtureKernel{q.k, q.f.dir, c}
	q.f.g.kernel = k
	q.f.g.config.StateDir = q.f.dir
	l := c.limits()
	l.MemoryMaxBytes = q.k.snapshot.Parent.MemoryMaxBytes
	e := &k.fixtureKernel.e
	e.ProtocolVersion = 1
	e.Identity = k.snapshot.Identity
	e.MemoryMaxBytes = l.MemoryMaxBytes
	e.SwapMaxBytes = 0
	e.SwapCurrentBytes = 0
	e.GuestMemTotalBytes = 16 << 30
	e.TaskLimit = &TaskLimitEvidence{MaxTasks: l.PidsMaxTasks, CurrentTasks: 1}
	e.CgroupPopulated = true
	e.AddressSpace = &AddressSpaceEvidence{Processes: 1, MaxHardBytes: l.AddressSpaceMaxBytes, MaxVirtualBytes: 64 << 20, Irreversible: true}
	e.HugePools = append([]HugePool(nil), l.HugePools...)
	e.WholePoolUpperBytes = 0
	e.HugeOvercommitDisabled = true
	e.HugeSurplusPages = 0
	for _, pool := range l.HugePools {
		e.WholePoolUpperBytes += pool.PageSizeKB * 1024 * pool.Pages
	}
	if l.SystemMemoryMaxBytes != 0 {
		e.System = &SystemEvidence{Scope: "system-cgroup-charged-pages", MemoryMaxBytes: l.SystemMemoryMaxBytes, SwapMaxBytes: 0, PID1Member: true}
	}
	birth := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks, AddressSpaceMaxBytes: c.AddressSpaceMaxBytes}
	k.fixtureKernel.process = &birth
	b := BootstrapCommand{ProtocolVersion: 1, Identity: c.Identity, IssuerEpoch: 1, BudgetCommitmentID: c.BudgetCommitmentID, Limits: l}
	state := &record{Version: 1, Identity: c.Identity, Floor: 1, Bootstrap: &bootstrapRecord{Original: b, Applied: true, SystemPlacementObserved: l.SystemMemoryMaxBytes != 0}, Birth: &birth}
	require.NoError(t, state.validate(c.Identity))
	s, err := lockState(context.Background(), q.f.dir)
	require.NoError(t, err)
	require.NoError(t, s.save(state))
	s.close()
	return &handoffFixture{q, k}
}
func (f *handoffFixture) run(ctx context.Context, a normalSplitQuotaAuthorization, mode string, disk normalQuotaHandoffIO) (Evidence, error) {
	s, e := lockState(ctx, f.q.f.dir)
	if e != nil {
		return Evidence{}, e
	}
	defer s.close()
	r, e := s.load()
	if e != nil {
		return Evidence{}, e
	}
	return f.q.f.g.normalQuotaHandoffLocked(ctx, s, r, a, mode, disk)
}
func (f *handoffFixture) state(t *testing.T) *record {
	raw, e := os.ReadFile(filepath.Join(f.q.f.dir, "state.json"))
	require.NoError(t, e)
	var r record
	require.NoError(t, json.Unmarshal(raw, &r))
	return &r
}
func (f *handoffFixture) view(t *testing.T) (View, error) {
	var out View
	e := f.q.f.g.withRecord(context.Background(), func(r *record, s *lockedState) error {
		if e := f.q.f.g.checkNormalQuotaObservationLocked(s, r); e != nil {
			return e
		}
		l := r.Bootstrap.Original.Limits
		if r.Held != nil {
			l = r.Held.limits()
		}
		a, e := f.k.ReadBounds(r.Identity, l)
		if e != nil {
			return e
		}
		out = r.view(a)
		return out.Validate()
	})
	return out, e
}
func TestNormalQuotaHandoffPublishesSameOriginalHeld(t *testing.T) {
	f := newHandoffFixture(t)
	original := f.state(t)
	e, err := f.run(context.Background(), f.q.auth, "apply", normalQuotaHandoffDiskIO())
	require.NoError(t, err)
	require.NoError(t, e.matches(f.q.auth.original.Command))
	r := f.state(t)
	require.Equal(t, uint32(3), r.Version)
	require.True(t, r.Applied)
	require.Equal(t, f.q.auth.original.Command, *r.Held)
	require.Equal(t, *r.Held, *r.Original)
	require.Equal(t, original.Bootstrap, r.Bootstrap)
	require.Equal(t, original.Birth, r.Birth)
	v, err := f.view(t)
	require.NoError(t, err)
	l, err := v.ConfirmedLimits()
	require.NoError(t, err)
	require.Equal(t, f.q.auth.original.Command.MemoryMaxBytes, l.MemoryMaxBytes)
	require.Equal(t, []string{"arrays", "normal"}, f.k.quotaWrites)
	_, err = f.run(context.Background(), f.q.auth, "observe", normalQuotaHandoffDiskIO())
	require.NoError(t, err)
	require.Len(t, f.k.quotaWrites, 2)
	// 内部v3引用不改变原对外View/数组引用投影。
	raw, _ := json.Marshal(v)
	var decoded View
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, v.Held, decoded.Held)
}
func TestNormalQuotaHandoffPendingAndLostStateReply(t *testing.T) {
	for _, stage := range []string{"before-pending-save", "after-pending-rename", "after-held-rename", "final-state-sync"} {
		t.Run(stage, func(t *testing.T) {
			f := newHandoffFixture(t)
			disk := normalQuotaHandoffDiskIO()
			save := disk.save
			calls := 0
			disk.save = func(s *lockedState, r *record) error {
				calls++
				if stage == "before-pending-save" && calls == 1 {
					return errors.New("injected before original state")
				}
				e := save(s, r)
				if stage == "after-pending-rename" && calls == 1 || stage == "after-held-rename" && calls == 2 {
					return errors.New("injected visible rename before acknowledged persistence")
				}
				return e
			}
			if stage == "final-state-sync" {
				disk.syncState = func(*lockedState) error { return errors.New("injected final state sync") }
			}
			_, err := f.run(context.Background(), f.q.auth, "apply", disk)
			require.ErrorIs(t, err, ErrUnknown)
			r := f.state(t)
			if stage == "before-pending-save" {
				require.Empty(t, f.k.quotaWrites)
				require.Nil(t, r.Original)
				return
			}
			if stage == "after-pending-rename" {
				require.Empty(t, f.k.quotaWrites)
				require.False(t, r.Applied)
				require.Nil(t, r.Held)
				v, e := f.view(t)
				require.NoError(t, e)
				_, e = v.ConfirmedLimits()
				require.ErrorIs(t, e, ErrUnknown)
				_, err = f.run(context.Background(), f.q.auth, "replay", normalQuotaHandoffDiskIO())
				require.NoError(t, err)
			} else {
				require.Len(t, f.k.quotaWrites, 2)
				require.True(t, r.Applied)
				_, e := f.view(t)
				require.ErrorIs(t, e, ErrUnknown)
				_, e = f.run(context.Background(), f.q.auth, "observe", normalQuotaHandoffDiskIO())
				require.ErrorIs(t, e, ErrUnknown)
				_, err = f.run(context.Background(), f.q.auth, "recover", normalQuotaHandoffDiskIO())
				require.NoError(t, err)
				require.Len(t, f.k.quotaWrites, 2)
			}
			v, e := f.view(t)
			require.NoError(t, e)
			_, e = v.ConfirmedLimits()
			require.NoError(t, e)
		})
	}
}
func TestNormalQuotaHandoffPartialCapPreservesOldHeld(t *testing.T) {
	f := newHandoffFixture(t)
	f.k.failAfter = "arrays"
	_, err := f.run(context.Background(), f.q.auth, "apply", normalQuotaHandoffDiskIO())
	require.ErrorIs(t, err, ErrUnknown)
	r := f.state(t)
	require.False(t, r.Applied)
	require.Nil(t, r.Held)
	require.Equal(t, f.q.auth.original.Command, *r.Original)
	require.Equal(t, control.Uint64(512<<20), r.Bootstrap.Original.Limits.MemoryMaxBytes)
	_, err = f.run(context.Background(), f.q.auth, "recover", normalQuotaHandoffDiskIO())
	require.ErrorIs(t, err, ErrUnknown)
	require.Equal(t, []string{"arrays"}, f.k.quotaWrites)
	changed := f.q.auth
	changed.original.Command.Revision++
	_, err = f.run(context.Background(), changed, "replay", normalQuotaHandoffDiskIO())
	require.ErrorIs(t, err, ErrUnknown)
	f.k.failAfter = ""
	_, err = f.run(context.Background(), f.q.auth, "replay", normalQuotaHandoffDiskIO())
	require.NoError(t, err)
	require.Equal(t, []string{"arrays", "normal"}, f.k.quotaWrites)
}
func TestNormalQuotaHandoffNewGuardMultiHistoryAndFence(t *testing.T) {
	f := newHandoffFixture(t)
	_, err := f.run(context.Background(), f.q.auth, "apply", normalQuotaHandoffDiskIO())
	require.NoError(t, err)
	grow := f.q.auth
	grow.original.Original = cloneSplit(f.k.snapshot)
	grow.original.Command.Revision = 2
	grow.original.Command.MemoryMaxBytes = 512 << 20
	grow.original.TargetArraysBytes = 64 << 20
	f.k.expected = grow.original.Command
	_, err = f.run(context.Background(), grow, "apply", normalQuotaHandoffDiskIO())
	require.NoError(t, err)
	old := f.q.f.g
	f.q.f.g = &Guard{config: old.config, kernel: f.k}
	_, err = f.view(t)
	require.ErrorIs(t, err, ErrUnknown)
	_, err = f.run(context.Background(), grow, "replay", normalQuotaHandoffDiskIO())
	require.ErrorIs(t, err, ErrUnknown, "completed new Root must explicitly recover; no old cap replay")
	// 模型只提高真实文件中的floor；恢复可以同步旧完成事实但不能在旧epoch重写cap。
	s, e := lockState(context.Background(), f.q.f.dir)
	require.NoError(t, e)
	r, e := s.load()
	require.NoError(t, e)
	r.Floor = 2
	require.NoError(t, s.save(r))
	s.close()
	_, err = f.run(context.Background(), grow, "apply", normalQuotaHandoffDiskIO())
	require.ErrorIs(t, err, ErrUnknown)
	count := len(f.k.quotaWrites)
	_, err = f.run(context.Background(), grow, "recover", normalQuotaHandoffDiskIO())
	require.NoError(t, err)
	require.Len(t, f.k.quotaWrites, count)
	v, e := f.view(t)
	require.NoError(t, e)
	_, e = v.ConfirmedLimits()
	require.NoError(t, e)
}
func TestNormalQuotaHandoffRejectsChangedBudgetAndMissingSource(t *testing.T) {
	for _, name := range []string{"budget", "outside", "pools", "source", "revision", "owner"} {
		t.Run(name, func(t *testing.T) {
			f := newHandoffFixture(t)
			a := f.q.auth
			switch name {
			case "budget":
				a.original.Command.BudgetCommitmentID = "foreign-budget"
			case "outside":
				a.original.Command.OutsideUpperBytes += 64 << 10
			case "pools":
				a.original.Command.HugePools = append([]HugePool(nil), a.original.Command.HugePools...)
				a.original.Command.HugePools[0].Pages++
			case "source":
				a.check = func(context.Context, *lockedState, *record, normalSplitQuotaOriginal) error {
					return fmt.Errorf("actual continuous Source not qualified")
				}
			case "revision":
				a.original.Command.Revision++
			case "owner":
				a.original.Command.IssuerEpoch++
			}
			_, e := f.run(context.Background(), a, "apply", normalQuotaHandoffDiskIO())
			require.ErrorIs(t, e, ErrUnknown)
			require.Empty(t, f.k.quotaWrites)
			require.Nil(t, f.state(t).Original)
		})
	}
}
func TestNormalQuotaStateVersionAndCancellationPreserveMarker(t *testing.T) {
	f := newHandoffFixture(t)
	_, err := f.run(context.Background(), f.q.auth, "apply", normalQuotaHandoffDiskIO())
	require.NoError(t, err)
	r := f.state(t)
	for _, version := range []uint32{1, 2} {
		copy := *r
		copy.Version = version
		require.Error(t, copy.validate(r.Identity), "older record version must not ignore quota marker")
	}
	copy := *r
	copy.NormalSplitQuota = nil
	require.Error(t, copy.validate(r.Identity))
	// 取消沿原资源协议，不降低v3、不修改quota及实际cap。其余实际核验使用同模型。
	command := f.q.auth.original.Command
	command.Revision++
	request := CancelRequest{ProtocolVersion: 1, IssuerEpoch: r.Floor, Command: command}
	_, err = f.q.f.g.CancelOriginal(context.Background(), request)
	require.NoError(t, err)
	after := f.state(t)
	require.Equal(t, uint32(3), after.Version)
	require.Equal(t, r.NormalSplitQuota, after.NormalSplitQuota)
	require.Equal(t, r.Held, after.Held)
	require.Len(t, f.k.quotaWrites, 2)
	v, e := f.view(t)
	require.NoError(t, e)
	_, e = v.ConfirmedLimits()
	require.NoError(t, e)
}

// 直接调用公共接口；先证明当前Read确实匹配，负例不能借陈旧N读回假绿。
func TestNormalQuotaPublicConfirmationRequiresDurableHandoff(t *testing.T) {
	for _, stage := range []string{"after-held-rename", "final-state-sync", "new-guard", "same-bytes-new-inode"} {
		t.Run(stage, func(t *testing.T) {
			f := newHandoffFixture(t)
			ctx := context.Background()
			disk := normalQuotaHandoffDiskIO()
			if stage == "after-held-rename" {
				save := disk.save
				calls := 0
				disk.save = func(s *lockedState, r *record) error {
					calls++
					e := save(s, r)
					if calls == 2 && e == nil {
						return fmt.Errorf("lost held persistence acknowledgement")
					}
					return e
				}
			}
			if stage == "final-state-sync" {
				disk.syncState = func(*lockedState) error { return fmt.Errorf("final state fsync failed") }
			}
			_, err := f.run(ctx, f.q.auth, "apply", disk)
			if stage == "after-held-rename" || stage == "final-state-sync" {
				require.ErrorIs(t, err, ErrUnknown)
			} else {
				require.NoError(t, err)
			}
			c := f.q.auth.original.Command
			current, e := f.k.Read(c)
			require.NoError(t, e)
			require.NoError(t, current.matches(c))
			if stage == "new-guard" {
				old := f.q.f.g
				f.q.f.g = &Guard{config: old.config, kernel: f.k}
			}
			if stage == "same-bytes-new-inode" {
				s, e := lockState(ctx, f.q.f.dir)
				require.NoError(t, e)
				r, e := s.load()
				require.NoError(t, e)
				require.NoError(t, s.save(r))
				s.close()
			}
			before, e := os.ReadFile(filepath.Join(f.q.f.dir, "state.json"))
			require.NoError(t, e)
			writes := len(f.k.quotaWrites)
			_, e = f.q.f.g.Observe(ctx, c)
			require.ErrorIs(t, e, ErrUnknown)
			_, e = f.q.f.g.Apply(ctx, c)
			require.ErrorIs(t, e, ErrUnknown)
			_, e = f.q.f.g.ReplayOriginal(ctx, c)
			require.ErrorIs(t, e, ErrUnknown)
			_, e = f.q.f.g.InspectState(ctx)
			require.ErrorIs(t, e, ErrUnknown)
			after, e := os.ReadFile(filepath.Join(f.q.f.dir, "state.json"))
			require.NoError(t, e)
			require.Equal(t, before, after)
			require.Len(t, f.k.quotaWrites, writes)
			require.Zero(t, f.k.fixtureKernel.writes)
			_, e = f.run(ctx, f.q.auth, "recover", normalQuotaHandoffDiskIO())
			require.NoError(t, e)
			_, e = f.q.f.g.Observe(ctx, c)
			require.NoError(t, e)
			_, e = f.q.f.g.Apply(ctx, c)
			require.NoError(t, e)
			v, e := f.q.f.g.InspectState(ctx)
			require.NoError(t, e)
			_, e = v.ConfirmedLimits()
			require.NoError(t, e)
			require.Len(t, f.k.quotaWrites, writes)
			require.Zero(t, f.k.fixtureKernel.writes)
		})
	}
}

func TestNormalQuotaMetadataRestartRecoversRetainedQuotaWithoutCapReplay(t *testing.T) {
	ctx := context.Background()
	f := newHandoffFixture(t)
	_, err := f.run(ctx, f.q.auth, "apply", normalQuotaHandoffDiskIO())
	require.NoError(t, err)
	quota := f.q.auth.original.Command
	metadata := quota
	metadata.Revision++
	metadata.BudgetCommitmentID = "same-cap-new-budget"
	_, err = f.q.f.g.Apply(ctx, metadata)
	require.NoError(t, err)
	require.Equal(t, 1, f.k.fixtureKernel.writes)
	v, err := f.q.f.g.InspectState(ctx)
	require.NoError(t, err)
	_, err = v.ConfirmedLimits()
	require.NoError(t, err)
	current, e := f.k.Read(metadata)
	require.NoError(t, e)
	require.NoError(t, current.matches(metadata))
	r := f.state(t)
	require.Equal(t, quota, r.NormalSplitQuota.Command)
	require.Equal(t, metadata, *r.Original)
	require.Equal(t, metadata, *r.Held)
	// 真额度变化不可偷走generic路径，降限维持原安全错误。
	for _, delta := range []int64{-1, 1} {
		changed := metadata
		changed.Revision++
		changed.MemoryMaxBytes = control.Uint64(int64(changed.MemoryMaxBytes) + delta*(1<<20))
		_, e = f.q.f.g.Apply(ctx, changed)
		require.Error(t, e)
		if delta < 0 {
			require.ErrorIs(t, e, ErrUnsafeNormalDecrease)
		}
	}
	old := f.q.f.g
	f.q.f.g = &Guard{config: old.config, kernel: f.k}
	before, e := os.ReadFile(filepath.Join(f.q.f.dir, "state.json"))
	require.NoError(t, e)
	_, e = f.q.f.g.Observe(ctx, metadata)
	require.ErrorIs(t, e, ErrUnknown)
	_, e = f.q.f.g.Apply(ctx, metadata)
	require.ErrorIs(t, e, ErrUnknown)
	_, e = f.q.f.g.InspectState(ctx)
	require.ErrorIs(t, e, ErrUnknown)
	_, e = f.run(ctx, f.q.auth, "recover", normalQuotaHandoffDiskIO())
	require.NoError(t, e)
	after, e := os.ReadFile(filepath.Join(f.q.f.dir, "state.json"))
	require.NoError(t, e)
	require.Equal(t, before, after, "recover must only fsync retained metadata, no old cap or budget replay")
	_, e = f.q.f.g.Observe(ctx, metadata)
	require.NoError(t, e)
	v, e = f.q.f.g.InspectState(ctx)
	require.NoError(t, e)
	_, e = v.ConfirmedLimits()
	require.NoError(t, e)
	require.Equal(t, metadata, *v.Held)
	require.Len(t, f.k.quotaWrites, 2)
	require.Equal(t, 1, f.k.fixtureKernel.writes)
}

func TestNormalQuotaPendingMetadataFenceDoesNotSignUnknown(t *testing.T) {
	ctx := context.Background()
	f := newHandoffFixture(t)
	_, err := f.run(ctx, f.q.auth, "apply", normalQuotaHandoffDiskIO())
	require.NoError(t, err)
	command := f.q.auth.original.Command
	command.Revision++
	command.BudgetCommitmentID = "pending-same-cap-budget"
	f.k.fixtureKernel.fail = true
	_, err = f.q.f.g.Apply(ctx, command)
	require.ErrorIs(t, err, ErrUnknown)
	f.k.fixtureKernel.fail = false
	r := f.state(t)
	require.False(t, r.Applied)
	require.Equal(t, command, *r.Original)
	_, err = f.q.f.g.FenceBound(ctx, command.Identity, 2)
	require.NoError(t, err)
	r = f.state(t)
	require.Equal(t, control.Uint64(2), r.Floor)
	require.False(t, r.Applied)
	_, err = f.q.f.g.Observe(ctx, command)
	require.ErrorIs(t, err, ErrUnknown)
	// 新owner可显式确认同额度metadata；只同步完整原Q，不重放旧owner cap。
	writes := len(f.k.quotaWrites)
	_, err = f.run(ctx, f.q.auth, "recover", normalQuotaHandoffDiskIO())
	require.NoError(t, err)
	require.Len(t, f.k.quotaWrites, writes)
	v, err := f.q.f.g.InspectState(ctx)
	require.NoError(t, err)
	_, err = v.ConfirmedLimits()
	require.NoError(t, err)
	require.Equal(t, command, *v.Held)
}
