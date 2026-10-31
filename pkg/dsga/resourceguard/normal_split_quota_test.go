//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

// 仅以模型替代cgroup/完整Source授权。flock、O_EXCL、准确四阶段字节/fsync是真实的；
// 这些测试不能证明生产Source闭合或实际normal降限。不会给普通Apply开旁路。
type quotaFixtureKernel struct {
	*splitFixtureKernel
	quotaWrites []string
	failAfter   string
}

func (k *quotaFixtureKernel) normalSplitQuotaWrite(name string, expected normalSplitNode, target control.Uint64) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	n := &k.snapshot.Parent
	if name == "arrays" {
		n = k.snapshot.Arrays
	}
	if !splitSameNode(*n, expected) {
		return fmt.Errorf("injected original inode differs")
	}
	k.quotaWrites = append(k.quotaWrites, name)
	n.MemoryMaxBytes = target
	if k.failAfter == name {
		return fmt.Errorf("injected post-effect write error")
	}
	return nil
}

type quotaFixture struct {
	f    *splitFixture
	k    *quotaFixtureKernel
	auth normalSplitQuotaAuthorization
}

func newQuotaFixture(t *testing.T) *quotaFixture {
	t.Helper()
	f := newSplitFixture(t)
	_, e := f.run(context.Background(), normalSplitPrepare)
	require.NoError(t, e)
	k := &quotaFixtureKernel{splitFixtureKernel: f.k}
	f.g.kernel = k
	k.snapshot.Parent.MemoryMaxBytes = 512 << 20
	k.snapshot.Parent.PidsMaxTasks = 16
	k.snapshot.Parent.SwapMaxBytes = 0
	k.snapshot.Parent.MemoryCurrentBytes = 17 << 20
	k.snapshot.Private.MemoryCurrentBytes = 1 << 20
	k.snapshot.Private.Empty = false
	k.snapshot.Arrays.MemoryCurrentBytes = 16 << 20
	l := f.in.source.Limits
	cmd := Command{ProtocolVersion: 1, Identity: k.snapshot.Identity, IssuerEpoch: 1, Revision: 1, BudgetCommitmentID: "quota-test-original", PostmasterPID: 123, PostmasterStartTicks: 1, MemoryMaxBytes: 256 << 20, PidsMaxTasks: 16, AddressSpaceMaxBytes: l.AddressSpaceMaxBytes, SystemMemoryMaxBytes: l.SystemMemoryMaxBytes, OutsideUpperBytes: l.OutsideUpperBytes, HugePools: append([]HugePool(nil), l.HugePools...)}
	v := normalSplitQuotaOriginal{Command: cmd, SourceSHA256: strings.Repeat("ab", 32), SplitIntentSHA256: strings.Repeat("cd", 32), SplitCompletionSHA256: strings.Repeat("ef", 32), PGRequestSHA256: strings.Repeat("12", 32), MappingGeneration: 1, Original: cloneSplit(k.snapshot), TargetArraysBytes: 32 << 20, ForceUpperBytes: 64 << 10, CounterGapBytes: 64 << 10}
	require.NoError(t, v.validate())
	return &quotaFixture{f, k, normalSplitQuotaAuthorization{v, func(ctx context.Context, _ *lockedState, _ *record, _ normalSplitQuotaOriginal) error {
		return ctx.Err()
	}}}
}
func (f *quotaFixture) run(ctx context.Context, auth normalSplitQuotaAuthorization, mode string, disk normalSplitQuotaIO) (NormalSplitTopologySnapshot, error) {
	s, e := lockState(ctx, f.f.dir)
	if e != nil {
		return NormalSplitTopologySnapshot{}, e
	}
	defer s.close()
	return f.f.g.normalSplitQuotaLocked(ctx, s, nil, auth, mode, disk)
}
func (f *quotaFixture) gate(t *testing.T) error {
	s, e := lockState(context.Background(), f.f.dir)
	require.NoError(t, e)
	defer s.close()
	return f.f.g.rejectNormalSplitQuotaPendingLocked(s)
}
func TestNormalSplitQuotaActualDomainsOrderedAndGrow(t *testing.T) {
	f := newQuotaFixture(t)
	beforePrivate := *f.k.snapshot.Private
	out, e := f.run(context.Background(), f.auth, "apply", normalSplitQuotaDiskIO())
	require.NoError(t, e)
	require.Equal(t, []string{"arrays", "normal"}, f.k.quotaWrites)
	require.Equal(t, f.auth.original.Command.MemoryMaxBytes, out.Parent.MemoryMaxBytes)
	require.Equal(t, f.auth.original.TargetArraysBytes, out.Arrays.MemoryMaxBytes)
	require.Equal(t, beforePrivate, *out.Private)
	require.NoError(t, f.gate(t))
	for _, mode := range []string{"observe", "apply", "replay"} {
		_, e = f.run(context.Background(), f.auth, mode, normalSplitQuotaDiskIO())
		require.NoError(t, e)
	}
	require.Len(t, f.k.quotaWrites, 2)
	grow := f.auth
	grow.original.Original = cloneSplit(f.k.snapshot)
	grow.original.Command.Revision = 2
	grow.original.Command.MemoryMaxBytes = 512 << 20
	grow.original.TargetArraysBytes = 64 << 20
	require.NoError(t, grow.original.validate())
	out, e = f.run(context.Background(), grow, "apply", normalSplitQuotaDiskIO())
	require.NoError(t, e)
	require.Equal(t, []string{"arrays", "normal", "normal", "arrays"}, f.k.quotaWrites)
	require.Equal(t, control.Uint64(512<<20), out.Parent.MemoryMaxBytes)
	require.NoError(t, f.gate(t))
	_, e = os.Stat(filepath.Join(f.f.dir, "state.json"))
	require.True(t, os.IsNotExist(e), "primitive does not forge resource Held handoff")
}
func TestNormalSplitQuotaPartialWritesOnlyExactReplay(t *testing.T) {
	for _, at := range []string{"arrays", "normal"} {
		t.Run(at, func(t *testing.T) {
			f := newQuotaFixture(t)
			f.k.failAfter = at
			_, e := f.run(context.Background(), f.auth, "apply", normalSplitQuotaDiskIO())
			require.ErrorIs(t, e, ErrUnknown)
			require.ErrorIs(t, f.gate(t), ErrUnknown)
			count := len(f.k.quotaWrites)
			_, e = f.run(context.Background(), f.auth, "apply", normalSplitQuotaDiskIO())
			require.ErrorIs(t, e, ErrUnknown)
			require.Len(t, f.k.quotaWrites, count)
			changed := f.auth
			changed.original.PGRequestSHA256 = strings.Repeat("34", 32)
			_, e = f.run(context.Background(), changed, "replay", normalSplitQuotaDiskIO())
			require.ErrorIs(t, e, ErrUnknown)
			require.Len(t, f.k.quotaWrites, count)
			f.k.failAfter = ""
			_, e = f.run(context.Background(), f.auth, "replay", normalSplitQuotaDiskIO())
			require.NoError(t, e)
			require.Equal(t, []string{"arrays", "normal"}, f.k.quotaWrites)
			require.NoError(t, f.gate(t))
		})
	}
}
func TestNormalSplitQuotaVisibleCompleteNeverReplacesDurability(t *testing.T) {
	f := newQuotaFixture(t)
	disk := normalSplitQuotaDiskIO()
	sync := disk.syncFile
	disk.syncFile = func(s *lockedState, n string) error {
		if strings.HasSuffix(n, ".complete.json") {
			return fmt.Errorf("injected complete fsync failure")
		}
		return sync(s, n)
	}
	_, e := f.run(context.Background(), f.auth, "apply", disk)
	require.ErrorIs(t, e, ErrUnknown)
	require.ErrorIs(t, f.gate(t), ErrUnknown)
	require.Len(t, f.k.quotaWrites, 2)
	_, e = f.run(context.Background(), f.auth, "observe", normalSplitQuotaDiskIO())
	require.ErrorIs(t, e, ErrUnknown)
	_, e = f.run(context.Background(), f.auth, "apply", normalSplitQuotaDiskIO())
	require.ErrorIs(t, e, ErrUnknown)
	_, e = f.run(context.Background(), f.auth, "replay", normalSplitQuotaDiskIO())
	require.NoError(t, e)
	require.Len(t, f.k.quotaWrites, 2)
	require.NoError(t, f.gate(t))
	old := f.f.g
	f.f.g = &Guard{config: old.config, kernel: f.k}
	require.ErrorIs(t, f.gate(t), ErrUnknown, "new Guard must lose original successful fsync witness")
	_, e = f.run(context.Background(), f.auth, "observe", normalSplitQuotaDiskIO())
	require.ErrorIs(t, e, ErrUnknown)
	_, e = f.run(context.Background(), f.auth, "replay", normalSplitQuotaDiskIO())
	require.NoError(t, e)
	require.Len(t, f.k.quotaWrites, 2)
}
func TestNormalSplitQuotaWriteAheadAndCancellation(t *testing.T) {
	for _, when := range []string{"intent-create", "intent-sync", "cancel-after-array"} {
		t.Run(when, func(t *testing.T) {
			f := newQuotaFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			disk := normalSplitQuotaDiskIO()
			switch when {
			case "intent-create":
				disk.create = func(*lockedState, string, []byte) error { return fmt.Errorf("injected O_EXCL failure") }
			case "intent-sync":
				disk.syncFile = func(*lockedState, string) error { return fmt.Errorf("injected initial fsync failure") }
			case "cancel-after-array":
				create := disk.create
				disk.create = func(s *lockedState, n string, b []byte) error {
					e := create(s, n, b)
					if strings.HasSuffix(n, ".first-applied.json") {
						cancel()
					}
					return e
				}
			}
			_, e := f.run(ctx, f.auth, "apply", disk)
			require.ErrorIs(t, e, ErrUnknown)
			if when == "cancel-after-array" {
				require.Equal(t, []string{"arrays"}, f.k.quotaWrites)
			} else {
				require.Empty(t, f.k.quotaWrites)
			}
			if when != "intent-create" {
				require.ErrorIs(t, f.gate(t), ErrUnknown)
				_, e = f.run(context.Background(), f.auth, "replay", normalSplitQuotaDiskIO())
				require.NoError(t, e)
			}
		})
	}
}
func TestNormalSplitQuotaRejectsDriftAndIncompleteEnvelope(t *testing.T) {
	for _, name := range []string{"nil-source", "source-error", "private-cap", "arrays-inode", "parent-inode", "swap", "helper-active", "charged-over-target", "force-over-target", "counter-overflow", "parent-only-shrink", "readonly-observe"} {
		t.Run(name, func(t *testing.T) {
			f := newQuotaFixture(t)
			auth := f.auth
			mode := "apply"
			switch name {
			case "nil-source":
				auth.check = nil
			case "source-error":
				auth.check = func(context.Context, *lockedState, *record, normalSplitQuotaOriginal) error {
					return errors.New("original continuous Source not qualified")
				}
			case "private-cap":
				f.k.snapshot.Private.MemoryMaxBytes += 64 << 10
			case "arrays-inode":
				f.k.snapshot.Arrays.Inode++
			case "parent-inode":
				f.k.snapshot.Parent.Inode++
			case "swap":
				f.k.snapshot.Arrays.SwapCurrentBytes = 4096
			case "helper-active":
				f.k.snapshot.Arrays.Empty = false
			case "charged-over-target":
				f.k.snapshot.Arrays.MemoryCurrentBytes = auth.original.TargetArraysBytes + 4096
			case "force-over-target":
				auth.original.ForceUpperBytes = 1 << 40
			case "counter-overflow":
				auth.original.CounterGapBytes = ^control.Uint64(0)
			case "parent-only-shrink":
				auth.original.TargetArraysBytes = auth.original.Original.Arrays.MemoryMaxBytes
			case "readonly-observe":
				mode = "observe"
			}
			_, e := f.run(context.Background(), auth, mode, normalSplitQuotaDiskIO())
			require.ErrorIs(t, e, ErrUnknown)
			require.Empty(t, f.k.quotaWrites)
		})
	}
}
func TestNormalSplitQuotaOrphansAndOtherPendingFence(t *testing.T) {
	for _, kind := range []string{"orphan-phase", "unknown-file", "other-intent", "lower-revision"} {
		t.Run(kind, func(t *testing.T) {
			f := newQuotaFixture(t)
			key := normalSplitQuotaKey(f.auth.original)
			if kind == "orphan-phase" {
				require.NoError(t, os.WriteFile(filepath.Join(f.f.dir, normalSplitQuotaName(key, "complete")), []byte("{}"), 0600))
			} else if kind == "unknown-file" {
				require.NoError(t, os.WriteFile(filepath.Join(f.f.dir, normalSplitQuotaPrefix+"foreign"), []byte("{}"), 0600))
			} else if kind == "other-intent" {
				f.k.failAfter = "arrays"
				_, e := f.run(context.Background(), f.auth, "apply", normalSplitQuotaDiskIO())
				require.Error(t, e)
				f.k.failAfter = ""
				f.auth.original.Command.Revision++
			} else {
				_, e := f.run(context.Background(), f.auth, "apply", normalSplitQuotaDiskIO())
				require.NoError(t, e)
				f.auth.original.Original = cloneSplit(f.k.snapshot)
				f.auth.original.Command.MemoryMaxBytes = 512 << 20
				f.auth.original.TargetArraysBytes = 64 << 20
				f.auth.original.Command.BudgetCommitmentID = "changed-same-revision"
			}
			before := append([]string(nil), f.k.quotaWrites...)
			_, e := f.run(context.Background(), f.auth, "apply", normalSplitQuotaDiskIO())
			require.ErrorIs(t, e, ErrUnknown)
			require.True(t, reflect.DeepEqual(before, f.k.quotaWrites))
		})
	}
}
