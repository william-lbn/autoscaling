//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

// U模型只替代cgroup内核；目录、flock、O_EXCL、准确文件字节及fsync均真实执行。
// 不把这些fixture的假dev/inode/Source/Origin当作Guest或normal资格。
type splitFixtureKernel struct {
	*fixtureKernel
	mu       sync.Mutex
	snapshot NormalSplitTopologySnapshot
	physical PhysicalKernelObservation
	writes   []string
	fail     string
}

func (k *splitFixtureKernel) physicalOriginPGEmpty() error { return nil }
func (k *splitFixtureKernel) physicalOriginRead() (PhysicalKernelObservation, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.physical, nil
}
func cloneSplit(s NormalSplitTopologySnapshot) NormalSplitTopologySnapshot {
	if s.Private != nil {
		n := *s.Private
		s.Private = &n
	}
	if s.Arrays != nil {
		n := *s.Arrays
		s.Arrays = &n
	}
	return s
}
func (k *splitFixtureKernel) normalSplitRead() (NormalSplitTopologySnapshot, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return cloneSplit(k.snapshot), nil
}
func (k *splitFixtureKernel) normalSplitEnable() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.writes = append(k.writes, "enable")
	if k.fail == "enable" {
		return fmt.Errorf("injected enable failure")
	}
	k.snapshot.Parent.Controllers = "memory pids"
	return nil
}
func (k *splitFixtureKernel) normalSplitCreate(name string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.writes = append(k.writes, "mkdir:"+name)
	n := &normalSplitNode{Device: k.snapshot.Parent.Device, Inode: 1001, MemoryMaxBytes: control.Uint64(math.MaxUint64), PidsMaxTasks: control.Uint64(math.MaxUint64), SwapMaxBytes: control.Uint64(math.MaxUint64), MemoryHighUnlimited: true, Empty: true, RootControlled: true}
	if name == "arrays" {
		n.Inode = 1002
		k.snapshot.Arrays = n
	} else {
		k.snapshot.Private = n
	}
	if k.fail == "mkdir:"+name {
		return fmt.Errorf("injected post-mkdir failure")
	}
	return nil
}
func (k *splitFixtureKernel) normalSplitSet(name string, cap, pids control.Uint64) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.writes = append(k.writes, "set:"+name)
	n := k.snapshot.Private
	if name == "arrays" {
		n = k.snapshot.Arrays
	}
	n.MemoryMaxBytes, n.PidsMaxTasks, n.SwapMaxBytes = cap, pids, 0
	if k.fail == "set:"+name {
		return fmt.Errorf("injected partial original bound")
	}
	return nil
}

type splitFixture struct {
	g   *Guard
	k   *splitFixtureKernel
	in  normalSplitInputs
	dir string
}

func newSplitFixture(t *testing.T) *splitFixture {
	t.Helper()
	old := newPreSystemFixture(t)
	in := old.in
	ref, err := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	require.NoError(t, err)
	in.origin.Policy.ProfileID, in.origin.Policy.KernelRelease = ref.ProfileID, ref.KernelRelease
	in.origin.Policy.KernelArtifactSHA256, in.origin.Policy.KernelConfigSHA256, in.origin.Policy.KernelNotesSHA256 = ref.KernelArtifactSHA256, ref.KernelConfigSHA256, ref.KernelNotesSHA256
	in.origin.First.KernelRelease, in.origin.First.KernelNotesSHA256 = ref.KernelRelease, ref.KernelNotesSHA256
	hard := uint32(512)
	in.origin.First.RuntimeCapabilities.HardPIDMaxLimit = &hard
	require.NoError(t, in.origin.Validate())
	originSHA, err := in.origin.ReferenceSHA256()
	require.NoError(t, err)
	policy := NormalSplitTopologyPolicy{ProtocolVersion: 1, Scope: normalSplitScope, PhysicalProfileID: ref.ProfileID, RuntimeDocumentSHA256: in.origin.RuntimeDocumentSHA256, ResourceSourceSHA256: physicalDigest(in.sourceRaw), PrivateMemoryMaxBytes: 128 << 20, InitialArraysMemoryMaxBytes: 64 << 20, PrivatePidsMaxTasks: 8}
	rawPolicy, err := json.Marshal(policy)
	require.NoError(t, err)
	next := normalSplitInputs{source: in.source, sourceRaw: in.sourceRaw, policy: policy, policyRaw: rawPolicy, origin: in.origin, originSHA: originSHA}
	require.NoError(t, next.validate(old.g.config))
	raw, err := json.Marshal(in.origin)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(old.directory, physicalOriginDataName), raw, 0600))
	confirm, err := json.Marshal(physicalOriginConfirmation{ProtocolVersion: 1, OriginSHA256: originSHA})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(old.directory, physicalOriginConfirmName), confirm, 0600))
	snapshot := NormalSplitTopologySnapshot{Identity: in.origin.First.Identity, Parent: normalSplitNode{Device: in.origin.First.Identity.CgroupDevice, Inode: in.origin.First.Identity.CgroupInode, MemoryMaxBytes: control.Uint64(math.MaxUint64), PidsMaxTasks: control.Uint64(math.MaxUint64), SwapMaxBytes: control.Uint64(math.MaxUint64), Empty: true, RootControlled: true, MemoryHighUnlimited: true}, StateDevice: old.k.snapshot.StateDevice, StateInode: old.k.snapshot.StateInode}
	k := &splitFixtureKernel{fixtureKernel: old.k.fixtureKernel, snapshot: snapshot, physical: in.origin.First}
	g := &Guard{config: old.g.config, kernel: k}
	return &splitFixture{g: g, k: k, in: next, dir: old.directory}
}
func (f *splitFixture) run(ctx context.Context, op normalSplitOperation) (NormalSplitTopologyResult, error) {
	s, err := lockState(ctx, f.dir)
	if err != nil {
		return NormalSplitTopologyResult{}, err
	}
	defer s.close()
	return f.g.normalSplitLocked(ctx, f.in, op, s)
}
func splitFile(t *testing.T, f *splitFixture, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.dir, name))
	require.NoError(t, err)
	return b
}

// PG17-U-SPLIT-001: real journal+fixed original caps; no old resource or normal permit.
func TestNormalSplitTopologyOriginalPreparation(t *testing.T) {
	f := newSplitFixture(t)
	parent := f.k.snapshot.Parent
	out, err := f.run(context.Background(), normalSplitPrepare)
	require.NoError(t, err)
	require.True(t, out.DurabilityConfirmed)
	require.False(t, out.NormalDecreasePermit)
	require.Equal(t, f.in.policy.PrivateMemoryMaxBytes, out.Current.Private.MemoryMaxBytes)
	require.Equal(t, f.in.policy.InitialArraysMemoryMaxBytes, out.Current.Arrays.MemoryMaxBytes)
	require.Equal(t, parent.MemoryMaxBytes, out.Current.Parent.MemoryMaxBytes)
	require.Equal(t, parent.PidsMaxTasks, out.Current.Parent.PidsMaxTasks)
	require.Equal(t, []string{"enable", "mkdir:private", "set:private", "mkdir:arrays", "set:arrays"}, f.k.writes)
	_, err = os.Stat(filepath.Join(f.dir, "state.json"))
	require.True(t, os.IsNotExist(err))
	raw := splitFile(t, f, normalSplitIntentName)
	done := splitFile(t, f, normalSplitCompleteName)
	for _, op := range []normalSplitOperation{normalSplitObserve, normalSplitPrepare, normalSplitReplay} {
		r, e := f.run(context.Background(), op)
		require.NoError(t, e)
		require.Equal(t, op != normalSplitObserve, r.DurabilityConfirmed)
		require.Equal(t, out.Original.Nonce, r.Original.Nonce)
		require.Equal(t, raw, splitFile(t, f, normalSplitIntentName))
		require.Equal(t, done, splitFile(t, f, normalSplitCompleteName))
	}
	require.Len(t, f.k.writes, 5)
}

// PG17-U-SPLIT-002: uncertain write only replays original nonce and known inode prefix.
func TestNormalSplitTopologyPendingReplayAndUnrecordedInode(t *testing.T) {
	for _, stage := range []string{"enable", "set:private", "set:arrays", "mkdir:private", "mkdir:arrays"} {
		t.Run(stage, func(t *testing.T) {
			f := newSplitFixture(t)
			f.k.fail = stage
			_, err := f.run(context.Background(), normalSplitPrepare)
			require.ErrorIs(t, err, ErrUnknown)
			raw := splitFile(t, f, normalSplitIntentName)
			writes := len(f.k.writes)
			for _, op := range []normalSplitOperation{normalSplitPrepare, normalSplitObserve} {
				_, e := f.run(context.Background(), op)
				require.ErrorIs(t, e, ErrUnknown)
				require.Len(t, f.k.writes, writes)
			}
			f.k.fail = ""
			out, err := f.run(context.Background(), normalSplitReplay)
			if strings.HasPrefix(stage, "mkdir:") {
				require.ErrorIs(t, err, ErrUnknown)
				require.NotNil(t, f.k.snapshot.Private)
				_, e := os.Stat(filepath.Join(f.dir, normalSplitCompleteName))
				require.True(t, os.IsNotExist(e))
			} else {
				require.NoError(t, err)
				require.False(t, out.NormalDecreasePermit)
			}
			require.Equal(t, raw, splitFile(t, f, normalSplitIntentName))
		})
	}
}

// PG17-U-SPLIT-003: binding/history/current facts never synthesize original permission.
func TestNormalSplitTopologyRejectsSourceAndInitialHistory(t *testing.T) {
	cases := map[string]func(*splitFixture){
		"old-profile":  func(f *splitFixture) { f.in.origin.Policy.ProfileID = NormalV1PhysicalKernelProfileID },
		"source-bytes": func(f *splitFixture) { f.in.sourceRaw = append(f.in.sourceRaw, ' ') },
		"source-parent-pids": func(f *splitFixture) {
			f.in.policy.PrivatePidsMaxTasks = f.in.source.Limits.PidsMaxTasks
			f.in.policyRaw, _ = json.Marshal(f.in.policy)
		},
		"wrong-state":          func(f *splitFixture) { f.k.snapshot.StateInode++ },
		"nonempty":             func(f *splitFixture) { f.k.snapshot.Parent.Empty = false },
		"charged":              func(f *splitFixture) { f.k.snapshot.Parent.MemoryCurrentBytes = 1 },
		"parent-inode":         func(f *splitFixture) { f.k.snapshot.Parent.Inode++ },
		"delegated":            func(f *splitFixture) { f.k.snapshot.Parent.RootControlled = false },
		"preexisting-child":    func(f *splitFixture) { n := f.k.snapshot.Parent; f.k.snapshot.Private = &n },
		"controller":           func(f *splitFixture) { f.k.snapshot.Parent.Controllers = "memory" },
		"physical-drift":       func(f *splitFixture) { f.k.physical.PossibleCPUs++ },
		"missing-confirmation": func(f *splitFixture) { require.NoError(t, os.Remove(filepath.Join(f.dir, physicalOriginConfirmName))) },
		"history": func(f *splitFixture) {
			require.NoError(t, os.WriteFile(filepath.Join(f.dir, "state.json"), []byte("{}"), 0600))
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			f := newSplitFixture(t)
			change(f)
			_, err := f.run(context.Background(), normalSplitPrepare)
			require.Error(t, err)
			require.Empty(t, f.k.writes)
			_, e := os.Stat(filepath.Join(f.dir, normalSplitIntentName))
			require.True(t, os.IsNotExist(e))
		})
	}
}

// PG17-U-SPLIT-004: arrays may change only as finite observed fact; P/topology immutable.
func TestNormalSplitTopologyCompletedDriftAndVariableArrays(t *testing.T) {
	t.Run("arrays-finite-observation", func(t *testing.T) {
		f := newSplitFixture(t)
		_, err := f.run(context.Background(), normalSplitPrepare)
		require.NoError(t, err)
		f.k.snapshot.Arrays.MemoryMaxBytes = 32 << 20
		f.k.snapshot.Arrays.MemoryCurrentBytes = 48 << 20
		f.k.snapshot.Parent.Empty = false
		f.k.snapshot.Private.Empty = false
		out, err := f.run(context.Background(), normalSplitObserve)
		require.NoError(t, err)
		require.False(t, out.NormalDecreasePermit)
		require.False(t, out.DurabilityConfirmed)
		require.Equal(t, control.Uint64(32<<20), out.Current.Arrays.MemoryMaxBytes)
		require.Equal(t, control.Uint64(48<<20), out.Current.Arrays.MemoryCurrentBytes)
	})
	changes := map[string]func(*NormalSplitTopologySnapshot){"private-cap": func(s *NormalSplitTopologySnapshot) { s.Private.MemoryMaxBytes += 64 << 10 }, "private-pids": func(s *NormalSplitTopologySnapshot) { s.Private.PidsMaxTasks++ }, "arrays-unlimited": func(s *NormalSplitTopologySnapshot) { s.Arrays.MemoryMaxBytes = control.Uint64(math.MaxUint64) }, "arrays-pids": func(s *NormalSplitTopologySnapshot) { s.Arrays.PidsMaxTasks = 2 }, "arrays-inode": func(s *NormalSplitTopologySnapshot) { s.Arrays.Inode++ }, "private-inode": func(s *NormalSplitTopologySnapshot) { s.Private.Inode++ }, "swap": func(s *NormalSplitTopologySnapshot) { s.Arrays.SwapMaxBytes = 1 }, "high": func(s *NormalSplitTopologySnapshot) { s.Arrays.MemoryHighUnlimited = false }, "descendant-controller": func(s *NormalSplitTopologySnapshot) { s.Private.Controllers = "memory" }, "delegated": func(s *NormalSplitTopologySnapshot) { s.Arrays.RootControlled = false }}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			f := newSplitFixture(t)
			_, err := f.run(context.Background(), normalSplitPrepare)
			require.NoError(t, err)
			before := splitFile(t, f, normalSplitCompleteName)
			change(&f.k.snapshot)
			writes := len(f.k.writes)
			_, err = f.run(context.Background(), normalSplitReplay)
			require.ErrorIs(t, err, ErrUnknown)
			require.Len(t, f.k.writes, writes)
			require.Equal(t, before, splitFile(t, f, normalSplitCompleteName))
		})
	}
}

// PG17-U-SPLIT-005: real resource flock serializes callers and respects cancellation.
func TestNormalSplitTopologySerializesAndHonorsOriginalLock(t *testing.T) {
	f := newSplitFixture(t)
	s, err := lockState(context.Background(), f.dir)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = f.run(ctx, normalSplitPrepare)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, f.k.writes)
	s.close()
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, e := f.run(context.Background(), normalSplitPrepare); errs <- e }()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}
	require.Len(t, f.k.writes, 5)
}

// PG17-U-SPLIT-006: malicious/corrupt journal is Unknown, never corrected or deleted.
func TestNormalSplitTopologyMalformedJournalNoRepair(t *testing.T) {
	for _, name := range []string{normalSplitIntentName, normalSplitCompleteName, splitIdentityName("private")} {
		t.Run(name, func(t *testing.T) {
			f := newSplitFixture(t)
			_, err := f.run(context.Background(), normalSplitPrepare)
			require.NoError(t, err)
			path := filepath.Join(f.dir, name)
			raw := splitFile(t, f, name)
			require.NoError(t, os.WriteFile(path, append(raw, ' '), 0600))
			before := splitFile(t, f, name)
			_, err = f.run(context.Background(), normalSplitReplay)
			require.Error(t, err)
			require.Equal(t, before, splitFile(t, f, name))
			require.Len(t, f.k.writes, 5)
		})
	}
	t.Run("symlink", func(t *testing.T) {
		f := newSplitFixture(t)
		require.NoError(t, os.Symlink("elsewhere", filepath.Join(f.dir, normalSplitIntentName)))
		_, err := f.run(context.Background(), normalSplitPrepare)
		require.ErrorIs(t, err, ErrUnknown)
		require.Empty(t, f.k.writes)
	})
}

// PG17-U-SPLIT-007: canonical strict DTO, overflow and original-loader fail closed.
func TestNormalSplitTopologyStrictPolicyAndFixedEntry(t *testing.T) {
	f := newSplitFixture(t)
	raw := string(f.in.policyRaw)
	for _, bad := range []string{strings.Replace(raw, "134217728", "0134217728", 1), raw[:len(raw)-1] + `,"unknown":1}`, strings.Replace(raw, `"private_pids_max_tasks":"8"`, `"private_pids_max_tasks":8`, 1), raw[:len(raw)-1] + `,"scope":"other"}`} {
		var p NormalSplitTopologyPolicy
		if bad == raw {
			continue
		}
		require.Error(t, json.Unmarshal([]byte(bad), &p))
	}
	for _, a := range []control.Uint64{0, math.MaxUint64, math.MaxInt64, 1} {
		p := f.in.policy
		p.InitialArraysMemoryMaxBytes = a
		require.Error(t, p.validate())
	}
	require.NoError(t, normalSplitBootMarker("console=ttyS0 "+NormalSplitTopologyKernelArg))
	for _, cmdline := range []string{"", "neonvm.dsga.normal_split", "neonvm.dsga.normal_split=", NormalSplitTopologyKernelArg + " " + NormalSplitTopologyKernelArg, NormalSplitTopologyKernelArg + " neonvm.dsga.normal_split", NormalSplitTopologyKernelArg + " neonvm.dsga.normal_split=other"} {
		require.Error(t, normalSplitBootMarker(cmdline))
	}
	if _, err := f.g.PrepareNormalSplitTopology(context.Background()); err == nil {
		t.Fatal("test host acquired original fixed ISO/boot qualification")
	}
	require.Empty(t, f.k.writes)
	require.True(t, errors.Is(fmt.Errorf("%w", ErrUnknown), ErrUnknown))
}
