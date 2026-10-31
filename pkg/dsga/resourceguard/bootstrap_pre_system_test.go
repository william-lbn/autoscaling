//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// 仅Source/内核为明确模型，实际临时WAL/flock/fsync；与PreSys同一生产locked核心。
type bootstrapPreSystemFixtureKernel struct {
	*preSystemFixtureKernel
	in prebootstrapSystemInputs
}

func (k *bootstrapPreSystemFixtureKernel) bootstrapPreSystemInputs(_ *lockedState) (prebootstrapSystemInputs, error) {
	return k.in, nil
}
func (k *bootstrapPreSystemFixtureKernel) ReadBounds(id Identity, l Limits) (Evidence, error) {
	e, err := k.fixtureKernel.ReadBounds(id, l)
	if err != nil {
		return e, err
	}
	k.phaseMu.Lock()
	system := k.snapshot.System
	k.phaseMu.Unlock()
	e.System = &system
	return e, nil
}
func setupBootstrapPreSystem(t *testing.T, profile string) (*preSystemFixture, *bootstrapPreSystemFixtureKernel, BootstrapCommand) {
	f := newPreSystemFixtureProfile(t, profile)
	_, err := f.run(context.Background(), preSystemPrepare)
	require.NoError(t, err)
	k := &bootstrapPreSystemFixtureKernel{preSystemFixtureKernel: f.k, in: f.in}
	f.g.kernel = k
	k.e.GuestMemTotalBytes = 1 << 30
	k.e.TaskLimit = &TaskLimitEvidence{MaxTasks: f.in.source.Limits.PidsMaxTasks}
	k.e.AddressSpace = &AddressSpaceEvidence{Irreversible: true}
	return f, k, f.in.origin.ExpectedBootstrap
}
func runBootstrapPreSystem(t *testing.T, f *preSystemFixture, c BootstrapCommand, observe, replay bool) (Evidence, error) {
	t.Helper()
	s, err := lockState(context.Background(), f.directory)
	require.NoError(t, err)
	defer s.close()
	return f.g.bootstrapLocked(context.Background(), c, observe, replay, s)
}
func readBootstrapPreSystemRecord(t *testing.T, f *preSystemFixture) *record {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.directory, "state.json"))
	require.NoError(t, err)
	var r record
	require.NoError(t, json.Unmarshal(raw, &r))
	return &r
}

func TestBootstrapContinuesOnlyOriginalPreSystem(t *testing.T) {
	for _, profile := range []string{NormalV1PhysicalKernelProfileID, NormalSplitV1PhysicalKernelProfileID} {
		t.Run(profile, func(t *testing.T) {
			f, k, b := setupBootstrapPreSystem(t, profile)
			before, err := os.ReadFile(filepath.Join(f.directory, preSystemIntentName))
			require.NoError(t, err)
			e, err := runBootstrapPreSystem(t, f, b, false, false)
			require.NoError(t, err)
			require.True(t, e.System.PID1Member)
			r := readBootstrapPreSystemRecord(t, f)
			require.NotNil(t, r.Bootstrap.PreSystem)
			require.True(t, r.Bootstrap.Applied)
			require.True(t, r.Bootstrap.SystemPlacementObserved)
			require.NoError(t, r.validate(b.Identity))
			require.Equal(t, 1, k.writes)
			original := *r.Bootstrap.PreSystem
			for _, op := range []struct{ observe, replay bool }{{false, false}, {true, false}, {false, true}} {
				_, err = runBootstrapPreSystem(t, f, b, op.observe, op.replay)
				require.NoError(t, err)
			}
			require.Equal(t, 1, k.writes)
			require.Equal(t, original, *readBootstrapPreSystemRecord(t, f).Bootstrap.PreSystem)
			after, err := os.ReadFile(filepath.Join(f.directory, preSystemIntentName))
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestBootstrapPreSystemRejectsUnboundAndChangedStage(t *testing.T) {
	cases := []string{"missing-intent", "missing-completion", "corrupt-completion", "missing-origin-confirm", "cap-drift", "source-drift", "command-drift", "small-guest", "populated-pg"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			f, k, b := setupBootstrapPreSystem(t, NormalSplitV1PhysicalKernelProfileID)
			switch name {
			case "missing-intent":
				require.NoError(t, os.Remove(filepath.Join(f.directory, preSystemIntentName)))
			case "missing-completion":
				require.NoError(t, os.Remove(filepath.Join(f.directory, preSystemCompleteName)))
			case "corrupt-completion":
				require.NoError(t, os.WriteFile(filepath.Join(f.directory, preSystemCompleteName), []byte(`{}`), 0600))
			case "missing-origin-confirm":
				require.NoError(t, os.Remove(filepath.Join(f.directory, physicalOriginConfirmName)))
			case "cap-drift":
				k.snapshot.System.MemoryMaxBytes++
			case "source-drift":
				k.in.sourceRaw = append(append([]byte(nil), k.in.sourceRaw...), byte(' '))
			case "command-drift":
				b.BudgetCommitmentID = "other-original-commitment"
			case "small-guest":
				k.e.GuestMemTotalBytes = 256 << 20
			case "populated-pg":
				k.e.CgroupPopulated = true
				k.snapshot.PGEmpty = false
			}
			_, err := runBootstrapPreSystem(t, f, b, false, false)
			require.Error(t, err)
			require.Zero(t, k.writes)
			_, err = os.Stat(filepath.Join(f.directory, "state.json"))
			require.True(t, os.IsNotExist(err))
		})
	}
	t.Run("legacy-nonempty-without-source", func(t *testing.T) {
		g, k, b, _ := systemFixture(t)
		k.e.System.ProcessCount = 1
		k.e.System.PID1Member = true
		k.e.System.RootUserspaceCount = 0
		k.e.System.MemoryMaxBytes = b.Limits.SystemMemoryMaxBytes
		k.e.System.SwapMaxBytes = 0
		_, err := g.Bootstrap(context.Background(), b)
		require.Error(t, err)
		require.Zero(t, k.writes)
	})
}

func TestBootstrapPreSystemPendingKeepsOriginalReference(t *testing.T) {
	f, k, b := setupBootstrapPreSystem(t, NormalSplitV1PhysicalKernelProfileID)
	k.fail = ""
	k.fixtureKernel.fail = true
	_, err := runBootstrapPreSystem(t, f, b, false, false)
	require.ErrorIs(t, err, ErrUnknown)
	r := readBootstrapPreSystemRecord(t, f)
	require.False(t, r.Bootstrap.Applied)
	require.False(t, r.Bootstrap.SystemPlacementObserved)
	require.NotNil(t, r.Bootstrap.PreSystem)
	ref := *r.Bootstrap.PreSystem
	require.Equal(t, 1, k.writes)
	_, err = runBootstrapPreSystem(t, f, b, false, false)
	require.ErrorIs(t, err, ErrUnknown)
	require.Equal(t, 1, k.writes)
	saved := k.in.policySHA
	k.in.policySHA = physicalDigest([]byte("other-policy"))
	_, err = runBootstrapPreSystem(t, f, b, false, true)
	require.Error(t, err)
	require.Equal(t, 1, k.writes)
	require.Equal(t, ref, *readBootstrapPreSystemRecord(t, f).Bootstrap.PreSystem)
	k.in.policySHA = saved
	k.fixtureKernel.fail = false
	_, err = runBootstrapPreSystem(t, f, b, false, true)
	require.NoError(t, err)
	r = readBootstrapPreSystemRecord(t, f)
	require.True(t, r.Bootstrap.Applied)
	require.True(t, r.Bootstrap.SystemPlacementObserved)
	require.Equal(t, ref, *r.Bootstrap.PreSystem)
}

func TestBootstrapPreSystemCompletionNeverRepairsLostSources(t *testing.T) {
	f, k, b := setupBootstrapPreSystem(t, NormalSplitV1PhysicalKernelProfileID)
	_, err := runBootstrapPreSystem(t, f, b, false, false)
	require.NoError(t, err)
	raw, err := os.ReadFile(filepath.Join(f.directory, "state.json"))
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(f.directory, preSystemCompleteName)))
	_, err = runBootstrapPreSystem(t, f, b, true, false)
	require.ErrorIs(t, err, ErrUnknown)
	after, err := os.ReadFile(filepath.Join(f.directory, "state.json"))
	require.NoError(t, err)
	require.Equal(t, raw, after)
	require.Equal(t, 1, k.writes)
	r := readBootstrapPreSystemRecord(t, f)
	r.Bootstrap.PreSystem.PolicySHA256 = "bad"
	require.Error(t, r.validate(b.Identity))
	r.Bootstrap.PreSystem = nil
	require.Error(t, r.validate(b.Identity), "version2 may not silently lose the original reference")
	r.Bootstrap.PreSystemStageVersion = 0
	r.Bootstrap.Applied = false
	r.Bootstrap.SystemPlacementObserved = false
	require.NoError(t, r.validate(b.Identity))
	r.Bootstrap.PreSystem = &bootstrapPreSystemReference{ProtocolVersion: 1}
	require.Error(t, r.validate(b.Identity))
	legacy, _, old, _ := freshBootstrapFixture(t)
	_, err = legacy.Bootstrap(context.Background(), old)
	require.NoError(t, err)
	encoded, err := os.ReadFile(filepath.Join(legacy.config.StateDir, "state.json"))
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	var boot map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(fields["bootstrap"], &boot))
	_, present := boot["pre_system"]
	require.False(t, present)
}
