//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func populatedSystemServiceFixture(t *testing.T) (*Guard, *fixtureKernel, SourceConfig, View) {
	t.Helper()
	g, k, b, c := systemFixture(t)
	g.config.ExecutionModel = TrustedControllerExecutionModel
	require.NoError(t, func() error { _, e := g.Bootstrap(context.Background(), b); return e }())
	require.NoError(t, func() error { _, e := g.placeSystem(context.Background(), fixturePlacement(k)); return e }())
	birth := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks}
	require.NoError(t, g.launchWithBirth(context.Background(), nil, &birth, func() error { k.process = &birth; return nil }))
	k.e.CgroupPopulated = true
	source := sourceFixture(c)
	source.Config.ExecutionModel = TrustedControllerExecutionModel
	source.Config.SystemCgroup = true
	require.NoError(t, source.Validate())
	v, e := g.InspectState(context.Background())
	require.NoError(t, e)
	return g, k, source, v
}

func TestSystemServiceReadinessAllowsOriginalPGWithoutAnotherBirth(t *testing.T) {
	g, k, source, v := populatedSystemServiceFixture(t)
	before, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
	require.NoError(t, e)
	writes := k.writes
	for _, role := range []string{"kernel-version", "network", "udev", "udev-init", "acpi", "logs", "clock", "daemon", "resource"} {
		require.NoError(t, checkSystemServiceState(role, source, v), role)
	}
	execs := 0
	require.EqualError(t, g.launch(context.Background(), func() error { execs++; return nil }), "bootstrap cgroup is not empty")
	require.Zero(t, execs, "new System readiness must not authorize another PG")
	require.Equal(t, writes, k.writes)
	after, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
	require.NoError(t, e)
	require.Equal(t, before, after, "readiness cannot write WAL")
}

func TestSystemServiceReadinessKeepsCurrentHeldAndFailsClosed(t *testing.T) {
	t.Run("current-held", func(t *testing.T) {
		g, k, source, _ := populatedSystemServiceFixture(t)
		c := Command{ProtocolVersion: 1, Identity: k.id, IssuerEpoch: 1, Revision: 1, BudgetCommitmentID: "runtime-held", PostmasterPID: k.process.PostmasterPID, PostmasterStartTicks: k.process.PostmasterStartTicks, MemoryMaxBytes: 80 << 20, OutsideUpperBytes: source.Limits.OutsideUpperBytes, SystemMemoryMaxBytes: source.Limits.SystemMemoryMaxBytes, HugePools: append([]HugePool(nil), source.Limits.HugePools...)}
		_, e := g.Apply(context.Background(), c)
		require.NoError(t, e)
		v, e := g.InspectState(context.Background())
		require.NoError(t, e)
		require.NotEqual(t, source.Limits.MemoryMaxBytes, v.Actual.MemoryMaxBytes)
		writes := k.writes
		require.NoError(t, checkSystemServiceState("resource", source, v))
		require.Equal(t, writes, k.writes)
	})
	for _, which := range []string{"compute", "allow-start", "unknown-role", "source-uid", "source-budget", "source-mode", "identity", "bounds", "placement", "system-cap", "unknown-write"} {
		t.Run(which, func(t *testing.T) {
			g, k, source, v := populatedSystemServiceFixture(t)
			role := "resource"
			raw, e := json.Marshal(v)
			require.NoError(t, e)
			var clone View
			require.NoError(t, json.Unmarshal(raw, &clone))
			v = clone
			switch which {
			case "compute", "allow-start":
				role = which
			case "unknown-role":
				role = "resource;exec-forbidden"
			case "source-uid":
				source.Config.VMUID = "different-vm"
			case "source-budget":
				source.BudgetCommitmentID = "different-bootstrap"
			case "source-mode":
				source.Config.ExecutionModel = ""
			case "identity":
				v.Identity.GuestBootID = "00000000-0000-0000-0000-000000000099"
			case "bounds":
				v.Actual.MemoryMaxBytes++
			case "placement":
				v.Actual.System.RootUserspaceCount = 1
			case "system-cap":
				v.Actual.System.MemoryMaxBytes++
			case "unknown-write":
				c := Command{ProtocolVersion: 1, Identity: v.Identity, IssuerEpoch: 1, Revision: 1, BudgetCommitmentID: "unknown-runtime", PostmasterPID: k.process.PostmasterPID, PostmasterStartTicks: k.process.PostmasterStartTicks, MemoryMaxBytes: 80 << 20, OutsideUpperBytes: source.Limits.OutsideUpperBytes, SystemMemoryMaxBytes: source.Limits.SystemMemoryMaxBytes, HugePools: append([]HugePool(nil), source.Limits.HugePools...)}
				v.Original = &c
				v.Applied = false
			}
			before, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
			require.NoError(t, e)
			writes := k.writes
			err := checkSystemServiceState(role, source, v)
			require.Error(t, err)
			if which == "unknown-write" {
				require.True(t, errors.Is(err, ErrUnknown), err)
			}
			require.Equal(t, writes, k.writes)
			after, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
			require.NoError(t, e)
			require.Equal(t, before, after)
		})
	}
	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, CheckSystemService(ctx, "resource"), context.Canceled)
	})
}
