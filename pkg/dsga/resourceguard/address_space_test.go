package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const (
	finiteAS          control.Uint64 = 128 << 20
	processLimitsText                = "Limit Soft Limit Hard Limit Units\nMax address space 134217728 134217728 bytes\n"
	processStatusText                = "Uid:\t1000 1000 1000 1000\nNoNewPrivs:\t1\nCapInh:\t0000000000000000\nCapPrm:\t0000000000000000\nCapEff:\t0000000000000000\nCapAmb:\t0000000000000000\nVmSize:\t65536 kB\n"
)

func TestAddressSpaceProcStrictPrivilegesAndExistingMappings(t *testing.T) {
	hard, virtual, err := processAddressSpace(processLimitsText, processStatusText, finiteAS)
	require.NoError(t, err)
	require.Equal(t, finiteAS, hard)
	require.Equal(t, control.Uint64(64<<20), virtual)
	_, _, err = processAddressSpace(processLimitsText, processStatusText, finiteAS+(64<<10))
	require.NoError(t, err, "读取上界允许原额度内更低且不可放宽的hard")
	_, _, err = processAddressSpace(processLimitsText, processStatusText+"State:\tZ (zombie)\n", finiteAS)
	require.ErrorIs(t, err, errAddressSpaceChurn)
	for _, tc := range []struct{ name, limits, status string }{
		{"missing-limit", "", processStatusText},
		{"unlimited", strings.ReplaceAll(processLimitsText, "134217728", "unlimited"), processStatusText},
		{"duplicate-limit", processLimitsText + processLimitsText, processStatusText},
		{"noncanonical-limit", strings.ReplaceAll(processLimitsText, "134217728", "0134217728"), processStatusText},
		{"above-original", strings.ReplaceAll(processLimitsText, "134217728", "134217729"), processStatusText},
		{"soft-over-hard", strings.Replace(processLimitsText, "134217728", "134217729", 1), processStatusText},
		{"wrong-limit-unit", strings.ReplaceAll(processLimitsText, "bytes", "kB"), processStatusText},
		{"zero-limit", strings.ReplaceAll(processLimitsText, "134217728", "0"), processStatusText},
		{"overflow", strings.ReplaceAll(processLimitsText, "134217728", "18446744073709551616"), processStatusText},
		{"missing-status", processLimitsText, ""},
		{"new-privs", processLimitsText, strings.ReplaceAll(processStatusText, "NoNewPrivs:\t1", "NoNewPrivs:\t0")},
		{"duplicate-nnp", processLimitsText, processStatusText + "NoNewPrivs:\t1\n"},
		{"existing-va-exceeded", processLimitsText, strings.ReplaceAll(processStatusText, "65536", "131073")},
		{"wrong-size-unit", processLimitsText, strings.ReplaceAll(processStatusText, "kB", "bytes")},
		{"noncanonical-size", processLimitsText, strings.ReplaceAll(processStatusText, "65536", "+65536")},
		{"zero-size", processLimitsText, strings.ReplaceAll(processStatusText, "65536", "0")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := processAddressSpace(tc.limits, tc.status, finiteAS)
			require.Error(t, err)
		})
	}
	for _, key := range []string{"CapInh", "CapPrm", "CapEff", "CapAmb", "VmSize", "NoNewPrivs"} {
		t.Run("missing-"+key, func(t *testing.T) {
			var lines []string
			for _, line := range strings.Split(processStatusText, "\n") {
				if !strings.HasPrefix(line, key+":") {
					lines = append(lines, line)
				}
			}
			_, _, err := processAddressSpace(processLimitsText, strings.Join(lines, "\n"), finiteAS)
			require.Error(t, err)
		})
		if strings.HasPrefix(key, "Cap") {
			t.Run("retained-"+key, func(t *testing.T) {
				status := strings.ReplaceAll(processStatusText, key+":\t0000000000000000", key+":\t0000000000000001")
				_, _, err := processAddressSpace(processLimitsText, status, finiteAS)
				require.Error(t, err)
			})
		}
	}
}

func TestAddressSpacePIDListsAndBoundedStableObservation(t *testing.T) {
	pids, err := addressSpacePIDs("200\n100\n", 64)
	require.NoError(t, err)
	require.Equal(t, []int{100, 200}, pids)
	for _, raw := range []string{"1", "0", "0100", "100\n100", "100\n\n", " 100", "100 ", "4194305", "100\n200\n300"} {
		_, err := addressSpacePIDs(raw, 2)
		require.Error(t, err, raw)
	}
	limits := Limits{MemoryMaxBytes: 0, PidsMaxTasks: 64, AddressSpaceMaxBytes: finiteAS, SystemMemoryMaxBytes: 0, OutsideUpperBytes: 0, HugePools: nil}
	for _, mode := range []string{"reorder", "empty", "fork-once", "exit-once", "reuse-once", "duplicate-once", "busy", "bad-limit"} {
		t.Run(mode, func(t *testing.T) {
			reads, calls := 0, 0
			read := func() (string, error) {
				reads++
				switch mode {
				case "empty":
					return "", nil
				case "fork-once":
					if reads == 1 {
						return "100", nil
					}
				case "duplicate-once":
					if reads == 1 {
						return "100\n100", nil
					}
				case "busy":
					if reads%2 == 1 {
						return "100", nil
					}
					return "100\n200", nil
				}
				if reads%2 == 1 {
					return "100\n200", nil
				}
				return "200\n100", nil
			}
			inspect := func(pid int) (processAddressSample, error) {
				calls++
				if mode == "exit-once" && calls == 1 {
					return processAddressSample{}, os.ErrNotExist
				}
				if mode == "bad-limit" {
					return processAddressSample{}, fmt.Errorf("unlimited process")
				}
				start := control.Uint64(pid)
				if mode == "reuse-once" && calls == 3 {
					start++
				}
				return processAddressSample{start: start, hard: finiteAS, virtual: 64 << 20}, nil
			}
			evidence, err := collectAddressSpaces(limits, read, inspect)
			switch mode {
			case "busy":
				require.ErrorIs(t, err, ErrUnknown)
				require.Nil(t, evidence)
				require.Equal(t, 6, reads)
			case "bad-limit":
				require.Error(t, err)
				require.NotErrorIs(t, err, ErrUnknown)
				require.Nil(t, evidence)
				require.Equal(t, 1, reads)
			default:
				require.NoError(t, err)
				require.True(t, evidence.Irreversible)
				if mode == "empty" {
					require.Zero(t, evidence.Processes)
					require.Zero(t, calls)
				} else {
					require.Equal(t, control.Uint64(2), evidence.Processes)
					require.Equal(t, finiteAS, evidence.MaxHardBytes)
				}
			}
		})
	}
}

func TestAddressSpaceOriginalSourceAndEnvironment(t *testing.T) {
	_, _, b, _ := taskFixture(t)
	for _, tc := range []struct {
		name         string
		limit, tasks control.Uint64
		ok           bool
	}{
		{"legacy", 0, 0, true}, {"finite", finiteAS, 64, true}, {"no-tasks", finiteAS, 0, false}, {"unaligned", finiteAS + 1, 64, false}, {"overflow", control.Uint64(math.MaxInt64) + 1, 64, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := b.Limits
			l.AddressSpaceMaxBytes = tc.limit
			l.PidsMaxTasks = tc.tasks
			require.Equal(t, tc.ok, l.Validate() == nil)
		})
	}
	original := []string{"A=1", addressSpaceEnvironmentKey + "=1", "B=2", addressSpaceEnvironmentKey, addressSpaceEnvironmentKey + "=999"}
	require.Equal(t, []string{"A=1", "B=2"}, addressSpaceEnvironment(original, 0))
	require.Equal(t, []string{"A=1", "B=2", addressSpaceEnvironmentKey + "=134217728"}, addressSpaceEnvironment(original, finiteAS))
	require.Len(t, original, 5)
	raw, err := json.Marshal(b.Limits)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "address_space")
	for _, raw := range []string{`{}`, `{"processes":"0","max_hard_bytes":"0","max_virtual_bytes":"0"}`, `{"processes":"0","max_hard_bytes":"0","max_virtual_bytes":"0","irreversible":null}`, `{"processes":"0","max_hard_bytes":"0","max_virtual_bytes":"0","irreversible":true,"extra":0}`, `{"processes":"0","processes":"0","max_hard_bytes":"0","max_virtual_bytes":"0","irreversible":true}`} {
		var e AddressSpaceEvidence
		require.Error(t, json.Unmarshal([]byte(raw), &e))
	}
}

// 内核是明确的peer；WAL/文件锁/fsync/重建/只读与写前拒绝均执行真实Mac路径。
func addressSpaceFixture(t *testing.T) (*Guard, *fixtureKernel, BootstrapCommand, Command) {
	t.Helper()
	g, k, b, c := taskFixture(t)
	b.Limits.AddressSpaceMaxBytes = finiteAS
	c.AddressSpaceMaxBytes = finiteAS
	k.e.AddressSpace = &AddressSpaceEvidence{Processes: 0, MaxHardBytes: 0, MaxVirtualBytes: 0, Irreversible: true}
	return g, k, b, c
}

func startAddressSpaceFixture(t *testing.T) (*Guard, *fixtureKernel, Command) {
	t.Helper()
	g, k, b, c := addressSpaceFixture(t)
	ctx := context.Background()
	_, err := g.Bootstrap(ctx, b)
	require.NoError(t, err)
	p := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks, AddressSpaceMaxBytes: finiteAS}
	require.NoError(t, g.launchWithBirth(ctx, nil, &p, func() error { k.process = &p; return nil }))
	k.e.CgroupPopulated = true
	k.e.TaskLimit.CurrentTasks = 4
	k.e.AddressSpace = &AddressSpaceEvidence{Processes: 4, MaxHardBytes: finiteAS, MaxVirtualBytes: 64 << 20, Irreversible: true}
	return g, k, c
}

func assertAddressSpaceUnchanged(t *testing.T, g *Guard, k *fixtureKernel, action func()) {
	t.Helper()
	path := filepath.Join(g.config.StateDir, "state.json")
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	writes := k.writes
	action()
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, writes, k.writes)
}

func TestAddressSpaceFreezeBirthAndRuntimeBeforeWAL(t *testing.T) {
	g, k, b, c := addressSpaceFixture(t)
	ctx := context.Background()
	_, err := g.Bootstrap(ctx, b)
	require.NoError(t, err)
	p := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks, AddressSpaceMaxBytes: 0}
	assertAddressSpaceUnchanged(t, g, k, func() {
		require.Error(t, g.launchWithBirth(ctx, nil, &p, func() error { t.Fatal("changed birth reached exec"); return nil }))
	})
	g, k, c = startAddressSpaceFixture(t)
	for _, n := range []control.Uint64{0, finiteAS - (64 << 10), finiteAS + (64 << 10)} {
		assertAddressSpaceUnchanged(t, g, k, func() { next := c; next.AddressSpaceMaxBytes = n; _, err := g.Apply(ctx, next); require.Error(t, err) })
	}
	_, err = g.Apply(ctx, c)
	require.NoError(t, err)
	// 重建保留完整出生和来源；同值读取不重写，owner变动不改变原AS。
	g = &Guard{config: g.config, kernel: k}
	view, err := g.InspectState(ctx)
	require.NoError(t, err)
	held, err := view.ConfirmedLimits()
	require.NoError(t, err)
	require.Equal(t, finiteAS, held.AddressSpaceMaxBytes)
	_, err = g.Fence(ctx, 2)
	require.NoError(t, err)
	next := c
	next.IssuerEpoch = 2
	next.Revision++
	_, err = g.Apply(ctx, next)
	require.NoError(t, err)
	next.Revision++
	next.MemoryMaxBytes--
	_, err = g.Apply(ctx, next)
	require.ErrorIs(t, err, ErrUnsafeNormalDecrease)
}

func TestAddressSpaceDriftBlocksWriteReadbackAndRepair(t *testing.T) {
	for _, mode := range []string{"missing", "reversible", "hard-drift", "existing-va", "empty-inconsistent", "too-many", "no-task", "populated"} {
		t.Run(mode, func(t *testing.T) {
			g, k, c := startAddressSpaceFixture(t)
			ctx := context.Background()
			_, err := g.Apply(ctx, c)
			require.NoError(t, err)
			view, err := g.InspectState(ctx)
			require.NoError(t, err)
			digest, err := HeldSourceSHA256(view)
			require.NoError(t, err)
			switch mode {
			case "missing":
				k.e.AddressSpace = nil
			case "reversible":
				k.e.AddressSpace.Irreversible = false
			case "hard-drift":
				k.e.AddressSpace.MaxHardBytes++
			case "existing-va":
				k.e.AddressSpace.MaxVirtualBytes = finiteAS + 1
			case "empty-inconsistent":
				k.e.AddressSpace.Processes = 0
			case "too-many":
				k.e.AddressSpace.Processes = 65
			case "no-task":
				k.e.TaskLimit = nil
			case "populated":
				k.e.CgroupPopulated = false
			}
			assertAddressSpaceUnchanged(t, g, k, func() {
				next := c
				next.Revision++
				_, err := g.Apply(ctx, next)
				require.Error(t, err)
				_, err = g.Observe(ctx, c)
				require.Error(t, err)
				_, err = g.RepairHeld(ctx, HeldRepairRequest{ProtocolVersion: 1, Identity: c.Identity, IssuerEpoch: 1, SourceSHA256: digest})
				require.Error(t, err)
			})
		})
	}
}

func TestAddressSpaceBootstrapUnknownRetainsOriginalAndLegacyCannotUpgrade(t *testing.T) {
	g, k, b, _ := addressSpaceFixture(t)
	ctx := context.Background()
	k.fail = true
	_, err := g.Bootstrap(ctx, b)
	require.ErrorIs(t, err, ErrUnknown)
	k.fail = false
	_, err = g.Bootstrap(ctx, b)
	require.ErrorIs(t, err, ErrUnknown)
	require.Equal(t, 1, k.writes)
	changed := b
	changed.Limits.AddressSpaceMaxBytes = 0
	_, err = g.ReplayBootstrap(ctx, changed)
	require.Error(t, err)
	require.Equal(t, 1, k.writes)
	_, err = g.ReplayBootstrap(ctx, b)
	require.NoError(t, err)
	require.Equal(t, 2, k.writes)
	// 模拟遗留无AS的原创建，不能从运行命令补字段升级为已受限PG。
	var c Command
	g, k, b, c = taskFixture(t)
	_, err = g.Bootstrap(ctx, b)
	require.NoError(t, err)
	p := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks, AddressSpaceMaxBytes: 0}
	require.NoError(t, g.launchWithBirth(ctx, nil, &p, func() error { k.process = &p; return nil }))
	c.AddressSpaceMaxBytes = finiteAS
	assertAddressSpaceUnchanged(t, g, k, func() { _, err := g.Apply(ctx, c); require.Error(t, err) })
}

func TestAddressSpaceCancellationAndUnknownOriginalKeepExactSource(t *testing.T) {
	for _, mode := range []string{"cancel", "changed-cancel", "drift-cancel", "unknown-readback"} {
		t.Run(mode, func(t *testing.T) {
			g, k, c := startAddressSpaceFixture(t)
			ctx := context.Background()
			if mode == "unknown-readback" {
				k.fail = true
				_, err := g.Apply(ctx, c)
				require.ErrorIs(t, err, ErrUnknown)
				writes := k.writes
				k.fail = false
				k.e.SwapMaxBytes = 0
				k.e.HugeOvercommitDisabled = true
				k.e.WholePoolUpperBytes = 16 << 20
				k.e.HugePools = append([]HugePool(nil), c.HugePools...)
				k.e.AddressSpace.Irreversible = false
				_, err = g.Observe(ctx, c)
				require.ErrorIs(t, err, ErrUnknown)
				require.Equal(t, writes, k.writes)
				k.e.AddressSpace.Irreversible = true
				_, err = g.Observe(ctx, c)
				require.NoError(t, err)
				require.Equal(t, writes, k.writes)
				view, err := g.InspectState(ctx)
				require.NoError(t, err)
				require.Equal(t, finiteAS, view.Held.AddressSpaceMaxBytes)
				return
			}
			request := CancelRequest{ProtocolVersion: 1, IssuerEpoch: 1, Command: c}
			if mode == "changed-cancel" {
				request.Command.AddressSpaceMaxBytes = 0
			}
			if mode == "drift-cancel" {
				k.e.AddressSpace.Irreversible = false
			}
			if mode != "cancel" {
				assertAddressSpaceUnchanged(t, g, k, func() { _, err := g.CancelOriginal(ctx, request); require.Error(t, err) })
				return
			}
			writes := k.writes
			receipt, err := g.CancelOriginal(ctx, request)
			require.NoError(t, err)
			require.NoError(t, receipt.ValidateFor(c, 1))
			require.Equal(t, writes, k.writes)
			require.Equal(t, finiteAS, receipt.State.Bootstrap.Limits.AddressSpaceMaxBytes)
			g = &Guard{config: g.config, kernel: k}
			_, err = g.Apply(ctx, c)
			require.Error(t, err)
			next := c
			next.Revision++
			_, err = g.Apply(ctx, next)
			require.NoError(t, err)
			require.Equal(t, writes+1, k.writes)
		})
	}
}

func TestAddressSpaceBootstrapRejectsIncompleteFactsBeforeCreatingWAL(t *testing.T) {
	for _, mode := range []string{"missing", "reversible", "nonempty-va", "nonempty-processes"} {
		t.Run(mode, func(t *testing.T) {
			g, k, b, _ := addressSpaceFixture(t)
			switch mode {
			case "missing":
				k.e.AddressSpace = nil
			case "reversible":
				k.e.AddressSpace.Irreversible = false
			case "nonempty-va":
				k.e.AddressSpace.MaxVirtualBytes = 1
			case "nonempty-processes":
				k.e.AddressSpace.Processes = 1
			}
			_, err := g.Bootstrap(context.Background(), b)
			require.Error(t, err)
			require.Zero(t, k.writes)
			_, err = os.Stat(filepath.Join(g.config.StateDir, "state.json"))
			require.ErrorIs(t, err, os.ErrNotExist, "不能先记录一个凭据不完整的新来源")
		})
	}
}

func TestAddressSpaceDurableBirthOriginalHeldAndCancellationRejectFieldRemoval(t *testing.T) {
	for _, mode := range []string{"birth", "original", "held", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			g, k, c := startAddressSpaceFixture(t)
			ctx := context.Background()
			if mode == "cancelled" {
				_, err := g.CancelOriginal(ctx, CancelRequest{ProtocolVersion: 1, IssuerEpoch: 1, Command: c})
				require.NoError(t, err)
			} else {
				_, err := g.Apply(ctx, c)
				require.NoError(t, err)
			}
			path := filepath.Join(g.config.StateDir, "state.json")
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &fields))
			var source map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(fields[mode], &source))
			delete(source, "address_space_max_bytes")
			fields[mode], err = json.Marshal(source)
			require.NoError(t, err)
			raw, err = json.Marshal(fields)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, raw, 0o600))
			writes := k.writes
			restored := &Guard{config: g.config, kernel: k}
			_, err = restored.InspectState(ctx)
			require.Error(t, err)
			require.Equal(t, writes, k.writes)
		})
	}
}
