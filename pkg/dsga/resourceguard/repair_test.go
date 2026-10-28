package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 此适配器仅模拟内核动作，另核真实Mac fsync后的原持有日志。沿用旧
// Bootstrap/Apply夹具的Unknown前置条件会错误拒绝本次“同一已承诺上界
// 修复”，所以不放宽旧夹具，而为本入口单独验证明确的持久Held条件。
type heldRepairKernel struct{ *fixtureKernel }

// 仅在真实Guard已经读出原记录、写前重读内核时破坏测试临时文件，
// 迫使后续真实rename/fsync失败；不在生产代码中增加故障注入开关。
type repairPersistenceKernel struct{ *heldRepairKernel }

func (k *repairPersistenceKernel) Read(c Command) (Evidence, error) {
	e, err := k.fixtureKernel.Read(c)
	if err != nil {
		return e, err
	}
	path := filepath.Join(k.stateDir, "state.json")
	if err := os.Remove(path); err != nil {
		return Evidence{}, err
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		return Evidence{}, err
	}
	return e, nil
}

func (k *heldRepairKernel) ApplyBounds(limits Limits) error {
	raw, err := os.ReadFile(filepath.Join(k.stateDir, "state.json"))
	if err != nil {
		return err
	}
	var r record
	if json.Unmarshal(raw, &r) != nil || r.Bootstrap == nil || !r.Bootstrap.Applied {
		return fmt.Errorf("repair fixture lacks durable confirmed bootstrap")
	}
	held := r.Bootstrap.Original.Limits
	if r.Original != nil {
		if !r.Applied || !reflect.DeepEqual(r.Original, r.Held) {
			return fmt.Errorf("repair fixture crossed an unknown original command")
		}
		held = r.Held.limits()
	}
	if !reflect.DeepEqual(held, limits) {
		return fmt.Errorf("repair effect preceded durable complete held upper")
	}
	return k.applyFixture(limits)
}

func heldRepairFixture(t *testing.T, runtime bool) (*Guard, *fixtureKernel, Command, HeldRepairRequest) {
	t.Helper()
	g, k, b, c := taskFixture(t)
	// 使用真实Bootstrap/出生/Apply路径建立原事实，不手造已完成记录。
	ctx := context.Background()
	_, err := g.Bootstrap(ctx, b)
	require.NoError(t, err)
	if runtime {
		p := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks}
		require.NoError(t, g.launchWithBirth(ctx, nil, &p, func() error { k.process = &p; return nil }))
		_, err = g.Apply(ctx, c)
		require.NoError(t, err)
	}
	g.kernel = &heldRepairKernel{fixtureKernel: k}
	view, err := g.InspectState(ctx)
	require.NoError(t, err)
	digest, err := HeldSourceSHA256(view)
	require.NoError(t, err)
	request := HeldRepairRequest{ProtocolVersion: 1, Identity: c.Identity, IssuerEpoch: 1, SourceSHA256: digest}
	return g, k, c, request
}

func TestHeldRepairRestoresOnlyOriginalUpperAndKeepsSourceAcrossOwner(t *testing.T) {
	for _, runtime := range []bool{false, true} {
		t.Run(fmt.Sprint(runtime), func(t *testing.T) {
			g, k, c, request := heldRepairFixture(t, runtime)
			ctx := context.Background()
			before, err := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
			require.NoError(t, err)
			writes := k.writes
			_, err = g.RepairHeld(ctx, request)
			require.NoError(t, err)
			require.Equal(t, writes, k.writes, "重复选择已恢复来源必须只读")
			k.e.MemoryMaxBytes = c.MemoryMaxBytes / 2
			k.e.HugePools[0].Pages /= 2
			k.e.WholePoolUpperBytes /= 2
			view, err := g.InspectState(ctx)
			require.NoError(t, err)
			_, err = view.ConfirmedLimits()
			require.ErrorIs(t, err, ErrUnknown)
			// 确认普通观察/重试不隐式修复已完成后的漂移。
			if runtime {
				_, err = g.Observe(ctx, c)
				require.Error(t, err)
				_, err = g.ReplayOriginal(ctx, c)
				require.Error(t, err)
			}
			require.Equal(t, writes, k.writes)
			evidence, err := g.RepairHeld(ctx, request)
			require.NoError(t, err)
			require.NoError(t, evidence.matches(c))
			require.Equal(t, writes+1, k.writes)
			after, err := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
			require.NoError(t, err)
			require.Equal(t, before, after, "修复不更换原命令/预算/出生或代次")
			_, err = g.Fence(ctx, 2)
			require.NoError(t, err)
			k.e.MemoryMaxBytes /= 2
			_, err = g.RepairHeld(ctx, request)
			require.Error(t, err, "旧floor不能修复，连幂等写也必须栅栏")
			request.IssuerEpoch = 2
			evidence, err = g.RepairHeld(ctx, request)
			require.NoError(t, err)
			require.NoError(t, evidence.matches(c))
			view, err = g.InspectState(ctx)
			require.NoError(t, err)
			digest, err := HeldSourceSHA256(view)
			require.NoError(t, err)
			require.Equal(t, request.SourceSHA256, digest)
			if runtime {
				require.Equal(t, c, *view.Original, "新owner不能改老命令的epoch/revision")
			}
		})
	}
}

func TestHeldRepairRejectsUnknownDriftAndChangedOriginalBeforeWrite(t *testing.T) {
	for _, what := range []string{"memory-decrease", "pool-decrease", "hstate", "swap-limit", "swap-current", "surplus", "overcommit", "occupancy", "guest-small", "task-drift", "tasks-exceeded", "pool-accounting", "foreign-guest", "foreign-process", "wrong-floor", "wrong-source", "unknown-next", "same-values-new-source", "missing-state"} {
		t.Run(what, func(t *testing.T) {
			g, k, c, request := heldRepairFixture(t, true)
			writes := k.writes
			k.e.MemoryMaxBytes = c.MemoryMaxBytes / 2
			switch what {
			case "memory-decrease":
				k.e.MemoryMaxBytes = c.MemoryMaxBytes + 4096
			case "pool-decrease":
				k.e.HugePools[0].Pages++
			case "hstate":
				k.e.HugePools[0].PageSizeKB++
			case "swap-limit":
				k.e.SwapMaxBytes = 4096
			case "swap-current":
				k.e.SwapCurrentBytes = 4096
			case "surplus":
				k.e.HugeSurplusPages = 1
			case "overcommit":
				k.e.HugeOvercommitDisabled = false
			case "occupancy":
				k.e.MemoryCurrentBytes = c.MemoryMaxBytes + 1
			case "guest-small":
				k.e.GuestMemTotalBytes = 1
			case "task-drift":
				k.e.TaskLimit.MaxTasks++
			case "tasks-exceeded":
				k.e.TaskLimit.CurrentTasks = c.PidsMaxTasks + 1
			case "pool-accounting":
				k.e.WholePoolUpperBytes++
			case "foreign-guest":
				k.id.GuestBootID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
			case "foreign-process":
				k.process.PostmasterStartTicks++
			case "wrong-floor":
				request.IssuerEpoch++
			case "wrong-source":
				request.SourceSHA256 = strings.Repeat("a", 64)
			case "unknown-next", "same-values-new-source":
				s, err := lockState(context.Background(), g.config.StateDir)
				require.NoError(t, err)
				r, err := s.load()
				require.NoError(t, err)
				next := cloneCommand(c)
				next.Revision++
				r.Original, r.Applied = &next, false
				if what == "same-values-new-source" {
					r.Held, r.Applied = &next, true
				}
				require.NoError(t, s.save(r))
				s.close()
			case "missing-state":
				require.NoError(t, os.Remove(filepath.Join(g.config.StateDir, "state.json")))
			}
			_, err := g.RepairHeld(context.Background(), request)
			require.Error(t, err)
			require.Equal(t, writes, k.writes, "拒绝必须发生在任何内核写之前")
		})
	}
}

func TestHeldRepairPartialFailureRetainsSameUpperAndRestartsExplicitly(t *testing.T) {
	g, k, c, request := heldRepairFixture(t, true)
	k.e.MemoryMaxBytes /= 2
	k.e.HugePools[0].Pages /= 2
	k.e.WholePoolUpperBytes /= 2
	k.fail = true
	_, err := g.RepairHeld(context.Background(), request)
	require.ErrorIs(t, err, ErrUnknown)
	// 夹具第一项memory写完成后失败，pool仍未恢复；重建真实Guard读同一
	// Mac文件，不能仅凭原Applied=true或目标memory文件就宣告修复成功。
	g = &Guard{config: g.config, kernel: &heldRepairKernel{fixtureKernel: k}}
	view, err := g.InspectState(context.Background())
	require.NoError(t, err)
	require.Equal(t, c, *view.Held)
	_, err = view.ConfirmedLimits()
	require.ErrorIs(t, err, ErrUnknown)
	writes := k.writes
	_, err = g.Observe(context.Background(), c)
	require.Error(t, err)
	require.Equal(t, writes, k.writes)
	k.fail = false
	evidence, err := g.RepairHeld(context.Background(), request)
	require.NoError(t, err)
	require.NoError(t, evidence.matches(c))
	require.Equal(t, writes+1, k.writes)
}

func TestHeldRepairBootstrapCannotAdoptUnrecordedProcess(t *testing.T) {
	for _, populated := range []bool{false, true} {
		g, k, _, request := heldRepairFixture(t, false)
		k.e.MemoryMaxBytes /= 2
		k.e.CgroupPopulated = populated
		if !populated {
			k.e.TaskLimit.CurrentTasks = 1 // populated=false不证明无僵尸任务。
		}
		writes := k.writes
		_, err := g.RepairHeld(context.Background(), request)
		require.Error(t, err)
		require.Equal(t, writes, k.writes)
	}
}

func TestHeldRepairPersistenceFailureCannotReachKernelWrite(t *testing.T) {
	g, k, _, request := heldRepairFixture(t, true)
	k.e.MemoryMaxBytes /= 2
	g.kernel = &repairPersistenceKernel{heldRepairKernel: &heldRepairKernel{fixtureKernel: k}}
	writes := k.writes
	_, err := g.RepairHeld(context.Background(), request)
	require.ErrorIs(t, err, ErrUnknown)
	require.ErrorContains(t, err, "held repair persistence")
	require.Equal(t, writes, k.writes)
	_, err = g.InspectState(context.Background())
	require.Error(t, err, "损坏原来源不能由普通观察重建")
}

func TestHeldRepairKeepsFileLockAcrossKernelAndOwnerFence(t *testing.T) {
	g, k, _, request := heldRepairFixture(t, true)
	k.e.MemoryMaxBytes /= 2
	k.entered, k.release = make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := g.RepairHeld(ctx, request); done <- err }()
	select {
	case <-k.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("repair never reached kernel")
	}
	cancel()
	second := &Guard{config: g.config, kernel: g.kernel}
	blocked, stop := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer stop()
	_, err := second.Fence(blocked, 2)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(k.release)
	require.NoError(t, <-done)
	_, err = second.Fence(context.Background(), 2)
	require.NoError(t, err)
	_, err = g.RepairHeld(context.Background(), request)
	require.Error(t, err)
}

func TestHeldRepairNeverRepairsImmutableSystemPlacementOrLimits(t *testing.T) {
	for _, what := range []string{"system-max", "system-current", "system-swap", "pid1", "root-userspace"} {
		t.Run(what, func(t *testing.T) {
			g, k, b, c := systemFixture(t)
			ctx := context.Background()
			_, err := g.Bootstrap(ctx, b)
			require.NoError(t, err)
			_, err = g.placeSystem(ctx, fixturePlacement(k))
			require.NoError(t, err)
			p := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks}
			require.NoError(t, g.launchWithBirth(ctx, nil, &p, func() error { k.process = &p; return nil }))
			_, err = g.Apply(ctx, c)
			require.NoError(t, err)
			g.kernel = &heldRepairKernel{fixtureKernel: k}
			view, err := g.InspectState(ctx)
			require.NoError(t, err)
			digest, err := HeldSourceSHA256(view)
			require.NoError(t, err)
			request := HeldRepairRequest{ProtocolVersion: 1, Identity: c.Identity, IssuerEpoch: 1, SourceSHA256: digest}
			k.e.MemoryMaxBytes /= 2
			switch what {
			case "system-max":
				k.e.System.MemoryMaxBytes--
			case "system-current":
				k.e.System.MemoryCurrentBytes = c.SystemMemoryMaxBytes + 1
			case "system-swap":
				k.e.System.SwapMaxBytes = 4096
			case "pid1":
				k.e.System.PID1Member = false
			case "root-userspace":
				k.e.System.RootUserspaceCount++
			}
			writes := k.writes
			_, err = g.RepairHeld(ctx, request)
			require.Error(t, err)
			require.Equal(t, writes, k.writes)
		})
	}
}
