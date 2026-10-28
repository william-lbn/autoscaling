package resourceguard

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProductionNormalDecreaseRejectsBeforeIntentAndKernelWrite(t *testing.T) {
	for _, scenario := range []string{"bootstrap", "runtime-held", "actual-drift"} {
		t.Run(scenario, func(t *testing.T) {
			g, k, b, command := freshBootstrapFixture(t)
			ctx := context.Background()
			_, err := g.Bootstrap(ctx, b)
			require.NoError(t, err)
			process := ProcessIdentity{ProtocolVersion: 1, Identity: command.Identity, PostmasterPID: command.PostmasterPID, PostmasterStartTicks: command.PostmasterStartTicks}
			require.NoError(t, g.launchWithBirth(ctx, nil, &process, func() error { k.process = &process; return nil }))
			switch scenario {
			case "bootstrap":
				command.MemoryMaxBytes = 48 << 20
			case "runtime-held":
				command.MemoryMaxBytes = 80 << 20
				_, err := g.Apply(ctx, command)
				require.NoError(t, err)
				command.Revision++
				command.MemoryMaxBytes = 64 << 20
			case "actual-drift":
				// 目标虽高于原64MiB，仍低于实际96MiB；最终写者必须独立拒绝。
				k.e.MemoryMaxBytes = 96 << 20
				command.MemoryMaxBytes = 80 << 20
			}
			k.e.MemoryCurrentBytes = 1 // 低占用不能成为未来分配/后台活性的证明。
			path := filepath.Join(g.config.StateDir, "state.json")
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			writes := k.writes
			_, err = g.Apply(ctx, command)
			require.ErrorIs(t, err, ErrUnsafeNormalDecrease)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after, "拒绝前不能替换原Held或产生新的未知意图")
			require.Equal(t, writes, k.writes, "拒绝前不能部分写入任何资源维度")
		})
	}
}

func TestLegacyPendingNormalDecreaseRemainsObservableWithoutReplay(t *testing.T) {
	g, k, b, command := freshBootstrapFixture(t)
	ctx := context.Background()
	_, err := g.Bootstrap(ctx, b)
	require.NoError(t, err)
	process := ProcessIdentity{ProtocolVersion: 1, Identity: command.Identity, PostmasterPID: command.PostmasterPID, PostmasterStartTicks: command.PostmasterStartTicks}
	require.NoError(t, g.launchWithBirth(ctx, nil, &process, func() error { k.process = &process; return nil }))
	_, err = g.Apply(ctx, command)
	require.NoError(t, err)
	// 仅构造旧版本已经持久的未确认降限记录，模拟升级前效果及完成落账丢失；
	// 生产没有编辑/重置WAL入口。升级只能保留、只读核实事实，不能重放不安全写。
	legacy := cloneCommand(command)
	legacy.Revision++
	legacy.MemoryMaxBytes = 48 << 20
	state, err := lockState(ctx, g.config.StateDir)
	require.NoError(t, err)
	record, err := state.load()
	require.NoError(t, err)
	record.Original, record.Applied = &legacy, false
	require.NoError(t, state.save(record))
	state.close()
	k.e.MemoryMaxBytes = legacy.MemoryMaxBytes
	writes := k.writes
	path := filepath.Join(g.config.StateDir, "state.json")
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	_, err = g.ReplayOriginal(ctx, legacy)
	require.ErrorIs(t, err, ErrUnsafeNormalDecrease)
	require.ErrorIs(t, err, ErrUnknown, "安全拒绝不把原未知命令改成NoEffect")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, err = g.Observe(ctx, legacy)
	require.NoError(t, err)
	view, err := g.InspectState(ctx)
	require.NoError(t, err)
	require.True(t, view.Applied)
	require.Equal(t, &legacy, view.Held)
	_, err = g.Apply(ctx, legacy)
	require.NoError(t, err, "已确认原命令幂等重试仍只读")
	require.Equal(t, writes, k.writes)
	// 后续安全增加按已经确认的当前Held继续，不能永久借旧Bootstrap抹掉历史。
	next := cloneCommand(legacy)
	next.Revision++
	next.MemoryMaxBytes++
	_, err = g.Apply(ctx, next)
	require.NoError(t, err)
	require.Equal(t, writes+1, k.writes)
}
