package resourceguard

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// 取消水位属于Guest资源域，不能在同Guest下一次PG出生时丢失，也不能
// 反过来强制当前出生永远等于已取消命令的旧PID。真实文件锁/WAL执行；
// 空组、exec和内核约束为夹具，不据此认证外层跨boot恢复或Linux进程退出。
func TestCancellationBarrierSurvivesLaterPGBirth(t *testing.T) {
	for _, mode := range []string{"bootstrap", "held", "pid-reused"} {
		t.Run(mode, func(t *testing.T) {
			g, k, cancelled := cancelGuardFixture(t)
			ctx := context.Background()
			if mode == "held" {
				_, err := g.Apply(ctx, cancelled)
				require.NoError(t, err)
				cancelled.Revision++
				cancelled.MemoryMaxBytes = 80 << 20
			}
			_, err := g.CancelOriginal(ctx, CancelRequest{ProtocolVersion: 1, IssuerEpoch: cancelled.IssuerEpoch, Command: cancelled})
			require.NoError(t, err)
			before, err := g.InspectState(ctx)
			require.NoError(t, err)
			k.e.CgroupPopulated = false
			k.process = nil
			nextBirth := *before.Birth
			nextBirth.PostmasterStartTicks++
			if mode != "pid-reused" {
				nextBirth.PostmasterPID++
			}
			require.NoError(t, g.launchWithBirth(ctx, nil, &nextBirth, func() error {
				k.process = &nextBirth
				return nil
			}))
			// 重建Guard后重新读盘；不能只断言exec回调成功而漏掉新日志失效。
			g = &Guard{config: g.config, kernel: k}
			after, err := g.InspectState(ctx)
			require.NoError(t, err, "合法新出生不能使原取消屏障所在日志失效")
			require.Equal(t, &nextBirth, after.Birth)
			require.Equal(t, before.Cancelled, after.Cancelled)
			require.Equal(t, before.Held, after.Held)
			require.Equal(t, before.Original, after.Original)
			_, err = after.ConfirmedLimits()
			require.NoError(t, err)
			writes := k.writes
			_, err = g.Apply(ctx, cancelled)
			require.Error(t, err, "旧取消命令不能借新PG出生重新获得执行权")
			_, err = g.ReplayOriginal(ctx, cancelled)
			require.Error(t, err)
			_, err = g.CancelOriginal(ctx, CancelRequest{ProtocolVersion: 1, IssuerEpoch: cancelled.IssuerEpoch, Command: cancelled})
			require.Error(t, err, "新出生不能补出旧出生取消的当前回执")
			stale := Cancellation{ProtocolVersion: 1, VMShrinkPermit: false, Command: cancelled, State: after}
			require.Error(t, stale.ValidateFor(cancelled, cancelled.IssuerEpoch))
			require.Equal(t, writes, k.writes)
			next := cloneCommand(cancelled)
			next.Revision, err = after.NextRevision()
			require.NoError(t, err)
			require.Equal(t, cancelled.Revision+1, next.Revision)
			next.PostmasterPID, next.PostmasterStartTicks = nextBirth.PostmasterPID, nextBirth.PostmasterStartTicks
			_, err = g.Apply(ctx, next)
			require.NoError(t, err, "新出生的合法后继命令按持久水位继续")
			require.Equal(t, writes+1, k.writes)
			after, err = g.InspectState(ctx)
			require.NoError(t, err)
			require.Equal(t, &next, after.Held)
			require.Equal(t, &cancelled, after.Cancelled)
		})
	}
}
