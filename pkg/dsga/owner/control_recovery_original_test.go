package owner

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 本组直达生产Agent使用的Root启动恢复循环，覆盖Root已经设置、但自身完成
// WAL尚未确认的窗口。Session/SDK/外层UID/RV写前日志与恢复循环均为正式实现；
// PG/Root内核为协议peer，beginGuest只构造明确标识的前驱领域，不是整机E2E。
func TestResourceRecoveryObservesOnlyRetainedGuestReadyBeforePG(t *testing.T) {
	for _, mode := range []string{"complete", "root-original-missing", "partial-effect"} {
		t.Run(mode, func(t *testing.T) {
			api, store, coord := newAPI(t)
			root, config := guestRoot(t, store)
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				resources, err := session.NewResourceWorker(ctx, config)
				require.NoError(t, err)
				defer resources.CloseIdleConnections()
				req := pgRequest(t, session.Actor())
				prepared, shared := preparedGuest(t, req, root.view.Identity)
				pg := pgConfig(t, req, func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, "/dsg/v1/budget/shared", r.URL.Path)
					pgReply(w, shared)
				})
				worker, err := session.NewPGWorker(ctx, pg, resources)
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				api.mu.Lock()
				api.vm.Status.MemorySize = resource.NewQuantity(int64(req.TargetVMMemoryBytes), resource.BinarySI)
				api.mu.Unlock()
				root.mu.Lock()
				root.loseReply = true
				root.afterApply = func() {
					// 效果先发生，完成日志未确认；保持原完整命令以供Observe。
					root.view.Applied, root.view.Held = false, nil
					switch mode {
					case "root-original-missing":
						root.view.Original = nil
					case "partial-effect":
						root.view.Actual.MemoryMaxBytes -= 4096
					}
				}
				root.mu.Unlock()
				beginGuest(t, store, req, prepared)
				_, err = worker.Advance(ctx)
				var lost *resourceguard.ClientError
				require.ErrorAs(t, err, &lost)
				require.True(t, lost.MayHaveExecuted)
				require.Equal(t, journal.Unconfirmed, guestPhase(t, store).Phase)
				before, err := store.Load(ctx)
				require.NoError(t, err)
				restored, err := session.NewResourceRecoveryWorker(ctx, config)
				require.NoError(t, err)
				defer restored.CloseIdleConnections()
				require.NotNil(t, restored.client.Pending(), "必须从原外层WAL重建Pending")
				original := *restored.client.Pending()
				runCtx, stop := context.WithCancel(ctx)
				defer stop()
				if mode != "complete" {
					root.mu.Lock()
					root.afterObserve = func() {
						// 第一轮响应必须完整返回给正式SDK，再于第二轮停住。
						// 不能只靠取消首个HTTP制造“未知未被清理”的假通过。
						if root.observes == 2 {
							stop()
						}
					}
					root.mu.Unlock()
				}
				err = restored.NewRecoveryControlEndpoint().RunResourceRecovery(runCtx, 5*time.Millisecond)
				if mode == "complete" {
					require.NoError(t, err, "Root原Observe应解除PG建立前的恢复环依赖")
					require.Nil(t, restored.client.Pending())
				} else {
					require.ErrorIs(t, err, context.Canceled)
					require.Equal(t, &original, restored.client.Pending(), "缺来源或部分效果不能清未知")
				}
				after, err := store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, before.State(), after.State(), "启动查询不得结清外层步骤或预算")
				root.mu.Lock()
				require.Equal(t, 1, root.posts, "启动恢复不得补发setter")
				wantObserves := 2
				if mode == "complete" {
					wantObserves = 1
				}
				require.Equal(t, wantObserves, root.observes)
				require.Equal(t, mode == "complete", root.view.Applied)
				root.mu.Unlock()
				if mode == "complete" {
					// 交给正式PG执行器后重新核VM/PG/预算，并只读同一原命令。
					// 启动期Root自身确认不能替代GuestReady完整业务完成证据。
					next, err := session.NewPGWorker(ctx, pg, restored)
					require.NoError(t, err)
					defer next.CloseIdleConnections()
					progress, err := next.Advance(ctx)
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
					settled := guestPhase(t, store)
					require.Equal(t, journal.Settled, settled.Phase)
					require.Equal(t, journal.Completed, settled.Observation.Result)
					require.Equal(t, before.State().Active.Steps[settled.Ordinal-1].Command, settled.Command)
					root.mu.Lock()
					require.Equal(t, 1, root.posts)
					require.Equal(t, 2, root.observes)
					root.mu.Unlock()
				}
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestResourceRecoveryRejectsSDKPendingWithoutOuterDispatch(t *testing.T) {
	_, store, coord := newAPI(t)
	root, config := guestRoot(t, store)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceRecoveryWorker(ctx, config)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		// Root当前约束完全确认仍不能越过外层缺源；RetainOriginal只固定本地
		// SDK未知，并不自行构成派发权限。正式恢复须在任何Observe前拒绝。
		command := resourceguard.Command{ProtocolVersion: 1, Identity: root.view.Identity, IssuerEpoch: session.Actor().Epoch, Revision: 1, BudgetCommitmentID: "orphan-local", PostmasterPID: 123, PostmasterStartTicks: 456, MemoryMaxBytes: root.view.Bootstrap.Limits.MemoryMaxBytes, PidsMaxTasks: 0, SystemMemoryMaxBytes: 0, OutsideUpperBytes: root.view.Bootstrap.Limits.OutsideUpperBytes, HugePools: root.view.Bootstrap.Limits.HugePools}
		require.NoError(t, resources.client.RetainOriginal(command))
		before, err := store.Load(ctx)
		require.NoError(t, err)
		_, err = resources.PrepareRetainedPGRecovery(ctx)
		require.ErrorContains(t, err, "lacks complete original PG recovery binding", "不能为绕过gate收养孤立Pending")
		err = resources.NewRecoveryControlEndpoint().RunResourceRecovery(ctx, time.Millisecond)
		require.ErrorContains(t, err, "no matching original GuestReady dispatch")
		require.Equal(t, &command, resources.client.Pending())
		after, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), after.State())
		root.mu.Lock()
		require.Zero(t, root.posts)
		require.Zero(t, root.observes)
		root.mu.Unlock()
		return nil
	})
	require.NoError(t, err)
}

func TestRetainedPGRecoveryCannotRaceAnExistingExecutionLoop(t *testing.T) {
	_, store, coord := newAPI(t)
	_, config := guestRoot(t, store)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceRecoveryWorker(ctx, config)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		pending, err := resources.PrepareRetainedPGRecovery(ctx)
		require.NoError(t, err)
		require.False(t, pending, "初始原约束仍由只Root gate核验")
		// 与Run持有同一原子槽；即使没有Pending，启动选择也不能与现有
		// PG/资源循环并行调用SDK。只核排他门禁，不假造外层回执或效果。
		resources.running.Store(true)
		defer resources.running.Store(false)
		_, err = resources.PrepareRetainedPGRecovery(ctx)
		require.ErrorContains(t, err, "already running")
		return nil
	})
	require.NoError(t, err)
}
