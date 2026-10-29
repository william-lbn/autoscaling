package owner

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 使用已有真实首次生产、来源Reader、Run管理及UID/RV CAS前驱。
// 新增窗口只验证本批改变的完成/读回/再关闭和后继Grow，不重复旧失败矩阵。
func TestProductionServiceLifecycleCompletionAndSuccessor(t *testing.T) {
	for _, mode := range []string{"normal", "lost-record", "source-unavailable", "first-unknown-grow", "repeated"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "service-lifecycle-"+mode) })
	}
}

func TestProductionServiceLifecycleRecoversUnknownThenAutomaticGrowShrink(t *testing.T) {
	productionInitialService(t, "service-lifecycle-first-unknown-automatic")
}

func TestProductionServiceLifecycleClearsExpiredRunOnlyAfterDurableCompletion(t *testing.T) {
	productionInitialService(t, "closure-transfer-rearm-complete-lifecycle")
}

// 前驱包括真实失租/更高owner接管、原关闭接续和Runner真实新floor发布。
// 本Run在原生过期时启动，在同Session内完成持久rearm→新开放→业务完成。
// 只恢复期限和原生开放后仍零续租，只有完整完成证据才解除本地等待。
func verifyExpiredServiceLifecycle(t *testing.T, ctx context.Context, api *ownerAPI, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture) {
	t.Helper()
	worker.CloseIdleConnections()
	worker, err := session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	endpoint, stop := reopenRun(t, ctx, worker)
	defer stop()
	state, err := endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
	require.NoError(t, err)
	_, _, _, selector := state.State.ServiceClosure.Selection()
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationRearmService, selector))
	require.NoError(t, err)
	pg.mu.Lock()
	require.False(t, pg.leaseExpired)
	require.Zero(t, pg.leaseRenewals)
	pg.reopenFixture = &serviceReopenPG{mode: "normal", api: api, root: root, posts: 0, closes: 0, queries: 0, renewals: 0, selectionFault: false, setterFault: false, closeFault: false, cancelAfterOpen: nil}
	pg.mu.Unlock()
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
	require.NoError(t, err)
	state, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
	require.NoError(t, err)
	require.Equal(t, journal.Settled, state.State.ServiceClosure.Reopen.Phase)
	pg.mu.Lock()
	require.Zero(t, pg.leaseRenewals)
	require.Zero(t, pg.reopenFixture.renewals)
	pg.mu.Unlock()
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, state.State.ServiceClosure.Reopen.CommandSHA256))
	require.Error(t, err, "完成CAS已应用、回复丢失，Run必须据持久证据重新审计")
	require.Eventually(t, func() bool {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		return pg.leaseRenewals > 0
	}, 30*time.Second, 20*time.Millisecond)
	state, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
	require.NoError(t, err)
	require.Nil(t, state.State.ServiceClosure)
	require.NotNil(t, state.State.ServiceLifecycle)
	pg.mu.Lock()
	require.Equal(t, 1, pg.rearmFixture.posts)
	require.Equal(t, 1, pg.reopenFixture.posts)
	require.Zero(t, pg.reopenFixture.closes)
	pg.mu.Unlock()
}

func verifyProductionServiceLifecycle(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture) *PGWorker {
	t.Helper()
	before, err := store.Load(ctx)
	require.NoError(t, err)
	original := before.State()
	r := original.ServiceClosure.Reopen
	require.Equal(t, journal.Settled, r.Phase)
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(worker.CloseIdleConnections)
	endpoint, stop := reopenRun(t, ctx, worker)
	command := closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, r.CommandSHA256)
	wrong := command.Clone()
	wrong.Selector = strings.Repeat("ab", 32)
	_, err = endpoint.Submit(ctx, wrong)
	require.ErrorIs(t, err, ErrServiceSafety)
	if mode == "lost-record" {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	if mode == "source-unavailable" {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
	}
	_, err = endpoint.Submit(ctx, command)
	if mode == "lost-record" || mode == "source-unavailable" {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}
	state, err := endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
	require.NoError(t, err)
	if mode == "source-unavailable" {
		require.Nil(t, state.State.ServiceLifecycle)
		require.NotNil(t, state.State.ServiceClosure.Reopen.Closing)
		require.False(t, state.State.ServiceClosure.Reopen.Closing.Close.Pending())
		require.Equal(t, original.InitialHeld, state.State.InitialHeld)
		stop()
		return worker
	}
	require.Nil(t, state.State.ServiceClosure)
	require.NotNil(t, state.State.ServiceLifecycle)
	cycle := control.Uint64(1)
	if original.ServiceLifecycle != nil {
		cycle = original.ServiceLifecycle.Cycle + 1
	}
	require.Equal(t, cycle, state.State.ServiceLifecycle.Cycle)
	require.Equal(t, original.InitialHeld.Admission, state.State.InitialHeld.Admission)
	if strings.HasPrefix(mode, "first-unknown-") {
		require.Equal(t, journal.Unconfirmed, state.State.InitialHeld.Admission.Phase)
		require.NotNil(t, state.State.InitialHeld.Covered)
	}
	// 完成丢回复由同一原摘要确认；已完成路径只能认证，不能发新setter、
	// 改写完成序列，或将旧首次开放重新恢复成SDK pending。
	sequence := state.State.ServiceLifecycle.RecordedSequence
	_, err = endpoint.Submit(ctx, command)
	require.NoError(t, err)
	state, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
	require.NoError(t, err)
	require.Equal(t, sequence, state.State.ServiceLifecycle.RecordedSequence)
	stop()
	pg.mu.Lock()
	require.Equal(t, 1, pg.reopenFixture.posts, "完成/重读不得额外开放")
	require.Zero(t, pg.reopenFixture.closes)
	pg.reopenFixture = nil
	pg.mu.Unlock()
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(worker.CloseIdleConnections)
	pending, closing := worker.client.PendingAdmission()
	require.Nil(t, pending)
	require.Nil(t, closing)
	_, err = worker.Advance(ctx)
	require.NoError(t, err, "完成后首次未知只能由覆盖证明放行，不能重查/重发旧开放")
	if mode == "repeated" {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
		_, err = worker.Advance(ctx)
		require.NoError(t, err, "已恢复服务再次来源失效，应保存独立当前关闭")
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
		current, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, "closing", current.State().ServiceLifecycle.SafetyStage)
		pg.mu.Lock()
		pg.reopenFixture = &serviceReopenPG{mode: "normal", api: api, root: root, posts: 0, closes: 0, queries: 0, renewals: 0, selectionFault: false, setterFault: false, closeFault: false, cancelAfterOpen: nil}
		pg.mu.Unlock()
		endpoint, stop := reopenRun(t, ctx, worker)
		_, _, _, selected := current.State().ServiceClosure.Selection()
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selected))
		require.NoError(t, err)
		stop()
		return verifyProductionServiceLifecycle(t, ctx, "second-cycle", api, store, session, resources, worker, config, core, source, pg, root)
	}
	return worker
}
