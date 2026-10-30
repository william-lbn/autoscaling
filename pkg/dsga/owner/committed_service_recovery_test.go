package owner

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 原Prepare/Host/VM/Guest/PG/Finalize均由生产执行器完成，再注入来源
// 异常和独立关闭。此处只恢复同一完整资源/预算，不补原开放Completed。
// PG/Root/Kube/QMP仍为明确协议peer，不替代原生或Linux物理返还验收。
func TestProductionCommittedServiceRecovery(t *testing.T) {
	for _, mode := range []string{"unreceived", "applied", "settled", "lost-complete", "complete-takeover"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "resume-safety-recover-"+mode) })
	}
	t.Run("planned", func(t *testing.T) { productionInitialService(t, "resume-safety-planned-recover-source-drift") })
}

func TestProductionCommittedServiceRecoveryThenAutomaticShrink(t *testing.T) {
	productionInitialService(t, "resume-safety-recover-automatic")
}

func TestProductionCommittedServiceRecoveryCrashWindows(t *testing.T) {
	for _, mode := range []string{"lost-start", "setter-before", "setter-after", "lost-record", "source-after-selection", "source-after-unknown-setter", "terminal-unavailable", "terminal-conflict"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "resume-safety-recover-"+mode) })
	}
}

func TestProductionCommittedServiceRecoveryExpiredLeaseAfterElectorTakeover(t *testing.T) {
	productionInitialService(t, "resume-safety-recover-expired")
}

func verifyProductionCommittedServiceRecovery(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, core *kubefake.Clientset, session *Session, resources *ResourceWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, transfer *businessOwnerTransfer, runner *vmguard.Runner) *PGWorker {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	original := s.State()
	recoveryMode := strings.TrimPrefix(mode, "resume-safety-recover-")
	require.NotNil(t, original.Active)
	pendingFinalization := original.ServiceClosure.Reason == "finalize_source_unconfirmed"
	expectedOutcome := journal.RecoveredAfterResourceCommit
	if original.ServiceClosure.Reason == "cleanup_source_unconfirmed" {
		expectedOutcome = journal.RecoveredAfterCancellation
	}
	if pendingFinalization {
		require.Nil(t, original.BudgetHeld)
		require.NotNil(t, original.InitialHeld)
	} else {
		require.NotNil(t, original.BudgetHeld)
	}
	require.False(t, original.ServiceClosure.Pending())
	worker, err := session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(worker.CloseIdleConnections)
	pg.mu.Lock()
	pg.reopenFixture = &serviceReopenPG{mode: "normal", api: api, root: root, posts: 0, closes: 0, queries: 0, renewals: 0, selectionFault: false, setterFault: false, closeFault: false, cancelAfterOpen: nil}
	pg.mu.Unlock()
	endpoint, stop := reopenRun(t, ctx, worker)
	_, _, _, selector := original.ServiceClosure.Selection()
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
	require.ErrorIs(t, err, ErrServiceSafety)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, original.Active, s.State().Active)
	require.Nil(t, s.State().ServiceClosure.Reopen, "来源坏时不能选择新开放")
	if strings.HasPrefix(recoveryMode, "terminal-") {
		// 原WAL已保存完整PG终态，当前PG历史仍须独立确认。缺源或
		// 矛盾只保留SDK原栅栏，不能按当前容量猜测原成功或补造NoEffect。
		pg.mu.Lock()
		saved := pg.resizeReceipt
		if recoveryMode == "terminal-unavailable" {
			pg.rejectResizeQuery = true
		} else {
			drift := *saved
			result := *drift.Receipt.Result
			result.HighBuffers--
			drift.Receipt.Result = &result
			pg.resizeReceipt = &drift
		}
		pg.mu.Unlock()
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
		require.ErrorIs(t, err, ErrServiceSafety)
		unchanged, loadErr := store.Load(ctx)
		require.NoError(t, loadErr)
		if pendingFinalization {
			require.Equal(t, original.Active, unchanged.State().Active)
			require.Equal(t, original.InitialHeld, unchanged.State().InitialHeld)
			require.Nil(t, unchanged.State().BudgetHeld)
			require.NotNil(t, unchanged.State().ServiceClosure.Finalization)
			require.Nil(t, unchanged.State().ServiceClosure.Reopen)
		} else {
			require.Equal(t, original, unchanged.State())
		}
		require.NotNil(t, worker.client.Pending())
		pg.mu.Lock()
		require.Zero(t, pg.reopenFixture.posts)
		pg.resizeReceipt, pg.rejectResizeQuery = saved, false
		pg.mu.Unlock()
	}
	pg.mu.Lock()
	pg.reopenFixture.mode = recoveryMode
	if recoveryMode == "expired" {
		pg.reopenFixture.mode = "source-after-unknown-setter"
	}
	pg.mu.Unlock()
	if recoveryMode == "lost-start" || recoveryMode == "lost-proof" {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
	fault := recoveryMode == "lost-proof" || recoveryMode == "expired" || recoveryMode == "lost-start" || recoveryMode == "setter-before" || recoveryMode == "setter-after" || recoveryMode == "lost-record" || recoveryMode == "source-after-selection" || recoveryMode == "source-after-unknown-setter"
	if fault {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}
	stop()
	selected, err := store.Load(ctx)
	require.NoError(t, err)
	if recoveryMode == "lost-proof" {
		require.True(t, pendingFinalization)
		require.Nil(t, selected.State().ServiceClosure.Reopen, "独立结算CAS丢回复不得派发新开放")
		require.NotNil(t, selected.State().ServiceClosure.Finalization)
		require.Equal(t, original.Active, selected.State().Active)
		require.Nil(t, selected.State().BudgetHeld)
		proof := selected.State().ServiceClosure.Finalization
		worker.CloseIdleConnections()
		worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
		require.NoError(t, err)
		t.Cleanup(worker.CloseIdleConnections)
		endpoint, stop = reopenRun(t, ctx, worker)
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
		require.NoError(t, err)
		stop()
		selected, err = store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, proof, selected.State().ServiceClosure.Finalization, "重建只能沿用完整原结算Proof")
	}
	r := selected.State().ServiceClosure.Reopen
	require.NotNil(t, r)
	require.Equal(t, original.Active, selected.State().Active)
	if recoveryMode == "expired" {
		_, _, worker = verifyProductionReopenLeaseRecovery(t, ctx, "normal", transfer, session, resources, worker, config, core, source, pg, root, runner)
		done, loadErr := store.Load(ctx)
		require.NoError(t, loadErr)
		require.Nil(t, done.State().Active)
		if pendingFinalization {
			require.Nil(t, done.State().InitialHeld)
			require.NotNil(t, done.State().BudgetHeld.Recovery)
			require.Equal(t, *original.Active, done.State().BudgetHeld.Recovery.Original)
		} else {
			require.Equal(t, original.BudgetHeld, done.State().BudgetHeld)
		}
		record := done.State().History[len(done.State().History)-1]
		require.Equal(t, expectedOutcome, record.Outcome)
		require.Equal(t, *original.Active, record.Operation)
		require.NoError(t, done.State().CheckClosedBudget(original.Active.Request))
		return worker
	}
	if recoveryMode == "source-after-selection" || recoveryMode == "source-after-unknown-setter" {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
	}
	pg.mu.Lock()
	posts, closes := pg.reopenFixture.posts, pg.reopenFixture.closes
	pg.reopenFixture.mode = "normal"
	pg.mu.Unlock()
	if recoveryMode == "complete-takeover" {
		session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
		require.NoError(t, runner.Tick(ctx), "新owner执行权必须由真实Runner发布")
	}
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(worker.CloseIdleConnections)
	endpoint, stop = reopenRun(t, ctx, worker)
	observe := r.CommandSHA256
	if r.Closing != nil {
		_, _, _, observe = r.Closing.Close.Selection()
	}
	_, _ = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationObserveServiceReopen, observe))
	pg.mu.Lock()
	require.Equal(t, posts, pg.reopenFixture.posts, "重建与查询只认证原结果")
	require.Equal(t, closes, pg.reopenFixture.closes)
	pg.mu.Unlock()
	if r.Closing != nil {
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationRetryServiceReopen, observe))
		require.NoError(t, err)
		posts++
	} else if recoveryMode == "lost-start" || recoveryMode == "setter-before" || recoveryMode == "source-after-selection" {
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceReopen, r.CommandSHA256))
		require.NoError(t, err)
		posts++
	}
	state, err := endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
	require.NoError(t, err)
	r = state.State.ServiceClosure.Reopen
	require.Equal(t, journal.Settled, r.Phase)
	require.Equal(t, original.Active, state.State.Active, "新开放事实本身不改原活动操作")
	if strings.HasSuffix(mode, "lost-complete") {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, r.CommandSHA256))
	if strings.HasSuffix(mode, "lost-complete") {
		var unknown *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &unknown)
	} else {
		require.NoError(t, err)
	}
	stop()
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(worker.CloseIdleConnections)
	endpoint, stop = reopenRun(t, ctx, worker)
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, r.CommandSHA256))
	require.NoError(t, err, "完成丢回复或SDK重建只确认原证据，不再发送setter")
	stop()
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active)
	require.Nil(t, s.State().ServiceClosure)
	if pendingFinalization {
		require.Nil(t, s.State().InitialHeld)
		require.NotNil(t, s.State().BudgetHeld.Recovery)
		require.Equal(t, *original.Active, s.State().BudgetHeld.Recovery.Original)
		require.Equal(t, journal.RecoverFinalization, s.State().BudgetHeld.Finalization.Action)
	} else {
		require.Equal(t, original.BudgetHeld, s.State().BudgetHeld)
	}
	require.Equal(t, original.HighWater, s.State().HighWater)
	record := s.State().History[len(s.State().History)-1]
	require.Equal(t, expectedOutcome, record.Outcome)
	require.Equal(t, *original.Active, record.Operation)
	require.NotNil(t, record.ServiceCoverage)
	require.NoError(t, s.State().CheckClosedBudget(original.Active.Request))
	pg.mu.Lock()
	require.Equal(t, posts, pg.reopenFixture.posts)
	require.Equal(t, closes, pg.reopenFixture.closes)
	require.Equal(t, 1, pg.resizePosts, "对账与恢复均不能重发原PG伸缩")
	pg.reopenFixture = nil
	pg.mu.Unlock()
	return worker
}
