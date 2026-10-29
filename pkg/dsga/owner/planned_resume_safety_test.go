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
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 复用正式首次开放及完整Grow前驱，只在Finalize已永久持有、Resume仍为空
// Planned时注入异常。Grow期间原准入可能仍开放；拒绝发新开放不足以关门。
func TestProductionPlannedResumeSourceFailureClosesWithoutInventingOpen(t *testing.T) {
	for _, mode := range []string{"source-drift", "lost-start", "lost-close", "transfer"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "resume-safety-planned-"+mode) })
	}
}

func verifyPlannedResumeServiceSafety(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, client *kubefake.Clientset, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, qmp *vmQMPPeer, transfer *businessOwnerTransfer) {
	t.Helper()
	mode = strings.TrimPrefix(mode, "recover-")
	before, err := store.Load(ctx)
	require.NoError(t, err)
	originalActive := before.State().Active
	require.True(t, before.State().BudgetHeld != nil || before.State().InitialHeld != nil || before.State().CapacityHeld != nil)
	step := budgetStep(t, store, journal.ResumeAdmission)
	require.Equal(t, journal.Planned, step.Phase)
	require.Empty(t, step.Command)
	require.True(t, pg.shared().Observed.AdmissionOpen, "首次准入在Grow终段仍可能开放")
	root.mu.Lock()
	drifted := mode != "post-start-drift"
	if drifted {
		root.view.Actual.MemoryMaxBytes++
	} else {
		// 本轮预审先读一次出生；实际Finalize的第二次出生观察之后
		// 改约束，让写前CAS后读回失败。回调已持fixture锁，不再取锁；
		// 产品代码不使用该回调，也不改变任何生产请求/租约期限。
		reads := 0
		root.afterProcess = func() {
			reads++
			if reads == 2 {
				root.view.Actual.MemoryMaxBytes++
				drifted = true
				root.afterProcess = nil
			}
		}
	}
	root.mu.Unlock()
	restoreSource := func() {
		root.mu.Lock()
		defer root.mu.Unlock()
		root.afterProcess = nil
		if drifted {
			root.view.Actual.MemoryMaxBytes--
			drifted = false
		}
	}
	defer restoreSource()
	if mode == "lost-start" || mode == "transfer" {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	if mode == "lost-close" {
		pg.faults(false, true)
	}
	posts, _, _ := pg.counts()
	progress, advanceErr := worker.Advance(ctx)
	s, err := store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, s.State().ServiceClosure, "最终开放尚未Start时，来源异常也必须持久关闭已有准入")
	c := s.State().ServiceClosure
	require.Nil(t, c.Original, "没有派发过开放，不能补造原开放命令或未知")
	if mode == "post-start-drift" {
		after := s.State().Active
		require.Equal(t, originalActive.Request, after.Request)
		for i, step := range originalActive.Steps {
			if step.Action == journal.FinalizeBudget {
				require.Equal(t, journal.Planned, step.Phase)
				require.Equal(t, journal.Unconfirmed, after.Steps[i].Phase, "本轮必须真实Start后失败，不能跳过写后窗口")
			} else {
				require.Equal(t, step, after.Steps[i])
			}
		}
		originalActive = after
	}
	require.Equal(t, originalActive, s.State().Active)
	require.Equal(t, before.State().BudgetHeld, s.State().BudgetHeld)
	require.Equal(t, before.State().InitialHeld, s.State().InitialHeld)
	require.Equal(t, before.State().CapacityHeld, s.State().CapacityHeld)
	require.Equal(t, before.State().History, s.State().History)
	switch mode {
	case "lost-start", "transfer":
		var lost *journal.UnconfirmedWrite
		require.ErrorAs(t, advanceErr, &lost)
		after, _, _ := pg.counts()
		require.Equal(t, posts, after, "关闭WAL写前回复丢失不得派发")
	case "lost-close":
		require.ErrorIs(t, advanceErr, ErrServiceSafety)
	default:
		require.NoError(t, advanceErr)
		require.Equal(t, pgexec.Recorded, progress)
	}
	if mode == "transfer" {
		session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, client, source)
		_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationContinueServiceClosure, c.CommandSHA256))
		require.NoError(t, err)
	} else {
		worker.CloseIdleConnections()
		worker, err = session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		if c.Pending() {
			_, err = worker.Advance(ctx)
			if mode == "lost-start" {
				require.ErrorIs(t, err, ErrServiceSafety)
				_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceClosure, c.CommandSHA256))
			}
			require.NoError(t, err)
		}
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.False(t, s.State().ServiceClosure.Pending())
	require.False(t, pg.shared().Observed.AdmissionOpen)
	require.Equal(t, originalActive, s.State().Active)
	require.Equal(t, before.State().BudgetHeld, s.State().BudgetHeld)
	require.Equal(t, before.State().InitialHeld, s.State().InitialHeld)
	require.Equal(t, before.State().CapacityHeld, s.State().CapacityHeld)
	restoreSource()
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	pending, closing := worker.client.PendingAdmission()
	require.Nil(t, pending, "已完成的关闭不能恢复出不存在的原开放未知")
	require.Nil(t, closing)
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, ErrServiceSafety)
	require.False(t, pg.shared().Observed.AdmissionOpen, "来源恢复后不得自动开放或继续原Planned")
	_, _, resizes := pg.counts()
	require.Equal(t, 1, resizes)
	qmp.mu.Lock()
	require.Equal(t, 1, qmp.writes, "关闭不重放VM动作或释放预算")
	qmp.mu.Unlock()
}
