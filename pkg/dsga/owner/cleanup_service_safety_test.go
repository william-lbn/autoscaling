package owner

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 沿首次服务→实际Agent/Runner/Prepare/Host/VM/Guest/PG/Finalize进入取消，
// 不手工造Cleanup或关闭WAL。PG/Root/QMP/Kube是明确peer；真实生产owner/
// SDK/CAS/Lease/恢复执行，不把本组合称为目标Linux物理验收。
func TestProductionCleanupSourceFailureClosesAndPreservesOriginal(t *testing.T) {
	for _, mode := range []string{"applied", "unreceived", "settled", "lost-start", "lost-close", "transfer", "post-send-source-drift", "readonly-qualification"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "cleanup-safety-"+mode) })
	}
}

func verifyCleanupServiceSafety(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, client *kubefake.Clientset, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, qmp *vmQMPPeer, transfer *businessOwnerTransfer) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	if mode == "readonly-qualification" {
		progress, err := worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		s, err = store.Load(ctx)
		require.NoError(t, err)
	}
	require.NoError(t, worker.admission.CheckRetainedService(ctx, s), "原完整预算来源健康，不能在Cleanup前误关")
	_, err = store.RequestAbort(ctx, s, session.Actor(), s.State().Active.Request.Key)
	require.NoError(t, err)
	if mode == "post-send-source-drift" {
		pg.afterOpen = func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes++; root.mu.Unlock() }
	} else if mode != "settled" && mode != "readonly-qualification" {
		pg.faults(mode != "applied" && mode != "lost-close", mode == "applied" || mode == "lost-close")
	}
	progress, err := worker.Advance(ctx)
	if mode == "settled" || mode == "readonly-qualification" || mode == "post-send-source-drift" {
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
	} else {
		require.Error(t, err, "原Cleanup开放前/后丢回复须保留未知")
	}
	before, err := store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, before.State().Active.Cleanup, "原Cleanup必须先持久；Advance错误=%v，独立关闭=%+v", err, before.State().ServiceClosure)
	var command budget.CommittedAbortCommand
	require.NoError(t, json.Unmarshal(before.State().Active.Cleanup.Command, &command))
	require.Equal(t, mode != "readonly-qualification", command.MaySend)
	var restore func()
	if mode == "post-send-source-drift" {
		require.NotNil(t, before.State().ServiceClosure, "setter写后来源失败同轮关闭")
		restore = func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes--; root.mu.Unlock() }
	} else {
		require.Nil(t, before.State().ServiceClosure, "健康来源不能把未知误关")
		if mode == "readonly-qualification" {
			cm, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, source.Claim.Name, metav1.GetOptions{})
			require.NoError(t, err)
			changed := cm.DeepCopy()
			stamp := metav1.Now()
			changed.DeletionTimestamp = &stamp
			require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
			restore = func() {
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
			}
		} else {
			root.mu.Lock()
			root.view.Actual.MemoryMaxBytes++
			root.mu.Unlock()
			restore = func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes--; root.mu.Unlock() }
		}
		if mode == "lost-start" || mode == "transfer" {
			api.mu.Lock()
			api.loseJournalReply = true
			api.mu.Unlock()
		}
		if mode == "lost-close" {
			pg.faults(false, true)
		}
		posts, _, _ := pg.counts()
		progress, err = worker.Advance(ctx)
		switch mode {
		case "lost-start", "transfer":
			var unknown *journal.UnconfirmedWrite
			require.ErrorAs(t, err, &unknown)
			after, _, _ := pg.counts()
			require.Equal(t, posts, after, "关闭写前回复丢失零派发")
		case "lost-close":
			require.ErrorIs(t, err, ErrServiceSafety)
		default:
			require.NoError(t, err)
			require.Equal(t, pgexec.Recorded, progress)
		}
	}
	defer func() { restore() }()
	s, err = store.Load(ctx)
	require.NoError(t, err)
	c := s.State().ServiceClosure
	require.NotNil(t, c)
	require.Equal(t, command.Admission, *c.Original)
	require.Equal(t, before.State().Active, s.State().Active)
	require.Equal(t, before.State().BudgetHeld, s.State().BudgetHeld)
	if mode == "transfer" {
		session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, client, source)
		result, err := worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationContinueServiceClosure, c.CommandSHA256))
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, result.Progress)
	} else if c.Pending() {
		worker.CloseIdleConnections()
		worker, err = session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
		require.NoError(t, err)
		_, err = worker.Advance(ctx)
		if mode == "lost-start" {
			require.ErrorIs(t, err, ErrServiceSafety)
			_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceClosure, c.CommandSHA256))
		}
		require.NoError(t, err)
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.False(t, s.State().ServiceClosure.Pending())
	require.False(t, pg.shared().Observed.AdmissionOpen)
	require.Equal(t, before.State().Active, s.State().Active)
	require.Equal(t, before.State().BudgetHeld, s.State().BudgetHeld)
	require.Equal(t, before.State().History, s.State().History)
	// 来源恢复不会重开；完整关闭后重建也不能重新保留Cleanup的旧开放。
	restore()
	restore = func() {}
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	pending, closing := worker.client.PendingAdmission()
	require.Nil(t, pending)
	require.Nil(t, closing)
	posts, _, resizes := pg.counts()
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, ErrServiceSafety)
	// 绕过管理HTTP直接调用公开恢复入口，最后执行器也必须拒绝任何setter。
	_, err = worker.ReplayCommittedAbortOriginal(ctx, s.State().Active.Request.Key)
	require.Error(t, err)
	_, err = worker.RecoverCapacityAdmission(ctx, s.State().Active.Request.Key)
	require.Error(t, err)
	afterPosts, _, afterResizes := pg.counts()
	require.Equal(t, posts, afterPosts)
	require.Equal(t, resizes, afterResizes)
	qmp.mu.Lock()
	require.Equal(t, 1, qmp.writes, "安全关闭不产生VM反向写")
	qmp.mu.Unlock()
}
