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
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

func TestProductionPGResourceRecovery(t *testing.T) {
	for _, mode := range []string{"committed", "committed-lost-proof", "committed-takeover", "unreceived", "expired"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "pg-safety-recover-"+mode) })
	}
}

// 正式首次预算/Host/Runner/Guest/PG写前后失败关闭，再走单PG生产执行器。
// 真实Elector、Session、SDK和typed CAS；PG/Root/QMP/Kube为明确协议peer。
// 更高floor隔离不造原PG终态，完整实际预算/其他资源仍由后继恢复处理。
func verifyProductionPGResourceRecovery(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, qmp *vmQMPPeer, transfer *businessOwnerTransfer, _ *vmguard.Runner) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	original := s.State()
	_, _, _, selector := original.ServiceClosure.Selection()
	// 写前CAS丢回复时旧SDK尚未进入Resize，因此Pending可能为空。正式
	// 重建必须从原WAL恢复它；不能沿用旧测试句柄来判断恢复是否清未知。
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	isolated := strings.Contains(mode, "unreceived") || strings.Contains(mode, "expired")
	if strings.Contains(mode, "expired") {
		// 原结果被后来的同boot终态淘汰；当前头不是原Key，但逻辑容量
		// 保持原提交后的真实值。只模拟历史保留，不能制造原NoEffect。
		pg.mu.Lock()
		op := *pg.after.Observed.Operation
		op.Key.Revision++
		op.Key.OperationID.Low++
		op.Phase, op.Reason = control.Denied, control.ReasonTimeout
		op.ExpectedCapacityGeneration = pg.after.Observed.CapacityGeneration
		op.TargetBuffers++
		pg.after.Observed.Operation = &op
		pg.mu.Unlock()
	}
	if isolated {
		s, err = store.Load(ctx)
		require.NoError(t, err)
		_, err = worker.executor.RecoverServicePG(ctx, s, selector, true)
		require.Error(t, err, "相同owner不能用缺历史清原PG未知")
		s, err = store.Load(ctx)
		require.NoError(t, err)
		require.Nil(t, s.State().ServiceClosure.PGRecovery)
		require.NotNil(t, worker.client.Pending())
		session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
	}
	posts, _, resizePosts := pg.counts()
	root.mu.Lock()
	guestPosts, guestCancels := root.posts, root.cancellations
	root.mu.Unlock()
	qmp.mu.Lock()
	vmWrites := qmp.writes
	qmp.mu.Unlock()
	if strings.Contains(mode, "lost-proof") {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	// 每次显式单PG恢复采用当前Store快照及原selector；不借完整管理
	// Reopen入口同时创建VM/Guest/Actual证明，使故障CAS准确落在本域。
	s, err = store.Load(ctx)
	require.NoError(t, err)
	_, err = worker.executor.RecoverServicePG(ctx, s, selector, true)
	if strings.Contains(mode, "lost-proof") {
		var lost *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &lost)
		require.NotNil(t, worker.client.Pending(), "证明CAS未确认不得提前清SDK")
	} else {
		require.NoError(t, err, "完整单PG来源可以独立成功，但不开放服务")
		require.Nil(t, worker.client.Pending(), "当前原Key重新查询后只清相同原SDK项")
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, s.State().ServiceClosure.PGRecovery)
	require.Nil(t, s.State().ServiceClosure.VMRecovery)
	require.Nil(t, s.State().ServiceClosure.ActualRecovery)
	require.Nil(t, s.State().ServiceClosure.Reopen)
	require.Equal(t, original.Active, s.State().Active)
	require.Equal(t, original.InitialHeld, s.State().InitialHeld)
	want := control.ResizeRecoveredCommitted
	if isolated {
		want = control.ResizeRecoveredIsolated
		require.Nil(t, s.State().ServiceClosure.PGRecovery.Status.Receipt.Result)
	}
	require.Equal(t, want, s.State().ServiceClosure.PGRecovery.Effect)
	if strings.HasSuffix(mode, "takeover") {
		_, _, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
		require.NotNil(t, worker.client.Pending(), "新SDK先从原WAL恢复未知")
	} else {
		worker.CloseIdleConnections()
		worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		require.NotNil(t, worker.client.Pending(), "SDK重建仍从原WAL恢复未知")
	}
	progress, err := worker.Advance(ctx)
	require.Equal(t, pgexec.Reconciled, progress)
	require.ErrorIs(t, err, ErrServiceSafety)
	require.Nil(t, worker.client.Pending())
	after, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, s.State().ServiceClosure.PGRecovery, after.State().ServiceClosure.PGRecovery)
	require.Equal(t, original.Active, after.State().Active)
	require.Nil(t, after.State().ServiceClosure.ActualRecovery, "普通轮询只复核已持久单PG证明")
	require.Nil(t, after.State().ServiceClosure.Reopen)
	require.Error(t, after.State().CheckStableServiceRecovery())
	// 同SDK Pending已清也必须复核当前来源；不能借空Pending忽略门禁漂移。
	pg.mu.Lock()
	closed := *pg.gate
	pg.gate.Open = true
	pg.mu.Unlock()
	progress, err = worker.Advance(ctx)
	require.Equal(t, pgexec.AwaitingFact, progress)
	require.ErrorIs(t, err, ErrServiceSafety)
	pg.mu.Lock()
	pg.gate = &closed
	pg.mu.Unlock()
	afterPosts, _, afterResizePosts := pg.counts()
	require.Equal(t, posts, afterPosts)
	require.Equal(t, resizePosts, afterResizePosts)
	require.False(t, pg.shared().Observed.AdmissionOpen)
	root.mu.Lock()
	require.Equal(t, guestPosts, root.posts)
	require.Equal(t, guestCancels, root.cancellations)
	root.mu.Unlock()
	qmp.mu.Lock()
	require.Equal(t, vmWrites, qmp.writes)
	qmp.mu.Unlock()
}
