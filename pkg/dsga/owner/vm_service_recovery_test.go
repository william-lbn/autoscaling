package owner

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

func TestProductionVMServiceRecovery(t *testing.T) {
	for _, mode := range []string{"unreceived", "unreceived-lost-select", "unreceived-lost-record-takeover", "completed", "partial"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "vm-safety-recover-"+mode) })
	}
}

// 使用正式首次服务/Prepare/Host/ResizeVM写前WAL，再在资源异常关闭后通过
// 单VM生产执行器保存独立选择。Runner的flock、持久记录、QMP适配器、Session、
// Lease与UID/RV CAS均走产品代码；QMP/PG/Root/Kube仍为明确协议peer，不是
// Linux/KVM物理返还证明。原ResizeVM始终Unknown，单VM证明不授予开放。
func verifyProductionVMServiceRecovery(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, core *kubefake.Clientset, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner, qmp *vmQMPPeer, transfer *businessOwnerTransfer) {
	t.Helper()
	completed, partial := strings.HasSuffix(mode, "completed"), strings.HasSuffix(mode, "partial")
	lostSelect, lostRecord := strings.Contains(mode, "lost-select"), strings.Contains(mode, "lost-record")
	before, err := store.Load(ctx)
	require.NoError(t, err)
	original := before.State()
	require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizeVM).Phase)
	if completed || partial {
		if partial {
			qmp.mu.Lock()
			qmp.lose = true
			qmp.mu.Unlock()
		}
		err = runner.Tick(ctx)
		if partial {
			require.ErrorIs(t, err, vmguard.ErrUnknown, "原Set丢回复后Runner必须保留may_have_sent")
			// 模拟已发送命令尚未达到原目标的实际中间态。只改peer观测，
			// 不改Runner WAL/回执，不将普通not_sent伪造成取消完成。
			setVMRecoveryPeerActual(qmp, 768<<20)
			require.ErrorIs(t, runner.Tick(ctx), vmguard.ErrUnknown)
		} else {
			require.NoError(t, err)
		}
	}
	verifyActiveServiceSafety(t, ctx, "active-safety-vm-unknown-root", api, store, core, session, resources, worker, config, source, pg, root, qmp, transfer)
	closed, err := store.Load(ctx)
	require.NoError(t, err)
	_, _, _, selector := closed.State().ServiceClosure.Selection()
	require.Nil(t, closed.State().ServiceClosure.VMRecovery, "管理前不能自动选择VM取消")
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	posts, _, resizePosts := pg.counts()
	root.mu.Lock()
	guestWrites, guestCancels := root.posts, root.cancellations
	root.mu.Unlock()
	qmp.mu.Lock()
	vmWrites := qmp.writes
	qmp.mu.Unlock()
	if lostSelect {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	s, err := store.Load(ctx)
	require.NoError(t, err)
	// 完整管理入口已能继续ActualRecovery和业务重开，本根显式只进入
	// 原VM域，不能靠旧“Reopen永远报错”断言收窄已实现的产品功能。
	_, _, err = worker.vm.RecoverServiceVM(ctx, s, selector, true)
	if lostSelect {
		var lost *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &lost)
	} else if completed {
		require.NoError(t, err, "原Runner完整完成可独立保存VM证明")
	} else {
		require.ErrorIs(t, err, vmguard.ErrUnknown)
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	selection := s.State().ServiceClosure.VMRecovery
	require.NotNil(t, selection)
	require.Nil(t, s.State().ServiceClosure.ActualRecovery)
	require.Nil(t, s.State().ServiceClosure.Reopen)
	require.True(t, selection.CancelUnsent)
	if completed {
		require.Equal(t, journal.Settled, selection.Phase)
	} else {
		require.Equal(t, journal.Unconfirmed, selection.Phase)
		// 普通Agent轮询只查询持久选择，不发送QMP或代替Runner产生取消
		// 证明；选择CAS丢回复时也不重建一个新的取消/目标命令。
		_, err = worker.Advance(ctx)
		require.ErrorIs(t, err, ErrServiceSafety)
		afterPoll, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, selection, afterPoll.State().ServiceClosure.VMRecovery)
		if partial {
			require.ErrorIs(t, runner.Tick(ctx), vmguard.ErrUnknown)
			_, err = worker.Advance(ctx)
			require.ErrorIs(t, err, ErrServiceSafety)
			still, err := store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, journal.Unconfirmed, still.State().ServiceClosure.VMRecovery.Phase)
			// 原在途动作后来达到完整目标，只读观察即可完成原Runner。
			setVMRecoveryPeerActual(qmp, uint64(original.Active.Request.TargetVMMemoryBytes))
		}
		// Runner独立GET读取已落盘选择；即使选择回复丢失，也只有它
		// 在原flock内可以封存未发送屏障，调用方没有直接QMP写权限。
		require.NoError(t, runner.Tick(ctx))
		if lostRecord {
			api.mu.Lock()
			api.loseJournalReply = true
			api.mu.Unlock()
		}
		_, err = worker.Advance(ctx)
		require.ErrorIs(t, err, ErrServiceSafety)
		if lostRecord {
			var lost *journal.UnconfirmedWrite
			require.ErrorAs(t, err, &lost)
		}
		s, err = store.Load(ctx)
		require.NoError(t, err)
	}
	proof := s.State().ServiceClosure.VMRecovery
	require.Equal(t, journal.Settled, proof.Phase)
	require.NotNil(t, proof.Capacity)
	require.Equal(t, !completed && !partial, proof.Capacity.Cancelled)
	require.Equal(t, original.Active, s.State().Active)
	require.Equal(t, original.InitialHeld, s.State().InitialHeld)
	require.Equal(t, original.BudgetBoundary, s.State().BudgetBoundary)
	require.Equal(t, closed.State().ServiceClosure.Request, s.State().ServiceClosure.Request)
	require.Error(t, s.State().CheckStableServiceRecovery())
	if strings.HasSuffix(mode, "takeover") {
		_, _, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
		// 新owner不能靠旧floor使用证明；真实Runner先确认更高执行权，
		// 保留原取消/观察者，原命令仍不可迟到执行。
		progress, err := worker.Advance(ctx)
		require.Equal(t, pgexec.AwaitingFact, progress)
		require.ErrorIs(t, err, ErrServiceSafety)
		require.NoError(t, runner.Tick(ctx))
	} else {
		worker.CloseIdleConnections()
		worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
	}
	progress, err := worker.Advance(ctx)
	require.Equal(t, pgexec.Reconciled, progress)
	require.ErrorIs(t, err, ErrServiceSafety)
	after, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, proof, after.State().ServiceClosure.VMRecovery, "重建/接管不能重写原观察者与原证明")
	require.Equal(t, original.Active, after.State().Active)
	require.Nil(t, after.State().ServiceClosure.ActualRecovery, "普通轮询不能自动创建完整实际预算或新开放")
	require.Nil(t, after.State().ServiceClosure.Reopen)
	require.False(t, pg.shared().Observed.AdmissionOpen)
	// 当前物理观测漂移不能被已保存Settled或相同原目标掩盖，拒绝时
	// 原独立证明仍不可变；完成回执不是未来无限期容量授权。
	qmp.mu.Lock()
	actual := qmp.actual
	qmp.mu.Unlock()
	drifted := actual + vmguard.VirtioBlockBytes
	setVMRecoveryPeerActual(qmp, drifted)
	require.NoError(t, runner.Tick(ctx), "Runner可发布当前漂移，但旧完成回执不能掩盖它")
	progress, err = worker.Advance(ctx)
	require.Equal(t, pgexec.AwaitingFact, progress)
	require.ErrorIs(t, err, ErrServiceSafety)
	drift, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, after.State(), drift.State())
	setVMRecoveryPeerActual(qmp, actual)
	endPosts, _, endResizePosts := pg.counts()
	require.Equal(t, posts, endPosts)
	require.Equal(t, resizePosts, endResizePosts)
	root.mu.Lock()
	require.Equal(t, guestWrites, root.posts)
	require.Equal(t, guestCancels, root.cancellations)
	root.mu.Unlock()
	qmp.mu.Lock()
	require.Equal(t, vmWrites, qmp.writes, "独立恢复不增加QMP Set，即使原效果部分完成")
	qmp.mu.Unlock()
}

// 只推进显式QMP peer及其已有状态回调，写计数保持不变。该夹具不能证明
// Linux requested/actual的物理关系，产品仍必须认证Runner完整原回执。
func setVMRecoveryPeerActual(qmp *vmQMPPeer, actual uint64) {
	qmp.mu.Lock()
	defer qmp.mu.Unlock()
	qmp.actual = actual
	qmp.onResize(actual)
}
