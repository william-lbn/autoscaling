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
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

func TestProductionGuestServiceRecovery(t *testing.T) {
	for _, mode := range []string{"applied", "applied-lost-record", "unreceived-lost-select", "unreceived-lost-cancel", "unreceived-apply-wins", "partial"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "guest-safety-recover-"+mode) })
	}
}

func TestProductionGuestServiceRecoveryTakeover(t *testing.T) {
	for _, mode := range []string{"applied-lost-record-takeover", "unreceived-lost-select-takeover"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "guest-safety-recover-"+mode) })
	}
}

// 正式首次服务、Prepare、Host、Runner和Guest写前后形成故障，再走真实
// Session/SDK/typed CAS/管理入口；PG/Root/Kube/QMP为显式协议peer。独立
// Guest对账完成仍不开放：PG/VM及完整实际预算须由后继恢复模块单独确认。
func verifyProductionGuestServiceRecovery(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, resourceConfig resourceguard.ClientConfig, config compute.Config, client *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, qmp *vmQMPPeer, transfer *businessOwnerTransfer, runner *vmguard.Runner) {
	t.Helper()
	unreceived := strings.Contains(mode, "unreceived")
	lostSelect := strings.Contains(mode, "lost-select")
	lostRecord := strings.Contains(mode, "lost-record")
	applyWins := strings.HasSuffix(mode, "apply-wins")
	before, err := store.Load(ctx)
	require.NoError(t, err)
	original := before.State()
	require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.GuestReady).Phase)
	root.mu.Lock()
	correct := root.view.Clone()
	guestWrites := root.posts
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	qmp.mu.Lock()
	vmWrites := qmp.writes
	qmp.mu.Unlock()
	_, err = worker.Advance(ctx)
	require.NoError(t, err)
	s, err := store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, s.State().ServiceClosure)
	require.False(t, s.State().ServiceClosure.Pending())
	require.False(t, pg.shared().Observed.AdmissionOpen)
	root.mu.Lock()
	root.view.Actual = correct.Actual
	if strings.HasSuffix(mode, "partial") {
		// Root已收到但只完成部分约束。数值不符时不能取消，也不能由
		// 旧Held/低占用补造Applied；恢复数值后才可只读原Observe。
		root.view.Held, root.view.Applied = nil, false
		root.view.Actual.MemoryMaxBytes++
	}
	root.mu.Unlock()
	_, _, _, selector := s.State().ServiceClosure.Selection()
	// 本组故障注入专门覆盖Guest的选择/回执CAS；先经生产读者独立确认
	// 已完成的原Runner，避免把更早VM选择的丢回复误标成Guest失败。
	// 独立VM来源保留，不修改原Active或旧永久预算，也不提前开放。
	s, _, err = worker.vm.RecoverServiceVM(ctx, s, selector, true)
	require.NoError(t, err)
	require.Equal(t, journal.Settled, s.State().ServiceClosure.VMRecovery.Phase)
	request := closureControl(session, config.Instance, control.CoordinationReopenService, selector)
	if lostSelect {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	if lostRecord {
		root.mu.Lock()
		root.afterObserve = func() { api.mu.Lock(); api.loseJournalReply = true; api.mu.Unlock() }
		root.mu.Unlock()
	}
	if strings.HasSuffix(mode, "lost-cancel") {
		root.mu.Lock()
		root.loseCancelReply = true
		root.mu.Unlock()
	}
	if applyWins {
		root.mu.Lock()
		root.beforeCancel = func(c resourceguard.Command) {
			root.posts++
			root.view.Original, root.view.Held, root.view.Applied = &c, &c, true
			root.view.Actual.MemoryMaxBytes = c.MemoryMaxBytes
			root.view.Actual.HugePools = append([]resourceguard.HugePool(nil), c.HugePools...)
			root.view.Actual.WholePoolUpperBytes = 0
			for _, p := range c.HugePools {
				root.view.Actual.WholePoolUpperBytes += p.PageSizeKB * 1024 * p.Pages
			}
		}
		root.mu.Unlock()
	}
	_, err = worker.executeControl(ctx, request)
	require.Error(t, err, "单Guest对账不授予开放")
	s, loadErr := store.Load(ctx)
	require.NoError(t, loadErr)
	require.NotNil(t, s.State().ServiceClosure.ResourceRecovery)
	g := s.State().ServiceClosure.ResourceRecovery.Guest
	if lostSelect || strings.HasSuffix(mode, "lost-cancel") || strings.HasSuffix(mode, "partial") || applyWins {
		require.Equal(t, journal.Unconfirmed, g.Phase)
	} else {
		require.Equal(t, journal.Settled, g.Phase)
	}
	root.mu.Lock()
	root.afterObserve = nil
	cancelWrites := root.cancellations
	root.mu.Unlock()
	if lostSelect {
		var lost *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &lost)
		require.Zero(t, cancelWrites, "选择CAS丢回复不给Root取消票据")
		_, err = worker.Advance(ctx)
		require.ErrorIs(t, err, ErrServiceSafety)
		root.mu.Lock()
		require.Zero(t, root.cancellations, "普通轮询不能派发取消")
		root.mu.Unlock()
	}
	if strings.HasSuffix(mode, "partial") {
		root.mu.Lock()
		require.Zero(t, root.cancellations)
		root.view.Actual = correct.Actual
		root.mu.Unlock()
	}
	if strings.HasSuffix(mode, "lost-cancel") {
		var lost *resourceguard.ClientError
		require.ErrorAs(t, err, &lost)
		require.NotNil(t, resources.client.Pending())
	}
	if lostRecord {
		var lost *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &lost)
	}
	// 新SDK从原WAL恢复同一Pending；创建时可以只读原Observe，却不能
	// 修改外层GuestReady。未收到的取消仍须再次显式选择，已封存取消只查。
	var restored *ResourceWorker
	if strings.HasSuffix(mode, "takeover") {
		session, restored, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, client, source)
		_, err = worker.Advance(ctx)
		require.ErrorIs(t, err, ErrServiceSafety, "Runner尚未承认新owner时保留原关闭/未知")
		require.NoError(t, runner.Tick(ctx))
		request = closureControl(session, config.Instance, control.CoordinationReopenService, selector)
	} else {
		worker.CloseIdleConnections()
		restored, err = session.NewResourceWorker(ctx, resourceConfig)
		require.NoError(t, err)
		defer restored.CloseIdleConnections()
		worker, err = session.NewProductionPGWorker(ctx, config, restored, client.CoreV1(), source)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
	}
	if lostSelect {
		_, err = worker.executeControl(ctx, request)
	} else {
		_, err = worker.Advance(ctx)
	}
	require.ErrorIs(t, err, ErrServiceSafety)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	g = s.State().ServiceClosure.ResourceRecovery.Guest
	require.Equal(t, journal.Settled, g.Phase)
	require.Equal(t, map[bool]string{false: "applied", true: "cancelled"}[unreceived && !applyWins], g.Effect)
	require.Equal(t, original.Active, s.State().Active)
	require.Equal(t, original.InitialHeld, s.State().InitialHeld)
	require.Equal(t, original.BudgetBoundary, s.State().BudgetBoundary)
	require.Nil(t, restored.client.Pending())
	require.False(t, pg.shared().Observed.AdmissionOpen)
	require.Error(t, s.State().CheckStableServiceRecovery())
	_, _, pgWrites := pg.counts()
	require.Zero(t, pgWrites)
	root.mu.Lock()
	if applyWins {
		guestWrites++ // 仅模拟已在网络中的原Apply抢先生效，恢复没有新Apply。
	}
	require.Equal(t, guestWrites, root.posts)
	if unreceived {
		require.Equal(t, 1, root.cancellations)
	} else {
		require.Zero(t, root.cancellations)
	}
	// 完成后出生身份改变须拒绝重认，不能更新原独立证明。
	process := *root.view.Birth
	process.PostmasterStartTicks++
	root.processOverride = &process
	root.mu.Unlock()
	progress, err := worker.Advance(ctx)
	require.Equal(t, pgexec.AwaitingFact, progress)
	require.ErrorIs(t, err, ErrServiceSafety)
	after, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, s.State(), after.State())
	qmp.mu.Lock()
	require.Equal(t, vmWrites, qmp.writes)
	qmp.mu.Unlock()
}
