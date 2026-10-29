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

func TestProductionPGActiveSourceFailureClosesAcrossCapacityAndOwnership(t *testing.T) {
	for _, mode := range []string{"applied", "unreceived", "lost-start", "lost-close", "race", "race-lost-continuation", "transfer"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "active-safety-pg-"+mode) })
	}
}

// 正式首次服务/Prepare/Host/Runner/Guest和PG写前均已执行。只在peer安排
// 原PG是否到达、容量竞态和网络丢回复；Lease、CAS、完整SDK恢复及关闭选择
// 均为生产实现。原PG/Root/QMP/Kube为明确peer，不能称Linux物理回收验收。
func verifyPGActiveServiceSafety(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, core *kubefake.Clientset, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, qmp *vmQMPPeer, transfer *businessOwnerTransfer) {
	t.Helper()
	before, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizePG).Phase)
	require.NoError(t, worker.inspectActiveService(ctx, before), "原PG前后合法容量不能被误关")
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	restored := false
	restore := func() {
		if !restored {
			root.mu.Lock()
			root.view.Actual.MemoryMaxBytes--
			root.mu.Unlock()
			restored = true
		}
	}
	defer restore()
	race := strings.Contains(mode, "race")
	if race {
		pg.mu.Lock()
		pg.beforeClose = func() {
			response := pgResponse(t, pg.request)
			pg.mu.Lock()
			defer pg.mu.Unlock()
			pg.resized = true
			pg.resizePosts++
			response.Observed = pg.current().Observed
			*response.Receipt.Result = response.Observed
			pg.resizeReceipt = &response
		}
		pg.mu.Unlock()
	}
	lostStart := strings.HasSuffix(mode, "lost-start") || strings.HasSuffix(mode, "transfer")
	if lostStart {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	if strings.HasSuffix(mode, "lost-close") {
		pg.faults(false, true)
	}
	posts, _, _ := pg.counts()
	progress, advanceErr := worker.Advance(ctx)
	s, err := store.Load(ctx)
	require.NoError(t, err)
	c := s.State().ServiceClosure
	require.NotNil(t, c, "PG已派发也必须关闭来源失效的服务")
	require.EqualValues(t, 17, s.State().ProtocolVersion)
	require.Equal(t, before.State().Active, s.State().Active)
	require.Equal(t, before.State().InitialHeld, s.State().InitialHeld)
	if lostStart {
		var lost *journal.UnconfirmedWrite
		require.ErrorAs(t, advanceErr, &lost)
		after, _, _ := pg.counts()
		require.Equal(t, posts, after, "原关闭写前未确认不得派发")
	} else if c.Pending() {
		require.ErrorIs(t, advanceErr, ErrServiceSafety)
	} else {
		require.NoError(t, advanceErr)
		require.Equal(t, pgexec.Recorded, progress)
	}
	if strings.HasSuffix(mode, "transfer") {
		session, _, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
		_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationContinueServiceClosure, c.CommandSHA256))
		require.NoError(t, err)
	} else {
		worker.CloseIdleConnections()
		worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		if c.Pending() {
			if strings.HasSuffix(mode, "lost-continuation") {
				pg.faults(false, true)
			}
			_, err = worker.Advance(ctx)
			if lostStart {
				require.ErrorIs(t, err, ErrServiceSafety)
				_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceClosure, c.CommandSHA256))
			}
			if strings.HasSuffix(mode, "lost-continuation") {
				require.ErrorIs(t, err, ErrServiceSafety)
				pending, loadErr := store.Load(ctx)
				require.NoError(t, loadErr)
				require.NotNil(t, pending.State().ServiceClosure.Continuation)
				worker.CloseIdleConnections()
				worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				_, err = worker.Advance(ctx)
			}
			require.NoError(t, err)
		}
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.False(t, s.State().ServiceClosure.Pending())
	require.False(t, pg.shared().Observed.AdmissionOpen)
	require.Equal(t, before.State().Active, s.State().Active)
	require.Equal(t, before.State().InitialHeld, s.State().InitialHeld)
	require.Equal(t, &before.State().Active.Request, worker.client.Pending(), "关门后PG原Resize未知仍占住SDK")
	if race {
		n := s.State().ServiceClosure.Continuation
		require.NotNil(t, n)
		require.EqualValues(t, 2, n.Request.ProtocolVersion)
		require.Equal(t, c.Request, n.Request.Original)
		require.Equal(t, c.Request, n.Request.PreviousClose)
		require.Equal(t, c.Origin, n.Origin, "容量竞态由当前owner自动恢复，无需等待失租")
		require.Equal(t, c.Request.ExpectedCapacityGeneration+1, n.Request.Close.ExpectedCapacityGeneration)
		require.Equal(t, journal.Unconfirmed, s.State().ServiceClosure.Phase)
	}
	restore()
	posts, _, resizes := pg.counts()
	qmp.mu.Lock()
	vmWrites := qmp.writes
	qmp.mu.Unlock()
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, ErrServiceSafety)
	afterPosts, _, afterResize := pg.counts()
	require.Equal(t, posts, afterPosts)
	require.Equal(t, resizes, afterResize)
	qmp.mu.Lock()
	require.Equal(t, vmWrites, qmp.writes)
	qmp.mu.Unlock()
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State().Active, s.State().Active)
	require.False(t, pg.shared().Observed.AdmissionOpen, "来源恢复不能自行重开或释放原承诺")
}
