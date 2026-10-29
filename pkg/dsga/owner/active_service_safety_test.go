package owner

import (
	"context"
	"encoding/json"
	"strings"
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

func TestProductionActiveSourceFailureClosesWithoutAdvancingUnknownResources(t *testing.T) {
	for _, mode := range []string{"prepare-root", "vm-unknown-root", "vm-unknown-outside", "guest-complete-root", "vm-unknown-lost-start", "vm-unknown-lost-close", "vm-unknown-transfer"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "active-safety-"+mode) })
	}
}

func TestProductionActiveQuiesceUnknownSourceFailureRetainsOriginalClose(t *testing.T) {
	for _, mode := range []string{"unreceived-root", "applied-lost-close"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "active-safety-quiesce-"+mode) })
	}
}

// 正式首次服务/Prepare/Host/Runner/Guest前驱停在具体在途边界，随后只改变
// 显式peer的来源；原SDK、TLS、WAL、真实Lease接管和UID/RV CAS均走生产代码。
func verifyActiveServiceSafety(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, client *kubefake.Clientset, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, qmp *vmQMPPeer, transfer *businessOwnerTransfer) {
	t.Helper()
	before, err := store.Load(ctx)
	require.NoError(t, err)
	require.NoError(t, worker.inspectActiveService(ctx, before), "合法VM/Guest过渡不能按空闲旧预算误关门")
	require.Equal(t, journal.Planned, budgetStep(t, store, journal.ResizePG).Phase)
	quiesce := strings.HasPrefix(mode, "active-safety-quiesce-")
	if !quiesce || strings.Contains(mode, "unreceived") {
		require.True(t, pg.shared().Observed.AdmissionOpen)
	} else {
		require.False(t, pg.shared().Observed.AdmissionOpen)
	}
	_, _, resizes := pg.counts()
	require.Zero(t, resizes)
	qmp.mu.Lock()
	vmWrites := qmp.writes
	qmp.mu.Unlock()
	var restore func()
	if strings.HasSuffix(mode, "outside") {
		var prepared budget.Prepared
		require.NoError(t, json.Unmarshal(budgetStep(t, store, journal.PrepareBudget).Observation.Data, &prepared))
		require.NotNil(t, prepared.OutsideClaim)
		original, err := client.CoreV1().ConfigMaps("testing").Get(ctx, prepared.OutsideClaim.Name, metav1.GetOptions{})
		require.NoError(t, err)
		changed := original.DeepCopy()
		changed.Finalizers = nil
		require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
		restore = func() {
			require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), original, original.Namespace))
		}
	} else {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
		restore = func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes--; root.mu.Unlock() }
	}
	defer func() { restore() }()
	lostStart := strings.HasSuffix(mode, "lost-start") || strings.HasSuffix(mode, "transfer")
	lostClose := strings.HasSuffix(mode, "lost-close")
	if lostStart {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	if lostClose {
		pg.faults(false, true)
	}
	posts, _, _ := pg.counts()
	progress, advanceErr := worker.Advance(ctx)
	s, err := store.Load(ctx)
	require.NoError(t, err)
	c := s.State().ServiceClosure
	require.NotNil(t, c, "PG未发送的VM/Guest在途也必须关闭已有准入")
	require.EqualValues(t, 16, s.State().ProtocolVersion)
	if quiesce {
		var original budget.QuiesceCommand
		require.NoError(t, json.Unmarshal(budgetStep(t, store, journal.Quiesce).Command, &original))
		require.Equal(t, &original.Close, c.Original, "独立关闭必须逐字封住原Quiesce，不能只看当前closed布尔值")
	} else {
		require.Nil(t, c.Original)
	}
	require.Equal(t, before.State().Active, s.State().Active)
	require.Equal(t, before.State().InitialHeld, s.State().InitialHeld)
	require.Equal(t, before.State().BudgetHeld, s.State().BudgetHeld)
	require.Equal(t, before.State().CapacityHeld, s.State().CapacityHeld)
	if lostStart {
		var lost *journal.UnconfirmedWrite
		require.ErrorAs(t, advanceErr, &lost)
		after, _, _ := pg.counts()
		require.Equal(t, posts, after, "关闭写前CAS丢回复必须零派发")
	} else if lostClose {
		require.ErrorIs(t, advanceErr, ErrServiceSafety)
	} else {
		require.NoError(t, advanceErr)
		require.Equal(t, pgexec.Recorded, progress)
	}
	if strings.HasSuffix(mode, "transfer") {
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
			if lostStart {
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
	require.Equal(t, before.State().Active, s.State().Active)
	require.Equal(t, before.State().InitialHeld, s.State().InitialHeld)
	restore()
	restore = func() {}
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, ErrServiceSafety)
	require.False(t, pg.shared().Observed.AdmissionOpen, "来源恢复不自动重开或重发原资源动作")
	_, _, resizes = pg.counts()
	require.Zero(t, resizes)
	qmp.mu.Lock()
	require.Equal(t, vmWrites, qmp.writes)
	qmp.mu.Unlock()
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State().Active, s.State().Active)
}
