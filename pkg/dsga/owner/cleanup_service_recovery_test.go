package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 原完整Grow、原Cleanup写前/丢回复、来源异常关闭均由生产者形成；再走
// 真实Run管理、来源重审、独立开放/完成，原取消保持原样。PG/Root/Kube/QMP
// 仍是明确协议peer，本组不替代原生PG或Linux物理资源认证。
func TestProductionCleanupServiceRecovery(t *testing.T) {
	for _, mode := range []string{"normal", "settled", "lost-complete", "source-after-selection", "complete-takeover"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "cleanup-safety-recover-"+mode) })
	}
}

func TestProductionCleanupServiceRecoveryThenAutomaticShrink(t *testing.T) {
	productionInitialService(t, "cleanup-safety-recover-automatic")
}

// 原实际/旧容量取消先由原Agent/Runner执行并因来源异常独立关闭。随后
// 只切换同一PG实例的显式协议peer，真实SDK从完整WAL重建；不注入新资源
// 完成，不替换旧预算，也不把原Cleanup未知改成已完成。
func verifyActualOrUnchangedCleanupRecovery(t *testing.T, ctx context.Context, api *ownerAPI, core *kubefake.Clientset, coord coordclient.CoordinationV1Interface, store *journal.Store, session *Session, resources *ResourceWorker, root *guestRootFixture, pg *committedAbortPG, shared control.SharedCostResponse) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	original := s.State()
	require.NotNil(t, original.Active)
	require.NotNil(t, original.Active.Cleanup)
	require.NotNil(t, original.ServiceClosure)
	require.False(t, original.ServiceClosure.Pending())
	current := pg.shared(shared)
	pg.mu.Lock()
	require.NotNil(t, pg.last)
	gate := *pg.last
	pg.mu.Unlock()
	var native initialServicePG
	peer := &native
	peer.store, peer.request, peer.before, peer.after, peer.gate = store, original.Active.Request, current, current, &gate
	for _, step := range original.Active.Steps {
		if step.Action == journal.ResizePG && step.Phase == journal.Settled && step.Observation != nil {
			var audit struct {
				Response control.ControlResponse `json:"response"`
			}
			require.NoError(t, json.Unmarshal(step.Observation.Data, &audit))
			peer.resizeReceipt = &audit.Response
		}
	}
	var reopening serviceReopenPG
	reopening.mode, reopening.api, reopening.root = "normal", api, root
	peer.reopenFixture = &reopening
	config := pgConfig(t, original.Active.Request, func(w http.ResponseWriter, r *http.Request) { peer.serve(t, w, r) }, func() control.Snapshot { return peer.shared().Observed })
	worker, err := session.NewProductionPGWorker(ctx, config, resources, core.CoreV1())
	require.NoError(t, err)
	t.Cleanup(worker.CloseIdleConnections)
	node, err := core.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	var ledger hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger))
	stopHost := keepUnchangedHostLease(t, ctx, core, ledger.Binding)
	defer stopHost()
	allocator, err := hostguard.NewStore(core.CoreV1(), core.CoordinationV1(), ledger.Binding, "host-fixture-holder")
	require.NoError(t, err)
	publisher, err := hostexec.New(core.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	root.mu.Lock()
	rootPosts := root.posts
	root.mu.Unlock()
	endpoint, stop := reopenRun(t, ctx, worker)
	_, _, _, selector := original.ServiceClosure.Selection()
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
	require.NoError(t, err)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	selected := s.State()
	require.Equal(t, original.Active, selected.Active)
	require.Equal(t, original.BudgetHeld, selected.BudgetHeld)
	require.Equal(t, original.CapacityHeld, selected.CapacityHeld)
	require.NotNil(t, selected.ServiceClosure.CleanupRecovery)
	require.Equal(t, journal.Settled, selected.ServiceClosure.Reopen.Phase)
	opening := selected.ServiceClosure.Reopen.CommandSHA256
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, opening))
	var lost *journal.UnconfirmedWrite
	require.ErrorAs(t, err, &lost, "预算、覆盖、水位与清Active必须同CAS，丢回复只能重读")
	stop()
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1())
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	endpoint, stop = reopenRun(t, ctx, worker)
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, opening))
	require.NoError(t, err)
	stop()
	s, err = store.Load(ctx)
	require.NoError(t, err)
	done := s.State()
	require.Nil(t, done.Active)
	require.Nil(t, done.ServiceClosure)
	record := done.History[len(done.History)-1]
	require.Equal(t, journal.RecoveredAfterCancellation, record.Outcome)
	require.Equal(t, *original.Active, record.Operation)
	kind := record.ServiceCoverage.CleanupRecovery.Kind
	if kind == "actual" {
		require.Nil(t, done.BudgetHeld)
		require.Equal(t, *original.Active, done.CapacityHeld.Recovery.Original)
		require.NoError(t, done.CheckClosedCapacity(original.Active.Request))
	} else {
		require.Equal(t, "unchanged", kind)
		require.Equal(t, original.BudgetHeld, done.BudgetHeld)
		var final budget.FinalizationCommand
		require.NoError(t, json.Unmarshal(done.BudgetHeld.Finalization.Command, &final))
		require.NoError(t, done.CheckClosedBudget(final.Finalized.Request))
	}
	changed, err := publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	require.NotNil(t, s.VirtualMachine().Status.MemorySize)
	n, err := allocator.Load(ctx)
	require.NoError(t, err)
	birth, _, _, err := vmguard.ReadPublished(ctx, core.CoreV1(), store.Binding(), nil)
	require.NoError(t, err)
	row, err := n.State().Placement(birth.Identity.RunnerPodUID)
	require.NoError(t, err)
	actual, exact := s.VirtualMachine().Status.MemorySize.AsInt64()
	require.True(t, exact)
	require.EqualValues(t, actual, row.MemoryBytes)
	peer.mu.Lock()
	posts, resizes := peer.reopenFixture.posts, peer.resizePosts
	peer.mu.Unlock()
	require.Equal(t, 1, posts, "完成丢回复与SDK重建不能再次开放")
	require.Zero(t, resizes, "恢复不重发PG变配")
	root.mu.Lock()
	afterRootPosts := root.posts
	root.mu.Unlock()
	require.Equal(t, rootPosts, afterRootPosts)
	// 后继零派发取消仍经正式Begin/Abort/Advance，检查新永久恢复头可被
	// 原循环继续消费；完整后继资源变配另由自动组合/PG17受影响组合验证。
	request := original.Active.Request
	request.Key.Revision++
	request.Key.OperationID.Low++
	request.ExpectedCapacityGeneration = current.Observed.CapacityGeneration
	request.BudgetCommitmentID += "-after-recovered-cleanup"
	s, _, err = store.Begin(ctx, s, session.Actor(), request, 1)
	require.NoError(t, err)
	_, err = store.RequestAbort(ctx, s, session.Actor(), request.Key)
	require.NoError(t, err)
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	t.Logf("%s取消恢复、完成CAS丢回复/SDK重建、Host实际容量结清和后继取消已完成", kind)
}
