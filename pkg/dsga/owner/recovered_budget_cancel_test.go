package owner

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 真正首次服务/Grow/异常关闭/Elector接管/独立结算后继续新Grow，正式
// Runner取消未派发动作，独立Cleanup和Host结清仍走生产链。PG/Root/Kube/
// QMP是明确协议peer，本测试不代表Mac真实cgroup或Linux物理返还验收。
func TestProductionRecoveredBudgetThenUnchangedCancel(t *testing.T) {
	productionInitialService(t, "finalize-safety-recover-planned-unchanged-takeover")
}

func verifyRecoveredBudgetUnchangedCancel(t *testing.T, ctx context.Context, api *ownerAPI, store *journal.Store, client *kubefake.Clientset, coord coordclient.CoordinationV1Interface, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, runner *vmguard.Runner, qmp *vmQMPPeer, pg *initialServicePG, root *guestRootFixture) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	before := s.State()
	require.Nil(t, before.Active)
	require.NotNil(t, before.BudgetHeld.Recovery)
	original := before.BudgetHeld.Recovery.Original
	require.Equal(t, journal.Planned, original.Steps[len(original.Steps)-2].Phase)
	f, err := before.BudgetFor(original.Request)
	require.NoError(t, err)
	require.Greater(t, f.SettlementEpoch, f.IssuerEpoch)
	require.Equal(t, session.Actor().Epoch, f.SettlementEpoch)
	// 复用原Node账本和正在续约的原Lease，不重新初始化同名来源。
	node, err := client.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	var ledger hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger))
	allocator, err := hostguard.NewStore(client.CoreV1(), client.CoordinationV1(), ledger.Binding, "host-fixture-holder")
	require.NoError(t, err)
	publisher, err := hostexec.New(client.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	changed, err := publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, changed)

	request := f.Request
	request.Key.IssuerEpoch = session.Actor().Epoch
	request.Key.Revision = before.HighWater.Revision + 1
	request.Key.OperationID.Low++
	request.ExpectedCapacityGeneration = f.Shared.Observed.CapacityGeneration
	request.TargetBuffers = 2048
	request.TargetVMMemoryBytes = 2 << 30
	request.BudgetCommitmentID += "-cancelled-after-settlement"
	_, disposition, err := store.Begin(ctx, s, session.Actor(), request, 1)
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	advancePartialStep(t, ctx, store, worker, journal.PrepareBudget, journal.Settled)
	_, err = publisher.Reconcile(ctx)
	require.NoError(t, err)
	advancePartialStep(t, ctx, store, worker, journal.ReserveHost, journal.Settled)
	advancePartialStep(t, ctx, store, worker, journal.ResizeVM, journal.Unconfirmed)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	_, err = store.RequestAbort(ctx, s, session.Actor(), request.Key)
	require.NoError(t, err)
	require.NoError(t, runner.Tick(ctx))
	advancePartialStep(t, ctx, store, worker, journal.ResizeVM, journal.Settled)
	vm := budgetStep(t, store, journal.ResizeVM)
	require.Equal(t, journal.NoEffect, vm.Observation.Result)
	pgBefore, _, resizeBefore := pg.counts()
	root.mu.Lock()
	rootPosts := root.posts
	root.mu.Unlock()
	// 原Cleanup Start已提交但回复丢失。新SDK只恢复相同只读清理，不能
	// 重发资源、替换原结算预算，或将原Finalize Planned补写Completed。
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, err = worker.Advance(ctx)
	var lost *journal.UnconfirmedWrite
	require.ErrorAs(t, err, &lost)
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.NoError(t, s.State().CheckUnchangedAbort())
	var cleanup budget.UnchangedAbortCommand
	require.NoError(t, json.Unmarshal(s.State().Active.Cleanup.Command, &cleanup))
	require.Nil(t, cleanup.Admission, "当前已开放，取消清理应始终只读")
	progress, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	done := s.State()
	require.Nil(t, done.Active)
	require.Equal(t, before.BudgetHeld, done.BudgetHeld)
	require.Equal(t, original, done.BudgetHeld.Recovery.Original)
	require.Equal(t, journal.AbortedAfterReconcile, done.History[len(done.History)-1].Outcome)
	require.NoError(t, done.CheckClosedBudget(f.Request))
	require.Equal(t, request.Key, done.BudgetBoundary.ThroughKey)
	reader, err := vmguard.NewReader(client.CoreV1(), binding(), string(session.Ownership().LeaseUID))
	require.NoError(t, err)
	current, err := reader.CheckCurrentBudget(ctx, done)
	require.NoError(t, err)
	require.NotNil(t, current.Cancellation, "当前预算读者应同时认证原完成与新取消")
	changed, err = publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	n, err := allocator.Load(ctx)
	require.NoError(t, err)
	birth, _, _, err := vmguard.ReadPublished(ctx, client.CoreV1(), binding(), nil)
	require.NoError(t, err)
	row, err := n.State().Placement(birth.Identity.RunnerPodUID)
	require.NoError(t, err)
	require.Equal(t, f.Request.TargetVMMemoryBytes, row.MemoryBytes)
	require.Equal(t, request.Key, row.Settled.ClosedThrough())
	pgAfter, _, resizeAfter := pg.counts()
	require.Equal(t, pgBefore, pgAfter)
	require.Equal(t, resizeBefore, resizeAfter)
	root.mu.Lock()
	rootAfter := root.posts
	root.mu.Unlock()
	require.Equal(t, rootPosts, rootAfter)
	qmp.mu.Lock()
	actual, writes := qmp.actual, qmp.writes
	qmp.mu.Unlock()
	require.Equal(t, uint64(f.Request.TargetVMMemoryBytes), actual)
	require.Equal(t, 1, writes, "后继未派发取消不得额外写QMP")
	t.Log("更高owner独立结算后取消、Start丢回复/SDK重建、原预算读者及Host结清全部完成，PG/Guest/QMP零新增写")
}
