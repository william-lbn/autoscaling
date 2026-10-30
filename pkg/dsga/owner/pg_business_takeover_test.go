package owner

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

func pgTakeoverHostStore(t *testing.T, ctx context.Context, client *kubefake.Clientset) *hostguard.Store {
	t.Helper()
	node, err := client.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	var ledger hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger))
	store, err := hostguard.NewStore(client.CoreV1(), client.CoordinationV1(), ledger.Binding, "host-fixture-holder")
	require.NoError(t, err)
	return store
}

func keepPGTakeoverHostOwner(t *testing.T, ctx context.Context, client *kubefake.Clientset) func() {
	t.Helper()
	return keepInitialHostOwner(t, ctx, client, pgTakeoverHostStore(t, ctx, client))
}

// 原PG已Grow、SDK回复未知后真实失租；不新发Resize，保存原终态，再按当前实际
// PG/Root/VM/完整资格结清取消预算与Host。这里的原生执行和Kube/QMP为明确peer，
// Elector、SDK、WAL及全部预算/Host读者使用生产实现，不外推真实Linux效果。
func verifyLostPGTakeoverSettlement(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, runner *vmguard.Runner, qmp *vmQMPPeer, pg *initialServicePG, prepared budget.Prepared, transfer *businessOwnerTransfer) {
	t.Helper()
	original := budgetStep(t, store, journal.ResizePG)
	require.Equal(t, journal.Unconfirmed, original.Phase)
	require.NotNil(t, worker.client.Pending())
	pg.mu.Lock()
	require.True(t, pg.resized)
	require.Equal(t, 1, pg.resizePosts)
	require.NotNil(t, pg.resizeReceipt)
	require.Equal(t, control.Committed, pg.resizeReceipt.Receipt.Result.Operation.Phase)
	pg.mu.Unlock()
	session, _, worker = transfer.takeover(t, ctx, session, resources, worker, config, client, source, runner)
	require.NotNil(t, worker.client.Pending(), "新owner从完整原WAL恢复未知SDK，不创建空栅栏")
	_, err := worker.ReplayOriginal(ctx, prepared.Request.Key)
	require.Error(t, err, "新owner不能重发旧epoch原PG Resize")
	snapshot, err := store.Load(ctx)
	require.NoError(t, err)
	_, err = store.RequestAbort(ctx, snapshot, session.Actor(), prepared.Request.Key)
	require.NoError(t, err)
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, err = worker.Advance(ctx)
	var lostRecord *journal.UnconfirmedWrite
	require.ErrorAs(t, err, &lostRecord, "原PG已完成回执Record实际提交后丢回复")
	settled := budgetStep(t, store, journal.ResizePG)
	require.Equal(t, journal.Settled, settled.Phase)
	require.Equal(t, journal.Completed, settled.Observation.Result, "TooLate不是NoEffect；原PG Grow确已提交")
	require.Equal(t, original.CommandSHA256, settled.CommandSHA256)
	require.Equal(t, original.Command, settled.Command)
	for range 8 {
		snapshot, err = store.Load(ctx)
		require.NoError(t, err)
		if snapshot.State().Active == nil {
			break
		}
		progress, err := worker.Advance(ctx)
		require.NoError(t, err)
		require.Contains(t, []pgexec.Progress{pgexec.Recorded, pgexec.Reconciled}, progress)
	}
	snapshot, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, snapshot.State().Active)
	require.Nil(t, worker.client.Pending(), "必须以原终态只读核对解除SDK，不能靠重建绕过未知")
	require.Nil(t, snapshot.State().InitialHeld)
	require.NotNil(t, snapshot.State().CapacityHeld)
	require.Equal(t, journal.AbortedAfterReconcile, snapshot.State().History[0].Outcome)
	require.NoError(t, snapshot.State().CheckClosedCapacity(prepared.Request))
	claims, err := budgetclaims.New(client.CoreV1(), binding())
	require.NoError(t, err)
	current, err := claims.ReadCurrent(ctx, snapshot)
	require.NoError(t, err)
	require.NoError(t, claims.RecheckCurrent(ctx, snapshot, current))
	allocator := pgTakeoverHostStore(t, ctx, client)
	coord, err := coordclient.NewForConfig(api.config)
	require.NoError(t, err)
	publisher, err := hostexec.New(client.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	changed, err := publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	node, err := allocator.Load(ctx)
	require.NoError(t, err)
	birth, _, _, err := vmguard.ReadPublished(ctx, client.CoreV1(), binding(), nil)
	require.NoError(t, err)
	placement, err := node.State().Placement(birth.Identity.RunnerPodUID)
	require.NoError(t, err)
	require.Equal(t, prepared.Request.TargetVMMemoryBytes, placement.MemoryBytes)
	require.Equal(t, prepared.Request.TargetVMMemoryBytes, placement.Settled.ActualMemoryBytes)
	pg.mu.Lock()
	require.Equal(t, 1, pg.resizePosts, "接管、取消及终态核对不得重发Resize")
	require.GreaterOrEqual(t, pg.resizeQueries, 2, "原取消TooLate和终态只读核验均实际执行")
	require.Equal(t, prepared.Request, pg.request, "原PG命令未改epoch或更换Key")
	pg.mu.Unlock()
	qmp.mu.Lock()
	require.Equal(t, 1, qmp.writes)
	qmp.mu.Unlock()
	// 不重复后继完整Grow矩阵；只以新owner新Key生产下一Shrink Prepare，确认
	// 旧512MiB Initial与旧未知没有污染当前1GiB预算和新操作来源。
	next := prepared.Request
	next.Key.IssuerEpoch = session.Actor().Epoch
	next.Key.Revision++
	next.Key.OperationID.Low++
	next.Kind, next.TargetBuffers = control.Shrink, 512
	next.ExpectedCapacityGeneration++
	next.BudgetCommitmentID = "post-pg-takeover-shrink"
	snapshot, err = store.Load(ctx)
	require.NoError(t, err)
	_, _, err = store.Begin(ctx, snapshot, session.Actor(), next, 0)
	require.NoError(t, err)
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	var following budget.Prepared
	require.NoError(t, json.Unmarshal(budgetStep(t, store, journal.PrepareBudget).Observation.Data, &following))
	require.Equal(t, next, following.Request)
	require.Equal(t, prepared.Request.TargetVMMemoryBytes, following.VMBeforeMemoryBytes)
	require.NoError(t, claims.CheckOutside(ctx, following))
}
