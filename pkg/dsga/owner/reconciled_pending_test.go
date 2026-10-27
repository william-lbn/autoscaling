package owner

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 真实VM扩容后、Guest/PG仍未变时固定取消Cleanup，Start已提交但丢回复。
// 重建生产worker必须从原WAL恢复同一实际代次开放命令；不需要旧Finalized，
// 不准为简化恢复使用空SDK。此阶段仅测恢复pending，尚不称取消已闭合。
func verifyPartialCapacityPending(t *testing.T, ctx context.Context, api *ownerAPI, store *journal.Store, session *Session, worker *PGWorker, resources *ResourceWorker, config compute.Config, core *kubefake.Clientset, p budget.Prepared, outside budget.OutsideReservation, outRef budget.ObjectRef, host budget.HostReservation) {
	t.Helper()
	step := budgetStep(t, store, journal.ResizeVM)
	var native vmguard.Command
	require.NoError(t, json.Unmarshal(step.Command, &native))
	runner, err := vmguard.NewReader(core.CoreV1(), store.Binding(), string(session.Ownership().LeaseUID))
	require.NoError(t, err)
	source, _, err := runner.CapacityForOriginal(ctx, native, step.CommandSHA256, session.Actor())
	require.NoError(t, err)
	view, err := resources.client.InspectState(ctx)
	require.NoError(t, err)
	aggregate, err := budget.RootAggregateFromView(view)
	require.NoError(t, err)
	shared, err := worker.client.SharedCost(ctx)
	require.NoError(t, err)
	capacity, err := budget.Reconcile(budget.ReconcileInputs{Prepared: p, Shared: shared, Root: aggregate, Outside: outside, OutsideClaim: outRef, Host: host, HostClaim: source.HostClaim, VM: source, GuestUsableMemoryBytes: p.GuestUsableMemoryBytes, IssuerEpoch: session.Actor().Epoch})
	require.NoError(t, err)
	s, err := store.Load(ctx)
	require.NoError(t, err)
	s, err = store.RequestAbort(ctx, s, session.Actor(), p.Request.Key)
	require.NoError(t, err)
	opening := control.AdmissionRequest{ProtocolVersion: 1, Instance: p.Request.Key.Instance, IssuerEpoch: session.Actor().Epoch, Revision: shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: capacity.CapacityGeneration, Open: true, BudgetCommitmentID: p.Request.BudgetCommitmentID}
	command := budget.ReconciledAbortCommand{ProtocolVersion: 1, Capacity: capacity, PreparedSHA256: source.PreparedSHA256, Admission: &opening}
	raw, err := json.Marshal(command)
	require.NoError(t, err)
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, _, err = store.StartCleanup(ctx, s, session.Actor(), p.Request.Key, p.Request.Key.Instance.ComputeInstanceID, raw)
	var unknown *journal.UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	restored, err := session.NewProductionPGWorker(ctx, config, resources, core.CoreV1())
	require.NoError(t, err)
	defer restored.CloseIdleConnections()
	pending, closing := restored.client.PendingAdmission()
	require.Nil(t, closing)
	require.NotNil(t, pending)
	require.Equal(t, opening, *pending)
	require.Equal(t, p.Request.ExpectedCapacityGeneration, pending.ExpectedCapacityGeneration, "PG尚未扩，不可误用目标成功后的generation+1")
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().BudgetHeld)
	require.Nil(t, s.State().CapacityHeld, "Unknown Cleanup不能先保存新完整预算")
	require.Equal(t, journal.Unconfirmed, s.State().Active.Cleanup.Phase)
}
