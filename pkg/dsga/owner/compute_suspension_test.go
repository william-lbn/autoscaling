package owner

import (
	"context"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/stretchr/testify/require"
)

func TestComputeSuspensionReconcilesBeforeAndAfterNativeWrite(t *testing.T) {
	for _, mode := range []string{"compute-suspension-before", "compute-suspension-after", "compute-suspension-safety-closed"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, mode) })
	}
}

func verifyComputeSuspensionRecovery(t *testing.T, ctx context.Context, mode string, store *journal.Store, worker *PGWorker, pg *initialServicePG) {
	t.Helper()
	if mode == "compute-suspension-safety-closed" {
		verifySafetyClosedComputeSuspension(t, ctx, store, worker, pg)
		return
	}
	const operation = "op_suspension_test"
	before, err := store.Load(ctx)
	require.NoError(t, err)
	posts, queries, _ := pg.counts()
	progress, err := worker.suspendCompute(ctx, before, operation)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	frozen, err := store.Load(ctx)
	require.NoError(t, err)
	closing := frozen.State().ServiceClosure
	require.NotNil(t, closing)
	require.Equal(t, operation, closing.SuspensionOperation)
	require.True(t, closing.Pending())
	currentPosts, _, _ := pg.counts()
	require.Equal(t, posts, currentPosts, "persisting the close consumed the native call's deadline")

	timeoutBefore := mode == "compute-suspension-before"
	pg.faults(timeoutBefore, !timeoutBefore)
	progress, err = worker.suspendCompute(ctx, frozen, operation)
	require.Error(t, err)
	require.Equal(t, pgexec.AwaitingFact, progress)
	pg.faults(false, false)
	retained, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, closing, retained.State().ServiceClosure)
	progress, err = worker.suspendCompute(ctx, retained, operation)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	settled, err := store.Load(ctx)
	require.NoError(t, err)
	closed := settled.State().ServiceClosure
	require.False(t, closed.Pending())
	require.Equal(t, closing.Request, closed.Request)
	require.Equal(t, closing.CommandSHA256, closed.CommandSHA256)
	require.Equal(t, before.State().InitialHeld, settled.State().InitialHeld)
	require.False(t, pg.shared().Observed.AdmissionOpen)
	currentPosts, currentQueries, _ := pg.counts()
	expected := posts + 1
	if timeoutBefore {
		expected++
	}
	require.Equal(t, expected, currentPosts)
	require.GreaterOrEqual(t, currentQueries-queries, 2)

	instance, epoch := worker.Binding()
	confirmed := *closed.Receipt
	require.False(t, mayReplayComputeSuspension(closing.Request, instance, epoch, confirmed))
	confirmed.Disposition = control.AdmissionNotFound
	confirmed.Observed.AdmissionRevision = closing.Request.Revision - 1
	require.True(t, mayReplayComputeSuspension(closing.Request, instance, epoch, confirmed))
	confirmed.Observed.IssuerFloorEpoch++
	require.False(t, mayReplayComputeSuspension(closing.Request, instance, epoch, confirmed))

	pg.mu.Lock()
	pg.drainClients = 1
	pg.mu.Unlock()
	progress, err = worker.suspendCompute(ctx, settled, operation)
	require.ErrorIs(t, err, ErrServiceSafety)
	require.Equal(t, pgexec.AwaitingFact, progress)
	require.Nil(t, worker.suspensionDrain)
	pg.mu.Lock()
	pg.drainClients = 0
	pg.mu.Unlock()
	progress, err = worker.suspendCompute(ctx, settled, operation)
	require.NoError(t, err)
	require.Equal(t, pgexec.Reconciled, progress)
	require.NotNil(t, worker.suspensionDrain)
	actual, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, settled.State(), actual.State(), "waiting for clients changed ownership or rewrote the close")
}

func verifySafetyClosedComputeSuspension(t *testing.T, ctx context.Context, store *journal.Store, worker *PGWorker, pg *initialServicePG) {
	t.Helper()
	const operation = "op_safety_closed_suspension"
	before, err := store.Load(ctx)
	require.NoError(t, err)
	budget, err := before.State().ServiceBudgetID()
	require.NoError(t, err)
	instance, epoch := worker.Binding()
	observed := pg.shared().Observed
	request := control.AdmissionRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch,
		Revision: observed.AdmissionRevision + 1, ExpectedCapacityGeneration: observed.CapacityGeneration,
		Open: false, BudgetCommitmentID: budget}
	selected, _, err := store.StartServiceClosure(ctx, before, before.State().Owner, request)
	require.NoError(t, err)
	fact, err := worker.client.SetAdmission(ctx, request)
	require.NoError(t, err)
	closed, err := store.RecordServiceClosure(ctx, selected, selected.State().Owner, fact, false)
	require.NoError(t, err)
	require.True(t, closed.State().HasSettledIdleServiceClosure())
	posts, _, _ := pg.counts()
	_, err = worker.suspendCompute(ctx, closed, operation)
	require.ErrorIs(t, err, ErrServiceSafety, "an arbitrary management request is not the accepted runtime selection")
	worker.suspensionOperation = operation
	pg.mu.Lock()
	pg.drainClients = 1
	pg.mu.Unlock()
	_, err = worker.suspendCompute(ctx, closed, operation)
	require.ErrorIs(t, err, ErrServiceSafety)
	require.Nil(t, worker.suspensionDrain)
	pg.mu.Lock()
	pg.drainClients = 0
	pg.mu.Unlock()
	progress, err := worker.suspendCompute(ctx, closed, operation)
	require.NoError(t, err)
	require.Equal(t, pgexec.Reconciled, progress)
	require.NotNil(t, worker.suspensionDrain)
	require.NoError(t, closed.State().CheckClosedSafetyDrain(*worker.suspensionDrain))
	require.Error(t, closed.State().CheckComputeSuspensionDrain(operation, *worker.suspensionDrain), "original safety close was not relabelled")
	for _, mutate := range []func(*control.PreparedResponse){
		func(f *control.PreparedResponse) { f.PreparedSlots++ },
		func(f *control.PreparedResponse) { f.Connections.Work.ClientProcesses++ },
		func(f *control.PreparedResponse) { f.Connections.Work.Observed.AdmissionRevision++ },
		func(f *control.PreparedResponse) { f.Connections.Work.Observed.IssuerFloorEpoch++ },
		func(f *control.PreparedResponse) { f.Connections.Work.Observed.Instance.PGBootID++ },
	} {
		changed := *worker.suspensionDrain
		mutate(&changed)
		require.Error(t, closed.State().CheckClosedSafetyDrain(changed))
	}
	after, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, closed.State(), after.State(), "suspension rewrote original closure, ownership or budget")
	currentPosts, _, _ := pg.counts()
	require.Equal(t, posts, currentPosts, "observing a safety-closed drain dispatched another native setter")
	require.False(t, pg.shared().Observed.AdmissionOpen)
}
