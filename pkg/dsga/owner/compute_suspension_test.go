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
	for _, mode := range []string{"compute-suspension-before", "compute-suspension-after"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, mode) })
	}
}

func verifyComputeSuspensionRecovery(t *testing.T, ctx context.Context, mode string, store *journal.Store, worker *PGWorker, pg *initialServicePG) {
	t.Helper()
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
