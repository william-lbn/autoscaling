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
)

// 资源前驱由正式生产执行器完成；Finalize原写前CAS的丢回复留原未知。
// 不制造Finalized或手改领域Completed，关闭后旧永久头与原未知均保留。
func TestProductionPendingFinalizationSourceFailureClosesAndKeepsPriorBudget(t *testing.T) {
	for _, mode := range []string{"planned", "unconfirmed", "lost-start", "transfer", "post-start-drift"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "finalize-safety-"+mode) })
	}
}

func verifyPendingFinalizationSafety(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, client *kubefake.Clientset, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, qmp *vmQMPPeer, transfer *businessOwnerTransfer) {
	t.Helper()
	mode = strings.TrimPrefix(mode, "finalize-safety-")
	before, err := store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, before.State().InitialHeld)
	require.Nil(t, before.State().BudgetHeld)
	require.Equal(t, journal.Planned, budgetStep(t, store, journal.FinalizeBudget).Phase)
	// 正常只读审计不能发新命令或改任何永久头/序列；原Resize SDK未知仍保留。
	require.NoError(t, worker.finalize.CheckBeforeFinalization(ctx, before))
	after, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), after.State())
	if mode == "unconfirmed" {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
		_, err = worker.Advance(ctx)
		var lost *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &lost)
		require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.FinalizeBudget).Phase)
	}
	if mode == "planned" || mode == "unconfirmed" {
		mode = "source-drift"
	}
	verifyPlannedResumeServiceSafety(t, ctx, mode, api, store, client, session, resources, worker, config, source, pg, root, qmp, transfer)
	s, err := store.Load(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 15, s.State().ProtocolVersion)
	require.Equal(t, before.State().InitialHeld, s.State().InitialHeld)
	require.Nil(t, s.State().BudgetHeld, "关闭不能提前生成目标成功预算")
	phase := budgetStep(t, store, journal.FinalizeBudget).Phase
	require.True(t, phase == journal.Planned || phase == journal.Unconfirmed)
}
