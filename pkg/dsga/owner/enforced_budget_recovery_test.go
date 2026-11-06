package owner

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func TestProductionEnforcedBudgetFormalRecovery(t *testing.T) {
	for _, mode := range []string{"restored", "takeover", "source-drift", "wrong-selector"} {
		t.Run(mode, func(t *testing.T) {
			productionInitialService(t, "enforced-budget-recovery-"+mode)
		})
	}
}

func verifyEnforcedBudgetRecovery(t *testing.T, ctx context.Context, mode string, f *businessOwnerTransfer,
	session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset,
	source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner) {
	t.Helper()
	require.True(t, pg.shared().Observed.AdmissionOpen)
	// 在原运行入口观察到实际 Guest 硬限漂移，先完成保护关闭。
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	closed, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, closed.State().ServiceClosure)
	require.False(t, pg.shared().Observed.AdmissionOpen)
	if mode != "source-drift" {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
	}
	if mode == "takeover" {
		session, _, worker = f.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
		require.NoError(t, runner.Tick(ctx))
	}
	before, err := f.store.Load(ctx)
	require.NoError(t, err)
	_, _, _, selector := before.State().ServiceClosure.Selection()
	if mode == "wrong-selector" {
		selector = strings.Repeat("ab", 32)
	}
	pg.mu.Lock()
	pg.reopenFixture = &serviceReopenPG{mode: "normal", api: f.api, root: root}
	pg.mu.Unlock()
	result, callErr := worker.executeControl(ctx, closureControl(session, config.Instance, lifecycleSequenceAction, selector))
	after, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State().Owner, after.State().Owner)
	require.Equal(t, before.State().InitialHeld, after.State().InitialHeld)
	require.Equal(t, before.State().HighWater, after.State().HighWater)
	require.Equal(t, before.State().History, after.State().History)
	pg.mu.Lock()
	opens, renewals := pg.reopenFixture.posts, pg.leaseRenewals
	pg.mu.Unlock()
	require.Zero(t, renewals)
	if mode == "source-drift" || mode == "wrong-selector" {
		require.Error(t, callErr)
		require.Equal(t, before.State(), after.State())
		require.False(t, pg.shared().Observed.AdmissionOpen)
		require.Zero(t, opens)
		return
	}
	require.NoError(t, callErr)
	require.Equal(t, pgexec.Recorded, result.Progress)
	require.Nil(t, after.State().ServiceClosure)
	life := after.State().ServiceLifecycle
	require.NotNil(t, life)
	require.Equal(t, "serving", life.SafetyStage)
	require.True(t, life.Current.BudgetBound())
	require.Equal(t, after.State().Owner.Epoch, life.Current.Snapshot().IssuerFloorEpoch)
	require.True(t, pg.shared().Observed.AdmissionOpen)
	require.Equal(t, 1, opens)
	var archive struct {
		Closed *journal.ServiceClosure `json:"service_closure"`
	}
	require.NoError(t, json.Unmarshal(life.Context, &archive))
	require.NotNil(t, archive.Closed)
	require.Nil(t, archive.Closed.LatestRearm())
	require.Equal(t, before.State().ServiceClosure.Request, archive.Closed.Request)
	// 完成答复丢失后再次确认只读已结清选择和真实 gate，不重复 setter。
	_, err = worker.completeServiceReopen(ctx, archive.Closed.Reopen.CommandSHA256)
	require.NoError(t, err)
	pg.mu.Lock()
	count := pg.reopenFixture.posts
	pg.mu.Unlock()
	require.Equal(t, opens, count)
}
