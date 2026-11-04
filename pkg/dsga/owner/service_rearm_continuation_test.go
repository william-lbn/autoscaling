package owner

import (
	"context"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// Elector 真实接管、Runner floor、正式 SDK HTTP、Run 管理与 UID/RV CAS；
// PG/Root/Kube 为已声明的合同 peer，不冒充真实 PostgreSQL 或物理 K8s。
func TestProductionClosedRearmContinuationUsesFreshHigherOwnerAndOriginalWAL(t *testing.T) {
	for _, mode := range []string{"normal", "lost-start", "setter-after", "source-after-selection"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			productionInitialService(t, "closure-transfer-rearm-closed-only-"+mode)
		})
	}
}
func verifyClosedRearmContinuation(t *testing.T, ctx context.Context, mode string, f *businessOwnerTransfer, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner) {
	t.Helper()
	initial, err := f.store.Load(ctx)
	require.NoError(t, err)
	original := initial.State().ServiceClosure.Rearm
	selected := original.CommandSHA256
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationContinueServiceRearm, selected))
	require.Error(t, err, "同 owner 不得再次延长期限")
	session, resources, worker = f.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
	require.NoError(t, runner.Tick(ctx))
	if mode == "lost-start" {
		f.api.mu.Lock()
		f.api.loseJournalReply = true
		f.api.mu.Unlock()
	}
	pg.mu.Lock()
	baselinePosts := pg.rearmFixture.posts
	pg.rearmFixture.loseAfter = mode == "setter-after"
	if mode == "source-after-selection" {
		pg.rearmFixture.afterObserve = func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes++; root.mu.Unlock() }
	}
	pg.mu.Unlock()
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationContinueServiceRearm, selected))
	if mode == "normal" {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
	}
	after, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, original, after.State().ServiceClosure.Rearm)
	require.NotNil(t, after.State().ServiceClosure.RearmContinuation)
	latest := after.State().ServiceClosure.LatestRearm()
	require.Greater(t, latest.Origin.Epoch, original.Origin.Epoch)
	require.Nil(t, after.State().ServiceClosure.Reopen)
	if mode == "source-after-selection" {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
	}
	if mode == "lost-start" || mode == "source-after-selection" {
		pg.mu.Lock()
		actualPosts := pg.rearmFixture.posts
		pg.mu.Unlock()
		require.Equal(t, baselinePosts, actualPosts)
		_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationObserveServiceRearm, latest.CommandSHA256))
		require.Error(t, err)
		_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationContinueServiceRearm, selected))
		require.Error(t, err)
		_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceRearm, latest.CommandSHA256))
		require.NoError(t, err)
	} else if mode == "setter-after" {
		_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationObserveServiceRearm, latest.CommandSHA256))
		require.NoError(t, err)
	}
	settled, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.Settled, settled.State().ServiceClosure.LatestRearm().Phase)
	require.Equal(t, initial.State().InitialHeld, settled.State().InitialHeld)
	require.Equal(t, initial.State().Active, settled.State().Active)
	frozen := settled.State()
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationContinueServiceRearm, selected))
	require.Error(t, err)
	unchanged, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, frozen, unchanged.State())
	pg.mu.Lock()
	actualPosts, expired := pg.rearmFixture.posts, pg.leaseExpired
	pg.reopenFixture = &serviceReopenPG{mode: "normal", api: f.api, root: root}
	pg.mu.Unlock()
	require.Equal(t, baselinePosts+1, actualPosts)
	require.False(t, expired)
	_, _, _, closed := settled.State().ServiceClosure.Selection()
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, closed))
	require.NoError(t, err)
	opened, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.Settled, opened.State().ServiceClosure.Reopen.Phase)
	require.Equal(t, original, opened.State().ServiceClosure.Rearm)
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, opened.State().ServiceClosure.Reopen.CommandSHA256))
	require.NoError(t, err)
	completed, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, completed.State().ServiceClosure)
	require.NotNil(t, completed.State().ServiceLifecycle)
	require.True(t, strings.Contains(string(completed.State().ServiceLifecycle.Context), "rearm_continuation"), "完整历史进入正式生命周期")
}
