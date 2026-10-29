package owner

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 前驱是真实首次服务、Root漂移、原关闭、新开放已应用丢回复且来源
// 再坏后的独立关闭。完整生产者/Reader/Session/SDK/Run/Store仍走正式链；
// PG/Root/Kube/QMP是明确peer，不将该组合当Linux物理返还认证。
func TestProductionServiceReopenRetryRecoversClosedUnknown(t *testing.T) {
	for _, mode := range []string{"normal", "lost-start", "setter-after", "lost-record", "retry-source-after-selection", "source-after-unknown-setter"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "service-retry-"+mode) })
	}
}

func TestProductionServiceReopenRetryThenAutomaticGrowShrink(t *testing.T) {
	productionInitialService(t, "service-retry-first-unknown-automatic")
}

func verifyProductionServiceReopenRetry(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture) *PGWorker {
	t.Helper()
	prior, err := store.Load(ctx)
	require.NoError(t, err)
	original := prior.State()
	require.Equal(t, journal.Unconfirmed, original.ServiceClosure.Reopen.Phase)
	require.False(t, original.ServiceClosure.Reopen.Closing.Close.Pending())
	_, _, _, selector := original.ServiceClosure.Reopen.Closing.Close.Selection()
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(worker.CloseIdleConnections)
	endpoint, stop := reopenRun(t, ctx, worker)
	command := closureControl(session, config.Instance, control.CoordinationRetryServiceReopen, selector)
	_, err = endpoint.Submit(ctx, command)
	require.ErrorIs(t, err, ErrServiceSafety, "来源仍坏时不能选择再次开放")
	unchanged, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, original, unchanged.State())
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()
	pg.mu.Lock()
	pg.reopenFixture = &serviceReopenPG{mode: mode, api: api, root: root, posts: 0, closes: 0, queries: 0, renewals: 0, selectionFault: false, setterFault: false, closeFault: false, cancelAfterOpen: nil}
	pg.mu.Unlock()
	if mode == "lost-start" {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	_, err = endpoint.Submit(ctx, command)
	if mode == "normal" || strings.HasPrefix(mode, "first-unknown-") {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
	}
	stop()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	r := s.State().ServiceClosure.Reopen
	require.EqualValues(t, 1, r.Attempt)
	require.Equal(t, original.InitialHeld, s.State().InitialHeld)
	var retired journal.ServiceReopen
	require.NoError(t, json.Unmarshal(r.Retired.Context, &retired))
	require.Equal(t, *original.ServiceClosure.Reopen, retired)
	pg.mu.Lock()
	posts, closes := pg.reopenFixture.posts, pg.reopenFixture.closes
	if mode == "lost-start" || mode == "retry-source-after-selection" {
		require.Zero(t, posts)
	} else {
		require.Equal(t, 1, posts)
	}
	pg.mu.Unlock()
	if mode == "retry-source-after-selection" || mode == "source-after-unknown-setter" {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
	}
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(worker.CloseIdleConnections)
	endpoint, stop = reopenRun(t, ctx, worker)
	// 重建后普通查询只恢复最新选择，不重发前驱开放或任何setter。
	selected := r.CommandSHA256
	if r.Closing != nil {
		_, _, _, selected = r.Closing.Close.Selection()
	}
	_, _ = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationObserveServiceReopen, selected))
	pg.mu.Lock()
	require.Equal(t, posts, pg.reopenFixture.posts)
	require.Equal(t, closes, pg.reopenFixture.closes)
	require.Zero(t, pg.reopenFixture.renewals)
	pg.mu.Unlock()
	_, err = endpoint.Submit(ctx, command)
	require.Error(t, err, "旧已消耗再关闭摘要不能重复选择")
	switch mode {
	case "lost-start", "retry-source-after-selection":
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceReopen, r.CommandSHA256))
		require.NoError(t, err)
		posts++
	case "source-after-unknown-setter":
		require.NotNil(t, r.Closing)
		require.False(t, r.Closing.Close.Pending())
		_, _, _, nextSelector := r.Closing.Close.Selection()
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationRetryServiceReopen, nextSelector))
		require.NoError(t, err)
		posts++
	}
	current, err := endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
	require.NoError(t, err)
	r = current.State.ServiceClosure.Reopen
	require.Equal(t, journal.Settled, r.Phase)
	require.Nil(t, r.Closing)
	if mode == "source-after-unknown-setter" {
		require.EqualValues(t, 2, r.Attempt)
	}
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, r.CommandSHA256))
	require.NoError(t, err)
	stop()
	complete, err := store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, complete.State().ServiceClosure)
	require.NotNil(t, complete.State().ServiceLifecycle)
	require.Equal(t, original.InitialHeld.Admission, complete.State().InitialHeld.Admission)
	pg.mu.Lock()
	require.Equal(t, posts, pg.reopenFixture.posts)
	require.Equal(t, closes, pg.reopenFixture.closes)
	pg.reopenFixture = nil
	pg.mu.Unlock()
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(worker.CloseIdleConnections)
	pending, closing := worker.client.PendingAdmission()
	require.Nil(t, pending)
	require.Nil(t, closing)
	_, err = worker.Advance(ctx)
	require.NoError(t, err, "完成后SDK不能恢复已退役开放或再关闭未知")
	return worker
}
