package owner

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 完整首次生产→开放已应用丢回复→来源坏→独立再关闭→真实Elector失租
// 接管→Runner新floor发布→期限恢复→业务重试/完成。所有恢复选择由正式
// 管理Run/Reader/TLS/SDK/typed UID-RV CAS产生；PG/Root/Kube/QMP是明确
// peer，不能把该组合当成原生PG或Linux物理返还认证。
func TestProductionReopenLeaseRecoveryAcrossCrashWindows(t *testing.T) {
	for _, mode := range []string{"normal", "lost-start", "setter-before", "setter-after", "lost-record", "source-after-selection", "source-after-setter", "cancel-after-setter", "repeated"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "service-retry-expired-"+mode) })
	}
}

func TestProductionReopenLeaseRecoveryThenAutomaticGrowShrink(t *testing.T) {
	productionInitialService(t, "service-retry-expired-automatic")
}

func verifyProductionReopenLeaseRecovery(t *testing.T, ctx context.Context, mode string, transfer *businessOwnerTransfer, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner) (*Session, *ResourceWorker, *PGWorker) {
	t.Helper()
	s, err := transfer.store.Load(ctx)
	require.NoError(t, err)
	original := s.State()
	prior := original.ServiceClosure.Reopen
	require.Equal(t, journal.Unconfirmed, prior.Phase)
	require.False(t, prior.Closing.Close.Pending())
	_, _, _, selector := prior.Closing.Close.Selection()
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()
	pg.mu.Lock()
	pg.reopenFixture = nil
	pg.leaseExpired = true
	pg.rearmFixture = &serviceRearmPG{epoch: session.Actor().Epoch, original: nil, applied: 0, posts: 0, queries: 0, loseBefore: mode == "setter-before", loseAfter: mode == "setter-after", loseRecord: mode == "lost-record", api: transfer.api, afterObserve: nil, afterApply: nil}
	pg.mu.Unlock()
	cycles := 1
	if mode == "repeated" {
		cycles = 3
	}
	for cycle := 1; cycle <= cycles; cycle++ {
		session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
		require.NoError(t, runner.Tick(ctx), "只允许真实Runner轮次发布新owner执行权")
		endpoint, stop := reopenRun(t, ctx, worker)
		// 来源仍坏时必须零选择/零setter；修复后同一原摘要才可继续。
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationRearmServiceReopen, selector))
		require.ErrorIs(t, err, ErrServiceSafety)
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
		pg.mu.Lock()
		if mode == "source-after-selection" {
			pg.rearmFixture.afterObserve = func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes++; root.mu.Unlock() }
		}
		if mode == "source-after-setter" {
			pg.rearmFixture.afterApply = func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes++; root.mu.Unlock() }
		}
		submitCtx, submitCancel := context.WithCancel(ctx)
		if mode == "cancel-after-setter" {
			pg.rearmFixture.afterApply = submitCancel
		}
		beforePosts := pg.rearmFixture.posts
		pg.mu.Unlock()
		if mode == "lost-start" {
			transfer.api.mu.Lock()
			transfer.api.loseJournalReply = true
			transfer.api.mu.Unlock()
		}
		_, err = endpoint.Submit(submitCtx, closureControl(session, config.Instance, control.CoordinationRearmServiceReopen, selector))
		submitCancel()
		if mode == "normal" || mode == "repeated" || mode == "automatic" {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
		stop()
		s, err = transfer.store.Load(ctx)
		require.NoError(t, err)
		r := s.State().ServiceClosure.Reopen.Rearm
		require.NotNil(t, r)
		require.EqualValues(t, cycle, r.Cycle)
		require.Equal(t, original.InitialHeld, s.State().InitialHeld)
		pg.mu.Lock()
		posts := pg.rearmFixture.posts
		pg.mu.Unlock()
		if mode == "lost-start" || mode == "source-after-selection" {
			require.Equal(t, beforePosts, posts)
		} else {
			require.Equal(t, beforePosts+1, posts)
		}
		if mode == "source-after-selection" || mode == "source-after-setter" {
			root.mu.Lock()
			root.view.Actual.MemoryMaxBytes--
			root.mu.Unlock()
		}
		worker.CloseIdleConnections()
		worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
		require.NoError(t, err)
		t.Cleanup(worker.CloseIdleConnections)
		endpoint, stop = reopenRun(t, ctx, worker)
		// 重建和普通Run都只能查询准确原恢复；旧关闭selector不得驱动它。
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationObserveServiceReopen, selector))
		require.ErrorIs(t, err, ErrServiceSafety)
		_, _ = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationObserveServiceReopenRearm, r.CommandSHA256))
		pg.mu.Lock()
		require.Equal(t, posts, pg.rearmFixture.posts)
		pg.mu.Unlock()
		if mode == "lost-start" || mode == "setter-before" || mode == "source-after-selection" {
			_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceReopenRearm, r.CommandSHA256))
			require.NoError(t, err)
			posts++
		}
		require.Eventually(t, func() bool {
			s, err := transfer.store.Load(ctx)
			require.NoError(t, err)
			return s.State().ServiceClosure.Reopen.Rearm.Phase == journal.Settled
		}, 5*time.Second, 10*time.Millisecond)
		stop()
		s, err = transfer.store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, prior.Request, s.State().ServiceClosure.Reopen.Request)
		require.Equal(t, journal.Unconfirmed, s.State().ServiceClosure.Reopen.Phase)
		require.False(t, pg.shared().Observed.AdmissionOpen)
		pg.mu.Lock()
		require.Equal(t, posts, pg.rearmFixture.posts)
		require.Zero(t, pg.leaseRenewals)
		pg.mu.Unlock()
		if cycle < cycles {
			selector = r.CommandSHA256
			pg.mu.Lock()
			pg.leaseExpired = true
			pg.mu.Unlock()
		}
	}
	// 恢复期限不能清业务关闭或自动重放原开放；显式同容量重试保存
	// 完整含rearm前驱，完成后才解除Run本地过期等待并恢复正常策略。
	endpoint, stop := reopenRun(t, ctx, worker)
	pg.mu.Lock()
	pg.reopenFixture = &serviceReopenPG{mode: "normal", api: transfer.api, root: root, posts: 0, closes: 0, queries: 0, renewals: 0, selectionFault: false, setterFault: false, closeFault: false, cancelAfterOpen: nil}
	pg.mu.Unlock()
	_, _, _, selected := s.State().ServiceClosure.Reopen.Closing.Close.Selection()
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationRetryServiceReopen, selected))
	require.NoError(t, err)
	state, err := endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
	require.NoError(t, err)
	r := state.State.ServiceClosure.Reopen
	var retired journal.ServiceReopen
	require.NoError(t, json.Unmarshal(r.Retired.Context, &retired))
	require.Equal(t, *s.State().ServiceClosure.Reopen, retired)
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, r.CommandSHA256))
	require.NoError(t, err)
	stop()
	complete, err := transfer.store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, complete.State().ServiceClosure)
	require.NotNil(t, complete.State().ServiceLifecycle)
	if original.Active != nil {
		// 13/14已有完整资源预算；期限恢复同样不能改写原Active，
		// 独立业务完成才在同CAS归档原操作与覆盖，永久预算逐字保留。
		require.Nil(t, complete.State().Active)
		require.Equal(t, original.BudgetHeld, complete.State().BudgetHeld)
		record := complete.State().History[len(complete.State().History)-1]
		require.Equal(t, *original.Active, record.Operation)
		require.Equal(t, journal.RecoveredAfterResourceCommit, record.Outcome)
	} else {
		require.Equal(t, original.InitialHeld.Admission, complete.State().InitialHeld.Admission)
	}
	pg.mu.Lock()
	require.Equal(t, 1, pg.reopenFixture.posts)
	require.Zero(t, pg.reopenFixture.closes)
	pg.reopenFixture, pg.rearmFixture = nil, nil
	pg.mu.Unlock()
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(worker.CloseIdleConnections)
	_, err = worker.Advance(ctx)
	require.NoError(t, err)
	t.Logf("再关闭期限恢复完成：mode=%s cycles=%d owner=%d，原未知及预算保留", strings.TrimSpace(mode), cycles, session.Actor().Epoch)
	return session, resources, worker
}
