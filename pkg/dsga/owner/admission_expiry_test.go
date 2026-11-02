package owner

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

// 原生期限为协议peer；真实Elector接管、SDK重建、原关闭WAL及受控重开均
// 走现生产入口。不冒充Linux或真实K8s验收，不伪造较低LeaseEpoch来允许同主恢复。
func TestProductionNativeExpiryReachesOriginalPersistentRecovery(t *testing.T) {
	for _, mode := range []string{"active", "initial-unknown", "observe-unknown", "takeover-before-closure", "fence-unknown"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "closure-transfer-native-expiry-"+mode) })
	}
}

func assertNativeExpiryRetainsOriginal(t *testing.T, before, after journal.Model) {
	t.Helper()
	// 唯一新事实是独立关闭和它的CAS序号/格式；原Active/Key、未知、全部
	// 永久预算、历史和高水位必须逐字相同，不能只比容量或当前PG布尔。
	after.ServiceClosure = before.ServiceClosure
	after.Sequence = before.Sequence
	after.ProtocolVersion = before.ProtocolVersion
	require.Equal(t, before, after)
}

func verifyNativeAdmissionExpiry(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, transfer *businessOwnerTransfer, runner *vmguard.Runner) {
	t.Helper()
	_, err := worker.Advance(ctx)
	require.NoError(t, err)
	unknown := strings.HasSuffix(mode, "initial-unknown")
	if unknown {
		pg.faults(false, true)
	}
	_, err = worker.Advance(ctx)
	if unknown {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}
	if strings.HasSuffix(mode, "-active") {
		s, err := store.Load(ctx)
		require.NoError(t, err)
		_, _, err = store.Begin(ctx, s, session.Actor(), pg.request, 1)
		require.NoError(t, err)
	}
	before, err := store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, before.State().ServiceClosure)
	if unknown {
		require.Equal(t, journal.Unconfirmed, before.State().InitialHeld.Admission.Phase)
	}
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	if strings.HasSuffix(mode, "fence-unknown") {
		pg.mu.Lock()
		pg.leaseDeadline, pg.leaseExpired = 100, true
		pg.mu.Unlock()
		for _, fault := range []string{"fence-error", "fence-pending", "deadline-changed", "expiry-cleared"} {
			t.Run(fault, func(t *testing.T) {
				_, err := worker.advanceNativeAdmissionExpiry(ctx)
				require.ErrorIs(t, err, ErrServiceSafety)
				same, err := store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, before.State(), same.State(), "原串行屏障或复读事实异常不得生成关闭WAL")
			})
		}
		pg.mu.Lock()
		require.Zero(t, pg.leaseRenewals)
		pg.mu.Unlock()
		return
	}
	if strings.HasSuffix(mode, "observe-unknown") {
		pg.mu.Lock()
		pg.leaseReject = true
		pg.mu.Unlock()
		_, err = worker.advanceNativeAdmissionExpiry(ctx)
		require.ErrorIs(t, err, ErrServiceSafety)
		same, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), same.State(), "观察失败不得建立关闭或清未知")
		pg.mu.Lock()
		pg.leaseReject = false
		pg.mu.Unlock()
		_, err = worker.advanceNativeAdmissionExpiry(ctx)
		require.ErrorIs(t, err, ErrServiceSafety)
		same, err = store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), same.State(), "有效未过期观察也不得关闭")
		pg.mu.Lock()
		pg.leaseExpired = true
		pg.before.Observed.IssuerFloorEpoch = session.Actor().Epoch + 1
		pg.after.Observed.IssuerFloorEpoch = session.Actor().Epoch + 1
		pg.mu.Unlock()
		_, err = worker.advanceNativeAdmissionExpiry(ctx)
		require.ErrorIs(t, err, ErrServiceSafety)
		same, err = store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), same.State(), "原生已被更高owner隔离时不得以旧owner写关闭WAL")
		return
	}
	pg.mu.Lock()
	pg.leaseDeadline, pg.leaseExpired = 100, true
	pg.rearmFixture = &serviceRearmPG{epoch: session.Actor().Epoch, api: api}
	pg.mu.Unlock()
	takeoverFirst := strings.HasSuffix(mode, "takeover-before-closure")
	if takeoverFirst {
		session, resources, worker = nativeExpiryTakeoverBeforeClosure(t, ctx, transfer, session, resources, worker, config, core, source)
		fact, err := worker.observeAdmissionLease(ctx)
		require.NoError(t, err)
		require.True(t, fact.Expired)
		require.Less(t, fact.Observed.IssuerFloorEpoch, session.Actor().Epoch, "真实Kube接管不等于已推进PG floor")
		require.NoError(t, runner.Tick(ctx))
		before, err = store.Load(ctx)
		require.NoError(t, err)
		require.Nil(t, before.State().ServiceClosure)
	}
	// 原关闭确已应用但丢ACK；第一轮后停止旧SDK，保留原Unconfirmed供真实
	// 更高owner接管。不得以重建清未知或偷偷重放setter。
	pg.faults(false, true)
	runCtx, stop := context.WithCancel(ctx)
	err = worker.Run(runCtx, PGLoopOptions{PollInterval: 10 * time.Millisecond, Observe: func(tick Tick) {
		require.ErrorIs(t, tick.Err, ErrServiceSafety)
		stop()
	}})
	stop()
	require.ErrorIs(t, err, context.Canceled)
	closed, err := store.Load(ctx)
	require.NoError(t, err)
	c := closed.State().ServiceClosure
	require.NotNil(t, c)
	require.True(t, c.Pending(), "丢ACK不能伪造关闭确认")
	require.False(t, c.Request.Open)
	assertNativeExpiryRetainsOriginal(t, before.State(), closed.State())
	pg.mu.Lock()
	require.Zero(t, pg.leaseRenewals)
	require.Zero(t, pg.rearmFixture.posts)
	pg.mu.Unlock()
	if takeoverFirst {
		// 当前owner已高于原租约；只查原关闭ACK，不再制造第二次接管。
		_, _, err = worker.advanceServiceSafety(ctx, "")
		require.NoError(t, err)
		fact, err := worker.observeAdmissionLease(ctx)
		require.NoError(t, err)
		require.Equal(t, session.Actor().Epoch, fact.Observed.IssuerFloorEpoch)
		require.EqualValues(t, 100, fact.DeadlineNS)
		require.Equal(t, session.Actor().Epoch-1, fact.LeaseEpoch)
		require.True(t, fact.Expired, "原Fence不能恢复期限或覆盖原租约历史")
	} else {
		session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
		_, _, _, selector := c.Selection()
		result, err := worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationContinueServiceClosure, selector))
		require.NoError(t, err)
		require.False(t, result.State.ServiceClosure.Pending())
	}
	require.NoError(t, runner.Tick(ctx), "Runner原发布必须确认真正新owner floor")
	// 沿原完整预算/Root/Runner审计、原rearm/status、新开放和持久生命周期
	// 完成入口恢复；只恢复原生期限或当前open都不能解除Run过期等待。
	verifyExpiredServiceLifecycle(t, ctx, api, session, resources, worker, config, core, source, pg, root)
	final, err := store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, final.State().ServiceClosure)
	require.NotNil(t, final.State().ServiceLifecycle)
	require.True(t, pg.shared().Observed.AdmissionOpen)
	_, _, resizes := pg.counts()
	require.Zero(t, resizes)
}

// 复用原真实Elector/Lease接管；这里刻意在还没有关闭WAL时失租，不能调用
// 专供已存在关闭的takeoverClosure夹具，也不能手写新Owner或降低LeaseEpoch。
func nativeExpiryTakeoverBeforeClosure(t *testing.T, ctx context.Context, f *businessOwnerTransfer, old *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource) (*Session, *ResourceWorker, *PGWorker) {
	t.Helper()
	before, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, before.State().ServiceClosure)
	nextRun := f.start(ctx)
	select {
	case <-nextRun.ready:
		t.Fatal("后继不能越过仍续约的原owner")
	case <-time.After(200 * time.Millisecond):
	}
	f.api.mu.Lock()
	f.api.denyLeaseHolder = old.Actor().HolderID
	f.api.mu.Unlock()
	select {
	case err := <-f.active.done:
		f.active.joined = true
		require.True(t, errors.Is(err, ErrLeadershipLost))
		f.active.cancel()
	case <-ctx.Done():
		t.Fatal("原owner失租未退出")
	}
	f.active = nextRun
	next := f.await(ctx, nextRun)
	require.Equal(t, old.Actor().Epoch+1, next.Actor().Epoch)
	after, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, after.State().ServiceClosure)
	require.Equal(t, before.State().Active, after.State().Active)
	require.Equal(t, before.State().InitialHeld, after.State().InitialHeld)
	worker.CloseIdleConnections()
	resources.CloseIdleConnections()
	nextResources, err := next.NewResourceWorker(ctx, f.resourceConfig)
	require.NoError(t, err)
	t.Cleanup(nextResources.CloseIdleConnections)
	nextWorker, err := next.NewProductionPGWorker(ctx, config, nextResources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(nextWorker.CloseIdleConnections)
	return next, nextResources, nextWorker
}

// 只在明确期限已过期的新负例中注入原Fence失败/未join和复读变化。真实
// ownerFence仍处理原请求/当前floor；不把Pending或变新deadline补成关闭证明。
func nativeExpiryFenceFailures(t *testing.T, pg *initialServicePG, original http.HandlerFunc) http.HandlerFunc {
	t.Helper()
	stage := 0
	return func(w http.ResponseWriter, r *http.Request) {
		pg.mu.Lock()
		expired := pg.leaseExpired
		if expired {
			stage++
		}
		current := stage
		pg.mu.Unlock()
		if !expired {
			original(w, r)
			return
		}
		if current == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		recorder := httptest.NewRecorder()
		original(recorder, r)
		var response control.FenceResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		if current == 2 {
			response.Disposition = control.FencePending
		}
		if current == 3 {
			pg.mu.Lock()
			pg.leaseDeadline++
			pg.mu.Unlock()
		}
		if current == 4 {
			pg.mu.Lock()
			pg.leaseExpired = false
			pg.mu.Unlock()
		}
		pgReply(w, response)
	}
}
