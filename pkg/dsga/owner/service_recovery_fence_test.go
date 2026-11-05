package owner

import (
	"context"
	"encoding/json"
	"errors"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"net/http"
	"sync"
	"testing"
	"time"
)

// 原完整预算/TLS/WAL→已结算关闭→真实Elector失租接管。Native仍旧floor，
// GREEN只能经原HTTP ownerFence推进，不预先抬floor，不冒真实PG/Host验收。
func TestProductionSettledClosureRecoveryFencesLowerNativeOwner(t *testing.T) {
	productionInitialService(t, "recovery-fence-settled")
}
func verifySettledRecoveryNativeFence(t *testing.T, ctx context.Context, f *businessOwnerTransfer, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner) {
	t.Helper()
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()
	old, err := f.store.Load(ctx)
	require.NoError(t, err)
	closure := old.State().ServiceClosure
	require.NotNil(t, closure)
	require.Equal(t, journal.Settled, closure.Phase)
	require.Nil(t, closure.Rearm)
	oldEpoch := session.Actor().Epoch
	pg.mu.Lock()
	pg.leaseExpired = true
	pg.leaseDeadline = 100
	pg.rearmFixture = &serviceRearmPG{epoch: oldEpoch, api: f.api}
	pg.mu.Unlock()
	session, resources, worker = f.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
	require.NoError(t, runner.Tick(ctx))
	require.Less(t, pg.shared().Observed.IssuerFloorEpoch, session.Actor().Epoch, "Native尚未Fence；不得从新Owner事实伪抬floor")
	_, _, _, selector := closure.Selection()
	before, err := f.store.Load(ctx)
	require.NoError(t, err)
	// 来源Unknown必须在任何Fence/新Rearm之前失败，原预算和闭包均保持。
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationRearmService, selector))
	require.ErrorIs(t, err, ErrServiceSafety)
	rejected, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), rejected.State())
	require.Equal(t, oldEpoch, pg.shared().Observed.IssuerFloorEpoch)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationRearmService, selector))
	require.NoError(t, err, "settled closure lower native floor requires actual Fence before original rearm CAS")
	after, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.Settled, after.State().ServiceClosure.Rearm.Phase)
	require.Equal(t, before.State().InitialHeld, after.State().InitialHeld)
	require.Equal(t, before.State().Active, after.State().Active)
	require.Equal(t, before.State().History, after.State().History)
	original := *after.State().ServiceClosure
	original.Rearm = nil
	require.Equal(t, *closure, original, "保留完整原关闭，不伪造continuation/open")
	require.Equal(t, session.Actor().Epoch, pg.shared().Observed.IssuerFloorEpoch)
	require.False(t, pg.shared().Observed.AdmissionOpen)
	pg.mu.Lock()
	require.Equal(t, 1, pg.rearmFixture.posts)
	require.Zero(t, pg.leaseRenewals)
	pg.mu.Unlock()
}

// 下表只核共享助手的原SDK/TLS/Session租约门；完整来源由上面的生产入口组合验证。
func TestServiceRecoveryFenceObservationGates(t *testing.T) {
	for _, mode := range []string{"lower-expired-closed", "same-floor", "higher-floor", "unarmed", "not-expired", "open", "observe-error"} {
		t.Run(mode, func(t *testing.T) { t.Parallel(); verifyRecoveryFenceHelper(t, mode) })
	}
}
func TestServiceRecoveryFenceUnknownAndLeaseDriftRetainState(t *testing.T) {
	for _, mode := range []string{"fence-before-error", "fence-after-error", "fresh-observe-error", "lease-uid-drift"} {
		t.Run(mode, func(t *testing.T) { t.Parallel(); verifyRecoveryFenceHelper(t, mode) })
	}
}
func verifyRecoveryFenceHelper(t *testing.T, mode string) {
	t.Helper()
	api, store, coord := newAPI(t)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		q := pgRequest(t, session.Actor())
		observed := pgResponse(t, q).Observed
		observed.Operation = nil
		observed.CapacityGeneration = 1
		observed.AdmissionOpen = false
		observed.AdmissionRevision = 2
		observed.IssuerFloorEpoch = session.Actor().Epoch - 1
		expired, armed := true, true
		deadline := control.Uint64(100)
		leaseEpoch := observed.IssuerFloorEpoch
		if mode == "same-floor" {
			observed.IssuerFloorEpoch = session.Actor().Epoch
		}
		if mode == "higher-floor" {
			observed.IssuerFloorEpoch = session.Actor().Epoch + 1
			leaseEpoch = observed.IssuerFloorEpoch
		}
		if mode == "unarmed" {
			armed, expired, deadline, leaseEpoch = false, false, 0, 0
		}
		if mode == "not-expired" {
			expired = false
		}
		if mode == "open" {
			expired = false
			observed.AdmissionOpen = true
		}
		var mu sync.Mutex
		observes, fences := 0, 0
		config := pgConfigWithFence(t, q, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/dsg/v1/admission/lease", r.URL.Path)
			var request control.AdmissionLeaseRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			require.Equal(t, "observe", request.Action, "助手不能renew/rearm/open")
			mu.Lock()
			defer mu.Unlock()
			observes++
			if mode == "observe-error" || mode == "fresh-observe-error" && observes > 1 {
				w.WriteHeader(503)
				return
			}
			fact := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, Armed: armed, Expired: expired, DeadlineNS: deadline, LeaseEpoch: leaseEpoch, TimeoutMS: 30000, Observed: observed}
			if mode == "lease-uid-drift" {
				api.mu.Lock()
				api.lease.UID = "different-original-lease-uid"
				api.mu.Unlock()
			}
			pgReply(w, fact)
		}, func(w http.ResponseWriter, r *http.Request) {
			var request control.FenceRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			require.Equal(t, session.Actor().Epoch, request.IssuerEpoch)
			mu.Lock()
			defer mu.Unlock()
			fences++
			if mode == "fence-before-error" {
				w.WriteHeader(503)
				return
			}
			observed.IssuerFloorEpoch = request.IssuerEpoch
			if mode == "fence-after-error" {
				w.WriteHeader(503)
				return
			}
			pgReply(w, control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: request.Instance, RequestedEpoch: request.IssuerEpoch, Disposition: control.Fenced, Observed: observed})
		}, func() control.Snapshot { mu.Lock(); defer mu.Unlock(); return observed })
		config.IssuerEpoch = session.Actor().Epoch
		client, err := compute.New(ctx, config)
		require.NoError(t, err)
		defer client.CloseIdleConnections()
		worker := &PGWorker{session: session, client: client}
		before, err := store.Load(ctx)
		require.NoError(t, err)
		fact, callErr := worker.observeServiceRecoveryLease(ctx)
		mu.Lock()
		actualObserves, actualFences := observes, fences
		mu.Unlock()
		switch mode {
		case "lower-expired-closed":
			require.NoError(t, callErr)
			require.Equal(t, 2, actualObserves)
			require.Equal(t, 1, actualFences)
			require.Equal(t, session.Actor().Epoch, fact.Observed.IssuerFloorEpoch)
			require.True(t, fact.Expired)
			require.Equal(t, deadline, fact.DeadlineNS)
			require.Equal(t, leaseEpoch, fact.LeaseEpoch)
		case "same-floor":
			require.NoError(t, callErr)
			require.Equal(t, 1, actualObserves)
			require.Zero(t, actualFences)
		default:
			require.Error(t, callErr)
			if mode == "fence-before-error" || mode == "fence-after-error" || mode == "fresh-observe-error" {
				require.Equal(t, 1, actualFences)
			} else {
				require.Zero(t, actualFences)
			}
		}
		// 助手只有原Observe/Fence；无任何外层WAL、rearm/open/预算动作。
		after, err := store.Load(context.Background())
		require.NoError(t, err)
		require.Equal(t, before.State(), after.State())
		return nil
	})
	if mode == "lease-uid-drift" {
		require.True(t, err == nil || errors.Is(err, ErrLeadershipLost) || errors.Is(err, context.Canceled))
	} else {
		require.NoError(t, err)
	}
}
