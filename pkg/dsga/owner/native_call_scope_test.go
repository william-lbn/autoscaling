package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/stretchr/testify/require"
)

func withNativeCallScopeSession(t *testing.T, run func(context.Context, *ownerAPI, *Session, *authorityScopeGETs)) {
	t.Helper()
	api, store, coord := newAPI(t)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		gets := countAuthorityScopeGETs(t, api, session)
		run(ctx, api, session, gets)
		return nil
	})
	require.NoError(t, err)
}

func TestNativeCallAuthorityScopeSingleUseCancellationAndBinding(t *testing.T) {
	for _, mode := range []string{"single-use", "revoked", "different-session", "different-actor", "caller-cancelled", "session-cancelled", "concurrent-consume"} {
		t.Run(mode, func(t *testing.T) {
			withNativeCallScopeSession(t, func(ctx context.Context, _ *ownerAPI, session *Session, gets *authorityScopeGETs) {
				gets.reset()
				nativeCtx, snapshot, finish, err := session.checkedCredentialCall(ctx)
				require.NoError(t, err)
				defer finish()
				require.NotNil(t, snapshot)
				require.Equal(t, 3, gets.count(), "scope出生必须真实Lease→VM→Lease")
				gets.reset()
				switch mode {
				case "single-use":
					require.NoError(t, session.CheckCredentialCall(nativeCtx))
					require.Zero(t, gets.count())
					require.NoError(t, session.CheckCredentialCall(nativeCtx))
					require.Equal(t, 3, gets.count(), "同ctx第二次HTTP不能沿用旧Check")
				case "revoked":
					finish()
					require.NoError(t, session.CheckCredentialCall(nativeCtx))
					require.Equal(t, 3, gets.count())
				case "different-session":
					other := *session
					require.NoError(t, other.CheckCredentialCall(nativeCtx))
					require.Equal(t, 3, gets.count(), "同actor也不能借另一个Session的scope")
					gets.reset()
					require.NoError(t, session.CheckCredentialCall(nativeCtx))
					require.Zero(t, gets.count())
				case "different-actor":
					original := session.actor
					session.actor.Epoch++
					require.Error(t, session.CheckCredentialCall(nativeCtx))
					require.Positive(t, gets.count())
					session.actor = original
				case "caller-cancelled":
					cancelled, cancel := context.WithCancel(nativeCtx)
					cancel()
					require.Error(t, session.CheckCredentialCall(cancelled))
				case "session-cancelled":
					original := session.ctx
					cancelled, cancel := context.WithCancel(original)
					cancel()
					session.ctx = cancelled
					require.ErrorIs(t, session.CheckCredentialCall(nativeCtx), ErrLeadershipLost)
					session.ctx = original
				case "concurrent-consume":
					errors := make(chan error, 2)
					var done sync.WaitGroup
					for range 2 {
						done.Add(1)
						go func() { defer done.Done(); errors <- session.CheckCredentialCall(nativeCtx) }()
					}
					done.Wait()
					close(errors)
					for err := range errors {
						require.NoError(t, err)
					}
					require.Equal(t, 3, gets.count(), "并发最多一次可消费；另一调用仍完整fresh Check")
				}
			})
		})
	}
}

func TestNativeCallAuthorityScopeAuthorityDriftAndVMRV(t *testing.T) {
	for _, mode := range []string{"lease-uid", "holder", "vm-uid", "vm-rv"} {
		t.Run(mode, func(t *testing.T) {
			withNativeCallScopeSession(t, func(ctx context.Context, api *ownerAPI, session *Session, _ *authorityScopeGETs) {
				before, err := session.store.Load(ctx)
				require.NoError(t, err)
				api.mu.Lock()
				originalLease, originalVM := api.lease.DeepCopy(), api.vm.DeepCopy()
				switch mode {
				case "lease-uid":
					api.lease.UID = "new-lease-uid"
				case "holder":
					other := "other-owner"
					api.lease.Spec.HolderIdentity = &other
				case "vm-uid":
					api.vm.UID = "new-vm-uid"
				case "vm-rv":
					api.vmVersion++
					api.vm.ResourceVersion = "scope-rv-changed"
				}
				api.mu.Unlock()
				if mode == "vm-rv" {
					// final-RV门必须在nil client有机会发任何Native请求前拒绝。
					worker := &PGWorker{session: session}
					err := worker.renewAuditedAdmissionLease(ctx, before, control.AdmissionLeaseResponse{}, nil)
					require.ErrorIs(t, err, ErrServiceSafety)
				} else {
					_, _, _, err := session.checkedCredentialCall(ctx)
					require.Error(t, err)
				}
				api.mu.Lock()
				api.lease, api.vm = originalLease, originalVM
				api.mu.Unlock()
			})
		})
	}
}

func TestNativeCallAuthorityScopeStaticTokenFreshFallback(t *testing.T) {
	withNativeCallScopeSession(t, func(ctx context.Context, _ *ownerAPI, session *Session, gets *authorityScopeGETs) {
		request := pgRequest(t, session.Actor())
		config := pgConfig(t, request, nil)
		config.IssuerEpoch = session.Actor().Epoch
		client, err := compute.New(ctx, config)
		require.NoError(t, err)
		defer client.CloseIdleConnections()
		worker := &PGWorker{session: session, client: client}
		before, err := session.store.Load(ctx)
		require.NoError(t, err)
		observed := pgResponse(t, request).Observed
		observed.Operation, observed.AdmissionOpen, observed.AdmissionRevision, observed.CapacityGeneration = nil, false, 0, 1
		fact := control.AdmissionLeaseResponse{ProtocolVersion: 1, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: request.Key.Instance, IssuerEpoch: session.Actor().Epoch, ExpectedCapacityGeneration: 1, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Armed: true, DeadlineNS: 1000000000, LeaseEpoch: session.Actor().Epoch, TimeoutMS: 30000, Observed: observed}
		gets.reset()
		require.NoError(t, worker.renewAuditedAdmissionLease(ctx, before, fact, nil))
		require.Equal(t, 4, gets.count(), "静态Token保外层fresh Check及原独立末VM Load")
		after, err := session.store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), after.State())
	})
}

// 明确不把本机peer计数当服务端权限/内核验收；其Unknown/Native fence沿原组合根回归。

func TestNativeCallAuthorityScopeHelperRetainsIndependentCheck(t *testing.T) {
	productionInitialService(t, "lifecycle-lease-native-call-scope")
}

func TestNativeCallAuthorityScopeStaticTokenProvider(t *testing.T) {
	productionInitialService(t, "native-lease-call-scope-static")
}

func TestNativeCallAuthorityScopeCreatorCancellationCannotBeDetached(t *testing.T) {
	withNativeCallScopeSession(t, func(ctx context.Context, _ *ownerAPI, session *Session, gets *authorityScopeGETs) {
		birth, cancel := context.WithCancel(ctx)
		scoped, _, finish, err := session.checkedCredentialCall(birth)
		require.NoError(t, err)
		defer finish()
		cancel()
		detached := context.WithoutCancel(scoped)
		gets.reset()
		require.NoError(t, session.CheckCredentialCall(detached), "剥离取消只允许退回原fresh Check，不借旧scope")
		require.Equal(t, 3, gets.count(), "原创建ctx已取消，即使消费者Err=nil也必须真实三GET")
	})
}

func TestNativeCallAuthorityScopeLastLeaseRVChangeKeepsOriginalGuard(t *testing.T) {
	withNativeCallScopeSession(t, func(ctx context.Context, api *ownerAPI, session *Session, gets *authorityScopeGETs) {
		request := pgRequest(t, session.Actor())
		observed := pgResponse(t, request).Observed
		observed.Operation, observed.AdmissionOpen, observed.AdmissionRevision, observed.CapacityGeneration = nil, false, 0, 1
		var posts atomic.Int32
		config := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/dsg/v1/admission/lease", r.URL.Path)
			posts.Add(1)
			var q control.AdmissionLeaseRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&q))
			pgReply(w, control.AdmissionLeaseResponse{ProtocolVersion: 1, Request: q, Armed: true, Renewed: q.Action == "renew", DeadlineNS: 20, LeaseEpoch: session.Actor().Epoch, TimeoutMS: 30000, Observed: observed})
		}, func() control.Snapshot { return observed })
		config.IssuerEpoch = session.Actor().Epoch
		client, err := compute.New(ctx, config)
		require.NoError(t, err)
		defer client.CloseIdleConnections()
		worker := &PGWorker{session: session, client: client}
		before, err := session.store.Load(ctx)
		require.NoError(t, err)
		fact := control.AdmissionLeaseResponse{ProtocolVersion: 1, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: request.Key.Instance, IssuerEpoch: session.Actor().Epoch, ExpectedCapacityGeneration: 1, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Armed: true, DeadlineNS: 10, LeaseEpoch: session.Actor().Epoch, TimeoutMS: 30000, Observed: observed}
		var leaseReads atomic.Int32
		gets.setBeforeGET(func(path string) {
			if strings.Contains(path, "/leases/") && leaseReads.Add(1) == 2 {
				api.mu.Lock()
				api.vmVersion++
				api.vm.ResourceVersion = "legitimate-rv-during-final-lease"
				api.mu.Unlock()
			}
		})
		defer gets.setBeforeGET(nil)
		err = worker.renewAuditedAdmissionLease(ctx, before, fact, nil)
		require.Zero(t, posts.Load(), "不得把较早Check内Snapshot当末RV观察而发送renew")
		require.ErrorIs(t, err, ErrServiceSafety, "最后L2期间合法RV变更仍须由独立末Load拒绝")
		after, err := session.store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), after.State())
	})
}
