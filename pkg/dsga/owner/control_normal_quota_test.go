package owner

import (
	"context"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// 正式Elector/Session/UID-RV日志/SDK/Run入口；Root/PG仍是协议peer，不冒物理恢复。
func TestOwnedControlRecoverNormalQuotaRequiresOriginalResourceAndGuestReady(t *testing.T) {
	for _, withResources := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing-resources", true: "missing-original-guestready"}[withResources], func(t *testing.T) {
			_, store, coord := newAPI(t)
			var resourceOwner *ResourceWorker
			var root *resourceOwnerFixture
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				if withResources {
					f, cfg := newResourceOwnerFixture(t, store)
					root = f
					resourceOwner, err = session.NewResourceWorker(ctx, cfg)
					require.NoError(t, err)
					defer resourceOwner.CloseIdleConnections()
				}
				request := pgRequest(t, session.Actor())
				var businessCalls atomic.Int32
				cfg := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
					businessCalls.Add(1)
					t.Errorf("未派发动作触及PG业务路由 %s", r.URL.Path)
					w.WriteHeader(500)
				})
				var worker *PGWorker
				if withResources {
					worker, err = session.NewPGWorker(ctx, cfg, resourceOwner)
				} else {
					worker, err = session.NewPGWorker(ctx, cfg)
				}
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				snapshot, err := store.Load(ctx)
				require.NoError(t, err)
				snapshot, disposition, err := store.Begin(ctx, snapshot, session.Actor(), request, 0)
				require.NoError(t, err)
				require.Equal(t, journal.Accepted, disposition)
				before := snapshot.State()
				endpoint := worker.NewControlEndpoint()
				loopCtx, stop := context.WithCancel(ctx)
				defer stop()
				done := make(chan error, 1)
				go func() {
					done <- worker.Run(loopCtx, PGLoopOptions{PollInterval: 10 * time.Millisecond, Control: endpoint})
				}()
				command := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(request.Key.Instance), ExpectedOwnerEpoch: session.Actor().Epoch, Action: control.CoordinationRecoverNormalQuota, Key: &request.Key}
				value, err := endpoint.Submit(ctx, command)
				if withResources {
					require.ErrorContains(t, err, "normal recovery original GuestReady unavailable")
				} else {
					require.ErrorContains(t, err, "original normal quota resource executor is unavailable")
				}
				require.Equal(t, pgexec.AwaitingFact, value.Progress)
				wrong := command.Clone()
				wrong.ExpectedOwnerEpoch++
				_, err = endpoint.Submit(ctx, wrong)
				require.ErrorIs(t, err, ErrControlUnavailable)
				cancelled, stopCall := context.WithCancel(ctx)
				stopCall()
				_, err = endpoint.Submit(cancelled, command)
				require.ErrorIs(t, err, context.Canceled)
				stop()
				require.ErrorIs(t, <-done, context.Canceled)
				after, err := store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, before, after.State(), "缺原GuestReady不可写WAL或结清旧操作")
				require.Zero(t, businessCalls.Load())
				if root != nil {
					root.mu.Lock()
					require.Zero(t, root.repairs)
					root.mu.Unlock()
				}
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestProductionControlRecoverNormalQuotaPreservesClosedAndExpiredGates(t *testing.T) {
	for _, mode := range []string{"closed", "expired"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "normal-quota-control-"+mode) })
	}
}

// 本批仅复用首次服务前驱；关闭由原生产Advance/原关闭WAL产生，过期由原
// typed PG lease peer产生。新动作不能增加recover RPC、PG写、QMP写或改预算。
func verifyNormalQuotaControlGates(t *testing.T, ctx context.Context, mode string, store *journal.Store, worker *PGWorker, pg *initialServicePG, root *guestRootFixture, qmp *vmQMPPeer) {
	t.Helper()
	require.False(t, expiredAdmissionControlAllowed(control.CoordinationRecoverNormalQuota))
	if strings.HasSuffix(mode, "closed") {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
		for range 3 {
			_, err := worker.Advance(ctx)
			require.True(t, err == nil || err == ErrServiceSafety)
			s, err := store.Load(ctx)
			require.NoError(t, err)
			if c := s.State().ServiceClosure; c != nil && c.Phase == journal.Settled {
				break
			}
		}
		s, err := store.Load(ctx)
		require.NoError(t, err)
		require.NotNil(t, s.State().ServiceClosure)
		require.Equal(t, journal.Settled, s.State().ServiceClosure.Phase)
		require.False(t, pg.shared().Observed.AdmissionOpen)
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
	} else {
		pg.mu.Lock()
		pg.leaseExpired = true
		pg.mu.Unlock()
	}
	before, err := store.Load(ctx)
	require.NoError(t, err)
	posts, _, resizePosts := pg.counts()
	root.mu.Lock()
	rootPosts, cancellations := root.posts, root.cancellations
	root.mu.Unlock()
	qmp.mu.Lock()
	writes := qmp.writes
	qmp.mu.Unlock()
	instance, epoch := worker.Binding()
	// 复用fixture的原请求完整Key；这不是新的Begin或目标。
	pg.mu.Lock()
	key := pg.request.Key
	pg.mu.Unlock()
	command := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(instance), ExpectedOwnerEpoch: epoch, Action: control.CoordinationRecoverNormalQuota, Key: &key}
	endpoint := worker.NewControlEndpoint()
	loopCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(loopCtx, PGLoopOptions{PollInterval: 50 * time.Millisecond, Control: endpoint})
	}()
	_, err = endpoint.Submit(ctx, command)
	require.ErrorIs(t, err, ErrServiceSafety, "闭门/过期不能借显式恢复扩权")
	stop()
	require.ErrorIs(t, <-done, context.Canceled)
	after, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), after.State())
	nowPosts, _, nowResize := pg.counts()
	require.Equal(t, posts, nowPosts)
	require.Equal(t, resizePosts, nowResize)
	root.mu.Lock()
	require.Equal(t, rootPosts, root.posts)
	require.Equal(t, cancellations, root.cancellations)
	root.mu.Unlock()
	qmp.mu.Lock()
	require.Equal(t, writes, qmp.writes)
	qmp.mu.Unlock()
	require.False(t, pg.shared().Observed.AdmissionOpen)
}
