package owner

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 走实际client-go选主、Session、生产worker和Run，不能只调用Model.Finish
// 冒充已接通Agent。Kube/Root/PG服务为显式协议夹具，不认证Linux/K8s。
func TestProductionLoopClosesUnstartedAbortBeforeDomainDispatch(t *testing.T) {
	for _, mode := range []string{"grow", "shrink", "lost-finish-reply", "root-unknown"} {
		t.Run(mode, func(t *testing.T) {
			api, store, coord := newAPI(t)
			root, rootConfig := newResourceOwnerFixture(t, store)
			core := kubefake.NewSimpleClientset()
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				resources, err := session.NewResourceWorker(ctx, rootConfig)
				require.NoError(t, err)
				defer resources.CloseIdleConnections()
				intent := pgRequest(t, session.Actor())
				if mode == "shrink" {
					intent.Kind = control.Shrink
				}
				config := pgConfig(t, intent, func(w http.ResponseWriter, r *http.Request) {
					t.Error("unstarted cancellation reached native business route", r.URL.Path)
					w.WriteHeader(http.StatusInternalServerError)
				})
				var credentials atomic.Int32
				token := config.Token
				config.Token = func(ctx context.Context) (string, error) {
					credentials.Add(1)
					return token(ctx)
				}
				worker, err := session.NewProductionPGWorker(ctx, config, resources, core.CoreV1())
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				snapshot, err := store.Load(ctx)
				require.NoError(t, err)
				snapshot, disposition, err := store.Begin(ctx, snapshot, session.Actor(), intent, 0)
				require.NoError(t, err)
				require.Equal(t, journal.Accepted, disposition)
				snapshot, err = store.RequestAbort(ctx, snapshot, session.Actor(), intent.Key)
				require.NoError(t, err)
				before := snapshot.State()
				beforeCredentials, beforeCore := credentials.Load(), len(core.Actions())
				root.mu.Lock()
				beforeDiscoveries, beforeFences := root.discoveries, root.fences
				id, limits := root.view.Identity, root.view.Bootstrap.Limits
				root.mu.Unlock()
				if mode == "root-unknown" {
					// 显式制造“本地未知而外层全Planned”的矛盾；没有真实Apply。
					// 收口必须拒绝并保留两者，不能为了结束Run重建空Root SDK。
					command := resourceguard.Command{ProtocolVersion: 1, Identity: id, IssuerEpoch: session.Actor().Epoch, Revision: 1, BudgetCommitmentID: intent.BudgetCommitmentID, PostmasterPID: 123, PostmasterStartTicks: 456, MemoryMaxBytes: limits.MemoryMaxBytes, OutsideUpperBytes: limits.OutsideUpperBytes, HugePools: limits.HugePools, PidsMaxTasks: 0, SystemMemoryMaxBytes: 0}
					require.NoError(t, resources.client.RetainOriginal(command))
					progress, err := worker.Advance(ctx)
					require.Error(t, err)
					require.Equal(t, pgexec.AwaitingFact, progress)
					require.Equal(t, &command, resources.client.Pending())
				} else {
					if mode == "lost-finish-reply" {
						api.mu.Lock()
						api.loseJournalReply = true
						api.mu.Unlock()
					}
					loopCtx, stop := context.WithCancel(ctx)
					defer stop()
					var unknown, ticks int
					err = worker.Run(loopCtx, PGLoopOptions{PollInterval: 10 * time.Millisecond, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, VMReplay: nil, Observe: func(tick Tick) {
						ticks++
						if tick.Err != nil {
							var unconfirmed *journal.UnconfirmedWrite
							require.ErrorAs(t, tick.Err, &unconfirmed)
							unknown++
						}
						if tick.Progress == pgexec.Recorded || tick.Progress == pgexec.Idle {
							stop()
						}
					}})
					require.ErrorIs(t, err, context.Canceled)
					wantUnknown := 0
					if mode == "lost-finish-reply" {
						wantUnknown = 1
					}
					require.Equal(t, wantUnknown, unknown)
					require.Equal(t, 1+wantUnknown, ticks)
				}
				latest, err := store.Load(ctx)
				require.NoError(t, err)
				if mode == "root-unknown" {
					require.Equal(t, before, latest.State())
				} else {
					require.Nil(t, latest.State().Active)
					require.Equal(t, before.HighWater, latest.State().HighWater)
					require.Equal(t, before.BudgetHeld, latest.State().BudgetHeld)
					require.Len(t, latest.State().History, 1)
					require.Equal(t, *before.Active, latest.State().History[0].Operation)
					require.Equal(t, journal.AbortedBeforeDispatch, latest.State().History[0].Outcome)
				}
				require.Equal(t, beforeCredentials, credentials.Load(), "no native Fence/cancel/admission request")
				require.Len(t, core.Actions(), beforeCore, "no Host/Runner/claim reads or writes after cancellation")
				root.mu.Lock()
				require.Equal(t, beforeDiscoveries, root.discoveries)
				require.Equal(t, beforeFences, root.fences)
				root.mu.Unlock()
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// 复用一次实际生产预算结算和最终开放，再验证后续零派发取消不抹掉长期预算。
// 这里只新增受影响场景，不另行重复整个未变的扩缩/Root/准入测试矩阵。
func verifyUnstartedAbortRetainsBudget(t *testing.T, ctx context.Context, worker *PGWorker, store *journal.Store, previous control.Request) {
	t.Helper()
	snapshot, err := store.Load(ctx)
	require.NoError(t, err)
	actor := snapshot.State().Owner
	snapshot, err = store.Finish(ctx, snapshot, actor, previous.Key)
	require.NoError(t, err)
	original := snapshot.State()
	require.NotNil(t, original.BudgetHeld)
	require.Nil(t, worker.client.Pending())
	for _, kind := range []control.Kind{control.Grow, control.Shrink} {
		next := previous
		next.Kind = kind
		next.Key.Revision = snapshot.State().HighWater.Revision + 1
		next.Key.OperationID.Low += next.Key.Revision
		next.ExpectedCapacityGeneration++
		next.BudgetCommitmentID += "-cancel-" + string(kind)
		var disposition journal.Disposition
		snapshot, disposition, err = store.Begin(ctx, snapshot, actor, next, 0)
		require.NoError(t, err)
		require.Equal(t, journal.Accepted, disposition)
		_, err = store.RequestAbort(ctx, snapshot, actor, next.Key)
		require.NoError(t, err)
		progress, err := worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		snapshot, err = store.Load(ctx)
		require.NoError(t, err)
		require.Nil(t, snapshot.State().Active)
		require.Equal(t, original.BudgetHeld, snapshot.State().BudgetHeld, "same complete finalization, original claims and bounds must remain held")
		require.Equal(t, next.Key, *snapshot.State().HighWater)
		require.Equal(t, journal.AbortedBeforeDispatch, snapshot.State().History[len(snapshot.State().History)-1].Outcome)
		budget, err := snapshot.State().BudgetFor(previous)
		require.NoError(t, err)
		require.Equal(t, previous, budget.Request)
	}
}
