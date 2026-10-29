package owner

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

func TestProductionNativeLeaseStopsOnMissingOrCorruptOuterWAL(t *testing.T) {
	for _, mode := range []string{"missing", "corrupt"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "native-lease-"+mode) })
	}
}

// 真正Run完成本地取消后保留同worker，第二次Run可只读原历史；管理新Begin
// 则必须先协商原生期限。旧网关失败不能被“之前全是本地取消”豁免或先写WAL。
func TestProductionLocalAbortHistoryCannotBypassLeaseForNewBegin(t *testing.T) {
	_, store, coord := newAPI(t)
	_, rootConfig := newResourceOwnerFixture(t, store)
	core := kubefake.NewSimpleClientset()
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceWorker(ctx, rootConfig)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		request := pgRequest(t, session.Actor())
		snapshot := pgResponse(t, request).Observed
		snapshot.Operation, snapshot.CapacityGeneration, snapshot.AdmissionOpen, snapshot.AdmissionRevision = nil, 1, false, 0
		var nativeCalls atomic.Int32
		config := pgConfig(t, request, func(w http.ResponseWriter, _ *http.Request) {
			nativeCalls.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}, func() control.Snapshot { return snapshot })
		worker, err := session.NewProductionPGWorker(ctx, config, resources, core.CoreV1())
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		s, err := store.Load(ctx)
		require.NoError(t, err)
		s, _, err = store.Begin(ctx, s, session.Actor(), request, 0)
		require.NoError(t, err)
		_, err = store.RequestAbort(ctx, s, session.Actor(), request.Key)
		require.NoError(t, err)
		firstCtx, firstStop := context.WithCancel(ctx)
		err = worker.Run(firstCtx, PGLoopOptions{PollInterval: 10 * time.Millisecond, Control: nil, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: func(tick Tick) {
			require.NoError(t, tick.Err)
			require.Equal(t, pgexec.Recorded, tick.Progress)
			firstStop()
		}})
		firstStop()
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, nativeCalls.Load())
		before, err := store.Load(ctx)
		require.NoError(t, err)
		require.Nil(t, before.State().Active)
		endpoint := worker.NewControlEndpoint()
		loopCtx, stop := context.WithCancel(ctx)
		defer stop()
		ready, done := make(chan struct{}, 1), make(chan error, 1)
		go func() {
			done <- worker.Run(loopCtx, PGLoopOptions{PollInterval: 10 * time.Millisecond, Control: endpoint, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: func(Tick) {
				select {
				case ready <- struct{}{}:
				default:
				}
			}})
		}()
		select {
		case <-ready:
		case err := <-done:
			t.Fatalf("原本地取消历史不能要求新PG访问：%v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		command := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(request.Key.Instance), ExpectedOwnerEpoch: session.Actor().Epoch, Action: control.CoordinationState, Request: nil, Key: nil, Selector: ""}
		_, err = endpoint.Submit(ctx, command)
		require.NoError(t, err)
		require.Zero(t, nativeCalls.Load())
		request.Key.Revision++
		request.Key.OperationID.Low++
		command.Action, command.Request = control.CoordinationBegin, &request
		_, err = endpoint.Submit(ctx, command)
		require.Error(t, err)
		require.EqualValues(t, 1, nativeCalls.Load(), "新的Begin必须先访问原生期限")
		after, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), after.State(), "期限协商失败不能先接受新Begin")
		stop()
		require.ErrorIs(t, <-done, context.Canceled)
		return nil
	})
	require.NoError(t, err)
}

func TestProductionNativeLeaseRuntimeRequiresNativeSupport(t *testing.T) {
	productionInitialService(t, "native-lease-runtime")
}

func TestProductionExpiredNativeLeaseRetainsRestrictedManagement(t *testing.T) {
	for _, mode := range []string{"startup", "running", "queued"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "native-lease-expired-"+mode) })
	}
}

// 生产预算/Session/SDK/来源均真实执行，PG期限为显式协议peer；真实后台
// 过期/迟到拒绝由配套PG17测试验证，不把本测试计为物理或Linux验收。
func verifyNativeAdmissionLease(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, worker *PGWorker, pg *initialServicePG) {
	t.Helper()
	if strings.Contains(mode, "expired-") {
		verifyExpiredNativeManagement(t, ctx, mode, store, worker, pg)
		return
	}
	before, err := store.Load(ctx)
	require.NoError(t, err)
	require.NoError(t, worker.renewAdmissionLease(ctx))
	pg.mu.Lock()
	deadline, count := pg.leaseDeadline, pg.leaseRenewals
	pg.mu.Unlock()
	require.Equal(t, 1, count)
	healthy, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), healthy.State(), "续租不写预算或业务WAL")
	if strings.HasSuffix(mode, "runtime") {
		loopCtx, stop := context.WithCancel(ctx)
		err := worker.Run(loopCtx, PGLoopOptions{Control: nil, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, PollInterval: 100 * time.Millisecond, Observe: func(tick Tick) { require.NoError(t, tick.Err); stop() }})
		require.ErrorIs(t, err, context.Canceled)
		stop()
		pg.mu.Lock()
		require.Greater(t, pg.leaseRenewals, count)
		pg.leaseReject = true
		pg.mu.Unlock()
		err = worker.Run(ctx, PGLoopOptions{Control: nil, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, PollInterval: 100 * time.Millisecond, Observe: nil})
		require.Error(t, err, "旧网关不能降级成无限期服务")
		return
	}
	api.mu.Lock()
	old := api.vm.DeepCopy()
	if strings.HasSuffix(mode, "missing") {
		delete(api.vm.Annotations, control.JournalAnnotation)
	} else {
		api.vm.Annotations[control.JournalAnnotation] = "{broken"
	}
	api.mu.Unlock()
	require.Error(t, worker.renewAdmissionLease(ctx), "坏/缺WAL不应发送新续租")
	pg.mu.Lock()
	require.Equal(t, deadline, pg.leaseDeadline)
	require.Equal(t, count, pg.leaseRenewals)
	pg.mu.Unlock()
	api.mu.Lock()
	api.vm = old
	api.mu.Unlock()
	restored, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), restored.State(), "坏日志不得释放或改造原预算")
}

// 使用真实Session/Run串行投递，过期与PG状态由显式peer给出。本用例只验证
// 过期后的管理可用性和无新派发，不声称已经实现持久rearm或受控重开。
func verifyExpiredNativeManagement(t *testing.T, ctx context.Context, mode string, store *journal.Store, worker *PGWorker, pg *initialServicePG) {
	t.Helper()
	startup := strings.HasSuffix(mode, "startup")
	pg.mu.Lock()
	pg.leaseExpired, pg.leaseTimeoutMS = startup, 1000
	pg.mu.Unlock()
	before, err := store.Load(ctx)
	require.NoError(t, err)
	posts, queries, resizePosts := pg.counts()
	endpoint := worker.NewControlEndpoint()
	loopCtx, stop := context.WithCancel(ctx)
	defer stop()
	ticks, done := make(chan Tick, 1), make(chan error, 1)
	go func() {
		done <- worker.Run(loopCtx, PGLoopOptions{PollInterval: 100 * time.Millisecond, Control: endpoint, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: func(tick Tick) {
			select {
			case ticks <- tick:
			default:
			}
		}})
	}()
	await := func() Tick {
		select {
		case tick := <-ticks:
			return tick
		case err := <-done:
			t.Fatalf("过期服务必须保留管理循环，实际提前退出：%v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		return Tick{Progress: "", Err: nil}
	}
	first := await()
	if !startup {
		require.NoError(t, first.Err)
		pg.mu.Lock()
		pg.leaseExpired = true
		pg.mu.Unlock()
		if !strings.HasSuffix(mode, "queued") {
			for tick := await(); ; tick = await() {
				if tick.Err != nil {
					require.ErrorIs(t, tick.Err, ErrServiceSafety)
					break
				}
			}
		}
	} else {
		require.ErrorIs(t, first.Err, ErrServiceSafety)
	}
	pg.mu.Lock()
	deadline, renewals := pg.leaseDeadline, pg.leaseRenewals
	pg.mu.Unlock()
	instance, epoch := worker.Binding()
	command := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(instance), ExpectedOwnerEpoch: epoch, Action: control.CoordinationState, Request: nil, Key: nil, Selector: ""}
	state, err := endpoint.Submit(ctx, command)
	require.NoError(t, err)
	require.Equal(t, before.State(), *state.State)
	for _, action := range []control.CoordinationAction{control.CoordinationBegin, control.CoordinationCancel, control.CoordinationReplayPG, control.CoordinationReplayQuiesce, control.CoordinationReplayAdmission, control.CoordinationReplayResource, control.CoordinationReplayPrepare, control.CoordinationReplayVM, control.CoordinationReplayAbort, control.CoordinationRecoverUnchanged, control.CoordinationRecoverCapacity, control.CoordinationReplayInitial} {
		t.Run(string(action), func(t *testing.T) {
			denied := command
			denied.Action = action
			request := pg.request
			key := request.Key
			switch action {
			case control.CoordinationBegin:
				denied.Request = &request
			case control.CoordinationReplayInitial:
				denied.Selector = strings.Repeat("a", 64)
			default:
				denied.Key = &key
			}
			require.NoError(t, denied.Validate())
			_, err := endpoint.Submit(ctx, denied)
			require.ErrorIs(t, err, ErrServiceSafety, "过期不能执行新业务、原重放或旧开放恢复")
		})
	}
	state, err = endpoint.Submit(ctx, command)
	require.NoError(t, err, "被拒绝的管理动作不能杀死查询入口")
	require.Equal(t, before.State(), *state.State)
	stop()
	require.ErrorIs(t, <-done, context.Canceled)
	gotPosts, gotQueries, gotResize := pg.counts()
	require.Equal(t, posts, gotPosts)
	require.Equal(t, queries, gotQueries)
	require.Equal(t, resizePosts, gotResize)
	pg.mu.Lock()
	require.Equal(t, deadline, pg.leaseDeadline)
	require.Equal(t, renewals, pg.leaseRenewals, "过期管理不能盲续租或自动rearm")
	pg.mu.Unlock()
	require.False(t, pg.shared().Observed.AdmissionOpen)
}
