package owner

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 使用正式Elector、生成clientset、UID/RV日志和PGWorker.Run。PG的能力/业务
// 路由仍是明确TLS协议夹具；此组合验证管理入口串行接线，不外推Linux/物理E2E。
func TestOwnedControlLoopPersistsBeginCancelAndLostReply(t *testing.T) {
	for _, loseReply := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "lost-begin-reply"}[loseReply], func(t *testing.T) {
			api, store, coord := newAPI(t)
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				request := pgRequest(t, session.Actor())
				config := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
					t.Error("unstarted command reached native execution", r.URL.Path)
					w.WriteHeader(http.StatusInternalServerError)
				})
				worker, err := session.NewPGWorker(ctx, config)
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				endpoint := worker.NewControlEndpoint()
				loopCtx, stop := context.WithCancel(ctx)
				defer stop()
				loopDone := make(chan error, 1)
				var ticks atomic.Int32
				go func() {
					loopDone <- worker.Run(loopCtx, PGLoopOptions{PollInterval: 5 * time.Millisecond, Control: endpoint, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: func(Tick) { ticks.Add(1) }})
				}()
				command := control.CoordinationCommand{ProtocolVersion: 1, Instance: request.Key.Instance, ExpectedOwnerEpoch: session.Actor().Epoch, Action: control.CoordinationState, Request: nil, Key: nil, Selector: ""}
				value, err := endpoint.Submit(ctx, command)
				require.NoError(t, err)
				require.NotNil(t, value.State)
				require.Nil(t, value.State.Active)
				command.Action, command.Request = control.CoordinationBegin, &request
				expired := command.Clone()
				expired.Request.DeadlineUnixMillis = 1
				value, err = endpoint.Submit(ctx, expired)
				require.NoError(t, err)
				require.Equal(t, journal.DeadlineExpired, value.Disposition)
				require.Nil(t, value.State.Active)
				require.Equal(t, pgexec.Idle, value.Progress)
				if loseReply {
					api.mu.Lock()
					api.loseJournalReply = true
					api.mu.Unlock()
				}
				value, err = endpoint.Submit(ctx, command)
				if loseReply {
					var unknown *journal.UnconfirmedWrite
					require.ErrorAs(t, err, &unknown)
				} else {
					require.NoError(t, err)
					require.Equal(t, journal.Accepted, value.Disposition)
					require.Equal(t, request, value.State.Active.Request)
				}
				// 同一完整Begin只查原接受，不能因丢回复新造operation/epoch。
				value, err = endpoint.Submit(ctx, command)
				require.NoError(t, err)
				require.Equal(t, journal.Replayed, value.Disposition)
				require.Equal(t, pgexec.Replayed, value.Progress)
				conflict := command.Clone()
				conflict.Request.TargetBuffers++
				value, err = endpoint.Submit(ctx, conflict)
				require.NoError(t, err)
				require.Equal(t, journal.Conflict, value.Disposition)
				require.Equal(t, request, value.State.Active.Request)
				wrongOwner := command.Clone()
				wrongOwner.ExpectedOwnerEpoch++
				_, err = endpoint.Submit(ctx, wrongOwner)
				require.ErrorIs(t, err, ErrControlUnavailable)
				// 所有领域恢复入口都走正式原WAL校验，不能对全Planned动作
				// 发任何SDK写，也不能用错误选择杀死owner循环。
				command.Request, command.Key = nil, &request.Key
				for _, action := range []control.CoordinationAction{control.CoordinationReplayPG, control.CoordinationReplayQuiesce, control.CoordinationReplayAdmission, control.CoordinationReplayResource, control.CoordinationReplayPrepare, control.CoordinationReplayVM, control.CoordinationReplayAbort, control.CoordinationRecoverUnchanged, control.CoordinationRecoverCapacity} {
					command.Action = action
					_, err = endpoint.Submit(ctx, command)
					require.Error(t, err, action)
				}
				command.Action = control.CoordinationCancel
				value, err = endpoint.Submit(ctx, command)
				require.NoError(t, err)
				require.True(t, value.State.Active.AbortRequested)
				command.Action, command.Key = control.CoordinationState, nil
				require.Eventually(t, func() bool {
					value, err = endpoint.Submit(ctx, command)
					return err == nil && value.State.Active == nil
				}, time.Second, 10*time.Millisecond)
				require.Len(t, value.State.History, 1)
				require.Equal(t, journal.AbortedBeforeDispatch, value.State.History[0].Outcome)
				require.Equal(t, request.Key, *value.State.HighWater)
				require.Positive(t, ticks.Load())
				stop()
				require.ErrorIs(t, <-loopDone, context.Canceled)
				select {
				case <-endpoint.Done():
				default:
					t.Fatal("Run exit did not close management endpoint")
				}
				_, err = endpoint.Submit(ctx, command)
				require.ErrorIs(t, err, ErrControlUnavailable)
				err = worker.Run(ctx, PGLoopOptions{PollInterval: time.Second, Control: endpoint, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: nil})
				require.ErrorContains(t, err, "cannot be reused")
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestOwnedControlLoopCancellationAndSerialExecution(t *testing.T) {
	_, store, coord := newAPI(t)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		request := pgRequest(t, session.Actor())
		worker, err := session.NewPGWorker(ctx, pgConfig(t, request, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }))
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		endpoint := worker.NewControlEndpoint()
		command := control.CoordinationCommand{ProtocolVersion: 1, Instance: request.Key.Instance, ExpectedOwnerEpoch: session.Actor().Epoch, Action: control.CoordinationBegin, Request: &request, Key: nil, Selector: ""}
		cancelled, stopCall := context.WithCancel(ctx)
		stopCall()
		_, err = endpoint.Submit(cancelled, command)
		require.ErrorIs(t, err, context.Canceled)
		// 队列尚无接收者时到期，不能分离一个未来仍会Begin的后台任务。
		waiting, stopWait := context.WithTimeout(ctx, 20*time.Millisecond)
		defer stopWait()
		_, err = endpoint.Submit(waiting, command)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		before, err := store.Load(ctx)
		require.NoError(t, err)
		require.Nil(t, before.State().Active)
		entered, release := make(chan struct{}), make(chan struct{})
		loopCtx, stopLoop := context.WithCancel(ctx)
		defer stopLoop()
		done := make(chan error, 1)
		go func() {
			done <- worker.Run(loopCtx, PGLoopOptions{PollInterval: time.Second, Control: endpoint, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: func(Tick) { close(entered); <-release }})
		}()
		<-entered
		other := worker.NewControlEndpoint()
		err = worker.Run(ctx, PGLoopOptions{PollInterval: time.Second, Control: other, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: nil})
		require.ErrorContains(t, err, "already running")
		stopLoop()
		close(release)
		require.ErrorIs(t, <-done, context.Canceled)
		after, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), after.State())
		return nil
	})
	require.NoError(t, err)
}

// WithControlEndpointTestFixture只编译进测试二进制，供外部owner_test把真实UDS
// 服务接到本包现有的正式Elector/UID-RV/PG协议夹具；不新增生产注入回调。
func WithControlEndpointTestFixture(t *testing.T, run func(context.Context, *ControlEndpoint, control.Request)) {
	t.Helper()
	_, store, coord := newAPI(t)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		request := pgRequest(t, session.Actor())
		worker, err := session.NewPGWorker(ctx, pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
			t.Error("unstarted UDS cancellation reached native business route", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		endpoint := worker.NewControlEndpoint()
		loopCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- worker.Run(loopCtx, PGLoopOptions{PollInterval: 5 * time.Millisecond, Control: endpoint, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: nil})
		}()
		defer func() {
			stop()
			require.ErrorIs(t, <-done, context.Canceled)
		}()
		run(ctx, endpoint, request)
		return nil
	})
	require.NoError(t, err)
}

func TestOwnedControlLoopReplaysOnlyExplicitOriginalPG(t *testing.T) {
	_, store, coord := newAPI(t)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		request := pgRequest(t, session.Actor())
		// 这里只覆盖PG域接线；非PG前驱使用原有显式Model夹具，不称完整预算E2E。
		beginPG(t, store, request)
		var posts atomic.Int32
		config := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/dsg/v1/resize" && posts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			pgReply(w, pgResponse(t, request))
		})
		worker, err := session.NewPGWorker(ctx, config)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		endpoint := worker.NewControlEndpoint()
		loopCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		recorded := make(chan struct{}, 1)
		go func() {
			done <- worker.Run(loopCtx, PGLoopOptions{PollInterval: 5 * time.Millisecond, Control: endpoint, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: func(tick Tick) {
				if tick.Progress == pgexec.Recorded {
					select {
					case recorded <- struct{}{}:
					default:
					}
				}
			}})
		}()
		defer func() { stop(); require.ErrorIs(t, <-done, context.Canceled) }()
		select {
		case <-recorded:
		case <-ctx.Done():
			t.Fatal("original PG readback did not settle")
		}
		require.EqualValues(t, 1, posts.Load(), "normal polling must not replay POST")
		require.Equal(t, &request, worker.client.Pending())
		command := control.CoordinationCommand{ProtocolVersion: 1, Instance: request.Key.Instance, ExpectedOwnerEpoch: session.Actor().Epoch, Action: control.CoordinationReplayPG, Request: nil, Key: &request.Key, Selector: ""}
		wrong := command.Clone()
		wrong.Key.OperationID.Low++
		_, err = endpoint.Submit(ctx, wrong)
		require.Error(t, err)
		require.EqualValues(t, 1, posts.Load())
		value, err := endpoint.Submit(ctx, command)
		require.NoError(t, err)
		require.Equal(t, pgexec.Replayed, value.Progress)
		require.EqualValues(t, 2, posts.Load())
		require.Nil(t, worker.client.Pending())
		return nil
	})
	require.NoError(t, err)
}

func TestOwnedControlLoopCancellationStopsActiveManagementRead(t *testing.T) {
	api, store, coord := newAPI(t)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		request := pgRequest(t, session.Actor())
		worker, err := session.NewPGWorker(ctx, pgConfig(t, request, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }))
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		endpoint := worker.NewControlEndpoint()
		loopCtx, stop := context.WithCancel(ctx)
		defer stop()
		done := make(chan error, 1)
		idle := make(chan struct{}, 2)
		go func() {
			done <- worker.Run(loopCtx, PGLoopOptions{PollInterval: time.Minute, Control: endpoint, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: func(Tick) { idle <- struct{}{} }})
		}()
		<-idle
		command := control.CoordinationCommand{ProtocolVersion: 1, Instance: request.Key.Instance, ExpectedOwnerEpoch: session.Actor().Epoch, Action: control.CoordinationState, Request: nil, Key: nil, Selector: ""}
		_, err = endpoint.Submit(ctx, command)
		require.NoError(t, err)
		<-idle // State后的一次普通Advance已返回，下一次网络调用只能来自管理命令。
		started := make(chan struct{}, 1)
		api.mu.Lock()
		api.blockVMGet, api.blockVMGetStarted = true, started
		api.mu.Unlock()
		defer func() {
			api.mu.Lock()
			api.blockVMGet, api.blockVMGetStarted = false, nil
			api.mu.Unlock()
		}()
		reply := make(chan error, 1)
		go func() { _, err := endpoint.Submit(ctx, command); reply <- err }()
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("management read never entered original API")
		}
		stop() // Session与HTTP调用者都仍有效，只取消Run。
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("Run cancellation did not cancel and join management HTTP")
		}
		require.Error(t, <-reply)
		require.NoError(t, session.Context().Err(), "test did not rely on losing Session to stop the call")
		return nil
	})
	require.NoError(t, err)
}
