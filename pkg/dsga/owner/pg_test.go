package owner

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

func pgRequest(t *testing.T, actor journal.Owner) control.Request {
	t.Helper()
	raw, err := os.ReadFile("../../api/dsga/testdata/request-v1.json")
	require.NoError(t, err)
	var request control.Request
	require.NoError(t, json.Unmarshal(raw, &request))
	request.Key.Instance.VMUID = string(binding().VMUID)
	request.Key.Instance.ComputeInstanceID = "compute-1"
	request.Key.IssuerEpoch = actor.Epoch
	request.Key.Revision = 1
	return request
}

func beginPG(t *testing.T, store *journal.Store, request control.Request) {
	t.Helper()
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	snapshot, disposition, err := store.Begin(context.Background(), snapshot, snapshot.State().Owner, request, 0)
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	for _, step := range snapshot.State().Active.Steps {
		if step.Action == journal.ResizePG {
			break
		}
		var dispatch *journal.Dispatch
		snapshot, dispatch, err = store.Start(context.Background(), snapshot, snapshot.State().Owner, request.Key, step.Action, "non-pg-model-fixture", []byte(`{"test_only_non_pg_domain":true}`))
		require.NoError(t, err)
		snapshot, err = store.Record(context.Background(), snapshot, snapshot.State().Owner, request.Key, journal.Observation{Key: request.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256, Ordinal: dispatch.Step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: dispatch.Step.TargetID, Data: []byte(`{"test_only_non_pg_domain":true}`)})
		require.NoError(t, err)
	}
}

func pgResponse(t *testing.T, request control.Request) control.ControlResponse {
	t.Helper()
	raw, err := os.ReadFile("../../api/dsga/testdata/response-v1.json")
	require.NoError(t, err)
	var fact control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &fact))
	hash, err := request.ContextSHA256()
	require.NoError(t, err)
	fact.Receipt.Key = request.Key
	for _, snapshot := range []*control.Snapshot{fact.Receipt.Result, &fact.Observed} {
		snapshot.Instance = request.Key.Instance
		snapshot.IssuerFloorEpoch = request.Key.IssuerEpoch
		snapshot.Operation.Key = request.Key
		snapshot.Operation.ContextSHA256 = hash
	}
	require.NoError(t, fact.ValidateResize(request, request.Key.Instance))
	return fact
}

func pgReply(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(value)
}

// TLS仅提供协议事实夹具，真实验签/SQL/PG在跨仓native门禁另验。
func pgConfig(t *testing.T, request control.Request, handler http.HandlerFunc, views ...func() control.Snapshot) compute.Config {
	t.Helper()
	require.LessOrEqual(t, len(views), 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dsg/v1/capabilities":
			history := uint32(16)
			provider := false
			boot := request.Key.Instance.PGBootID
			pgReply(w, control.Capabilities{ProtocolVersion: 1, ProductionReady: false, VMShrinkPermit: false, Supported: true, BufferAPIVersion: 6, Enabled: true, InstanceBound: true, HistoryCapacity: &history, ProviderCanRelease: &provider, Instance: &control.DiscoveryIdentity{VMUID: request.Key.Instance.VMUID, ComputeInstanceID: request.Key.Instance.ComputeInstanceID, PGBootID: &boot}})
		case "/dsg/v1/issuer/fence":
			var observed control.Snapshot
			if len(views) == 1 {
				// 多操作组合返回同点实际状态，不能把第二/三次请求仍固定为
				// generation1；旧单操作夹具不传此入口，行为维持原样。
				observed = views[0]()
			} else {
				observed = pgResponse(t, request).Observed
				observed.Operation = nil
				observed.CapacityGeneration = 1
			}
			pgReply(w, control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: request.Key.Instance, RequestedEpoch: request.Key.IssuerEpoch, Disposition: control.Fenced, Observed: observed})
		default:
			handler(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return compute.Config{BaseURL: server.URL, Instance: request.Key.Instance, IssuerEpoch: 0, Timeout: 3 * time.Second, Transport: server.Client().Transport.(*http.Transport), Token: func(context.Context) (string, error) { return "private-test-token", nil }, Pending: nil}
}

func TestPGWorkerInitialDiscoveryOrRetainedHighWaterBinding(t *testing.T) {
	for _, withHistory := range []bool{false, true} {
		_, store, client := newAPI(t)
		elector, err := New(client, store, testOptions())
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
			request := pgRequest(t, session.Actor())
			if withHistory {
				beginPG(t, store, request)
			}
			config := pgConfig(t, request, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) })
			config.Instance.PGBootID = 0
			worker, err := session.NewPGWorker(ctx, config)
			require.NoError(t, err)
			defer worker.CloseIdleConnections()
			instance, _ := worker.Binding()
			require.Equal(t, request.Key.Instance, instance)
			if withHistory {
				config.Instance.ComputeInstanceID = "another-compute"
				_, err = session.NewPGWorker(ctx, config)
				require.Error(t, err, "credential refresh cannot replace a retained Compute identity")
				changed := request
				changed.Key.Instance.PGBootID++
				config = pgConfig(t, changed, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) })
				config.Instance.PGBootID = 0
				_, err = session.NewPGWorker(ctx, config)
				require.Error(t, err, "retained journal cannot silently discover a new PG boot")
			}
			config.Instance.ComputeInstanceID = "wrong-bootstrap-compute"
			var tokenCalled atomic.Bool
			config.Token = func(context.Context) (string, error) { tokenCalled.Store(true); return "fixture-token", nil }
			_, err = session.NewPGWorker(ctx, config)
			require.Error(t, err)
			require.False(t, tokenCalled.Load(), "even pristine startup must reject a different Compute before token/HTTP discovery")
			return nil
		})
		cancel()
		require.NoError(t, err)
	}
}

func TestOwnedPGLoopKeepsClientAndRequiresExplicitReplayAfterLostReply(t *testing.T) {
	_, store, client := newAPI(t)
	elector, err := New(client, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		request := pgRequest(t, session.Actor())
		beginPG(t, store, request)
		var posts, queries, unknown atomic.Int32
		config := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/dsg/v1/resize" {
				if posts.Add(1) == 1 {
					http.Error(w, "lost reply fixture", http.StatusServiceUnavailable)
					return
				}
			} else {
				queries.Add(1)
			}
			pgReply(w, pgResponse(t, request))
		})
		worker, err := session.NewPGWorker(ctx, config)
		require.NoError(t, err)
		loopCtx, stop := context.WithCancel(ctx)
		err = worker.Run(loopCtx, PGLoopOptions{PollInterval: 10 * time.Millisecond, Wake: nil, Replay: nil, Observe: func(tick Tick) {
			if tick.Err != nil {
				unknown.Add(1)
			}
			if tick.Progress == pgexec.Recorded {
				stop()
			}
		}})
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 1, posts.Load())
		require.EqualValues(t, 1, queries.Load())
		require.EqualValues(t, 1, unknown.Load())
		require.Equal(t, &request, worker.client.Pending())
		wrong := request.Key
		wrong.Revision++
		_, err = worker.ReplayOriginal(ctx, wrong)
		require.Error(t, err)
		require.EqualValues(t, 1, posts.Load())
		progress, err := worker.ReplayOriginal(ctx, request.Key)
		require.NoError(t, err)
		require.Equal(t, pgexec.Replayed, progress)
		require.Nil(t, worker.client.Pending())
		require.EqualValues(t, 2, posts.Load())
		return nil
	})
	require.NoError(t, err)
}

func TestOwnedLoopCancelsActualHTTPOnLeaseLossAndPreservesWriteAhead(t *testing.T) {
	api, store, client := newAPI(t)
	elector, err := New(client, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	testDone := ctx.Done()
	var original control.Request
	entered := make(chan struct{})
	observedCancel := make(chan struct{})
	disabledAt := make(chan time.Time, 1)
	var once sync.Once
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		original = pgRequest(t, session.Actor())
		beginPG(t, store, original)
		config := pgConfig(t, original, func(_ http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			once.Do(func() { close(entered) })
			select {
			case <-r.Context().Done():
				close(observedCancel)
			case <-testDone:
			}
		})
		worker, err := session.NewPGWorker(ctx, config)
		require.NoError(t, err)
		go func() {
			select {
			case <-entered:
				api.mu.Lock()
				api.denyLeaseWrites = true
				api.mu.Unlock()
				disabledAt <- time.Now()
			case <-ctx.Done():
			}
		}()
		// 调用方背景context比Session更长，真正取消必须来自生产Session绑定，
		// 不能仅因测试传入了同一个短ctx而让缺失的所有权取消链接蒙混过关。
		return worker.Run(context.Background(), PGLoopOptions{PollInterval: 10 * time.Millisecond, Wake: nil, Replay: nil, Observe: nil})
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, ErrLeadershipLost))
	select {
	case started := <-disabledAt:
		require.Less(t, time.Since(started), 2*time.Second, "in-flight request must stop on ownership loss, before its 3s HTTP timeout")
	case <-ctx.Done():
		t.Fatal("test never disabled lease renewal")
	}
	select {
	case <-observedCancel:
	case <-ctx.Done():
		t.Fatal("in-flight HTTP survived session cancellation")
	}
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	require.Equal(t, original, snapshot.State().Active.Request)
	for _, step := range snapshot.State().Active.Steps {
		if step.Action == journal.ResizePG {
			require.Equal(t, journal.Unconfirmed, step.Phase)
			require.Nil(t, step.Observation)
		}
	}
}

func TestNewWorkerRejectsForeignEpochBootAndPendingBeforeCredentials(t *testing.T) {
	_, store, client := newAPI(t)
	elector, err := New(client, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		request := pgRequest(t, session.Actor())
		beginPG(t, store, request)
		var credentials atomic.Int32
		base := compute.Config{BaseURL: "https://never-connect.invalid", Instance: request.Key.Instance, IssuerEpoch: session.Actor().Epoch, Timeout: time.Second, Transport: nil, Token: func(context.Context) (string, error) { credentials.Add(1); return "test-token", nil }, Pending: nil}
		for _, mode := range []string{"epoch", "vm", "boot", "pending"} {
			t.Run(mode, func(t *testing.T) {
				cfg := base
				switch mode {
				case "epoch":
					cfg.IssuerEpoch++
				case "vm":
					cfg.Instance.VMUID = "other-vm"
				case "boot":
					cfg.Instance.PGBootID--
				case "pending":
					cfg.Pending = &request
				}
				_, err := session.NewPGWorker(ctx, cfg)
				require.Error(t, err)
			})
		}
		require.Zero(t, credentials.Load())
		return nil
	})
	require.NoError(t, err)
}

func TestClosedWakeAndReplayChannelsDoNotSpin(t *testing.T) {
	_, store, client := newAPI(t)
	elector, err := New(client, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		request := pgRequest(t, session.Actor())
		config := pgConfig(t, request, func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "unexpected PG side effect", http.StatusInternalServerError)
		})
		worker, err := session.NewPGWorker(ctx, config)
		require.NoError(t, err)
		wake := make(chan struct{})
		close(wake)
		replay := make(chan control.Key)
		close(replay)
		admissionReplay := make(chan control.Key)
		close(admissionReplay)
		vmReplay := make(chan control.Key)
		close(vmReplay)
		loopCtx, stop := context.WithTimeout(ctx, 260*time.Millisecond)
		defer stop()
		var ticks int
		err = worker.Run(loopCtx, PGLoopOptions{PollInterval: 100 * time.Millisecond, Wake: wake, Replay: replay, AdmissionReplay: admissionReplay, ResourceReplay: nil, VMReplay: vmReplay, Observe: func(tick Tick) { ticks++; require.NoError(t, tick.Err); require.Equal(t, pgexec.Idle, tick.Progress) }})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.LessOrEqual(t, ticks, 7)
		return nil
	})
	require.NoError(t, err)
}

func TestPendingFromPreviousIntentCannotTurnNewStepIntoFalseUnknown(t *testing.T) {
	_, store, client := newAPI(t)
	elector, err := New(client, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		original := pgRequest(t, session.Actor())
		beginPG(t, store, original)
		var posts atomic.Int32
		config := pgConfig(t, original, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/dsg/v1/resize" {
				posts.Add(1)
				http.Error(w, "lost reply fixture", http.StatusServiceUnavailable)
				return
			}
			pgReply(w, pgResponse(t, original))
		})
		worker, err := session.NewPGWorker(ctx, config)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		_, err = worker.Advance(ctx)
		require.Error(t, err)
		progress, err := worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		require.Equal(t, &original, worker.client.Pending())
		// 这里只构造明确标注的其他领域终态来考验防重置边界，不能充当生产
		// 预算/VM事实。即使外层已关闭，SDK未知状态也不能静默忘掉后派发新PG。
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		for _, step := range snapshot.State().Active.Steps {
			if step.Phase == journal.Planned {
				var dispatch *journal.Dispatch
				snapshot, dispatch, err = store.Start(ctx, snapshot, session.Actor(), original.Key, step.Action, "non-pg-model-fixture", []byte(`{"test_only_non_pg_domain":true}`))
				require.NoError(t, err)
				snapshot, err = store.Record(ctx, snapshot, session.Actor(), original.Key, journal.Observation{Key: original.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256, Ordinal: dispatch.Step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: dispatch.Step.TargetID, Data: []byte(`{"test_only_non_pg_domain":true}`)})
				require.NoError(t, err)
			}
		}
		_, err = store.Finish(ctx, snapshot, session.Actor(), original.Key)
		require.NoError(t, err)
		next := original
		next.Key.Revision++
		next.TargetBuffers = 2048
		next.ExpectedCapacityGeneration++
		beginPG(t, store, next)
		_, err = worker.Advance(ctx)
		require.Error(t, err)
		require.EqualValues(t, 1, posts.Load())
		snapshot, err = store.Load(ctx)
		require.NoError(t, err)
		for _, step := range snapshot.State().Active.Steps {
			if step.Action == journal.ResizePG {
				require.Equal(t, journal.Planned, step.Phase)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func TestCredentialProviderWaitHasOwnDeadlineBeforeHTTPDo(t *testing.T) {
	_, store, client := newAPI(t)
	elector, err := New(client, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		request := pgRequest(t, session.Actor())
		config := pgConfig(t, request, func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "must never send", http.StatusInternalServerError)
		})
		config.Timeout = 200 * time.Millisecond
		var entered atomic.Bool
		config.Token = func(ctx context.Context) (string, error) { entered.Store(true); <-ctx.Done(); return "", ctx.Err() }
		started := time.Now()
		worker, err := session.NewPGWorker(ctx, config)
		require.Error(t, err)
		require.Nil(t, worker)
		require.True(t, entered.Load())
		require.Less(t, time.Since(started), time.Second, "HTTP Client.Timeout cannot bound a provider that runs before Do")
		require.NoError(t, session.Context().Err())
		return nil
	})
	require.NoError(t, err)
}
