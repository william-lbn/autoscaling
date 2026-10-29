package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 这里仅构造已选 WAL，原生回执/Root 为明确合同 peer。真实 Elector 在
// 构造之后接管，Session/SDK/typed UID/RV CAS/Run 走正式代码；不将该组合
// 当成新开放生产来源审计、真实 PG、Runner 或 Linux 端到端认证。
func serviceReopenRecoveryWAL(t *testing.T, store *journal.Store, mode string) (*journal.Snapshot, budget.Initial) {
	t.Helper()
	ctx := context.Background()
	raw, err := os.ReadFile("../journal/testdata/initial-domain.json")
	require.NoError(t, err)
	var fixture struct {
		Initial budget.Initial                 `json:"initial"`
		Command budget.InitialAdmissionCommand `json:"command"`
		Receipt budget.InitialAdmissionReceipt `json:"receipt"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	s, err := store.Load(ctx)
	require.NoError(t, err)
	actor := s.State().Owner
	i, command := fixture.Initial, fixture.Command
	i.IssuerEpoch, i.Shared.Observed.IssuerFloorEpoch = actor.Epoch, actor.Epoch
	command.InitialSHA256, err = i.Digest()
	require.NoError(t, err)
	command.Request.IssuerEpoch = actor.Epoch
	s, err = store.BindResource(ctx, s, actor, i.PGAggregate.Identity)
	require.NoError(t, err)
	s, err = store.BindPG(ctx, s, actor, i.Instance)
	require.NoError(t, err)
	s, err = store.BindInitialBudget(ctx, s, actor, i)
	require.NoError(t, err)
	s, _, err = store.StartInitialAdmission(ctx, s, actor, command)
	require.NoError(t, err)
	closeRequest := command.Request
	closeRequest.Open, closeRequest.Revision = false, closeRequest.Revision+1
	selection := control.AdmissionCloseRequest{ProtocolVersion: 1, Original: command.Request, Close: closeRequest}
	s, _, err = store.StartInitialServiceClosure(ctx, s, actor, selection)
	require.NoError(t, err)
	closed := serviceReopenNativeResponse(t, closeRequest, i.Shared.Observed, false)
	proof := control.AdmissionClosure{ProtocolVersion: 1, VMShrinkPermit: false, Original: command.Request, ClosedBy: closed, PriorMayHaveApplied: true}
	s, err = store.RecordInitialServiceClosure(ctx, s, actor, proof, false)
	require.NoError(t, err)
	lease := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: i.Instance, IssuerEpoch: actor.Epoch, ExpectedCapacityGeneration: 1, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Renewed: false, Armed: true, Expired: false, DeadlineNS: 100, LeaseEpoch: actor.Epoch, TimeoutMS: 30000, Observed: closed.Observed}
	s, _, err = store.StartServiceReopen(ctx, s, actor, s.State().ServiceClosure.CommandSHA256, lease)
	require.NoError(t, err)
	r := s.State().ServiceClosure.Reopen
	opened := serviceReopenNativeResponse(t, r.Request, closed.Observed, false)
	if mode == "open-settled" {
		s, err = store.RecordServiceReopen(ctx, s, actor, opened, false)
		require.NoError(t, err)
	}
	if strings.HasPrefix(mode, "close-") || strings.HasPrefix(mode, "continuation-") {
		lease.Observed = opened.Observed
		s, _, err = store.StartServiceReopenClosure(ctx, s, actor, r.CommandSHA256, lease)
		require.NoError(t, err)
		if mode == "close-settled" {
			request := s.State().ServiceClosure.Reopen.Closing.Close.Request
			proof := control.AdmissionClosure{ProtocolVersion: 1, VMShrinkPermit: false, Original: r.Request, ClosedBy: serviceReopenNativeResponse(t, request, closed.Observed, false), PriorMayHaveApplied: true}
			s, err = store.RecordServiceReopenClosure(ctx, s, actor, proof, false)
			require.NoError(t, err)
		}
	}
	return s, i
}

func serviceReopenNativeResponse(t *testing.T, request control.AdmissionRequest, observed control.Snapshot, query bool) control.AdmissionResponse {
	t.Helper()
	digest, err := request.ContextSHA256()
	require.NoError(t, err)
	observed.AdmissionOpen, observed.AdmissionRevision = request.Open, request.Revision
	disposition := control.AdmissionApplied
	if query {
		disposition = control.AdmissionReplayed
	}
	fact := control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, Disposition: disposition, ContextSHA256: digest, Observed: observed}
	require.NoError(t, fact.ValidateFor(request, request.Instance, query))
	return fact
}

func TestServiceReopenSDKRestoresLatestUnknownAndOnlyQueries(t *testing.T) {
	for _, mode := range []string{"open-notfound", "open-newer", "open-applied", "open-lost-record", "open-settled", "close-notfound", "close-applied", "close-lost-record", "close-settled", "continuation-notfound", "continuation-applied", "continuation-lost-record"} {
		t.Run(mode, func(t *testing.T) {
			api, store, coord := newAPI(t)
			original, initial := serviceReopenRecoveryWAL(t, store, mode)
			elector, err := New(coord, store, DefaultOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				s, err := store.Load(ctx)
				require.NoError(t, err)
				require.Greater(t, session.Actor().Epoch, original.State().Owner.Epoch)
				r := s.State().ServiceClosure.Reopen
				observed := r.Before.Observed
				observed.IssuerFloorEpoch = session.Actor().Epoch
				if strings.HasPrefix(mode, "continuation-") {
					before := r.Before
					before.Request.IssuerEpoch, before.Observed.IssuerFloorEpoch = session.Actor().Epoch, session.Actor().Epoch
					before.Observed.AdmissionRevision = r.Closing.Close.Request.Revision
					s, _, err = store.ContinueServiceReopenClosure(ctx, s, session.Actor(), r.Closing.Close.CommandSHA256, before)
					require.NoError(t, err)
					r = s.State().ServiceClosure.Reopen
				}
				root, resourceConfig := guestRoot(t, store)
				qualifiedRootFixture(root)
				root.mu.Lock()
				root.view.Bootstrap, root.view.Birth = initial.PGAggregate.Bootstrap, &initial.PGAggregate.Birth
				root.view.Actual.MemoryMaxBytes = initial.PGAggregate.Bootstrap.Limits.MemoryMaxBytes
				root.view.Actual.HugePools = initial.PGAggregate.Bootstrap.Limits.HugePools
				root.mu.Unlock()
				resources, err := session.NewResourceWorker(ctx, resourceConfig)
				require.NoError(t, err)
				defer resources.CloseIdleConnections()
				request := pgRequest(t, session.Actor())
				request.Key.Instance = initial.Instance
				var sends, queries atomic.Int64
				config := pgConfig(t, request, func(w http.ResponseWriter, req *http.Request) {
					switch req.URL.Path {
					case "/dsg/v1/admission/status":
						queries.Add(1)
						var original control.AdmissionRequest
						require.NoError(t, json.NewDecoder(req.Body).Decode(&original))
						require.Equal(t, r.Request, original)
						fact := serviceReopenNativeResponse(t, original, observed, true)
						switch mode {
						case "open-notfound":
							fact.Disposition, fact.Observed = control.AdmissionNotFound, observed
						case "open-newer":
							fact.Observed.AdmissionRevision++
						}
						pgReply(w, fact)
					case "/dsg/v1/admission/close-original/status":
						queries.Add(1)
						var q control.AdmissionCloseRequest
						require.NoError(t, json.NewDecoder(req.Body).Decode(&q))
						require.Equal(t, control.AdmissionCloseRequest{ProtocolVersion: 1, Original: r.Request, Close: r.Closing.Close.Request}, q)
						if mode == "close-notfound" {
							w.WriteHeader(http.StatusConflict)
							return
						}
						pgReply(w, control.AdmissionClosure{ProtocolVersion: 1, VMShrinkPermit: false, Original: r.Request, ClosedBy: serviceReopenNativeResponse(t, q.Close, observed, true), PriorMayHaveApplied: true})
					case "/dsg/v1/admission/continue-close/status":
						queries.Add(1)
						var q control.AdmissionContinuationRequest
						require.NoError(t, json.NewDecoder(req.Body).Decode(&q))
						require.Equal(t, r.Closing.Close.Continuation.Request, q)
						if mode == "continuation-notfound" {
							w.WriteHeader(http.StatusConflict)
							return
						}
						digest, err := q.ContextSHA256()
						require.NoError(t, err)
						pgReply(w, control.AdmissionContinuationResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: q, ContextSHA256: digest, ClosedBy: serviceReopenNativeResponse(t, q.Close, observed, true), PriorMayHaveApplied: true})
					default:
						sends.Add(1)
						t.Errorf("只读恢复出现其他领域请求：%s", req.URL.Path)
						w.WriteHeader(http.StatusInternalServerError)
					}
				})
				worker, err := session.NewPGWorker(ctx, config, resources)
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				if mode == "open-notfound" {
					wrong := config
					wrong.AdmissionPending = original.State().ServiceClosure.Original
					_, err = session.NewPGWorker(ctx, wrong, resources)
					require.Error(t, err, "不能用旧首次开放替代新开放 WAL")
				}
				pending, closing := worker.client.PendingAdmission()
				terminal := strings.HasSuffix(mode, "settled")
				if terminal {
					require.Nil(t, pending)
					require.Nil(t, closing)
				} else {
					require.Equal(t, &r.Request, pending)
					if r.Closing != nil {
						_, _, selected, _ := r.Closing.Close.Selection()
						require.Equal(t, &selected, closing)
						if r.Closing.Close.Continuation != nil {
							require.Equal(t, &r.Closing.Close.Continuation.Request, worker.client.PendingAdmissionContinuation())
						}
					} else {
						require.Nil(t, closing)
					}
				}
				if strings.HasSuffix(mode, "lost-record") {
					api.mu.Lock()
					api.loseJournalReply = true
					api.mu.Unlock()
				}
				_, err = worker.Advance(ctx)
				unresolved := strings.HasSuffix(mode, "notfound") || mode == "open-newer"
				if terminal || unresolved {
					require.ErrorIs(t, err, ErrServiceSafety)
				} else if strings.HasSuffix(mode, "lost-record") {
					var unknown *journal.UnconfirmedWrite
					require.ErrorAs(t, err, &unknown)
				} else {
					require.NoError(t, err)
				}
				after, err := store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, original.State().InitialHeld, after.State().InitialHeld)
				require.Equal(t, journal.Unconfirmed, after.State().InitialHeld.Admission.Phase)
				if r.Closing != nil {
					require.Equal(t, unresolved, after.State().ServiceClosure.Reopen.Closing.Close.Pending())
					require.Equal(t, journal.Unconfirmed, after.State().ServiceClosure.Reopen.Phase)
				} else if !unresolved {
					require.Equal(t, journal.Settled, after.State().ServiceClosure.Reopen.Phase)
				}
				rebuilt, err := session.NewPGWorker(ctx, config, resources)
				require.NoError(t, err)
				defer rebuilt.CloseIdleConnections()
				pending, closing = rebuilt.client.PendingAdmission()
				if unresolved {
					require.Equal(t, &r.Request, pending, "未知须从准确原 WAL 再恢复")
				} else {
					require.Nil(t, pending)
					require.Nil(t, closing)
					require.Nil(t, rebuilt.client.PendingAdmissionContinuation())
				}
				if terminal {
					require.Zero(t, queries.Load())
				} else {
					require.EqualValues(t, 1, queries.Load())
				}
				require.Zero(t, sends.Load())
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// 本例只激活 Run 的原生过期分支，不构造生产来源审计器：已存在完整
// ServiceClosure 的路径必须先读原 WAL，不访问未配置的来源 Reader。
// 完整生产来源/派发/再关闭与业务重开在后继集成组合中另验。
func TestExpiredRunReadsReopenWALWithoutRenewOrSetter(t *testing.T) {
	_, store, coord := newAPI(t)
	_, initial := serviceReopenRecoveryWAL(t, store, "open-notfound")
	elector, err := New(coord, store, DefaultOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		before, err := store.Load(ctx)
		require.NoError(t, err)
		r := before.State().ServiceClosure.Reopen
		observed := r.Before.Observed
		observed.IssuerFloorEpoch = session.Actor().Epoch
		root, resourceConfig := guestRoot(t, store)
		qualifiedRootFixture(root)
		root.mu.Lock()
		root.view.Bootstrap, root.view.Birth = initial.PGAggregate.Bootstrap, &initial.PGAggregate.Birth
		root.view.Actual.MemoryMaxBytes = initial.PGAggregate.Bootstrap.Limits.MemoryMaxBytes
		root.view.Actual.HugePools = initial.PGAggregate.Bootstrap.Limits.HugePools
		root.mu.Unlock()
		resources, err := session.NewResourceWorker(ctx, resourceConfig)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		request := pgRequest(t, session.Actor())
		request.Key.Instance = initial.Instance
		var queries, mutations atomic.Int64
		config := pgConfig(t, request, func(w http.ResponseWriter, req *http.Request) {
			switch req.URL.Path {
			case "/dsg/v1/admission/lease":
				var q control.AdmissionLeaseRequest
				require.NoError(t, json.NewDecoder(req.Body).Decode(&q))
				if q.Action != "observe" {
					mutations.Add(1)
					t.Errorf("过期 Run 不得自动续租/rearm：%s", q.Action)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				pgReply(w, control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: q, Renewed: false, Armed: true, Expired: true, DeadlineNS: 100, LeaseEpoch: r.Origin.Epoch, TimeoutMS: 30000, Observed: observed})
			case "/dsg/v1/admission/status":
				queries.Add(1)
				var q control.AdmissionRequest
				require.NoError(t, json.NewDecoder(req.Body).Decode(&q))
				require.Equal(t, r.Request, q)
				fact := serviceReopenNativeResponse(t, q, observed, true)
				fact.Disposition, fact.Observed = control.AdmissionNotFound, observed
				pgReply(w, fact)
			default:
				mutations.Add(1)
				t.Errorf("过期只读恢复出现其他领域请求：%s", req.URL.Path)
				w.WriteHeader(http.StatusInternalServerError)
			}
		}, func() control.Snapshot { return observed })
		worker, err := session.NewPGWorker(ctx, config, resources)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		worker.safety = &serviceSafety{claims: nil, runner: nil}
		endpoint := worker.NewControlEndpoint()
		queried := make(chan struct{}, 1)
		var options PGLoopOptions
		options.PollInterval, options.Control = 10*time.Millisecond, endpoint
		options.Observe = func(Tick) {
			if queries.Load() > 0 {
				select {
				case queried <- struct{}{}:
				default:
				}
			}
		}
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- worker.Run(runCtx, options) }()
		joined := false
		defer func() {
			stop()
			if !joined {
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("取消后新开放只读 Run 未归还")
				}
			}
		}()
		select {
		case <-queried:
		case <-ctx.Done():
			t.Fatal("过期 Run 没有查询新开放原 WAL")
		}
		state, err := endpoint.Submit(ctx, closureControl(session, initial.Instance, control.CoordinationState, ""))
		require.NoError(t, err)
		require.Equal(t, journal.Unconfirmed, state.State.ServiceClosure.Reopen.Phase)
		begin := closureControl(session, initial.Instance, control.CoordinationBegin, "")
		begin.Request = &request
		_, err = endpoint.Submit(ctx, begin)
		require.ErrorIs(t, err, ErrServiceSafety)
		stop()
		runErr := <-done
		joined = true
		require.ErrorIs(t, runErr, context.Canceled)
		after, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), after.State(), "只读等待不写新意图或改变原未知")
		require.Greater(t, queries.Load(), int64(0))
		require.Zero(t, mutations.Load())
		return nil
	})
	require.NoError(t, err)
}
