package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 复用真实Elector、UID/RV Store和TLS Root/PG客户端；不冒充物理PG或平台资格。
// barrier仅在测试中用buffer同步表示已等待的原control sender，Observe检查顺序，
// 不靠sleep或Go select伪随机构造RED；生产写队列仍无缓冲，执行门未替换。
func TestPGControlCreditBeforeNextPeriodicAudit(t *testing.T) {
	runControlCreditSchedule(t, "single", 1)
}

func TestPGControlCreditBothReadyLanesMakeBoundedProgress(t *testing.T) {
	runControlCreditSchedule(t, "both-ready", 1)
}
func TestPGControlCreditFloodStillAuditsAndRenews(t *testing.T) {
	runControlCreditSchedule(t, "flood", 3)
}

func runControlCreditSchedule(t *testing.T, mode string, count int) {
	t.Helper()
	api, store, coord := newAPI(t)
	root, rootConfig := newResourceOwnerFixture(t, store)
	var resourceTokens atomic.Int32
	var tokenReadHook func()
	rootConfig.Token = func(context.Context) (string, error) {
		resourceTokens.Add(1)
		if tokenReadHook != nil {
			tokenReadHook()
		}
		return "resource-owner-fixture", nil
	}
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceWorker(ctx, rootConfig)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		request := pgRequest(t, session.Actor())
		native := pgResponse(t, request).Observed
		native.Operation, native.AdmissionOpen, native.AdmissionRevision, native.CapacityGeneration = nil, false, 0, 1
		var observes, renews, forbidden atomic.Int32
		var expired atomic.Bool
		var deadline control.Uint64 = 1000000000
		config := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/dsg/v1/admission/lease" {
				// expired后的原关闭检查可只读失败；从不把500解释为安全事实。
				if r.Method != http.MethodGet {
					forbidden.Add(1)
				}
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			var intent control.AdmissionLeaseRequest
			if json.NewDecoder(r.Body).Decode(&intent) != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			renewed := false
			if intent.Action == "renew" {
				renews.Add(1)
				renewed = !expired.Load() && intent.ExpectedDeadlineNS == deadline && intent.ExpectedCapacityGeneration == native.CapacityGeneration && intent.ExpectedAdmissionRevision == native.AdmissionRevision && intent.IssuerEpoch == native.IssuerFloorEpoch
				if renewed {
					deadline++
				}
			} else if intent.Action == "observe" {
				observes.Add(1)
			}
			pgReply(w, control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Expired: expired.Load(), Request: intent, Armed: true, Renewed: renewed, DeadlineNS: deadline, LeaseEpoch: native.IssuerFloorEpoch, TimeoutMS: 30000, Observed: native})
		}, func() control.Snapshot { return native })
		worker, err := session.NewPGWorker(ctx, config, resources)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		// 本夹具尚无业务预算/开放，启用原原生lease协商和到期续租分支；
		// serviceSafety仍按原Model判断，无伪造Initial/Source或准入许可。
		worker.safety = &serviceSafety{}
		endpoint := worker.NewControlEndpoint()
		require.Zero(t, cap(endpoint.calls), "生产写队列保持无缓冲")
		// 仅测试buffer同步代表已等待sender；Run看到的ready语义相同，生产Submit不改。
		endpoint.calls = make(chan controlCall, 1)
		before, err := store.Load(ctx)
		require.NoError(t, err)
		vmVersion := before.ResourceVersion()
		resourceTokens.Store(0)
		root.mu.Lock()
		fences, repairs, discoveries := root.fences, root.repairs, root.discoveries
		root.mu.Unlock()
		loopCtx, stop := context.WithCancel(ctx)
		defer stop()
		wake := make(chan struct{}, 1)
		var reads []controlCall // 仅Run goroutine访问；所有hook/Observe都由同一goroutine执行。
		var checked, audits int
		queued := false
		done := make(chan error, 1)
		instance, epoch := endpoint.Binding()
		makeRead := func(readCtx context.Context) controlCall {
			return controlCall{ctx: readCtx, command: control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(instance), ExpectedOwnerEpoch: epoch, Action: control.CoordinationState}, result: make(chan controlReply, 1)}
		}
		// flood在真实Root令牌回调中同步补下一条read；它不能因此得到第二个credit。
		var resourceCalls []controlCall
		tokenReadHook = func() {
			if mode == "both-ready" && len(endpoint.readCalls) == 0 {
				call := controlCall{ctx: loopCtx, command: control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(instance), ExpectedOwnerEpoch: epoch, Action: control.CoordinationResourceState}, result: make(chan controlReply, 1)}
				resourceCalls = append(resourceCalls, call)
				endpoint.readCalls <- call
			}
		}

		go func() {
			done <- worker.Run(loopCtx, PGLoopOptions{
				PollInterval: time.Millisecond, Control: endpoint, Wake: wake,
				beforePeriodicAdvance: func(c context.Context) {
					if queued {
						return
					}
					queued = true
					readCtx := loopCtx
					if mode == "cancelled" {
						var closeRead context.CancelFunc
						readCtx, closeRead = context.WithCancel(loopCtx)
						closeRead()
					}
					call := makeRead(readCtx)
					if mode == "expired" {
						call.command.Action, call.command.Request = control.CoordinationBegin, &request
						expired.Store(true)
					}
					require.NoError(t, call.command.Validate())
					reads = append(reads, call)
					endpoint.calls <- call // 容量1，屏障内确定已排队。
					if mode == "both-ready" {
						tokenReadHook()
					}
					if mode == "lease-changed" {
						api.mu.Lock()
						holder := "another-holder"
						api.lease.Spec.HolderIdentity = &holder
						api.mu.Unlock()
					}
					if mode == "root-floor-changed" {
						root.mu.Lock()
						root.view.IssuerFloorEpoch = session.Actor().Epoch + 1
						root.mu.Unlock()
					}
				},
				Observe: func(tick Tick) {
					audits++
					if mode == "both-ready" && audits == 2 {
						select {
						case reply := <-resourceCalls[0].result:
							assert.NoError(t, reply.err)
							assert.NotNil(t, reply.value.ResourceState)
						default:
							t.Error("ready resource lane did not consume its first credit")
							stop()
						}
					} else if audits > 1 && mode != "lease-changed" {
						if checked >= len(reads) {
							t.Error("periodic audit restarted before waiting control was admitted")
							stop()
							return
						}
						select {
						case reply := <-reads[checked].result:
							if mode == "cancelled" {
								assert.ErrorIs(t, reply.err, context.Canceled)
								assert.Nil(t, reply.value.State)
							} else if mode == "root-floor-changed" || mode == "expired" {
								assert.Error(t, reply.err)
								assert.Nil(t, reply.value.State)
							} else {
								assert.NoError(t, reply.err)
								assert.NotNil(t, reply.value.State)
								assert.Equal(t, epoch, reply.value.OwnerEpoch)
								assert.Equal(t, pgexec.Idle, reply.value.Progress)
							}
							checked++
							if mode == "flood" && checked < count {
								queued = false
							}
						default:
							t.Error("waiting control was starved by another periodic audit")
							stop()
							return
						}
					}
					if mode == "expired" && expired.Load() {
						assert.ErrorIs(t, tick.Err, ErrServiceSafety, "原过期安全等待必须保留")
					} else if mode != "lease-changed" {
						assert.NoError(t, tick.Err)
					}
					if checked == count {
						stop()
						return
					}
					select {
					case wake <- struct{}{}:
					default:
					}
				},
			})
		}()
		loopErr := <-done
		if mode == "lease-changed" {
			require.Error(t, loopErr)
			require.Equal(t, 1, len(reads))
			select {
			case reply := <-reads[0].result:
				require.Error(t, reply.err)
				require.Nil(t, reply.value.State)
			default:
				t.Error("changed Lease did not reject the queued read before termination")
			}
			require.Zero(t, resourceTokens.Load())
		} else {
			require.ErrorIs(t, loopErr, context.Canceled)
			require.Equal(t, count, checked)
			expectedAudits := count + 1
			if mode == "both-ready" {
				expectedAudits++
			}
			require.Equal(t, expectedAudits, audits, "每个control之后必须重新走完整周期，credit不可积累")
			if mode == "cancelled" {
				require.Zero(t, resourceTokens.Load())
			}
			if mode == "single" || mode == "flood" {
				require.Zero(t, resourceTokens.Load(), "state不触Root资源读取")
			}
		}
		require.GreaterOrEqual(t, observes.Load(), int32(1))
		require.GreaterOrEqual(t, renews.Load(), int32(1), "首次到期检查仍执行原审计后的原生lease续租")
		require.Zero(t, forbidden.Load(), "不得重放PG/Root mutation")
		root.mu.Lock()
		require.Equal(t, fences, root.fences)
		require.Equal(t, repairs, root.repairs)
		require.Equal(t, discoveries, root.discoveries)
		root.mu.Unlock()
		// 原VM资源日志没有Begin/资格/预算写，lease续租独立保留。
		api.mu.Lock()
		require.Equal(t, vmVersion, api.vm.ResourceVersion)
		api.mu.Unlock()
		if mode == "lease-changed" {
			cancel()
		} // 已确认Run拒绝并join；不要让外层Elector继续等待另一holder。
		return nil
	})
	if mode == "lease-changed" {
		require.ErrorIs(t, err, context.Canceled)
	} else {
		require.NoError(t, err)
	}
}
