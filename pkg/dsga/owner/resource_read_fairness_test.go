package owner

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/stretchr/testify/require"
)

// 复用真实Elector、UID/RV Store和TLS Root/PG客户端；不冒充物理PG或平台资格。
// barrier同步放入有界read槽，Observe在下一audit完成的原goroutine检查顺序，
// 不靠sleep或Go select伪随机结果构造RED。普通选择器仍可消费同一read槽。
func TestPGResourceStateReadBeforeNextPeriodicAudit(t *testing.T) {
	runResourceReadSchedule(t, "single", 1)
}

func TestPGResourceStateFloodKeepsAuditAndRenewal(t *testing.T) {
	runResourceReadSchedule(t, "flood", 3)
}

func TestPGResourceStateReadKeepsCancelLeaseAndFreshChecks(t *testing.T) {
	for _, mode := range []string{"cancelled", "lease-changed", "root-floor-changed"} {
		t.Run(mode, func(t *testing.T) { runResourceReadSchedule(t, mode, 1) })
	}
}

func runResourceReadSchedule(t *testing.T, mode string, count int) {
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
		var deadline control.Uint64 = 1000000000
		config := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/dsg/v1/admission/lease" {
				forbidden.Add(1)
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
				renewed = intent.ExpectedDeadlineNS == deadline && intent.ExpectedCapacityGeneration == native.CapacityGeneration && intent.ExpectedAdmissionRevision == native.AdmissionRevision && intent.IssuerEpoch == native.IssuerFloorEpoch
				if renewed {
					deadline++
				}
			} else if intent.Action == "observe" {
				observes.Add(1)
			}
			pgReply(w, control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Expired: false, Request: intent, Armed: true, Renewed: renewed, DeadlineNS: deadline, LeaseEpoch: native.IssuerFloorEpoch, TimeoutMS: 30000, Observed: native})
		}, func() control.Snapshot { return native })
		worker, err := session.NewPGWorker(ctx, config, resources)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		// 本夹具尚无业务预算/开放，启用原原生lease协商和到期续租分支；
		// serviceSafety仍按原Model判断，无伪造Initial/Source或准入许可。
		worker.safety = &serviceSafety{}
		endpoint := worker.NewControlEndpoint()
		require.Equal(t, 1, cap(endpoint.readCalls))
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
			return controlCall{ctx: readCtx, command: control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(instance), ExpectedOwnerEpoch: epoch, Action: control.CoordinationResourceState}, result: make(chan controlReply, 1)}
		}
		// flood在真实Root令牌回调中同步补下一条read；它不能因此得到第二个credit。
		tokenReadHook = func() {
			if mode == "flood" && len(reads) < count {
				call := makeRead(loopCtx)
				reads = append(reads, call)
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
					require.NoError(t, call.command.Validate())
					reads = append(reads, call)
					endpoint.readCalls <- call // 容量1，屏障内确定已排队。
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
					if audits > 1 && mode != "lease-changed" {
						if checked >= len(reads) {
							t.Error("periodic audit restarted before the queued pure read was admitted")
							stop()
							return
						}
						select {
						case reply := <-reads[checked].result:
							if mode == "cancelled" {
								require.ErrorIs(t, reply.err, context.Canceled)
								require.Nil(t, reply.value.ResourceState)
							} else if mode == "root-floor-changed" {
								require.Error(t, reply.err)
								require.Nil(t, reply.value.ResourceState)
							} else {
								require.NoError(t, reply.err)
								require.NotNil(t, reply.value.ResourceState)
								require.Equal(t, epoch, reply.value.OwnerEpoch)
								require.Equal(t, pgexec.Idle, reply.value.Progress)
							}
							checked++
						default:
							t.Error("queued resource_state was starved by another periodic audit")
							stop()
							return
						}
					}
					if mode != "lease-changed" {
						require.NoError(t, tick.Err)
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
				require.Nil(t, reply.value.ResourceState)
			default:
				t.Error("changed Lease did not reject the queued read before termination")
			}
			require.Zero(t, resourceTokens.Load())
		} else {
			require.ErrorIs(t, loopErr, context.Canceled)
			require.Equal(t, count, checked)
			require.Equal(t, count+1, audits, "每个read之后必须重新走完整周期，不连读耗尽安全审计")
			if mode == "cancelled" {
				require.Zero(t, resourceTokens.Load())
			}
			if mode == "single" || mode == "flood" {
				require.EqualValues(t, count, resourceTokens.Load())
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
		return nil
	})
	require.NoError(t, err)
}

// 单独核Submit接线，不执行Run或写任何领域；回复是本地错误哨兵，不冒PG事实。
func TestPGResourceStateLaneLeavesMutationAndRecoveryOnOriginalQueue(t *testing.T) {
	_, store, coord := newAPI(t)
	_, rootConfig := newResourceOwnerFixture(t, store)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceWorker(ctx, rootConfig)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		request := pgRequest(t, session.Actor())
		config := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
			t.Error("routing-only test reached PG mutation")
			w.WriteHeader(500)
		})
		worker, err := session.NewPGWorker(ctx, config, resources)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		endpoint := worker.NewControlEndpoint()
		sentinel := errors.New("routing-only-reply")
		check := func(e *ControlEndpoint, command control.CoordinationCommand, lane <-chan controlCall) {
			done := make(chan error, 1)
			go func() { _, err := e.Submit(ctx, command); done <- err }()
			select {
			case call := <-lane:
				require.Equal(t, command.Action, call.command.Action)
				call.result <- controlReply{err: sentinel}
			case <-ctx.Done():
				t.Fatal("command did not reach its exact queue")
			}
			require.ErrorIs(t, <-done, sentinel)
		}
		instance, epoch := endpoint.Binding()
		command := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(instance), ExpectedOwnerEpoch: epoch, Action: control.CoordinationResourceState}
		check(endpoint, command, endpoint.readCalls)
		command.Action = control.CoordinationState
		check(endpoint, command, endpoint.calls)
		command.Action, command.Request = control.CoordinationBegin, &request
		check(endpoint, command, endpoint.calls)
		require.Empty(t, endpoint.readCalls)
		recovery := resources.NewRecoveryControlEndpoint()
		require.Nil(t, recovery.readCalls)
		rid, re := recovery.Binding()
		command = control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(rid), ExpectedOwnerEpoch: re, Action: control.CoordinationResourceState}
		check(recovery, command, recovery.calls)
		command.ExpectedOwnerEpoch++
		_, err = recovery.Submit(ctx, command)
		require.ErrorIs(t, err, ErrControlUnavailable)
		require.Empty(t, endpoint.readCalls)
		return nil
	})
	require.NoError(t, err)
}
