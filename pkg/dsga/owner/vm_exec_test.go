package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 只替代QEMU/KVM：真正Unix socket和QMP帧与生产Runner对话，唯一qom-set还
// 经真实typed VM WAL查询确认。Guest/root/PG/Host内核均是明确协议夹具；测试
// 覆盖实际Agent/Session/Runner/写前CAS/预算/准入组合，不外推物理热拔或RBAC。
type vmQMPPeer struct {
	mu       sync.Mutex
	uuid     string
	minimum  uint64
	maximum  uint64
	actual   uint64
	writes   int
	lose     bool
	onResize func(uint64)
}

func (p *vmQMPPeer) serve(conn net.Conn) {
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return
	}
	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)
	if err := encoder.Encode(map[string]any{"QMP": map[string]any{"version": map[string]any{}, "capabilities": []string{}}}); err != nil {
		return
	}
	for {
		var req struct {
			Execute   string `json:"execute"`
			ID        string `json:"id"`
			Arguments struct {
				Path     string `json:"path"`
				Property string `json:"property"`
				Value    uint64 `json:"value"`
			} `json:"arguments"`
		}
		if err := decoder.Decode(&req); err != nil {
			return
		}
		p.mu.Lock()
		var value any = map[string]any{}
		switch req.Execute {
		case "qmp_capabilities":
		case "query-uuid":
			value = map[string]any{"UUID": p.uuid}
		case "query-memory-devices":
			value = []any{map[string]any{"type": "virtio-mem", "data": map[string]any{"id": "vm0", "memdev": "/objects/vmem0", "requested-size": p.actual - p.minimum, "size": p.actual - p.minimum, "max-size": p.maximum - p.minimum, "block-size": vmguard.VirtioBlockBytes}}}
		case "query-memory-size-summary":
			value = map[string]any{"base-memory": p.minimum, "plugged-memory": p.actual - p.minimum}
		case "qom-set":
			if req.Arguments.Path != "/machine/peripheral/vm0" || req.Arguments.Property != "requested-size" {
				p.mu.Unlock()
				return
			}
			p.actual = p.minimum + req.Arguments.Value
			p.writes++
			p.onResize(p.actual)
			if p.lose {
				p.lose = false
				p.mu.Unlock()
				return
			}
		default:
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()
		if err := encoder.Encode(map[string]any{"id": req.ID, "return": value}); err != nil {
			return
		}
	}
}

func vmPeer(t *testing.T, path string, p *vmQMPPeer) {
	t.Helper()
	listener, err := net.Listen("unix", path)
	require.NoError(t, err)
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() { defer wg.Done(); p.serve(conn) }()
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-done; wg.Wait() })
}

func vmCore(t *testing.T) *kubefake.Clientset {
	t.Helper()
	controller, mount := true, true
	pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: binding().Namespace, UID: "runner-uid", ResourceVersion: "pod-rv-1", OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: binding().Name, UID: binding().VMUID, Controller: &controller, BlockOwnerDeletion: nil}}}, Spec: core.PodSpec{NodeName: "node-one", ServiceAccountName: "dsga-runner-test", AutomountServiceAccountToken: &mount}, Status: core.PodStatus{Phase: core.PodRunning, PodIP: "127.0.0.1"}}
	node := dsgatest.InitialNode(&core.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-one", UID: "node-uid", ResourceVersion: "node-rv-1"}}, binding(), "runner", "runner-uid", 2<<30)
	client := kubefake.NewSimpleClientset(pod, node)
	var sequence atomic.Int64
	client.PrependReactor("create", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		cm := action.(kt.CreateAction).GetObject().(*core.ConfigMap).DeepCopy()
		cm.UID = types.UID(fmt.Sprintf("actual-uid-%d", sequence.Add(1)))
		cm.ResourceVersion = "1"
		if err := client.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace); err != nil {
			return true, nil, err
		}
		return true, cm, nil
	})
	client.PrependReactor("update", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		cm := action.(kt.UpdateAction).GetObject().(*core.ConfigMap).DeepCopy()
		old, err := client.Tracker().Get(core.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name)
		if err != nil {
			return true, nil, err
		}
		before := old.(*core.ConfigMap)
		if cm.UID != before.UID || cm.ResourceVersion != before.ResourceVersion {
			return true, nil, errors.New("fixture UID/RV conflict")
		}
		v, _ := strconv.Atoi(cm.ResourceVersion)
		cm.ResourceVersion = strconv.Itoa(v + 1)
		if err := client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace); err != nil {
			return true, nil, err
		}
		return true, cm, nil
	})
	return client
}

func vmReservation(t *testing.T, client *kubefake.Clientset, name, kind string, body any) budget.ObjectRef {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	controller, immutable := true, true
	cm, err := client.CoreV1().ConfigMaps(binding().Namespace).Create(context.Background(), &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: binding().Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: binding().Name, UID: binding().VMUID, Controller: &controller, BlockOwnerDeletion: &controller}}, Finalizers: []string{budgetclaims.ProtectionFinalizer}, Labels: map[string]string{budgetclaims.ClaimLabel: kind}}, Immutable: &immutable, Data: map[string]string{budgetclaims.ClaimKey: string(raw)}}, metav1.CreateOptions{})
	require.NoError(t, err)
	return budget.ObjectRef{Name: cm.Name, UID: string(cm.UID)}
}

func TestProductionAgentRunnerGrowClosesBudgetAndAdmissionOnOriginalWAL(t *testing.T) {
	for _, mode := range []string{"complete", "qmp-lost-reply", "start-lost-reply", "VM-record-lost-reply", "loop-waits-for-runner", "loop-holds-not-sent", "loop-explicit-VM-replay", "loop-explicit-VM-replay-lost-selection", "loop-explicit-VM-replay-host-gone-before", "loop-explicit-VM-replay-host-gone-after", "reject-VM-replay-may-have-sent"} {
		t.Run(mode, func(t *testing.T) {
			api, store, coord := newAPI(t)
			client := vmCore(t)
			api.mu.Lock()
			cpuMode := vmv1.CpuScalingModeSysfs
			api.vm.Spec.CpuScalingMode = &cpuMode
			api.vm.Spec.ServiceAccountName = "dsga-runner-test"
			api.vm.Spec.Guest.MemorySlotSize = *resource.NewQuantity(256<<20, resource.BinarySI)
			api.vm.Spec.Guest.MemorySlots.Min = 2
			api.vm.Spec.Guest.MemorySlots.Max = 8
			api.vm.Spec.Guest.MemorySlots.Use = 2
			api.vm.Status.Node = "node-one"
			api.vm.Status.MemorySize = resource.NewQuantity(512<<20, resource.BinarySI)
			api.mu.Unlock()
			// 短绝对路径满足Darwin sockaddr_un上限，WAL与QMP使用真实0700目录。
			dir, err := os.MkdirTemp("/private/tmp", "dsga-agent-")
			require.NoError(t, err)
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			runner, err := vmguard.Initialize(context.Background(), store, client.CoreV1(), coord, vmguard.Config{StateDir: dir, PodName: "runner", PodUID: "runner-uid", NodeName: "node-one", MinimumBytes: 512 << 20, MaximumBytes: 2 << 30})
			require.NoError(t, err)
			root, resourceConfig := guestRoot(t, store)
			root.mu.Lock()
			root.view.Actual.GuestMemTotalBytes = 480 << 20
			root.mu.Unlock()
			args := runner.QMPArguments()
			peer := &vmQMPPeer{
				uuid: args[1], minimum: 512 << 20, maximum: 2 << 30, actual: 512 << 20, lose: mode == "qmp-lost-reply", onResize: func(actual uint64) {
					s, err := store.Load(context.Background())
					require.NoError(t, err)
					require.NotNil(t, s.State().Active)
					step := budgetStep(t, store, journal.ResizeVM)
					require.Equal(t, journal.Unconfirmed, step.Phase, "实际QMP效果前必须原typed WAL已持久")
					root.mu.Lock()
					root.view.Actual.GuestMemTotalBytes = control.Uint64(actual - (256 << 20))
					root.mu.Unlock()
					api.mu.Lock()
					api.vm.Status.MemorySize = resource.NewQuantity(int64(actual), resource.BinarySI)
					api.vmVersion++
					api.vm.ResourceVersion = fmt.Sprintf("vm-rv-%d", api.vmVersion)
					api.mu.Unlock()
				}, mu: sync.Mutex{}, writes: 0,
			}
			vmPeer(t, filepath.Join(dir, "qmp.sock"), peer)
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				resources, err := session.NewResourceWorker(ctx, resourceConfig)
				require.NoError(t, err)
				defer resources.CloseIdleConnections()
				req := pgRequest(t, session.Actor())
				p, baseline := preparedGuest(t, req, root.view.Identity)
				final := baseline
				final.Observed = pgResponse(t, req).Observed
				final.CurrentMainAndArraysUpperBytes = final.StartupMainUpperBytes
				for _, a := range final.Observed.Arrays {
					final.CurrentMainAndArraysUpperBytes += a.CommittedUpperBytes
				}
				var committed atomic.Bool
				var resizePosts, opens atomic.Int64
				config := pgConfig(t, req, func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/dsg/v1/budget/shared":
						if committed.Load() {
							pgReply(w, final)
						} else {
							pgReply(w, baseline)
						}
					case "/dsg/v1/resize":
						resizePosts.Add(1)
						committed.Store(true)
						pgReply(w, pgResponse(t, req))
					case "/dsg/v1/resize/status":
						pgReply(w, pgResponse(t, req))
					case "/dsg/v1/admission":
						var a control.AdmissionRequest
						require.NoError(t, json.NewDecoder(r.Body).Decode(&a))
						hash, err := a.ContextSHA256()
						require.NoError(t, err)
						observed := final.Observed
						observed.AdmissionOpen = true
						observed.AdmissionRevision = a.Revision
						opens.Add(1)
						pgReply(w, control.AdmissionResponse{ProtocolVersion: 1, Request: a, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: observed, VMShrinkPermit: false})
					default:
						t.Errorf("unexpected Agent/PG route %s", r.URL.Path)
						w.WriteHeader(500)
					}
				})
				worker, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				require.NoError(t, runner.Tick(ctx))
				out := budget.OutsideReservation{ProtocolVersion: 1, Request: req, Identity: p.Identity, Incarnation: binding().Incarnation, ResidualProfileID: "explicit-qualified-platform-fixture", SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true, VMShrinkPermit: false}
				outRef := vmReservation(t, client, "outside-original", budgetclaims.OutsideKind, out)
				p.OutsideClaim = &outRef
				hostName, err := budgetclaims.HostClaimName(binding(), req)
				require.NoError(t, err)
				s, err := store.Load(ctx)
				require.NoError(t, err)
				_, _, err = store.Begin(ctx, s, session.Actor(), req, 1)
				require.NoError(t, err)
				priorBudgetDomain(t, ctx, store, session.Actor(), req, journal.PrepareBudget, req.Key.Instance.ComputeInstanceID, p)
				host := publishVMHost(t, ctx, client, coord, store, session)
				progress, err := worker.Advance(ctx)
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress, "生产Agent消费Node/CM原持有并结算ReserveHost")
				require.Equal(t, host.SourceID, budgetStep(t, store, journal.ReserveHost).TargetID)
				if mode == "start-lost-reply" {
					api.mu.Lock()
					api.loseJournalReply = true
					api.mu.Unlock()
				}
				progress, err = worker.Advance(ctx)
				if mode == "start-lost-reply" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
				}
				require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizeVM).Phase)
				_, err = worker.Advance(ctx)
				require.ErrorIs(t, err, vmguard.ErrUnknown, "轮询在原Runner回执之前只能观察，不得重发")
				if mode == "reject-VM-replay-may-have-sent" {
					peer.mu.Lock()
					peer.lose = true
					peer.mu.Unlock()
					require.ErrorIs(t, runner.Tick(ctx), vmguard.ErrUnknown)
					_, err = worker.ReplayVMOriginal(ctx, req.Key)
					require.ErrorIs(t, err, vmguard.ErrUnknown)
					s, err := store.Load(ctx)
					require.NoError(t, err)
					require.Empty(t, s.VirtualMachine().Annotations[journal.VMReplayAnnotation])
					require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizeVM).Phase)
					peer.mu.Lock()
					require.Equal(t, 1, peer.writes)
					peer.mu.Unlock()
					require.Zero(t, resizePosts.Load())
					require.Zero(t, opens.Load())
					return nil
				}
				replayVM := mode == "loop-explicit-VM-replay" || mode == "loop-explicit-VM-replay-lost-selection" || mode == "loop-explicit-VM-replay-host-gone-before" || mode == "loop-explicit-VM-replay-host-gone-after"
				loop := mode == "loop-waits-for-runner" || mode == "loop-holds-not-sent" || replayVM
				if loop {
					if mode == "loop-holds-not-sent" || replayVM {
						reads := 0
						client.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
							if action.(kt.GetAction).GetName() == outRef.Name {
								reads++
								if reads == 3 {
									// Runner完整prepared已落盘后，第二次授权遇到来源失联；
									// 不跨may-have-sent屏障，下一Tick必须给出原not_sent。
									return true, nil, fmt.Errorf("explicit fixture source unavailable after prepared")
								}
							}
							return false, nil, nil
						})
						require.Error(t, runner.Tick(ctx))
						require.ErrorIs(t, runner.Tick(ctx), vmguard.ErrNotSent)
					}
					if mode == "loop-explicit-VM-replay-host-gone-before" {
						require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, hostName))
						_, err = worker.ReplayVMOriginal(ctx, req.Key)
						require.Error(t, err)
						s, err := store.Load(ctx)
						require.NoError(t, err)
						require.Empty(t, s.VirtualMachine().Annotations[journal.VMReplayAnnotation])
						require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizeVM).Phase)
						peer.mu.Lock()
						require.Zero(t, peer.writes)
						peer.mu.Unlock()
						require.Zero(t, resizePosts.Load())
						require.Zero(t, opens.Load())
						return nil
					}
					loopCtx, stop := context.WithCancel(ctx)
					defer stop()
					waiting := 0
					var vmSignals chan control.Key
					if replayVM {
						vmSignals = make(chan control.Key, 1)
					}
					selected := false
					settleSelection := func() {
						s, err := store.Load(ctx)
						require.NoError(t, err)
						raw := s.VirtualMachine().Annotations[journal.VMReplayAnnotation]
						originalVersion := s.ResourceVersion()
						require.NotEmpty(t, raw)
						original := budgetStep(t, store, journal.ResizeVM)
						// 显式调用重复/上一次Select丢回复只能复用同一原选择，
						// 原命令与VM步骤仍逐字段不变，不能换nonce/命令重试。
						_, err = worker.ReplayVMOriginal(ctx, req.Key)
						require.NoError(t, err)
						s, err = store.Load(ctx)
						require.NoError(t, err)
						require.Equal(t, raw, s.VirtualMachine().Annotations[journal.VMReplayAnnotation])
						require.Equal(t, originalVersion, s.ResourceVersion(), "重复选择不再PATCH同一VM")
						require.Equal(t, original, budgetStep(t, store, journal.ResizeVM))
						if mode == "loop-explicit-VM-replay-host-gone-after" {
							require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, hostName))
							require.ErrorIs(t, runner.Tick(ctx), vmguard.ErrNotSent)
							stop()
							return
						}
						require.NoError(t, runner.Tick(ctx))
						require.NoError(t, runner.Tick(ctx), "已消费选择不重复内存写")
						selected = true
					}
					err = worker.Run(loopCtx, PGLoopOptions{PollInterval: 10 * time.Millisecond, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, VMReplay: vmSignals, Observe: func(tick Tick) {
						if tick.Err != nil {
							var lost *journal.UnconfirmedWrite
							if replayVM && errors.As(tick.Err, &lost) {
								require.Equal(t, "loop-explicit-VM-replay-lost-selection", mode)
								settleSelection()
								return
							}
							waiting++
							if replayVM {
								require.ErrorIs(t, tick.Err, vmguard.ErrNotSent)
								// Tick可能先于已就绪显式通道被select选中；等待可继续，
								// 只投递一次选择，不把额外轮询当第二次授权。
								if waiting == 1 {
									if mode == "loop-explicit-VM-replay-lost-selection" {
										api.mu.Lock()
										api.loseJournalReply = true
										api.mu.Unlock()
									}
									vmSignals <- req.Key
								}
							} else if mode == "loop-holds-not-sent" {
								require.ErrorIs(t, tick.Err, vmguard.ErrNotSent)
								if waiting == 2 {
									stop()
								}
							} else {
								require.ErrorIs(t, tick.Err, vmguard.ErrUnknown)
								require.Equal(t, 1, waiting)
								require.NoError(t, runner.Tick(ctx))
							}
						} else if replayVM && !selected {
							settleSelection()
						} else if budgetStep(t, store, journal.ResumeAdmission).Phase == journal.Settled {
							stop()
						}
					}})
					require.ErrorIs(t, err, context.Canceled, "实际循环不能因原VM仍待读回/显式not_sent而退出owner")
					if mode == "loop-holds-not-sent" || mode == "loop-explicit-VM-replay-host-gone-after" {
						if mode == "loop-holds-not-sent" {
							require.Equal(t, 2, waiting)
						}
						require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizeVM).Phase)
						peer.mu.Lock()
						require.Zero(t, peer.writes)
						peer.mu.Unlock()
						require.Zero(t, resizePosts.Load())
						require.Zero(t, opens.Load())
						return nil
					}
				}
				if !loop {
					err = runner.Tick(ctx)
					if mode == "qmp-lost-reply" {
						require.ErrorIs(t, err, vmguard.ErrUnknown)
						require.NoError(t, runner.Tick(ctx))
					} else {
						require.NoError(t, err)
					}
					if mode == "VM-record-lost-reply" {
						api.mu.Lock()
						api.loseJournalReply = true
						api.mu.Unlock()
					}
					progress, err = worker.Advance(ctx)
					if mode == "VM-record-lost-reply" {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						require.Equal(t, pgexec.Recorded, progress)
					}
					require.Equal(t, journal.Settled, budgetStep(t, store, journal.ResizeVM).Phase)
					for _, action := range []journal.Action{journal.GuestReady, journal.ResizePG, journal.FinalizeBudget, journal.ResumeAdmission} {
						progress, err = worker.Advance(ctx)
						require.NoError(t, err, string(action))
						require.Equal(t, pgexec.Recorded, progress, string(action))
						require.Equal(t, journal.Settled, budgetStep(t, store, action).Phase)
					}
				}
				latest, err := store.Load(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 4, latest.State().ProtocolVersion)
				budgetHeld, err := latest.State().BudgetFor(req)
				require.NoError(t, err)
				require.NotNil(t, budgetHeld.Sources.Runner)
				peer.mu.Lock()
				writes := peer.writes
				peer.mu.Unlock()
				require.Equal(t, 1, writes)
				require.EqualValues(t, 1, resizePosts.Load())
				require.EqualValues(t, 1, opens.Load())
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// 当前DSM/连接/前台事实是独立PG协议夹具；背景、DSM、2PC保留非零，验证
// 持续整体硬额度策略不会把这些成本清零来换VM缩容许可。
func vmDSM(s control.Snapshot) control.DSMResponse {
	work := control.WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 2, ClientProcesses: 1, IdleClients: 1, BackgroundProcesses: 3, AuxiliaryProcesses: 2, ObserverExcluded: true, ForegroundIdle: true, Observed: s, VMShrinkPermit: false, StartupClients: 0, FrontendReads: 0, FrontendExecutes: 0, BoundedInputs: 0, RetainedTransactions: 0}
	conn := control.ConnectionResponse{ProtocolVersion: 1, ConnectionAPIVersion: 1, Enabled: true, RequestedSerial: 2, AppliedSerial: 2, Acknowledged: true, PrivateManagementReady: true, PublicConnectionsBlocked: true, Work: work, VMShrinkPermit: false, RequestedOpen: false, AppliedOpen: false}
	return control.DSMResponse{ProtocolVersion: 1, DSMAPIVersion: 1, CurrentBootOnly: true, AccountingValid: true, ReservedUpperBytes: 4096, ControlUpperBytes: 4096, ExternalSegments: 1, UnknownSegments: 1, PageBytes: 4096, Prepared: control.PreparedResponse{ProtocolVersion: 1, PreparedAPIVersion: 1, PreparedSlots: 1, MaximumPreparedSlots: 4, RuntimePreparesBlocked: true, Connections: conn, VMShrinkPermit: false}, VMShrinkPermit: false, ExternalBudgetBytes: 0, PendingCreations: 0, LimitEnforced: false}
}

func TestProductionAgentRunnerShrinkRechecksGateAndOriginalObserve(t *testing.T) {
	for _, mode := range []string{"complete", "gate-reopened-before-VM", "root-drift-after-VM", "observe-start-lost", "explicit-original-replay", "host-settle", "host-settle-lost", "host-settle-source-gone", "host-settle-runner-drift", "host-settle-gc-lost", "host-settle-scheduler-loop"} {
		t.Run(mode, func(t *testing.T) {
			api, store, coord := newAPI(t)
			client := vmCore(t)
			api.mu.Lock()
			cpuMode := vmv1.CpuScalingModeSysfs
			api.vm.Spec.CpuScalingMode = &cpuMode
			api.vm.Spec.ServiceAccountName = "dsga-runner-test"
			api.vm.Spec.Guest.MemorySlotSize = *resource.NewQuantity(256<<20, resource.BinarySI)
			api.vm.Spec.Guest.MemorySlots.Min = 2
			api.vm.Spec.Guest.MemorySlots.Max = 8
			api.vm.Spec.Guest.MemorySlots.Use = 8
			api.vm.Status.Node = "node-one"
			api.vm.Status.MemorySize = resource.NewQuantity(2<<30, resource.BinarySI)
			api.mu.Unlock()
			dir, err := os.MkdirTemp("/private/tmp", "dsga-shrink-")
			require.NoError(t, err)
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			runner, err := vmguard.Initialize(context.Background(), store, client.CoreV1(), coord, vmguard.Config{StateDir: dir, PodName: "runner", PodUID: "runner-uid", NodeName: "node-one", MinimumBytes: 512 << 20, MaximumBytes: 2 << 30})
			require.NoError(t, err)
			root, resourceConfig := guestRoot(t, store)
			root.mu.Lock()
			root.view.Bootstrap.Limits.MemoryMaxBytes = 512 << 20
			root.view.Actual.MemoryMaxBytes = 512 << 20
			root.view.Actual.GuestMemTotalBytes = (2 << 30) - (256 << 20)
			root.mu.Unlock()
			// 初始2GiB/已完成PG释放为明确先前平台领域夹具；当前Agent→Runner→
			// 私有QMP及Observe/预算/准入全走实际代码，不声称执行过此前内核回收。
			args := runner.QMPArguments()
			peer := &vmQMPPeer{
				uuid: args[1], minimum: 512 << 20, maximum: 2 << 30, actual: 2 << 30, onResize: func(actual uint64) {
					require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizeVM).Phase)
					root.mu.Lock()
					root.view.Actual.GuestMemTotalBytes = control.Uint64(actual - (256 << 20))
					root.mu.Unlock()
					api.mu.Lock()
					api.vm.Status.MemorySize = resource.NewQuantity(int64(actual), resource.BinarySI)
					api.vmVersion++
					api.vm.ResourceVersion = fmt.Sprintf("vm-rv-%d", api.vmVersion)
					api.mu.Unlock()
				}, mu: sync.Mutex{}, writes: 0, lose: false,
			}
			vmPeer(t, filepath.Join(dir, "qmp.sock"), peer)
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				resources, err := session.NewResourceWorker(ctx, resourceConfig)
				require.NoError(t, err)
				defer resources.CloseIdleConnections()
				req := pgRequest(t, session.Actor())
				req.Kind, req.TargetBuffers = control.Shrink, 512
				if strings.HasPrefix(mode, "host-settle") {
					req.Key.Revision = 2
				}
				p, current := preparedGuest(t, req, root.view.Identity)
				p.Shared.Observed.AdmissionOpen = false
				p.Shared.Observed.AdmissionRevision = 5
				current.Observed.AdmissionOpen = false
				current.Observed.AdmissionRevision = 5
				base := req
				base.Kind, base.TargetBuffers = control.Grow, 1024
				var opens atomic.Int64
				config := pgConfig(t, base, func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/dsg/v1/budget/shared":
						pgReply(w, current)
					case "/dsg/v1/work/dsm":
						dsm := vmDSM(current.Observed)
						if mode == "gate-reopened-before-VM" {
							c := &dsm.Prepared.Connections
							c.RequestedSerial, c.AppliedSerial, c.RequestedOpen, c.AppliedOpen, c.PublicConnectionsBlocked = 3, 3, true, true, false
							c.Work.Observed.AdmissionOpen = true
							c.Work.ForegroundIdle = false
						}
						pgReply(w, dsm)
					case "/dsg/v1/admission":
						var a control.AdmissionRequest
						require.NoError(t, json.NewDecoder(r.Body).Decode(&a))
						hash, err := a.ContextSHA256()
						require.NoError(t, err)
						observed := current.Observed
						observed.AdmissionOpen = true
						observed.AdmissionRevision = a.Revision
						opens.Add(1)
						pgReply(w, control.AdmissionResponse{ProtocolVersion: 1, Request: a, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: observed, VMShrinkPermit: false})
					default:
						t.Errorf("unexpected shrink route %s", r.URL.Path)
						w.WriteHeader(500)
					}
				})
				worker, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				require.NoError(t, runner.Tick(ctx))
				out := budget.OutsideReservation{ProtocolVersion: 1, Request: req, Identity: p.Identity, Incarnation: binding().Incarnation, ResidualProfileID: "explicit-qualified-platform-fixture", SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true, VMShrinkPermit: false}
				outRef := vmReservation(t, client, "outside-original", budgetclaims.OutsideKind, out)
				p.OutsideClaim = &outRef
				s, err := store.Load(ctx)
				require.NoError(t, err)
				_, _, err = store.Begin(ctx, s, session.Actor(), req, 1)
				require.NoError(t, err)
				priorBudgetDomain(t, ctx, store, session.Actor(), req, journal.PrepareBudget, req.Key.Instance.ComputeInstanceID, p)
				publishVMHost(t, ctx, client, coord, store, session, mode)
				root.mu.Lock()
				aggregate, err := budget.RootAggregateFromView(root.view)
				root.mu.Unlock()
				require.NoError(t, err)
				closed := budget.Quiesced{ProtocolVersion: 1, Request: req, Close: control.AdmissionRequest{ProtocolVersion: 1, Instance: req.Key.Instance, IssuerEpoch: req.Key.IssuerEpoch, Revision: 5, ExpectedCapacityGeneration: req.ExpectedCapacityGeneration, Open: false, BudgetCommitmentID: req.BudgetCommitmentID}, DSM: vmDSM(p.Shared.Observed), PGAggregate: aggregate, VMShrinkPermit: false}
				require.NoError(t, closed.ValidateFor(p, session.Actor().Epoch))
				priorBudgetDomain(t, ctx, store, session.Actor(), req, journal.Quiesce, req.Key.Instance.ComputeInstanceID, closed)
				for _, action := range []journal.Action{journal.ResizePG, journal.ReleasePG} {
					priorBudgetDomain(t, ctx, store, session.Actor(), req, action, req.Key.Instance.ComputeInstanceID, map[string]bool{"explicit_prior_native_release_fixture": true})
				}
				progress, err := worker.Advance(ctx)
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress, "actual GuestReady")
				progress, err = worker.Advance(ctx)
				if mode == "gate-reopened-before-VM" {
					require.Error(t, err)
					require.Equal(t, pgexec.AwaitingFact, progress)
					require.Equal(t, journal.Planned, budgetStep(t, store, journal.ResizeVM).Phase)
					peer.mu.Lock()
					writes := peer.writes
					peer.mu.Unlock()
					require.Zero(t, writes)
					return nil
				}
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
				if mode == "explicit-original-replay" {
					reads := 0
					client.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
						if action.(kt.GetAction).GetName() == outRef.Name {
							reads++
							if reads == 3 {
								return true, nil, fmt.Errorf("explicit shrink fixture source unavailable after prepared")
							}
						}
						return false, nil, nil
					})
					require.Error(t, runner.Tick(ctx))
					require.ErrorIs(t, runner.Tick(ctx), vmguard.ErrNotSent)
					original := budgetStep(t, store, journal.ResizeVM)
					progress, err = worker.ReplayVMOriginal(ctx, req.Key)
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
					require.Equal(t, original, budgetStep(t, store, journal.ResizeVM))
				}
				require.NoError(t, runner.Tick(ctx))
				if mode == "root-drift-after-VM" {
					root.mu.Lock()
					root.view.Actual.MemoryMaxBytes++
					root.mu.Unlock()
				}
				progress, err = worker.Advance(ctx)
				if mode == "root-drift-after-VM" {
					require.Error(t, err)
					require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizeVM).Phase)
					peer.mu.Lock()
					writes := peer.writes
					peer.mu.Unlock()
					require.Equal(t, 1, writes)
					require.Zero(t, opens.Load())
					return nil
				}
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
				if mode == "observe-start-lost" {
					api.mu.Lock()
					api.loseJournalReply = true
					api.mu.Unlock()
					_, err = worker.Advance(ctx)
					require.Error(t, err)
					require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ObserveVM).Phase)
				}
				for _, action := range []journal.Action{journal.ObserveVM, journal.FinalizeBudget, journal.ResumeAdmission} {
					progress, err = worker.Advance(ctx)
					require.NoError(t, err, string(action))
					require.Equal(t, pgexec.Recorded, progress)
					require.Equal(t, journal.Settled, budgetStep(t, store, action).Phase)
				}
				require.Equal(t, budgetStep(t, store, journal.ResizeVM).Observation.Data, budgetStep(t, store, journal.ObserveVM).Observation.Data)
				latest, err := store.Load(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 4, latest.State().ProtocolVersion)
				peer.mu.Lock()
				writes := peer.writes
				peer.mu.Unlock()
				require.Equal(t, 1, writes)
				require.EqualValues(t, 1, opens.Load())
				if strings.HasPrefix(mode, "host-settle") {
					verifyClosedHostSettlement(t, ctx, api, client, coord, store, session, runner, peer, mode)
				}
				return nil
			})
			require.NoError(t, err)
		})
	}
}
