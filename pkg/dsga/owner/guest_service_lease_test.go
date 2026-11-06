package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
)

// 只借原完整生产前驱的局部handles；不填WAL阶段、不新建来源生产/授权框架。
// Kube/QMP/Root/Native仍是明确协议peer，本测试不替代Linux物理或K8s验收。
type guestAuditFixture struct {
	ctx     context.Context
	api     *ownerAPI
	store   *journal.Store
	core    *kubefake.Clientset
	session *Session
	worker  *PGWorker
	pg      *initialServicePG
	root    *guestRootFixture
	runner  *vmguard.Runner
	qmp     *vmQMPPeer
}

func withGuestAuditFixture(t *testing.T, kind control.Kind, check func(*guestAuditFixture)) {
	t.Helper()
	mode := "guest-service-audit-grow"
	if kind == control.Shrink {
		mode = "guest-service-audit-shrink"
	}
	productionInitialServiceWithFixtureHook(t, mode, func(t *testing.T, ctx context.Context, api *ownerAPI, store *journal.Store, core *kubefake.Clientset, coord coordclient.CoordinationV1Interface, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner, qmp *vmQMPPeer, request control.Request) bool {
		if kind == control.Shrink {
			// 原Grow完整执行并由原Finish封账，Host按原完整边界结算；接着新Shrink
			// 的Prepare/Quiesce/PG/Release真实推进到Guest Planned，绝不手填Completed。
			progress, err := worker.Advance(ctx)
			require.NoError(t, err)
			require.Equal(t, pgexec.Recorded, progress)
			s, err := store.Load(ctx)
			require.NoError(t, err)
			require.Nil(t, s.State().Active)
			first := s.State().History[len(s.State().History)-1].Operation.Request
			full, err := s.State().BudgetFor(first)
			require.NoError(t, err)
			node, err := core.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
			require.NoError(t, err)
			var ledger hostguard.Model
			require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger))
			allocator, err := hostguard.NewStore(core.CoreV1(), core.CoordinationV1(), ledger.Binding, "host-fixture-holder")
			require.NoError(t, err)
			publisher, err := hostexec.New(core.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
			require.NoError(t, err)
			_, err = publisher.SettleCurrent(ctx)
			require.NoError(t, err)
			request = first
			request.Kind, request.TargetBuffers, request.TargetVMMemoryBytes = control.Shrink, 512, 1<<30
			request.Key.Revision++
			request.Key.OperationID.Low++
			request.ExpectedCapacityGeneration = full.Shared.Observed.CapacityGeneration
			request.BudgetCommitmentID += "-guest-service-audit"
			_, _, err = store.Begin(ctx, s, session.Actor(), request, 1)
			require.NoError(t, err)
			progress, err = worker.Advance(ctx)
			require.NoError(t, err)
			require.Equal(t, pgexec.Recorded, progress)
			s, err = store.Load(ctx)
			require.NoError(t, err)
			prepared, _, err := s.State().OriginalServicePrepared()
			require.NoError(t, err)
			var shaping partialPGPeer
			pg.mu.Lock()
			shaping.admission = *pg.gate
			pg.mu.Unlock()
			shaping.configure(t, prepared)
			native := &initialServicePG{request: request, before: shaping.before, after: shaping.after, store: store}
			opening := shaping.admission
			native.gate = &opening
			config = pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/dsg/v1/work/dsm":
					pgReply(w, vmDSM(native.shared().Observed))
				case "/dsg/v1/resize":
					var received control.Request
					require.NoError(t, json.NewDecoder(r.Body).Decode(&received))
					require.Equal(t, request, received)
					require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizePG).Phase)
					native.mu.Lock()
					native.resized = true
					native.resizePosts++
					observed := native.current().Observed
					response := control.ControlResponse{ProtocolVersion: 1, Receipt: control.Receipt{Key: request.Key, Disposition: control.ReceiptReplayed, HasResult: true, Result: &observed}, Observed: observed}
					require.NoError(t, response.ValidateResize(request, request.Key.Instance))
					native.resizeReceipt = &response
					native.mu.Unlock()
					pgReply(w, response)
				default:
					native.serve(t, w, r)
				}
			}, func() control.Snapshot { return native.shared().Observed })
			worker.CloseIdleConnections()
			worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
			require.NoError(t, err)
			t.Cleanup(worker.CloseIdleConnections)
			_, err = publisher.Reconcile(ctx)
			require.NoError(t, err)
			for _, action := range []journal.Action{journal.Quiesce, journal.ResizePG, journal.ReleasePG} {
				advancePartialStep(t, ctx, store, worker, action, journal.Settled)
			}
			pg = native
		}
		require.Equal(t, journal.Planned, budgetStep(t, store, journal.GuestReady).Phase)
		check(&guestAuditFixture{ctx: ctx, api: api, store: store, core: core, session: session, worker: worker, pg: pg, root: root, runner: runner, qmp: qmp})
		return true
	})
}

// 旧API已有Advance；未新增checkpoint的旧实现实际执行原Guest动作但不会调用
// 完整续租生产者。动态接口只让同一行为断言在旧/新代码均编译，缺方法不算RED。
func advanceGuestAudit(f *guestAuditFixture, audit func(context.Context, *journal.Snapshot, control.SharedCostResponse) error) (pgexec.Progress, error) {
	if executor, ok := any(f.worker.resources).(interface {
		AdvanceWithAudit(context.Context, func(context.Context, *journal.Snapshot, control.SharedCostResponse) error) (pgexec.Progress, error)
	}); ok {
		return executor.AdvanceWithAudit(f.ctx, audit)
	}
	return f.worker.resources.Advance(f.ctx)
}
func armGuestAuditClock(f *guestAuditFixture, mode string) *atomic.Uint64 {
	now := new(atomic.Uint64)
	now.Store(uint64(1000 * time.Second))
	f.pg.mu.Lock()
	f.pg.leaseClock = func() control.Uint64 { return control.Uint64(now.Add(1)) }
	f.pg.leaseWindow = control.Uint64(30 * time.Second)
	f.pg.leaseDeadline = control.Uint64(now.Load()) + f.pg.leaseWindow
	f.pg.leaseExpired, f.pg.leaseRenewals = false, 0
	f.pg.rearmFixture = &serviceRearmPG{epoch: f.session.Actor().Epoch, api: f.api, loseRenewReply: mode == "renew-reply-lost"}
	f.pg.mu.Unlock()
	return now
}
func TestProductionGuestCompleteAuditCheckpoints(t *testing.T) {
	for _, kind := range []control.Kind{control.Grow, control.Shrink} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			withGuestAuditFixture(t, kind, func(f *guestAuditFixture) {
				now := armGuestAuditClock(f, "healthy")
				start := now.Load()
				f.root.mu.Lock()
				f.root.afterProcess = func() { now.Add(uint64(7 * time.Second)) }
				f.root.mu.Unlock()
				before, err := f.store.Load(f.ctx)
				require.NoError(t, err)
				phases := []journal.Phase{}
				progress, err := advanceGuestAudit(f, func(ctx context.Context, s *journal.Snapshot, shared control.SharedCostResponse) error {
					for _, step := range s.State().Active.Steps {
						if step.Action == journal.GuestReady {
							phases = append(phases, step.Phase)
						}
					}
					return f.worker.renewGuestServiceAudit(ctx, s, shared)
				})
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
				require.Equal(t, journal.Settled, budgetStep(t, f.store, journal.GuestReady).Phase, "旧Advance也必须真实完成原Guest动作才计semantic RED")
				require.Equal(t, []journal.Phase{journal.Planned, journal.Unconfirmed}, phases, "原完整写前及写后必须各调用一次审计生产者")
				require.Greater(t, now.Load()-start, uint64(30*time.Second))
				f.pg.mu.Lock()
				require.Equal(t, 2, f.pg.leaseRenewals)
				require.False(t, f.pg.leaseExpired)
				f.pg.mu.Unlock()
				after, err := f.store.Load(f.ctx)
				require.NoError(t, err)
				require.Nil(t, after.State().ServiceClosure)
				require.Equal(t, before.State().Active.Request, after.State().Active.Request)
				require.Equal(t, journal.Settled, budgetStep(t, f.store, journal.GuestReady).Phase)
				require.Equal(t, before.State().InitialHeld, after.State().InitialHeld)
				require.Equal(t, before.State().BudgetHeld, after.State().BudgetHeld)
				require.Error(t, f.worker.renewGuestServiceAudit(f.ctx, after, f.pg.shared()), "已完成Guest不是此单次Guest出口许可")
				f.pg.mu.Lock()
				require.Equal(t, 2, f.pg.leaseRenewals)
				f.pg.mu.Unlock()
			})
		})
	}
}
func TestProductionGuestAuditRejectsAuthorityDrift(t *testing.T) {
	for _, mode := range []string{"expired", "vm-rv", "lease-uid", "native-gate", "root-birth", "root-hardlimit", "held-host-missing", "outside-uid", "runner-state-uid", "tail-root", "tail-outside", "tail-qualification", "tail-pg", "tail-runner", "renew-reply-lost"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			withGuestAuditFixture(t, control.Grow, func(f *guestAuditFixture) {
				now := armGuestAuditClock(f, mode)
				before, err := f.store.Load(f.ctx)
				require.NoError(t, err)
				p, _, err := before.State().OriginalServicePrepared()
				require.NoError(t, err)
				q, err := f.worker.safety.claims.Qualification(f.ctx, *f.worker.qualification)
				require.NoError(t, err)
				f.api.mu.Lock()
				leaseUID := f.api.lease.UID
				f.api.mu.Unlock()
				if mode == "expired" {
					now.Add(uint64(31 * time.Second))
				}
				if mode == "root-birth" || mode == "root-hardlimit" {
					f.root.mu.Lock()
					if mode == "root-birth" {
						copy := *f.root.view.Birth
						copy.PostmasterStartTicks++
						f.root.processOverride = &copy
					} else {
						f.root.view.Actual.MemoryMaxBytes++
					}
					f.root.mu.Unlock()
				}
				if mode == "held-host-missing" {
					require.NoError(t, f.core.CoreV1().Nodes().Delete(f.ctx, "node-one", metav1.DeleteOptions{}))
				}
				replaceUID := func(name string) {
					cm, e := f.core.CoreV1().ConfigMaps(binding().Namespace).Get(f.ctx, name, metav1.GetOptions{})
					require.NoError(t, e)
					// 显式只读来源替换peer：合法Update不能改UID，故仅替换线程安全Tracker响应状态。
					cm.UID += "-replaced"
					e = f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, binding().Namespace)
					require.NoError(t, e)
				}
				if mode == "outside-uid" {
					replaceUID(p.OutsideClaim.Name)
				}
				if mode == "runner-state-uid" {
					birth, _, _, e := vmguard.ReadPublished(f.ctx, f.core.CoreV1(), binding(), &q.RunnerBirth)
					require.NoError(t, e)
					replaceUID(birth.StateClaim.Name)
				}
				if mode == "tail-root" || mode == "tail-outside" || mode == "tail-qualification" || mode == "tail-pg" || mode == "tail-runner" {
					var once atomic.Bool
					var processReads atomic.Uint32
					f.root.mu.Lock()
					f.root.afterProcess = func() {
						if mode == "tail-runner" && processReads.Add(1) != 2 {
							return
						}
						if !once.CompareAndSwap(false, true) {
							return
						}
						switch mode {
						case "tail-runner":
							birth, _, _, e := vmguard.ReadPublished(f.ctx, f.core.CoreV1(), binding(), &q.RunnerBirth)
							require.NoError(t, e)
							replaceUID(birth.StateClaim.Name)
						case "tail-root":
							f.root.view.Actual.MemoryMaxBytes++
						case "tail-outside":
							replaceUID(p.OutsideClaim.Name)
						case "tail-qualification":
							replaceUID(f.worker.qualification.Claim.Name)
						case "tail-pg":
							f.pg.mu.Lock()
							f.pg.before.Observed.CapacityGeneration++
							f.pg.mu.Unlock()
						}
					}
					f.root.mu.Unlock()
				}
				if mode == "vm-rv" || mode == "lease-uid" || mode == "native-gate" {
					f.pg.mu.Lock()
					f.pg.rearmFixture.afterObserve = func() {
						switch mode {
						case "native-gate":
							f.pg.gate.Revision++
						case "lease-uid":
							f.api.mu.Lock()
							f.api.lease.UID = "guest-replaced-lease"
							f.api.mu.Unlock()
						case "vm-rv":
							f.api.mu.Lock()
							f.api.vmVersion++
							f.api.vm.ResourceVersion = fmt.Sprintf("guest-drift-%d", f.api.vmVersion)
							f.api.mu.Unlock()
						}
					}
					f.pg.mu.Unlock()
				}
				callErr := f.worker.renewGuestServiceAudit(f.ctx, before, f.pg.shared())
				require.Error(t, callErr)
				if mode == "lease-uid" {
					f.api.mu.Lock()
					f.api.lease.UID = leaseUID
					f.api.mu.Unlock()
				}
				f.pg.mu.Lock()
				renewals := f.pg.leaseRenewals
				f.pg.mu.Unlock()
				if mode == "renew-reply-lost" {
					require.Equal(t, 1, renewals, "已续丢回复不能在同次调用重发")
				} else {
					require.Zero(t, renewals)
				}
				after, err := f.store.Load(f.ctx)
				require.NoError(t, err)
				require.Equal(t, before.State(), after.State(), "审计失败不改WAL、不Start/Record/开放")
				require.Equal(t, journal.Planned, budgetStep(t, f.store, journal.GuestReady).Phase)
			})
		})
	}
}
func TestProductionGuestAuditRejectsWrongScopeAndUnknown(t *testing.T) {
	withGuestAuditFixture(t, control.Grow, func(f *guestAuditFixture) {
		armGuestAuditClock(f, "healthy")
		s, err := f.store.Load(f.ctx)
		require.NoError(t, err)
		require.Error(t, f.worker.renewGuestServiceAudit(context.WithValue(f.ctx, recoveryLeaseScopeKey{}, true), s, f.pg.shared()))
		cancelled, cancel := context.WithCancel(f.ctx)
		cancel()
		require.ErrorIs(t, f.worker.renewGuestServiceAudit(cancelled, s, f.pg.shared()), context.Canceled)
		require.Error(t, f.worker.renewGuestServiceAudit(f.ctx, nil, f.pg.shared()))
		bad := f.pg.shared()
		bad.Observed.Arrays = append([]control.ArrayFact(nil), bad.Observed.Arrays...)
		bad.Observed.Arrays[0].AllocationUnknown = true
		require.Error(t, f.worker.renewGuestServiceAudit(f.ctx, s, bad))
		_, err = f.store.RequestAbort(f.ctx, s, f.session.Actor(), s.State().Active.Request.Key)
		require.NoError(t, err)
		s, err = f.store.Load(f.ctx)
		require.NoError(t, err)
		require.Error(t, f.worker.renewGuestServiceAudit(f.ctx, s, f.pg.shared()))
		f.pg.mu.Lock()
		require.Zero(t, f.pg.leaseRenewals)
		f.pg.mu.Unlock()
	})
}
func TestProductionGuestAuditUnknownApplyOnlyObservesOriginal(t *testing.T) {
	withGuestAuditFixture(t, control.Grow, func(f *guestAuditFixture) {
		armGuestAuditClock(f, "healthy")
		f.root.mu.Lock()
		f.root.loseReply = true
		initialWrites := f.root.posts
		f.root.mu.Unlock()
		calls := 0
		audit := func(ctx context.Context, s *journal.Snapshot, shared control.SharedCostResponse) error {
			calls++
			return f.worker.renewGuestServiceAudit(ctx, s, shared)
		}
		_, err := advanceGuestAudit(f, audit)
		require.Error(t, err)
		require.Equal(t, 1, calls, "Apply未知只可调用写前，不能借写后续租洗掉未知")
		original := budgetStep(t, f.store, journal.GuestReady)
		require.Equal(t, journal.Unconfirmed, original.Phase)
		require.NotNil(t, f.worker.resourceOwner.client.Pending())
		s, err := f.store.Load(f.ctx)
		require.NoError(t, err)
		require.Error(t, f.worker.renewGuestServiceAudit(f.ctx, s, f.pg.shared()))
		f.pg.mu.Lock()
		require.Equal(t, 1, f.pg.leaseRenewals)
		f.pg.mu.Unlock()
		progress, err := advanceGuestAudit(f, audit)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		require.Equal(t, 2, calls)
		require.Nil(t, f.worker.resourceOwner.client.Pending())
		complete := budgetStep(t, f.store, journal.GuestReady)
		require.Equal(t, original.CommandSHA256, complete.CommandSHA256)
		require.Equal(t, original.Command, complete.Command)
		f.root.mu.Lock()
		require.Equal(t, initialWrites+1, f.root.posts)
		require.Greater(t, f.root.observes, 0)
		f.root.mu.Unlock()
	})
}

// 原完整Grow→Shrink Release之后仍须逐项匹配原Key/代次/物理回执；
// 新Host或旧Held任一来源丢失均不能用另一个同数值预算补造续租许可。
func TestProductionGuestShrinkAuditRetainsBothHostSourcesAndPhysicalRelease(t *testing.T) {
	for _, mode := range []string{"current-host-uid", "held-host-uid", "release-key", "release-generation", "release-unknown", "release-provider-partial", "wrong-operation"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			withGuestAuditFixture(t, control.Shrink, func(f *guestAuditFixture) {
				armGuestAuditClock(f, "healthy")
				before, err := f.store.Load(f.ctx)
				require.NoError(t, err)
				p, _, err := before.State().OriginalServicePrepared()
				require.NoError(t, err)
				if mode == "current-host-uid" || mode == "held-host-uid" {
					var ref budget.ObjectRef
					if mode == "current-host-uid" {
						ref, err = f.worker.safety.claims.HostReference(f.ctx, p.Request)
						require.NoError(t, err)
					} else {
						var original budget.FinalizationCommand
						require.NotNil(t, before.State().BudgetHeld)
						require.NoError(t, json.Unmarshal(before.State().BudgetHeld.Finalization.Command, &original))
						require.NotNil(t, original.Finalized.Sources)
						ref = original.Finalized.Sources.HostClaim
					}
					cm, err := f.core.CoreV1().ConfigMaps(binding().Namespace).Get(f.ctx, ref.Name, metav1.GetOptions{})
					require.NoError(t, err)
					// 显式只读来源替换peer：合法Update不能改UID，故仅替换线程安全Tracker响应状态。
					cm.UID += "-replaced"
					err = f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, binding().Namespace)
					require.NoError(t, err)
				} else {
					f.pg.mu.Lock()
					raw, err := json.Marshal(f.pg.after)
					require.NoError(t, err)
					var changed control.SharedCostResponse
					require.NoError(t, json.Unmarshal(raw, &changed))
					switch mode {
					case "release-key":
						key := *changed.Observed.Arrays[0].ReleaseKey
						key.OperationID.Low++
						changed.Observed.Arrays[0].ReleaseKey = &key
					case "release-generation":
						changed.Observed.Arrays[0].ReleaseCapacityGeneration++
					case "release-unknown":
						changed.Observed.Arrays[0].AllocationUnknown = true
					case "release-provider-partial":
						changed.Observed.Arrays[0].Provider.ReleaseMayBePartial = true
					case "wrong-operation":
						changed.Observed.Operation.Key.OperationID.Low++
					}
					f.pg.after = changed
					f.pg.mu.Unlock()
				}
				require.Error(t, f.worker.renewGuestServiceAudit(f.ctx, before, f.pg.shared()))
				f.pg.mu.Lock()
				require.Zero(t, f.pg.leaseRenewals)
				f.pg.mu.Unlock()
				after, err := f.store.Load(f.ctx)
				require.NoError(t, err)
				require.Equal(t, before.State(), after.State())
				require.Equal(t, journal.Planned, budgetStep(t, f.store, journal.GuestReady).Phase)
			})
		})
	}
}
func TestProductionGuestAuditCannotUseCrossActorSnapshot(t *testing.T) {
	withGuestAuditFixture(t, control.Grow, func(f *guestAuditFixture) {
		armGuestAuditClock(f, "healthy")
		before, err := f.store.Load(f.ctx)
		require.NoError(t, err)
		next, err := f.store.Takeover(f.ctx, before, f.session.Actor(), "guest-audit-next-owner")
		require.NoError(t, err)
		require.Equal(t, before.State().Active, next.State().Active, "真实Takeover保留旧origin及命令")
		require.Error(t, f.worker.renewGuestServiceAudit(f.ctx, next, f.pg.shared()))
		f.pg.mu.Lock()
		require.Zero(t, f.pg.leaseRenewals)
		f.pg.mu.Unlock()
	})
}
