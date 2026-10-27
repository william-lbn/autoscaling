package owner

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceexec"
)

func budgetStep(t *testing.T, store *journal.Store, action journal.Action) journal.Step {
	t.Helper()
	s, err := store.Load(context.Background())
	require.NoError(t, err)
	require.NotNil(t, s.State().Active)
	for _, step := range s.State().Active.Steps {
		if step.Action == action {
			return step
		}
	}
	t.Fatal("missing budget step", action)
	return journal.Step{}
}

func priorBudgetDomain(t *testing.T, ctx context.Context, store *journal.Store, actor journal.Owner, req control.Request, action journal.Action, target string, body any) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	s, ticket, err := store.Start(ctx, s, actor, req.Key, action, target, []byte(`{"test_only_prior_domain":true}`))
	require.NoError(t, err)
	_, err = store.Record(ctx, s, actor, req.Key, journal.Observation{Key: req.Key, RequestSHA256: ticket.RequestSHA256, CommandSHA256: ticket.Step.CommandSHA256, Ordinal: ticket.Step.Ordinal, Action: action, Result: journal.Completed, SourceID: target, Data: raw})
	require.NoError(t, err)
}

// 仅测试夹具：构造显式v2无budget_held投影，核对原Active升级/查询顺序。
// 新增Runner来源完整保留；它不是旧v2已发布读者真实接受此新增来源的证明，
// 真实旧读者的拒绝另由固定旧源码合约验证。
// 生产代码严禁降级或改写日志；这里显式重新封装同一旧wire，检验新读者不能在
// Load时凭猜测升级。完整旧原命令/回执和owner均逐字节保留。
func legacyFinalizationFixture(t *testing.T, api *ownerAPI, model journal.Model) {
	t.Helper()
	model.ProtocolVersion, model.BudgetHeld = 2, nil
	require.NoError(t, model.Validate())
	raw, err := json.Marshal(model)
	require.NoError(t, err)
	var compressed bytes.Buffer
	w := zlib.NewWriter(&compressed)
	_, err = w.Write(raw)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	hash := sha256.Sum256(raw)
	wrapped, err := json.Marshal(struct {
		Version     uint32 `json:"version"`
		Encoding    string `json:"encoding"`
		Incarnation string `json:"incarnation"`
		SHA256      string `json:"sha256"`
		Data        []byte `json:"data"`
	}{1, "zlib-v1", model.Incarnation, hex.EncodeToString(hash[:]), compressed.Bytes()})
	require.NoError(t, err)
	api.mu.Lock()
	defer api.mu.Unlock()
	api.vm.Annotations[control.JournalAnnotation] = string(wrapped)
	api.vmVersion++
	api.vm.ResourceVersion = fmt.Sprintf("vm-rv-%d", api.vmVersion)
}

// 实际生产构造器、Session、TLS客户端、VM UID/RV CAS及最终结算/准入执行器。
// Core对象、PG/root内核、Prepare/Host/VM生产者是显式协议夹具；不冒充真实
// apiserver/RBAC/Host分配/QMP/系统上界资格、Linux物理或完整V3端到端验证。
func TestProductionBudgetFinalizationUsesOriginalClaimsAndRecoversOnlyReadback(t *testing.T) {
	for _, mode := range []string{"complete", "outside-uid", "host-releasing", "runner-uid", "node-uid", "vm-qmp", "lost-start", "lost-record", "outside-replaced-after-start", "runner-drift-after-start", "outside-gone-before-open", "outside-replaced-after-open", "legacy-final-without-claims", "root-birth-after-start", "pg-generation-after-start", "shrink-complete", "shrink-unverified", "shrink-observe-mismatch", "unprotected-claim", "protection-lost-after-start", "legacy-v2-upgrade", "legacy-v2-lost-upgrade", "legacy-v2-source-lost", "legacy-v2-unknown-open", "legacy-v2-unknown-source-lost", "runner-birth-recreated", "runner-state-recreated", "runner-receipt-gone", "runner-receipt-gone-after-start", "runner-receipt-gone-before-open"} {
		t.Run(mode, func(t *testing.T) {
			api, store, coord := newAPI(t)
			root, resourceConfig := guestRoot(t, store)
			client := kubefake.NewSimpleClientset()
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				resources, err := session.NewResourceWorker(ctx, resourceConfig)
				require.NoError(t, err)
				defer resources.CloseIdleConnections()
				req := pgRequest(t, session.Actor())
				shrink := strings.HasPrefix(mode, "shrink-")
				if shrink {
					req.Kind = control.Shrink
					req.TargetBuffers = 512
				}
				p, baseline := preparedGuest(t, req, root.view.Identity)
				p.OutsideClaim = &budget.ObjectRef{Name: "outside-original", UID: "outside-uid"}
				finalShared := baseline
				if !shrink {
					finalShared.Observed = pgResponse(t, req).Observed
				}
				finalShared.CurrentMainAndArraysUpperBytes = finalShared.StartupMainUpperBytes
				for _, a := range finalShared.Observed.Arrays {
					finalShared.CurrentMainAndArraysUpperBytes += a.CommittedUpperBytes
				}
				var outCM *core.ConfigMap
				var committed atomic.Bool
				var nativeDrift atomic.Bool
				var resizePosts, admissionPosts, admissionReads atomic.Int64
				var originalOpen atomic.Pointer[control.AdmissionRequest]
				wireReq := req
				if shrink {
					wireReq.Kind = control.Grow
					wireReq.TargetBuffers = 1024
				}
				config := pgConfig(t, wireReq, func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/dsg/v1/budget/shared":
						if committed.Load() {
							next := finalShared
							if nativeDrift.Load() {
								if mode == "shrink-unverified" {
									next.Observed.Arrays = append([]control.ArrayFact(nil), next.Observed.Arrays...)
									next.Observed.Arrays[0].Provider.PhysicalReleaseVerified = false
								} else {
									next.Observed.CapacityGeneration++
								}
							}
							pgReply(w, next)
						} else {
							pgReply(w, baseline)
						}
					case "/dsg/v1/resize":
						resizePosts.Add(1)
						committed.Store(true)
						pgReply(w, pgResponse(t, req))
					case "/dsg/v1/resize/status":
						pgReply(w, pgResponse(t, req))
					case "/dsg/v1/admission", "/dsg/v1/admission/status":
						query := r.URL.Path == "/dsg/v1/admission/status"
						if query {
							admissionReads.Add(1)
						} else {
							admissionPosts.Add(1)
						}
						if mode == "outside-replaced-after-open" {
							copy := outCM.DeepCopy()
							copy.UID = "after-open-new-uid"
							require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), copy, binding().Namespace))
						}
						var a control.AdmissionRequest
						require.NoError(t, json.NewDecoder(r.Body).Decode(&a))
						if query {
							require.NotNil(t, originalOpen.Load())
							require.Equal(t, *originalOpen.Load(), a, "必须查询完整原命令，不能换epoch或revision")
						} else {
							originalOpen.Store(&a)
							if strings.HasPrefix(mode, "legacy-v2-unknown-") {
								w.WriteHeader(http.StatusServiceUnavailable)
								return
							}
						}
						hash, err := a.ContextSHA256()
						require.NoError(t, err)
						observed := finalShared.Observed
						observed.AdmissionOpen = true
						observed.AdmissionRevision = a.Revision
						disposition := control.AdmissionApplied
						if query {
							disposition = control.AdmissionReplayed
						}
						pgReply(w, control.AdmissionResponse{ProtocolVersion: 1, Request: a, ContextSHA256: hash, Disposition: disposition, Observed: observed})
					default:
						t.Error("unexpected production budget route", r.URL.Path)
						w.WriteHeader(500)
					}
				})
				worker, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				api.mu.Lock()
				api.vm.Status.Node = "node-one"
				beforeMemory := req.TargetVMMemoryBytes
				if shrink {
					beforeMemory = p.VMBeforeMemoryBytes
				}
				api.vm.Status.MemorySize = resource.NewQuantity(int64(beforeMemory), resource.BinarySI)
				api.mu.Unlock()
				controller := true
				owner := metav1.OwnerReference{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: binding().Name, UID: binding().VMUID, Controller: &controller, BlockOwnerDeletion: &controller}
				h := dsgatest.Host(binding(), p, budget.HostReservation{ProtocolVersion: 1, Request: req, Identity: p.Identity, Incarnation: binding().Incarnation, SourceID: "host-original", RunnerPodName: p.RunnerPodName, RunnerPodUID: "runner-uid", NodeName: "node-one", NodeUID: "node-uid", ReservedMemoryBytes: max(req.TargetVMMemoryBytes, p.VMBeforeMemoryBytes) + (4 << 20), HostOverheadBytes: 4 << 20, HoldUntilExplicitSettlement: true, VMShrinkPermit: false})
				outside := budget.OutsideReservation{ProtocolVersion: 1, Request: req, Identity: p.Identity, Incarnation: binding().Incarnation, ResidualProfileID: "test-only-qualified-profile", SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true}
				claim := func(name, uid, kind string, body any) *core.ConfigMap {
					data, err := json.Marshal(body)
					require.NoError(t, err)
					immutable := true
					return &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: binding().Namespace, Name: name, UID: types.UID(uid), ResourceVersion: "claim-rv-1", OwnerReferences: []metav1.OwnerReference{owner}, Finalizers: []string{budgetclaims.ProtectionFinalizer}, Labels: map[string]string{budgetclaims.ClaimLabel: kind}}, Immutable: &immutable, Data: map[string]string{budgetclaims.ClaimKey: string(data)}}
				}
				outCM = claim(p.OutsideClaim.Name, p.OutsideClaim.UID, budgetclaims.OutsideKind, outside)
				hostCM := claim("host-original", "host-uid", budgetclaims.HostKind, h)
				pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: binding().Namespace, Name: p.RunnerPodName, UID: "runner-uid", ResourceVersion: "pod-rv-1", OwnerReferences: []metav1.OwnerReference{owner}}, Spec: core.PodSpec{NodeName: "node-one"}, Status: core.PodStatus{Phase: core.PodRunning, PodIP: "127.0.0.1"}}
				node := &core.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-one", UID: "node-uid", ResourceVersion: "node-rv-1"}}
				node = dsgatest.Node(node, binding(), p, h, budget.ObjectRef{Name: hostCM.Name, UID: string(hostCM.UID)})
				for _, obj := range []runtime.Object{outCM, hostCM, pod, node} {
					require.NoError(t, client.Tracker().Add(obj))
				}
				vmFact := budget.VMSettlement{ProtocolVersion: 1, Request: req, Identity: p.Identity, HostClaim: budget.ObjectRef{Name: hostCM.Name, UID: string(hostCM.UID)}, Host: h, RunnerFenceEpoch: req.Key.IssuerEpoch, RunnerCommandID: "runner-original-command", GuestUsableMemoryBytes: p.GuestUsableMemoryBytes, ActualMemoryBytes: req.TargetVMMemoryBytes, QMPObservedMemoryBytes: req.TargetVMMemoryBytes, NoPendingCommands: true, MigrationBlocked: true}
				if mode == "vm-qmp" {
					vmFact.QMPObservedMemoryBytes--
				}
				snap, err := store.Load(ctx)
				require.NoError(t, err)
				_, _, err = store.Begin(ctx, snap, session.Actor(), req, 1)
				require.NoError(t, err)
				priorBudgetDomain(t, ctx, store, session.Actor(), req, journal.PrepareBudget, req.Key.Instance.ComputeInstanceID, p)
				if shrink {
					// Shrink的排空/PG物理回执生产者在本用例为明确夹具；实际GuestReady、
					// 最终结算/准入仍走生产执行器，不能将此记为内核/VM端到端测试。
					for _, action := range []journal.Action{journal.Quiesce, journal.ResizePG, journal.ReleasePG} {
						priorBudgetDomain(t, ctx, store, session.Actor(), req, action, req.Key.Instance.ComputeInstanceID, map[string]bool{"test_only_prior_shrink_domain": true})
					}
				} else {
					priorBudgetDomain(t, ctx, store, session.Actor(), req, journal.ReserveHost, h.SourceID, h)
					vmFact = originalRunnerFixture(t, ctx, client, store, session, p, vmFact)
				}
				progress, err := worker.Advance(ctx)
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress, "actual GuestReady")
				if shrink {
					var guest resourceexec.Audit
					require.NoError(t, json.Unmarshal(budgetStep(t, store, journal.GuestReady).Observation.Data, &guest))
					vmFact.PGResourceSourceID = guest.PGAggregate.Bound.SourceID
					api.mu.Lock()
					api.vm.Status.MemorySize = resource.NewQuantity(int64(req.TargetVMMemoryBytes), resource.BinarySI)
					api.mu.Unlock()
					vmFact = originalRunnerFixture(t, ctx, client, store, session, p, vmFact)
					observed := vmFact
					if mode == "shrink-observe-mismatch" {
						observed.RunnerCommandID = "another-command"
					}
					priorBudgetDomain(t, ctx, store, session.Actor(), req, journal.ObserveVM, h.RunnerPodUID, observed)
					if mode == "shrink-unverified" {
						committed.Store(true) // 夹具中的先前Shrink已提交，后续查询才切到变更后的事实。
						nativeDrift.Store(true)
					}
					require.Zero(t, resizePosts.Load())
				} else {
					progress, err = worker.Advance(ctx)
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress, "actual PG resize")
					require.EqualValues(t, 1, resizePosts.Load())
				}
				switch mode {
				case "runner-birth-recreated", "runner-state-recreated":
					ref := vmFact.Runner.BirthClaim
					if mode == "runner-state-recreated" {
						ref = vmFact.Runner.StateClaim
					}
					obj, err := client.Tracker().Get(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, ref.Name)
					require.NoError(t, err)
					cm := obj.(*core.ConfigMap).DeepCopy()
					cm.UID = "same-name-new-runner-source"
					require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, binding().Namespace))
				case "runner-receipt-gone":
					require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, vmFact.Runner.ReceiptClaim.Name))
				case "unprotected-claim":
					outCM.Finalizers = nil
					require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), outCM, binding().Namespace))
				case "outside-uid":
					outCM.UID = "recreated-outside"
					require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), outCM, binding().Namespace))
				case "host-releasing":
					now := metav1.Now()
					hostCM.DeletionTimestamp = &now
					require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), hostCM, binding().Namespace))
				case "runner-uid":
					pod.UID = "new-runner"
					require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, binding().Namespace))
				case "node-uid":
					node.UID = "new-node"
					require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), node, ""))
				case "lost-start":
					api.mu.Lock()
					api.loseJournalReply = true
					api.mu.Unlock()
				}
				if mode == "legacy-final-without-claims" {
					root.mu.Lock()
					aggregate, err := budget.RootAggregateFromView(root.view)
					root.mu.Unlock()
					require.NoError(t, err)
					full := budget.Finalized{ProtocolVersion: 1, Request: req, IssuerEpoch: req.Key.IssuerEpoch, RunnerPodName: p.RunnerPodName, HostReservationID: h.SourceID, GuestUsableMemoryBytes: p.GuestUsableMemoryBytes, Shared: finalShared, Aggregate: &aggregate.Bound, SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety}
					priorBudgetDomain(t, ctx, store, session.Actor(), req, journal.FinalizeBudget, req.Key.Instance.ComputeInstanceID, full)
				}
				reads := 0
				client.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
					get := action.(kt.GetAction)
					if get.GetName() != p.OutsideClaim.Name {
						return false, nil, nil
					}
					reads++
					if reads == 3 && mode == "runner-receipt-gone-after-start" {
						// Finalize写前CAS成功后原Runner回执丢失，不能只凭此前采样
						// 写Completed或持有v4预算；原Finalize保留Unconfirmed。
						require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, vmFact.Runner.ReceiptClaim.Name))
					}
					if reads >= 3 && mode == "protection-lost-after-start" {
						cm := outCM.DeepCopy()
						cm.Finalizers = nil
						return true, cm, nil
					}
					if reads >= 3 && mode == "outside-replaced-after-start" {
						cm := outCM.DeepCopy()
						cm.UID = "after-start-new-uid"
						return true, cm, nil
					}
					if reads == 3 && mode == "root-birth-after-start" {
						root.mu.Lock()
						p := *root.view.Birth
						p.PostmasterStartTicks++
						root.processOverride = &p
						root.mu.Unlock()
					}
					if reads == 3 && mode == "pg-generation-after-start" {
						nativeDrift.Store(true)
					}
					if reads == 3 && mode == "lost-record" {
						api.mu.Lock()
						api.loseJournalReply = true
						api.mu.Unlock()
					}
					if reads == 3 && mode == "runner-drift-after-start" {
						api.mu.Lock()
						api.vm.Status.RestartCount++
						api.vmVersion++
						api.vm.ResourceVersion = fmt.Sprintf("vm-rv-%d", api.vmVersion)
						api.mu.Unlock()
					}
					return false, nil, nil
				})
				progress, err = worker.Advance(ctx)
				if mode == "complete" || mode == "shrink-complete" || mode == "outside-gone-before-open" || mode == "outside-replaced-after-open" || mode == "runner-receipt-gone-before-open" || strings.HasPrefix(mode, "legacy-v2-") {
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
					step := budgetStep(t, store, journal.FinalizeBudget)
					require.Equal(t, journal.Settled, step.Phase)
					var full budget.Finalized
					require.NoError(t, json.Unmarshal(step.Observation.Data, &full))
					require.NotNil(t, full.Sources)
					require.Equal(t, h.SourceID, full.HostReservationID)
					require.NoError(t, full.ValidateFor(req, finalShared, session.Actor().Epoch, p.RunnerPodName, 0, uint64(req.TargetVMMemoryBytes)))
					settled, err := store.Load(ctx)
					require.NoError(t, err)
					require.EqualValues(t, 4, settled.State().ProtocolVersion)
					require.Equal(t, step, settled.State().BudgetHeld.Finalization, "Completed与长期预算必须同CAS持久")
					if strings.HasPrefix(mode, "legacy-v2-") {
						if strings.HasPrefix(mode, "legacy-v2-unknown-") {
							a := control.AdmissionRequest{ProtocolVersion: 1, Instance: req.Key.Instance, IssuerEpoch: session.Actor().Epoch, Revision: finalShared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: finalShared.Observed.CapacityGeneration, Open: true, BudgetCommitmentID: req.BudgetCommitmentID}
							raw, err := json.Marshal(a)
							require.NoError(t, err)
							settled, _, err = store.Start(ctx, settled, session.Actor(), req.Key, journal.ResumeAdmission, req.Key.Instance.ComputeInstanceID, raw)
							require.NoError(t, err)
							_, err = worker.client.SetAdmission(ctx, a)
							require.Error(t, err, "夹具先实际派发原开放并丢回复，保留SDK未知")
						}
						legacyFinalizationFixture(t, api, settled.State())
						legacy, err := store.Load(ctx)
						require.NoError(t, err)
						require.Nil(t, legacy.State().BudgetHeld)
						if strings.HasSuffix(mode, "source-lost") {
							require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, outCM.Name))
						}
						if mode == "legacy-v2-lost-upgrade" {
							api.mu.Lock()
							api.loseJournalReply = true
							api.mu.Unlock()
						}
						progress, err = worker.Advance(ctx)
						if strings.HasSuffix(mode, "source-lost") {
							require.Error(t, err)
							require.Equal(t, pgexec.AwaitingFact, progress)
							latest, err := store.Load(ctx)
							require.NoError(t, err)
							require.EqualValues(t, 2, latest.State().ProtocolVersion)
							require.Nil(t, latest.State().BudgetHeld, "来源丢失不能补写长期预算")
							if strings.Contains(mode, "unknown") {
								require.EqualValues(t, 1, admissionReads.Load(), "原准入查询必须先于失败的预算来源复核")
								require.EqualValues(t, 1, admissionPosts.Load())
								require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResumeAdmission).Phase)
							} else {
								require.Zero(t, admissionPosts.Load())
							}
							return nil
						}
						if mode == "legacy-v2-lost-upgrade" {
							require.Error(t, err)
						} else {
							require.NoError(t, err)
							require.Equal(t, pgexec.Recorded, progress)
						}
						latest, err := store.Load(ctx)
						require.NoError(t, err)
						require.EqualValues(t, 4, latest.State().ProtocolVersion)
						require.Equal(t, step, latest.State().BudgetHeld.Finalization)
						require.Equal(t, step, budgetStep(t, store, journal.FinalizeBudget), "升级保持旧完成字节不变")
						progress, err = worker.Advance(ctx)
						require.NoError(t, err)
						require.Equal(t, pgexec.Recorded, progress)
						require.EqualValues(t, 1, admissionPosts.Load(), "升级/恢复不重发原开放或PG动作")
						require.EqualValues(t, 1, resizePosts.Load())
						if mode == "legacy-v2-unknown-open" {
							require.EqualValues(t, 2, admissionReads.Load())
						}
						return nil
					}
					if mode == "outside-gone-before-open" {
						require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, outCM.Name))
					}
					if mode == "runner-receipt-gone-before-open" {
						require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, vmFact.Runner.ReceiptClaim.Name))
					}
					progress, err = worker.Advance(ctx)
					if mode == "complete" || mode == "shrink-complete" {
						require.NoError(t, err)
						require.Equal(t, pgexec.Recorded, progress)
						require.EqualValues(t, 1, admissionPosts.Load())
						if mode == "complete" {
							verifyUnstartedAbortRetainsBudget(t, ctx, worker, store, req)
							require.EqualValues(t, 1, resizePosts.Load(), "later zero-dispatch cancellation must not resend PG resize")
							require.EqualValues(t, 1, admissionPosts.Load(), "later zero-dispatch cancellation must not reopen admission")
						}
					} else {
						require.Error(t, err)
						require.Equal(t, pgexec.AwaitingFact, progress)
						want := journal.Planned
						posts := int64(0)
						if mode == "outside-replaced-after-open" {
							want = journal.Unconfirmed
							posts = 1
						}
						require.Equal(t, want, budgetStep(t, store, journal.ResumeAdmission).Phase)
						require.Equal(t, posts, admissionPosts.Load())
					}
				} else if mode == "lost-start" || mode == "lost-record" {
					require.Error(t, err)
					require.Equal(t, pgexec.AwaitingFact, progress)
					step := budgetStep(t, store, journal.FinalizeBudget)
					if mode == "lost-start" {
						require.Equal(t, journal.Unconfirmed, step.Phase)
					} else {
						require.Equal(t, journal.Settled, step.Phase)
					}
					restored, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
					require.NoError(t, err)
					defer restored.CloseIdleConnections()
					for n := 0; n < 3; n++ {
						if budgetStep(t, store, journal.FinalizeBudget).Phase == journal.Settled {
							break
						}
						_, err = restored.Advance(ctx)
						require.NoError(t, err)
					}
					require.Equal(t, journal.Settled, budgetStep(t, store, journal.FinalizeBudget).Phase)
					latest, err := store.Load(ctx)
					require.NoError(t, err)
					require.Equal(t, budgetStep(t, store, journal.FinalizeBudget), latest.State().BudgetHeld.Finalization)
					require.EqualValues(t, 1, resizePosts.Load(), "finalization recovery must not repeat PG resize")
					require.Zero(t, admissionPosts.Load())
				} else {
					require.Error(t, err)
					require.Equal(t, pgexec.AwaitingFact, progress)
					want := journal.Planned
					if mode == "outside-replaced-after-start" || mode == "runner-drift-after-start" || mode == "root-birth-after-start" || mode == "pg-generation-after-start" || mode == "protection-lost-after-start" || mode == "runner-receipt-gone-after-start" {
						want = journal.Unconfirmed
					}
					action := journal.FinalizeBudget
					if mode == "legacy-final-without-claims" {
						action = journal.ResumeAdmission
					}
					require.Equal(t, want, budgetStep(t, store, action).Phase)
					require.Zero(t, admissionPosts.Load())
				}
				return nil
			})
			require.NoError(t, err)
		})
	}
}
