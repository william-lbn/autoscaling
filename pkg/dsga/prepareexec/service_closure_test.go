package prepareexec

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

type closureFixture struct {
	executor *Executor
	store    *journal.Store
	core     *kubefake.Clientset
	snapshot *journal.Snapshot
	command  command
	shared   control.SharedCostResponse
}

// 本组专测来源写者的关闭边界：真实typed HTTP Store/RFC6902 UID-RV CAS，
// Core对象与旧PG/Root预算明确为领域夹具。无需连接PG/Root/QMP；关闭门禁
// 必须早于这些调用。并发测试直接运行生产Outside写/读回和inspect检查，
// 不将本结果宣称为完整Prepare原生TLS链或Linux资源认证。
func newClosureFixture(t *testing.T, unknown bool) *closureFixture {
	t.Helper()
	raw, err := os.ReadFile("../journal/testdata/initial-domain.json")
	require.NoError(t, err)
	var domain struct {
		Initial budget.Initial                 `json:"initial"`
		Command budget.InitialAdmissionCommand `json:"command"`
		Receipt budget.InitialAdmissionReceipt `json:"receipt"`
	}
	require.NoError(t, json.Unmarshal(raw, &domain))
	i := domain.Initial
	b := journal.Binding{Namespace: "testing", Name: "prepare-vm", VMUID: "vm-uid", Incarnation: i.Incarnation}
	api, _ := dsgatest.NewSchedulerAPI(t, "prepare-host-fixture")
	var vm vmv1.VirtualMachine
	vm.Namespace, vm.Name, vm.UID, vm.ResourceVersion = b.Namespace, b.Name, b.VMUID, "1"
	vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: b.Incarnation, ComputeInstanceID: i.Instance.ComputeInstanceID, VMUID: "", ConfigSecretUID: "", Ready: false}
	vm.Status.Phase = vmv1.VmPending
	api.Mutate(func(a *dsgatest.SchedulerAPI) { a.VMs[b.Namespace+"/"+b.Name] = vm.DeepCopy() })
	store, err := journal.NewStore(api.VM.NeonvmV1().VirtualMachines(b.Namespace), b)
	require.NoError(t, err)
	ctx := context.Background()
	s, err := store.Initialize(ctx, "1", "prepare-owner")
	require.NoError(t, err)
	_, err = store.ReleaseBootstrap(ctx, s.ResourceVersion(), "prepare-config-uid")
	require.NoError(t, err)
	api.Mutate(func(a *dsgatest.SchedulerAPI) {
		v := a.VMs[b.Namespace+"/"+b.Name]
		v.Status.Phase, v.Status.PodName, v.Status.PodIP = vmv1.VmRunning, i.RunnerPodName, "10.0.0.8"
	})
	s, err = store.Load(ctx)
	require.NoError(t, err)
	s, err = store.BindResource(ctx, s, s.State().Owner, i.PGAggregate.Identity)
	require.NoError(t, err)
	s, err = store.BindPG(ctx, s, s.State().Owner, i.Instance)
	require.NoError(t, err)
	s, err = store.BindInitialBudget(ctx, s, s.State().Owner, i)
	require.NoError(t, err)
	s, _, err = store.StartInitialAdmission(ctx, s, s.State().Owner, domain.Command)
	require.NoError(t, err)
	s, err = store.RecordInitialAdmission(ctx, s, s.State().Owner, domain.Receipt)
	require.NoError(t, err)
	r := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: i.Instance, IssuerEpoch: s.State().Owner.Epoch, Revision: 1, OperationID: control.OperationID{High: 7, Low: 1}}, Kind: control.Grow, ExpectedCapacityGeneration: 1, TargetBuffers: 1024, TargetVMMemoryBytes: 2 << 30, BudgetCommitmentID: "prepare-new-target", DeadlineUnixMillis: control.Uint64(time.Now().Add(time.Minute).UnixMilli())}
	s, disposition, err := store.Begin(ctx, s, s.State().Owner, r, uint64(time.Now().UnixMilli()))
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	bootstrapRaw, err := json.Marshal(i.PGAggregate.Bootstrap)
	require.NoError(t, err)
	q := budget.PlatformQualification{ProtocolVersion: 1, Identity: i.PGAggregate.Identity, Incarnation: b.Incarnation, ReportID: "explicit-prepare-closure-domain", RootBootstrapSHA256: vmguard.SourceDigest(bootstrapRaw), RunnerBirth: i.RunnerBirth, RunnerIdentitySHA256: i.RunnerIdentitySHA256, RunnerPodName: i.RunnerPodName, RunnerPodUID: i.RunnerPodUID, NodeName: i.NodeName, NodeUID: i.NodeUID, RunnerRestartCount: i.RunnerRestartCount, HoldUntilExplicitSettlement: true, Targets: []budget.QualifiedTarget{{VMMemoryBytes: r.TargetVMMemoryBytes, GuestUsableMemoryBytes: (2 << 30) - (256 << 20), Limits: i.PGAggregate.Bootstrap.Limits, SystemOutsidePGBytes: i.SystemOutsidePG.UpperBytes, TransitionBytes: i.Transition.UpperBytes, SafetyBytes: i.Safety.UpperBytes}}}
	require.NoError(t, q.Validate())
	qRaw, err := json.Marshal(q)
	require.NoError(t, err)
	qualification := budget.QualificationSource{Claim: budget.ObjectRef{Name: "prepare-qualification", UID: "prepare-qualification-uid"}, SHA256: vmguard.SourceDigest(qRaw)}
	system, transition, safety := q.Targets[0].Bounds(qualification)
	upper, err := budget.GrowSharedUpper(domain.Receipt.Shared, r.TargetBuffers)
	require.NoError(t, err)
	p := budget.Prepared{ProtocolVersion: 1, VMShrinkPermit: false, Request: r, Identity: i.PGAggregate.Identity, RunnerPodName: i.RunnerPodName, RunnerRestartCount: i.RunnerRestartCount, VMBeforeMemoryBytes: i.VMMemoryBytes, GuestUsableMemoryBytes: q.Targets[0].GuestUsableMemoryBytes, Shared: domain.Receipt.Shared, TargetSharedUpperBytes: upper, Limits: q.Targets[0].Limits, SystemOutsidePG: system, Transition: transition, Safety: safety, OutsideClaim: nil}
	require.NoError(t, p.ValidateFor(r, p.Identity))
	rootHash, err := budget.RootDigest(i.PGAggregate)
	require.NoError(t, err)
	c := command{ProtocolVersion: 1, Qualification: qualification, Prepared: p, RootSHA256: rootHash, OutsideName: ""}
	c.OutsideName, err = outsideName(c)
	require.NoError(t, err)
	if unknown {
		raw, err = json.Marshal(c)
		require.NoError(t, err)
		s, _, err = store.Start(ctx, s, s.State().Owner, r.Key, journal.PrepareBudget, i.Instance.ComputeInstanceID, raw)
		require.NoError(t, err)
	}
	protected := true
	owner := []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: b.Name, UID: b.VMUID, Controller: &protected, BlockOwnerDeletion: &protected}}
	qualified := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: qualification.Claim.Name, Namespace: b.Namespace, UID: "prepare-qualification-uid", ResourceVersion: "qualification-rv", OwnerReferences: owner, Finalizers: []string{budgetclaims.ProtectionFinalizer}, Labels: map[string]string{budgetclaims.ClaimLabel: budgetclaims.QualificationKind}}, Immutable: &protected, Data: map[string]string{budgetclaims.ClaimKey: string(qRaw)}}
	pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: q.RunnerPodName, Namespace: b.Namespace, UID: "runner-uid", ResourceVersion: "pod-rv", OwnerReferences: owner}, Spec: core.PodSpec{NodeName: q.NodeName}, Status: core.PodStatus{Phase: core.PodRunning}}
	node := &core.Node{ObjectMeta: metav1.ObjectMeta{Name: q.NodeName, UID: "node-uid", ResourceVersion: "node-rv"}}
	client := kubefake.NewSimpleClientset(qualified, pod, node)
	claims, err := budgetclaims.New(client.CoreV1(), b)
	require.NoError(t, err)
	// 只给本次写/关闭测试提供必要对象；缺省原生客户端刻意不配置，
	// 保证关闭/旧快照的拒绝发生在任何PG/Root/Runner查询之前。
	e := &Executor{store: store, pg: nil, resources: nil, claims: claims, runner: nil, core: client.CoreV1(), actor: s.State().Owner, source: qualification}
	return &closureFixture{executor: e, store: store, core: client, snapshot: s, command: c, shared: domain.Receipt.Shared}
}

func (f *closureFixture) close(t *testing.T) *journal.Snapshot {
	t.Helper()
	ctx := context.Background()
	s, err := f.store.Load(ctx)
	require.NoError(t, err)
	r := f.command.Prepared.Request
	closeRequest := control.AdmissionRequest{ProtocolVersion: 1, Instance: r.Key.Instance, IssuerEpoch: s.State().Owner.Epoch, Revision: f.shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: r.ExpectedCapacityGeneration, Open: false, BudgetCommitmentID: r.BudgetCommitmentID}
	s, _, err = f.store.StartActiveServiceClosure(ctx, s, s.State().Owner, closeRequest)
	require.NoError(t, err)
	state := f.shared.Observed
	state.AdmissionOpen, state.AdmissionRevision = false, closeRequest.Revision
	hash, err := closeRequest.ContextSHA256()
	require.NoError(t, err)
	s, err = f.store.RecordServiceClosure(ctx, s, s.State().Owner, control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: closeRequest, Disposition: control.AdmissionApplied, ContextSHA256: hash, Observed: state}, false)
	require.NoError(t, err)
	return s
}

func TestPrepareClosureRejectsNewAndOriginalReplayBeforeSources(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "planned", true: "unknown"}[unknown], func(t *testing.T) {
			f := newClosureFixture(t, unknown)
			closed := f.close(t)
			prior := closed.State()
			progress, err := f.executor.Advance(context.Background())
			require.Equal(t, pgexec.AwaitingFact, progress)
			require.ErrorContains(t, err, "service closure")
			_, err = f.executor.ReplayOriginal(context.Background(), f.command.Prepared.Request.Key)
			require.ErrorContains(t, err, "service closure")
			_, err = f.executor.inspect(context.Background(), closed)
			require.ErrorContains(t, err, "service closure")
			_, err = f.executor.inspect(context.Background(), f.snapshot)
			require.ErrorContains(t, err, "service closure", "旧快照也必须先重读关闭，不能继续查询或创建来源")
			after, err := f.store.Load(context.Background())
			require.NoError(t, err)
			require.Equal(t, prior, after.State())
			require.Empty(t, f.core.Actions(), "关闭拒绝早于任何Core来源读写")
		})
	}
}

func TestPrepareLateOutsideCreateCannotRecordAcrossClosure(t *testing.T) {
	for _, lostReply := range []bool{false, true} {
		t.Run(map[bool]string{false: "returned", true: "reply-lost"}[lostReply], func(t *testing.T) {
			f := newClosureFixture(t, true)
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			var creates atomic.Int32
			f.core.PrependReactor("create", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
				creates.Add(1)
				close(entered)
				<-release
				cm := action.(kt.CreateAction).GetObject().(*core.ConfigMap).DeepCopy()
				cm.UID, cm.ResourceVersion = "late-outside-uid", "late-outside-rv"
				if err := f.core.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace); err != nil {
					return true, nil, err
				}
				if lostReply {
					return true, nil, errors.New("fixture CREATE committed after close and reply was lost")
				}
				return true, cm, nil
			})
			type outcome struct {
				ref budget.ObjectRef
				err error
			}
			finished := make(chan outcome, 1)
			go func() {
				ref, err := f.executor.outside(context.Background(), f.command, true)
				finished <- outcome{ref: ref, err: err}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("Outside CREATE did not reach the controlled in-flight boundary")
			}
			closed := f.close(t)
			original := closed.State()
			require.Equal(t, journal.Unconfirmed, original.Active.Steps[0].Phase)
			_, err := f.executor.Advance(context.Background())
			require.ErrorContains(t, err, "service closure")
			unblock()
			var result outcome
			select {
			case result = <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("late Outside CREATE did not finish")
			}
			cm, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).Get(context.Background(), f.command.OutsideName, metav1.GetOptions{})
			require.NoError(t, err, "真实晚到提交保留原CM，不因关闭就假定CREATE未执行")
			require.Equal(t, "late-outside-uid", string(cm.UID))
			require.EqualValues(t, 1, creates.Load())
			if lostReply {
				require.ErrorIs(t, result.err, resourceguard.ErrUnknown)
				require.Empty(t, result.ref.UID)
			} else {
				require.NoError(t, result.err)
				require.Equal(t, "late-outside-uid", result.ref.UID)
				_, err = f.executor.inspect(context.Background(), f.snapshot)
				require.ErrorContains(t, err, "service closure")
				// 即使忽略生产检查继续拿旧Snapshot尝试落账，真实HTTP
				// UID/RV CAS仍拒绝；没有伪造Prepared完成或撤销原Unknown。
				p := f.command.Prepared
				p.OutsideClaim = &result.ref
				raw, err := json.Marshal(p)
				require.NoError(t, err)
				step := f.snapshot.State().Active.Steps[0]
				_, err = f.store.Record(context.Background(), f.snapshot, f.executor.actor, p.Request.Key, journal.Observation{Key: p.Request.Key, RequestSHA256: f.snapshot.State().Active.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: step.TargetID, Data: raw})
				require.Error(t, err)
			}
			after, err := f.store.Load(context.Background())
			require.NoError(t, err)
			require.Equal(t, original, after.State())
			require.Nil(t, after.State().Active.Steps[0].Observation, "不把迟到CM或网络失败洗成Completed/NoEffect")
			for _, step := range after.State().Active.Steps[1:] {
				require.Equal(t, journal.Planned, step.Phase, "迟到Outside无权推进Host/Guest/VM/PG")
			}
			ref := budget.ObjectRef{Name: cm.Name, UID: string(cm.UID)}
			_, err = f.executor.claims.ReadOutside(context.Background(), ref)
			require.NoError(t, err)
			for _, fault := range []string{"uid", "protection", "mutable", "releasing"} {
				changed := cm.DeepCopy()
				switch fault {
				case "uid":
					changed.UID = "same-name-replacement"
				case "protection":
					changed.Finalizers = nil
				case "mutable":
					changed.Immutable = nil
				case "releasing":
					now := metav1.Now()
					changed.DeletionTimestamp = &now
				}
				require.NoError(t, f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
				_, err = f.executor.claims.ReadOutside(context.Background(), ref)
				require.Error(t, err, "旧引用不能按同名或同数值收养UID/保护已变的对象")
			}
		})
	}
}
