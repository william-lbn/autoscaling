package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	coordination "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmfake "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/fake"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 真实typed客户端/Store严格解码/UID-RV写前路径、Runner生产构造器和本地WAL。
// fake apiserver显式生成UID/RV并模拟CM CAS和丢回复；不是实际apiserver/RBAC。
// Prepare/Host资格和PG/Root物理证明是完整协议夹具，不能声称已实现生产者。
type runnerFixture struct {
	runner         *Runner
	core           *kubefake.Clientset
	store          *journal.Store
	vms            *vmfake.Clientset
	config         Config
	actor          journal.Owner
	loseStateReply bool
	nextUID        int
	badShrink      string
}

func TestRunnerInitialHostDenialPrecedesBirthAndSourceWrites(t *testing.T) {
	for _, what := range []string{"missing", "unprotected", "replacement-node", "short-memory"} {
		t.Run(what, func(t *testing.T) {
			f := newRunnerFixture(t)
			ctx := context.Background()
			node, err := f.core.CoreV1().Nodes().Get(ctx, f.config.NodeName, metav1.GetOptions{})
			require.NoError(t, err)
			switch what {
			case "missing":
				delete(node.Annotations, hostguard.LedgerAnnotation)
			case "unprotected":
				node.Finalizers = nil
			case "replacement-node":
				node.UID = "replacement-node-uid"
			case "short-memory":
				var m hostguard.Model
				require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &m))
				(*m.Placements)[0].MemoryBytes--
				raw, err := json.Marshal(m)
				require.NoError(t, err)
				node.Annotations[hostguard.LedgerAnnotation] = string(raw)
			}
			require.NoError(t, f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), node, ""))
			config := f.config
			config.StateDir = t.TempDir()
			require.NoError(t, os.Chmod(config.StateDir, 0o700))
			before := len(f.core.Actions())
			_, err = Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), config)
			require.Error(t, err)
			_, err = os.Stat(filepath.Join(config.StateDir, "birth-intent.json"))
			require.True(t, os.IsNotExist(err), "未获原容量时不能分配新的Runner boot身份")
			for _, action := range f.core.Actions()[before:] {
				require.NotEqual(t, "create", action.GetVerb(), "拒绝须早于所有原来源CREATE")
			}
		})
	}
}

func newRunnerFixture(t *testing.T) *runnerFixture {
	return runnerBirthFixture(t, true)
}

// initialize=false仅为新出生故障组合保留真实Initialize之前的原环境，
// 不删除已存在的出生或WAL来冒充新实例；旧测试仍走完整成功构造。
func runnerBirthFixture(t *testing.T, initialize bool) *runnerFixture {
	return runnerBirthFixtureForCompute(t, initialize, "compute-instance")
}

func runnerBirthFixtureForCompute(t *testing.T, initialize bool, computeInstanceID string) *runnerFixture {
	t.Helper()
	ctx := context.Background()
	id := fixtureIdentity()
	binding := journal.Binding{Namespace: "testing", Name: "vm", VMUID: types.UID(id.VMUID), Incarnation: id.Incarnation}
	mode := vmv1.CpuScalingModeSysfs
	vm := &vmv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Namespace: binding.Namespace, Name: binding.Name, UID: binding.VMUID, ResourceVersion: "1"}}
	vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: binding.Incarnation, ComputeInstanceID: computeInstanceID, VMUID: "", ConfigSecretUID: "", Ready: false}
	vm.Spec.CpuScalingMode, vm.Spec.ServiceAccountName = &mode, "dsga-runner-test"
	vm.Spec.Guest.MemorySlotSize = *resource.NewQuantity(1<<30, resource.BinarySI)
	vm.Spec.Guest.MemorySlots.Min, vm.Spec.Guest.MemorySlots.Max, vm.Spec.Guest.MemorySlots.Use = 1, 4, 1
	vms := vmfake.NewSimpleClientset(vm)
	store, err := journal.NewStore(vms.NeonvmV1().VirtualMachines(binding.Namespace), binding)
	require.NoError(t, err)
	s, err := store.Initialize(ctx, "1", "original-owner")
	require.NoError(t, err)
	s, err = store.ReleaseBootstrap(ctx, s.ResourceVersion(), "original-config-uid")
	require.NoError(t, err)
	vm = s.VirtualMachine()
	vm.Status.Phase, vm.Status.PodName, vm.Status.Node, vm.Status.PodIP = vmv1.VmRunning, "runner", "node", "10.0.0.8"
	vm.Status.MemorySize = resource.NewQuantity(1<<30, resource.BinarySI)
	_, err = vms.NeonvmV1().VirtualMachines(binding.Namespace).Update(ctx, vm, metav1.UpdateOptions{})
	require.NoError(t, err)
	controller, mount := true, true
	pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: binding.Namespace, UID: "pod-uid", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: binding.Name, UID: binding.VMUID, Controller: &controller, BlockOwnerDeletion: nil}}}, Spec: core.PodSpec{NodeName: "node", ServiceAccountName: vm.Spec.ServiceAccountName, AutomountServiceAccountToken: &mount}, Status: core.PodStatus{Phase: core.PodRunning, PodIP: vm.Status.PodIP}}
	node := &core.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid", ResourceVersion: "1"}}
	node = dsgatest.InitialNode(node, binding, "runner", "pod-uid", 1<<30)
	f := &runnerFixture{core: kubefake.NewSimpleClientset(pod, node), vms: vms, store: store, actor: s.State().Owner, runner: nil, config: Config{StateDir: "", PodName: "", PodUID: "", NodeName: "", MinimumBytes: 0, MaximumBytes: 0}, loseStateReply: false, nextUID: 0, badShrink: ""}
	f.core.PrependReactor("create", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		cm := action.(kt.CreateAction).GetObject().(*core.ConfigMap).DeepCopy()
		f.nextUID++
		cm.UID, cm.ResourceVersion = types.UID(fmt.Sprintf("claim-uid-%d", f.nextUID)), "1"
		if err := f.core.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace); err != nil {
			return true, nil, err
		}
		if kind := cm.Labels[ClaimLabel]; kind == bindingKind || kind == stateKind || kind == receiptKind {
			// 出生/状态/回执CREATE都已落库但丢回复：真实实现须以相同nonce/
			// 原文的GET恢复，不能再生成另一个boot或相信Create返回对象。
			return true, nil, errors.New("fixture CREATE reply lost after commit")
		}
		return true, cm.DeepCopy(), nil
	})
	f.core.PrependReactor("update", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		cm := action.(kt.UpdateAction).GetObject().(*core.ConfigMap).DeepCopy()
		old, err := f.core.Tracker().Get(core.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name)
		if err != nil {
			return true, nil, err
		}
		prior := old.(*core.ConfigMap)
		if cm.UID != prior.UID || cm.ResourceVersion != prior.ResourceVersion {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps", Group: ""}, cm.Name, errors.New("fixture CAS"))
		}
		n, _ := strconv.Atoi(prior.ResourceVersion)
		cm.ResourceVersion = strconv.Itoa(n + 1)
		if err := f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace); err != nil {
			return true, nil, err
		}
		if f.loseStateReply && cm.Labels[ClaimLabel] == stateKind {
			f.loseStateReply = false
			return true, nil, errors.New("fixture reply lost after update committed")
		}
		return true, cm.DeepCopy(), nil
	})
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	f.config = Config{StateDir: dir, PodName: "runner", PodUID: "pod-uid", NodeName: "node", MinimumBytes: 1 << 30, MaximumBytes: 4 << 30}
	if initialize {
		f.runner, err = Initialize(ctx, store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
		require.NoError(t, err)
	}
	seconds, transitions, holder := int32(60), int32(0), f.actor.HolderID
	stamp := metav1.NewMicroTime(time.Now())
	lease := &coordination.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: binding.Namespace, Name: journal.LeaseName(binding), UID: "lease-uid", ResourceVersion: "1", Annotations: map[string]string{journal.LeaseBindingAnnotation: journal.LeaseMarker(binding)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: binding.Name, UID: binding.VMUID, Controller: &controller, BlockOwnerDeletion: nil}}}, Spec: coordination.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &seconds, LeaseTransitions: &transitions, AcquireTime: &stamp, RenewTime: &stamp, Strategy: nil, PreferredHolder: nil}, TypeMeta: metav1.TypeMeta{Kind: "", APIVersion: ""},
	}
	_, err = f.core.CoordinationV1().Leases(binding.Namespace).Create(ctx, lease, metav1.CreateOptions{})
	require.NoError(t, err)
	return f
}

func (f *runnerFixture) completePrior(t *testing.T, request control.Request, action journal.Action, target string, data any) {
	f.completePriorCommand(t, request, action, target, map[string]bool{"test_only_prior_domain": true}, data)
}

func (f *runnerFixture) completePriorCommand(t *testing.T, request control.Request, action journal.Action, target string, command, data any) {
	t.Helper()
	ctx := context.Background()
	s, err := f.store.Load(ctx)
	require.NoError(t, err)
	raw, err := json.Marshal(data)
	require.NoError(t, err)
	commandRaw, err := json.Marshal(command)
	require.NoError(t, err)
	s, dispatch, err := f.store.Start(ctx, s, f.actor, request.Key, action, target, commandRaw)
	require.NoError(t, err)
	_, err = f.store.Record(ctx, s, f.actor, request.Key, journal.Observation{Key: request.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256, Ordinal: dispatch.Step.Ordinal, Action: action, Result: journal.Completed, SourceID: target, Data: raw})
	require.NoError(t, err)
}

func (f *runnerFixture) budgetClaim(t *testing.T, name, kind string, data any) budget.ObjectRef {
	t.Helper()
	raw, err := json.Marshal(data)
	require.NoError(t, err)
	cm := newClaim(f.store.Binding(), name, kind, true, raw)
	cm.Labels = map[string]string{budgetclaims.ClaimLabel: kind}
	cm.Finalizers = []string{budgetclaims.ProtectionFinalizer}
	cm.Data = map[string]string{budgetclaims.ClaimKey: string(raw)}
	created, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).Create(context.Background(), cm, metav1.CreateOptions{})
	require.NoError(t, err)
	return budget.ObjectRef{Name: created.Name, UID: string(created.UID)}
}

func (f *runnerFixture) beginGrow(t *testing.T) (Command, string) {
	return f.beginKind(t, control.Grow)
}

func (f *runnerFixture) beginKind(t *testing.T, kind control.Kind) (Command, string) {
	t.Helper()
	ctx := context.Background()
	c := fixtureCommand(f.runner.birth.Identity)
	c.Request.Kind, c.Request.Key.IssuerEpoch, c.Request.TargetBuffers = control.Grow, f.actor.Epoch, 1024
	c.BeforeBytes, c.Request.TargetVMMemoryBytes, c.GuestSHA256 = 1<<30, 2<<30, ""
	c.BindingClaim = f.runner.engine.binding
	if kind == control.Shrink {
		c.Request.Kind, c.Request.TargetBuffers, c.BeforeBytes = control.Shrink, 256, 3<<30
	}
	id := resourceguard.Identity{VMUID: c.Identity.VMUID, ComputeInstanceID: c.Request.Key.Instance.ComputeInstanceID, GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3, SystemCgroupDevice: 0, SystemCgroupInode: 0}
	s, err := f.store.Load(ctx)
	require.NoError(t, err)
	// 完整首次预算场景已经固定原PG/Root身份；复用其完整绑定，不在
	// 创建新Resize时重新命名原boot或丢掉system cgroup身份。未绑定的
	// 旧Runner领域夹具仍按上面的显式缺省身份初始化。
	if bound := s.State(); bound.PGInstance != nil && bound.ResourceIdentity != nil {
		c.Request.Key.Instance, id = *bound.PGInstance, *bound.ResourceIdentity
	}
	s, err = f.store.BindResource(ctx, s, f.actor, id)
	require.NoError(t, err)
	s, err = f.store.BindPG(ctx, s, f.actor, c.Request.Key.Instance)
	require.NoError(t, err)
	_, disposition, err := f.store.Begin(ctx, s, f.actor, c.Request, uint64(time.Now().UnixMilli()))
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	raw, err := os.ReadFile("../../api/dsga/testdata/response-v1.json")
	require.NoError(t, err)
	var fact control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &fact))
	state := fact.Observed
	state.Instance, state.IssuerFloorEpoch, state.Operation, state.CapacityGeneration = c.Request.Key.Instance, c.Request.Key.IssuerEpoch, nil, c.Request.ExpectedCapacityGeneration
	state.LowBuffers, state.HighBuffers = 512, 512
	shared := control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: 16 << 20, StartupArraysUpperBytes: 16 << 20, CurrentMainAndArraysUpperBytes: 80 << 20, Observed: state, VMShrinkPermit: false}
	p := budget.Prepared{ProtocolVersion: 1, Request: c.Request, Identity: id, RunnerPodName: "runner", VMBeforeMemoryBytes: c.BeforeBytes, GuestUsableMemoryBytes: c.Request.TargetVMMemoryBytes - 32<<20, Shared: shared, TargetSharedUpperBytes: 144 << 20, Limits: resourceguard.Limits{MemoryMaxBytes: 512 << 20, OutsideUpperBytes: 32 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 8}}, PidsMaxTasks: 0, SystemMemoryMaxBytes: 0}, SystemOutsidePG: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 24 << 20, SourceID: "qualified-system"}, Transition: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "qualified-transition"}, Safety: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "qualified-safety"}, VMShrinkPermit: false, RunnerRestartCount: 0, OutsideClaim: nil}
	if kind == control.Shrink {
		p.TargetSharedUpperBytes = 80 << 20
	}
	outside := budget.OutsideReservation{ProtocolVersion: 1, Request: c.Request, Identity: id, Incarnation: c.Identity.Incarnation, ResidualProfileID: "fixture-qualified-profile", SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true, VMShrinkPermit: false}
	outRef := f.budgetClaim(t, "outside-original", budgetclaims.OutsideKind, outside)
	p.OutsideClaim = &outRef
	require.NoError(t, p.ValidateFor(c.Request, id))
	f.completePrior(t, c.Request, journal.PrepareBudget, c.Request.Key.Instance.ComputeInstanceID, p)
	host := dsgatest.Host(f.store.Binding(), p, budget.HostReservation{ProtocolVersion: 1, Request: c.Request, Identity: id, Incarnation: c.Identity.Incarnation, SourceID: "original-scheduler-hold", RunnerPodName: "runner", RunnerPodUID: c.Identity.RunnerPodUID, NodeName: "node", NodeUID: c.Identity.NodeUID, ReservedMemoryBytes: max(c.Request.TargetVMMemoryBytes, c.BeforeBytes) + 64<<20, HostOverheadBytes: 64 << 20, HoldUntilExplicitSettlement: true, VMShrinkPermit: false})
	if f.badShrink == "short-host-hold" {
		host.ReservedMemoryBytes = c.Request.TargetVMMemoryBytes + host.HostOverheadBytes
	}
	c.HostClaim = f.budgetClaim(t, "host-original", budgetclaims.HostKind, host)
	node, err := f.core.CoreV1().Nodes().Get(ctx, host.NodeName, metav1.GetOptions{})
	require.NoError(t, err)
	_, err = f.core.CoreV1().Nodes().Update(ctx, dsgatest.Node(node, f.store.Binding(), p, host, c.HostClaim), metav1.UpdateOptions{})
	require.NoError(t, err)
	if kind == control.Grow {
		f.completePrior(t, c.Request, journal.ReserveHost, host.SourceID, host)
	} else {
		for _, action := range []journal.Action{journal.Quiesce, journal.ResizePG, journal.ReleasePG} {
			f.completePrior(t, c.Request, action, c.Request.Key.Instance.ComputeInstanceID, map[string]bool{"explicit_prior_native_domain_fixture": true})
		}
		latest := shared
		latest.Observed.CapacityGeneration++
		latest.Observed.LowBuffers, latest.Observed.HighBuffers = c.Request.TargetBuffers, c.Request.TargetBuffers
		hash, err := c.Request.ContextSHA256()
		require.NoError(t, err)
		latest.Observed.Operation = &control.OperationFact{Key: c.Request.Key, Kind: kind, Phase: control.Committed, Reason: control.ReasonNone, TargetBuffers: c.Request.TargetBuffers, ExpectedCapacityGeneration: c.Request.ExpectedCapacityGeneration, ContextSHA256: hash, CancelClosed: true, CancelRequested: false, Replayed: false}
		latest.Observed.Arrays = append([]control.ArrayFact(nil), shared.Observed.Arrays...)
		for i := range latest.Observed.Arrays {
			array := &latest.Observed.Arrays[i]
			key := c.Request.Key
			array.ReleaseKey, array.ReleaseCapacityGeneration, array.ReleaseState = &key, latest.Observed.CapacityGeneration, control.ReleaseVerified
			array.Provider = control.ProviderFact{HasResult: true, Status: control.ProviderNoop, SystemErrno: 0, RequestedOffset: 0, RequestedBytes: 0, AffectedOffset: 0, AffectedBytes: 0, PhysicalReleaseVerified: false, AllocationMayBePartial: false, ReleaseMayBePartial: false}
		}
		latest.Observed.Arrays[0].Provider = control.ProviderFact{HasResult: true, Status: control.ProviderOK, PhysicalReleaseVerified: true, RequestedOffset: 4096, AffectedOffset: 4096, RequestedBytes: 4096, AffectedBytes: 4096, SystemErrno: 0, AllocationMayBePartial: false, ReleaseMayBePartial: false}
		if f.badShrink == "unverified-pg-release" {
			latest.Observed.Arrays[0].Provider.PhysicalReleaseVerified = false
		}
		command := resourceguard.Command{ProtocolVersion: 1, Identity: id, IssuerEpoch: f.actor.Epoch, Revision: 1, BudgetCommitmentID: c.Request.BudgetCommitmentID, PostmasterPID: 123, PostmasterStartTicks: 456, MemoryMaxBytes: p.Limits.MemoryMaxBytes, OutsideUpperBytes: p.Limits.OutsideUpperBytes, HugePools: p.Limits.HugePools, PidsMaxTasks: 0, SystemMemoryMaxBytes: 0}
		if f.badShrink == "wrong-root-limit" {
			command.MemoryMaxBytes += 8 << 20
		}
		view := resourceguard.View{ProtocolVersion: 1, Identity: id, IssuerFloorEpoch: f.actor.Epoch, Bootstrap: resourceguard.BootstrapCommand{ProtocolVersion: 1, Identity: id, IssuerEpoch: f.actor.Epoch, BudgetCommitmentID: "original-bootstrap", Limits: p.Limits}, BootstrapApplied: true, Birth: &resourceguard.ProcessIdentity{ProtocolVersion: 1, Identity: id, PostmasterPID: command.PostmasterPID, PostmasterStartTicks: command.PostmasterStartTicks}, Original: &command, Held: &command, Applied: true, Actual: resourceguard.Evidence{ProtocolVersion: 1, Identity: id, MemoryMaxBytes: command.MemoryMaxBytes, MemoryCurrentBytes: 4 << 20, GuestMemTotalBytes: c.BeforeBytes, HugePools: p.Limits.HugePools, WholePoolUpperBytes: 16 << 20, HugeOvercommitDisabled: true, CgroupPopulated: true, VMShrinkPermit: false, SwapMaxBytes: 0, SwapCurrentBytes: 0, HugeSurplusPages: 0, TaskLimit: nil, System: nil}, VMShrinkPermit: false, SystemPlacementObserved: false}
		aggregate, err := budget.RootAggregateFromView(view)
		require.NoError(t, err)
		audit := resourceexec.Audit{ProtocolVersion: 2, Command: command, Evidence: view.Actual, Fence: control.FenceResponse{ProtocolVersion: 1, RequestedInstance: c.Request.Key.Instance, RequestedEpoch: f.actor.Epoch, Disposition: control.Fenced, Observed: latest.Observed, VMShrinkPermit: false}, Shared: latest, PGAggregate: aggregate, RunnerPodName: "runner", VMMemoryBytes: c.BeforeBytes, Query: false, RunnerRestarts: 0}
		if f.badShrink == "wrong-fence" {
			audit.Fence.RequestedEpoch++
		}
		f.completePriorCommand(t, c.Request, journal.GuestReady, c.Request.Key.Instance.ComputeInstanceID, command, audit)
		rawGuest, err := json.Marshal(audit)
		require.NoError(t, err)
		c.GuestSHA256 = SourceDigest(rawGuest)
	}
	raw, err = json.Marshal(p)
	require.NoError(t, err)
	c.PreparedSHA256 = SourceDigest(raw)
	raw, err = json.Marshal(host)
	require.NoError(t, err)
	c.HostSHA256 = SourceDigest(raw)
	raw, err = json.Marshal(c)
	require.NoError(t, err)
	s, err = f.store.Load(ctx)
	require.NoError(t, err)
	_, dispatch, err := f.store.Start(ctx, s, f.actor, c.Request.Key, journal.ResizeVM, c.Identity.RunnerPodUID, raw)
	require.NoError(t, err)
	return c, dispatch.Step.CommandSHA256
}

func TestRunnerProductionShrinkRequiresOriginalCompleteSafetySources(t *testing.T) {
	for _, what := range []string{"complete", "short-host-hold", "unverified-pg-release", "wrong-root-limit", "wrong-fence"} {
		t.Run(what, func(t *testing.T) {
			f := newRunnerFixture(t)
			f.badShrink = what
			// 先前稳定的3GiB VM/PG事实在本用例为显式协议夹具；没有将临时
			// QMP值写成Host/PG物理生产者。最终Runner处理/发布使用真实实现。
			native := &memoryFixture{dir: f.config.StateDir, actual: Observation{RequestedBytes: 3 << 30, ActualBytes: 3 << 30}, apply: true, mu: sync.Mutex{}, writes: 0, err: nil, entered: nil, release: nil}
			f.runner.engine.native = native
			c, hash := f.beginKind(t, control.Shrink)
			err := f.runner.Tick(context.Background())
			if what != "complete" {
				require.Error(t, err)
				require.Zero(t, native.writes)
				return
			}
			require.NoError(t, err)
			_, _, state, err := ReadPublished(context.Background(), f.core.CoreV1(), f.store.Binding(), &c.BindingClaim)
			require.NoError(t, err)
			require.NotNil(t, state.State.LastPublished)
			r, err := ReadReceipt(context.Background(), f.core.CoreV1(), f.store.Binding(), *state.State.LastPublished, c, hash)
			require.NoError(t, err)
			require.True(t, r.Complete())
			require.Equal(t, 1, native.writes)
		})
	}
}

func TestRunnerProductionPullPublishLostRepliesAndOriginalReceipt(t *testing.T) {
	f := newRunnerFixture(t)
	qemu := &qmpFixture{id: f.runner.birth.Identity, actual: Observation{RequestedBytes: 1 << 30, ActualBytes: 1 << 30}, mode: "lost-set-reply", walDir: f.config.StateDir, mu: sync.Mutex{}, writes: 0, requests: nil}
	f.runner.engine.native = &nativeQMP{id: qemu.id, path: startQMPFixture(t, qemu), timeout: time.Second}
	require.NoError(t, f.runner.Tick(context.Background()))
	_, ref, initial, err := ReadPublished(context.Background(), f.core.CoreV1(), f.store.Binding(), nil)
	require.NoError(t, err)
	require.Equal(t, control.Uint64(1<<30), initial.Actual.ActualBytes)
	c, journalSHA := f.beginGrow(t)
	require.ErrorIs(t, f.runner.Tick(context.Background()), ErrUnknown)
	_, _, uncertain, err := ReadPublished(context.Background(), f.core.CoreV1(), f.store.Binding(), &ref)
	require.NoError(t, err)
	require.NotNil(t, uncertain.State.Pending)
	// 旧动作已可能发送，Host来源失联不能阻止原目标只读恢复，更不能新派发。
	require.NoError(t, f.core.CoreV1().ConfigMaps("testing").Delete(context.Background(), c.HostClaim.Name, metav1.DeleteOptions{}))
	f.loseStateReply = true
	require.ErrorContains(t, f.runner.Tick(context.Background()), "reply lost")
	require.NoError(t, f.runner.Tick(context.Background()))
	_, _, done, err := ReadPublished(context.Background(), f.core.CoreV1(), f.store.Binding(), &ref)
	require.NoError(t, err)
	require.Nil(t, done.State.Pending)
	require.NotNil(t, done.State.LastPublished)
	result, err := ReadReceipt(context.Background(), f.core.CoreV1(), f.store.Binding(), *done.State.LastPublished, c, journalSHA)
	require.NoError(t, err)
	require.True(t, result.Complete())
	require.Equal(t, control.Uint64(2<<30), result.Actual.ActualBytes)
	qemu.mu.Lock()
	writes := qemu.writes
	qemu.mu.Unlock()
	require.Equal(t, 1, writes)
	replaced := *done.State.LastPublished
	replaced.UID = "same-name-new-uid"
	_, err = ReadReceipt(context.Background(), f.core.CoreV1(), f.store.Binding(), replaced, c, journalSHA)
	require.Error(t, err)
}

func TestRunnerProductionRejectsChangedSourcesBeforeNativeWrite(t *testing.T) {
	for _, what := range []string{"outside-uid", "host-releasing", "runner-uid", "node-uid", "lease-replaced", "lease-expired", "birth-replaced", "state-replaced"} {
		t.Run(what, func(t *testing.T) {
			f := newRunnerFixture(t)
			native := &memoryFixture{dir: f.config.StateDir, actual: Observation{RequestedBytes: 1 << 30, ActualBytes: 1 << 30}, apply: true, mu: sync.Mutex{}, writes: 0, err: nil, entered: nil, release: nil}
			f.runner.engine.native = native
			require.NoError(t, f.runner.Tick(context.Background()))
			c, _ := f.beginGrow(t)
			switch what {
			case "outside-uid", "host-releasing", "birth-replaced", "state-replaced":
				name := "outside-original"
				if what == "host-releasing" {
					name = c.HostClaim.Name
				}
				if what == "birth-replaced" {
					name = f.runner.engine.binding.Name
				}
				if what == "state-replaced" {
					name = f.runner.birth.StateClaim.Name
				}
				cm, err := f.core.CoreV1().ConfigMaps("testing").Get(context.Background(), name, metav1.GetOptions{})
				require.NoError(t, err)
				if what == "host-releasing" {
					stamp := metav1.Now()
					cm.DeletionTimestamp = &stamp
				} else {
					cm.UID = "replacement-uid"
				}
				require.NoError(t, f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, "testing"))
			case "runner-uid":
				pod, err := f.core.CoreV1().Pods("testing").Get(context.Background(), "runner", metav1.GetOptions{})
				require.NoError(t, err)
				pod.UID = "new-pod"
				require.NoError(t, f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, "testing"))
			case "node-uid":
				node, err := f.core.CoreV1().Nodes().Get(context.Background(), "node", metav1.GetOptions{})
				require.NoError(t, err)
				node.UID = "new-node"
				require.NoError(t, f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), node, ""))
			case "lease-replaced", "lease-expired":
				lease, err := f.core.CoordinationV1().Leases("testing").Get(context.Background(), journal.LeaseName(f.store.Binding()), metav1.GetOptions{})
				require.NoError(t, err)
				if what == "lease-replaced" {
					lease.UID = "replacement-lease"
				} else {
					stamp := metav1.NewMicroTime(time.Now().Add(-time.Hour))
					lease.Spec.RenewTime = &stamp
				}
				_, err = f.core.CoordinationV1().Leases("testing").Update(context.Background(), lease, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			require.Error(t, f.runner.Tick(context.Background()))
			require.Zero(t, native.writes)
		})
	}
}

func TestRunnerBirthRefusesMissingWALOrSameNameNewProcess(t *testing.T) {
	for _, what := range []string{"same-state", "missing-state", "new-empty-directory"} {
		t.Run(what, func(t *testing.T) {
			f := newRunnerFixture(t)
			if what == "missing-state" {
				require.NoError(t, os.Remove(filepath.Join(f.config.StateDir, "state.json")))
			}
			if what == "new-empty-directory" {
				f.config.StateDir = t.TempDir()
				require.NoError(t, os.Chmod(f.config.StateDir, 0o700))
			}
			_, err := Initialize(context.Background(), f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
			require.Error(t, err, "same-name Runner must never launch another QEMU after losing original birth state")
		})
	}
}
