package hostscheduler

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	coord "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

func schedulerFixture(t *testing.T) (*Service, *dsgatest.SchedulerAPI, NodeConfig) {
	t.Helper()
	f, binding := dsgatest.NewSchedulerAPI(t, "temporary-fixture-holder")
	n := NodeConfig{Binding: binding, Profile: hostguard.Profile{ID: "qualified-test-only", SchedulerName: "neonvm-scheduler", VMOverheadBytes: 64 << 20, PodOverheadBytes: 16 << 20}, SystemBytes: 1 << 30, InitializeAtResourceVersion: "1"}
	s, err := New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Lease.Spec.HolderIdentity = &s.holder })
	return s, f, n
}

func boundedPod(name string, node string, limit string) *core.Pod {
	return &core.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: name, UID: types.UID(name + "-uid"), ResourceVersion: "1"}, Spec: core.PodSpec{NodeName: node, SchedulerName: "neonvm-scheduler", Containers: []core.Container{{Name: "app", Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceMemory: resource.MustParse("1Mi")}, Limits: core.ResourceList{core.ResourceMemory: resource.MustParse(limit)}}}}}}
}

func legacyVM(t *testing.T, f *dsgatest.SchedulerAPI) *core.Pod {
	t.Helper()
	controller := true
	vm := &vmv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "vm", UID: "vm-uid", ResourceVersion: "1", Labels: map[string]string{api.LabelEnableAutoscaling: "true"}, Annotations: map[string]string{api.InternalAnnotationResourcesRequested: `{"vCPUs":1,"mem":2147483648}`, api.AnnotationAutoscalingUnit: `{"vCPUs":1,"mem":1073741824}`}}}
	vm.Spec.Guest.MemorySlotSize = resource.MustParse("1Gi")
	vm.Spec.Guest.MemorySlots = vmv1.MemorySlots{Min: 1, Use: 1, Max: 4}
	vm.Spec.Guest.CPUs = vmv1.CPUs{Min: 1000, Use: 1000, Max: 4000}
	vm.Status.MemorySize = resource.NewQuantity(1<<30, resource.BinarySI)
	pod := boundedPod("runner", "node-one", "16Mi")
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: vm.Name, UID: vm.UID, Controller: &controller, BlockOwnerDeletion: nil}}
	// 注解故意带旧/错误资源，生产容量路径必须从原VM重建。
	pod.Annotations = map[string]string{vmv1.VirtualMachineResourcesAnnotation: `{broken`, api.InternalAnnotationResourcesApproved: `{"vCPUs":1000,"mem":1}`}
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		f.Pods["testing/runner"], f.VMs["testing/vm"] = pod.DeepCopy(), vm.DeepCopy()
	})
	return pod
}

func TestSchedulerRestoresAllWorkloadsAndCanonicalVMBeforeFirstGrant(t *testing.T) {
	s, f, n := schedulerFixture(t)
	pod := legacyVM(t, f)
	ordinary := boundedPod("ordinary", "node-one", "2Gi")
	restart := core.ContainerRestartPolicyAlways
	ordinary.Spec.InitContainers = []core.Container{{Name: "sidecar", RestartPolicy: &restart, Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("512Mi")}}}, {Name: "init", Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("3Gi")}}}}
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/ordinary"] = ordinary })
	ctx := context.Background()
	_, _, err := s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.ErrorIs(t, err, hostguard.ErrUninitialized, "初始化完成前不得以本地空闲授予")
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	canonical, held, err := s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.NoError(t, err)
	require.EqualValues(t, 1<<30, held.MemoryBytes)
	resources, err := vmv1.VirtualMachineResourcesFromPod(canonical)
	require.NoError(t, err)
	require.EqualValues(t, 1, resources.MemorySlots.Use)
	require.Equal(t, "1", canonical.Annotations[observedVMVersion])
	snapshot, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	row, err := snapshot.State().Placement(string(ordinary.UID))
	require.NoError(t, err)
	require.EqualValues(t, (3<<30)+(512<<20), row.MemoryBytes, "init峰值须包含已运行的原生sidecar")
	require.Len(t, *snapshot.State().Placements, 2)
	// 重启换副本并不丢失持有，也不要求全局leader恰好是节点holder。
	restarted, err := New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
	require.NoError(t, err)
	_, again, err := restarted.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.NoError(t, err)
	require.Equal(t, held, again)
	big := boundedPod("too-big", "", "3Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/too-big"] = big })
	_, _, err = restarted.Reserve(ctx, n.Binding.NodeName, big, 0)
	require.ErrorContains(t, err, "capacity")
}

func TestSchedulerLegacyApprovalHoldsTargetBeforeOriginalVMCAS(t *testing.T) {
	s, f, n := schedulerFixture(t)
	pod := legacyVM(t, f)
	ctx := context.Background()
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	canonical, _, err := s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.LoseVMReply = true })
	err = s.ApproveLegacy(ctx, canonical, api.Resources{VCPU: 1000, Mem: 2 << 30})
	require.Error(t, err, "批准实际已落库但丢回复，不能清Node高水位")
	snapshot, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	held, err := snapshot.State().Placement(string(pod.UID))
	require.NoError(t, err)
	require.EqualValues(t, 2<<30, held.MemoryBytes)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		approved, err := api.ExtractApprovedScaling(f.VMs["testing/vm"])
		require.NoError(t, err)
		require.EqualValues(t, 2<<30, approved.Mem)
		require.Equal(t, 1, f.VMWrites)
	})
	require.ErrorContains(t, s.ApproveLegacy(ctx, canonical, api.Resources{VCPU: 1000, Mem: 2 << 30}), "source changed")
	canonical, _, err = s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.NoError(t, err)
	require.NoError(t, s.ApproveLegacy(ctx, canonical, api.Resources{VCPU: 1000, Mem: 1 << 30}))
	snapshot, err = s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	held, err = snapshot.State().Placement(string(pod.UID))
	require.NoError(t, err)
	require.EqualValues(t, 2<<30, held.MemoryBytes, "低目标不自动释放尚未证明归还的旧持有")
}

func TestSchedulerRejectsChangedSourcesAndUnboundedWorkloads(t *testing.T) {
	for _, what := range []string{"pod-uid", "vm-uid", "node", "scheduler", "deleted", "migration", "no-limit", "init-no-limit", "ephemeral", "profile", "lost-ledger"} {
		t.Run(what, func(t *testing.T) {
			s, f, n := schedulerFixture(t)
			pod := legacyVM(t, f)
			ctx := context.Background()
			require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				p := f.Pods["testing/runner"]
				switch what {
				case "pod-uid":
					p.UID = "replacement-pod"
				case "vm-uid":
					f.VMs["testing/vm"].UID = "replacement-vm"
				case "node":
					p.Spec.NodeName = "other-node"
				case "scheduler":
					p.Spec.SchedulerName = "other-scheduler"
				case "deleted":
					stamp := metav1.Now()
					p.DeletionTimestamp = &stamp
				case "migration":
					p.OwnerReferences = append(p.OwnerReferences, metav1.OwnerReference{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachineMigration", Name: "migration", UID: "migration-uid", Controller: nil, BlockOwnerDeletion: nil})
				case "no-limit", "init-no-limit", "ephemeral":
					p.OwnerReferences = nil
					if what == "no-limit" {
						p.Spec.Containers[0].Resources.Limits = nil
					}
					if what == "init-no-limit" {
						p.Spec.InitContainers = []core.Container{{Name: "unbounded"}}
					}
					if what == "ephemeral" {
						p.Spec.EphemeralContainers = []core.EphemeralContainer{{}}
					}
				case "profile":
					var m hostguard.Model
					require.NoError(t, json.Unmarshal([]byte(f.Node.Annotations[hostguard.LedgerAnnotation]), &m))
					m.Profile.ID = "unqualified-replacement"
					raw, err := json.Marshal(m)
					require.NoError(t, err)
					f.Node.Annotations[hostguard.LedgerAnnotation] = string(raw)
				case "lost-ledger":
					delete(f.Node.Annotations, hostguard.LedgerAnnotation)
				}
			})
			_, _, err := s.Reserve(ctx, n.Binding.NodeName, pod, 0)
			require.Error(t, err)
		})
	}
}

func TestSchedulerManagedFirstPlacementAndLegacyBypassExclusion(t *testing.T) {
	s, f, n := schedulerFixture(t)
	pod := legacyVM(t, f)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		vm := f.VMs["testing/vm"]
		vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: strings.Repeat("a", 32), ComputeInstanceID: "compute", VMUID: "", ConfigSecretUID: "", Ready: false}
		vm.Status.MemorySize = nil
	})
	b := journal.Binding{Namespace: "testing", Name: "vm", VMUID: "vm-uid", Incarnation: strings.Repeat("a", 32)}
	store, err := journal.NewStore(f.VM.NeonvmV1().VirtualMachines("testing"), b)
	require.NoError(t, err)
	ctx := context.Background()
	snapshot, err := store.Initialize(ctx, "1", "original-owner")
	require.NoError(t, err)
	_, err = store.ReleaseBootstrap(ctx, snapshot.ResourceVersion(), "original-secret-uid")
	require.NoError(t, err)
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	canonical, held, err := s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.NoError(t, err)
	require.True(t, held.Managed)
	require.False(t, api.HasAutoscalingEnabled(canonical))
	require.False(t, api.HasAutoMigrationEnabled(canonical))
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		require.NoError(t, hostguard.InspectInitialVM(&f.Node, b, pod.Name, string(pod.UID), 1<<30))
	})
	require.ErrorContains(t, s.ApproveLegacy(ctx, canonical, api.Resources{VCPU: 1000, Mem: 2 << 30}), "legacy")
}

func TestSchedulerActualElectionBootstrapsOriginalNodeAndRetainsOnStop(t *testing.T) {
	s, f, n := schedulerFixture(t)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		f.Lease.Spec = coord.LeaseSpec{HolderIdentity: nil, LeaseDurationSeconds: nil, AcquireTime: nil, RenewTime: nil, LeaseTransitions: nil, Strategy: nil, PreferredHolder: nil}
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, s.Run(ctx, zap.NewNop()))
	var state hostguard.Model
	require.Eventually(t, func() bool {
		snapshot, err := s.domains[n.Binding.NodeName].store.Load(context.Background())
		if err != nil {
			return false
		}
		state = snapshot.State()
		return state.ProtocolVersion == 3
	}, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, s.holder, state.LeaseFloor.HolderID)
	require.Error(t, s.Run(ctx, zap.NewNop()), "同一Service不得启动两个owner循环")
	cancel()
	snapshot, err := s.domains[n.Binding.NodeName].store.Load(context.Background())
	require.NoError(t, err)
	require.Equal(t, state.Origin, snapshot.State().Origin)
	f.Mutate(func(f *dsgatest.SchedulerAPI) { require.Equal(t, s.holder, *f.Lease.Spec.HolderIdentity) })
}
