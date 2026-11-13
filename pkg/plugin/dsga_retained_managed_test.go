package plugin

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostscheduler"
	"github.com/neondatabase/autoscaling/pkg/plugin/initevents"
	"github.com/neondatabase/autoscaling/pkg/plugin/metrics"
	"github.com/neondatabase/autoscaling/pkg/plugin/reconcile"
	"github.com/neondatabase/autoscaling/pkg/plugin/state"
	"github.com/neondatabase/autoscaling/pkg/util"
	"github.com/neondatabase/autoscaling/pkg/util/patch"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// 只构造已有原Node持有；VM/Birth故意不存在。事件只能保留Unknown原成本，
// 不得调用VM资源恢复、Runner读者或把坏传播注解当作恢复执行许可。
func retainedManagedPluginFixture(t *testing.T, ignored bool) (*PluginState, *dsgatest.SchedulerAPI, *core.Pod, hostguard.Placement, *int) {
	t.Helper()
	f, binding := dsgatest.NewSchedulerAPI(t, "original-node-holder")
	profile := hostguard.Profile{ID: "qualified-test-only", SchedulerName: "neonvm-scheduler", VMOverheadBytes: 64 << 20, PodOverheadBytes: 16 << 20}
	row := hostguard.Placement{Namespace: "testing", PodName: "original-runner", PodUID: "original-runner-uid", VMName: "vm", VMUID: "vm-uid", Incarnation: "1234567890abcdef1234567890abcdef", Managed: true, MemoryBytes: 2 << 30, HostOverheadBytes: profile.VMOverheadBytes, PodMemoryLimitBytes: 4 << 30}
	store, err := hostguard.NewStore(f.Core.CoreV1(), f.Core.CoordinationV1(), binding, "original-node-holder")
	require.NoError(t, err)
	_, err = store.InitializeAllocations(context.Background(), "1", 1<<30, profile, []hostguard.Placement{row})
	require.NoError(t, err)
	host, err := hostscheduler.New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, hostscheduler.Config{Nodes: []hostscheduler.NodeConfig{{Binding: binding, Profile: profile, SystemBytes: 1 << 30}}}, profile.SchedulerName)
	require.NoError(t, err)
	stamp, yes := metav1.Now(), true
	pod := &core.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Namespace: row.Namespace, Name: row.PodName, UID: types.UID(row.PodUID), ResourceVersion: "pod-rv", DeletionTimestamp: &stamp, Finalizers: []string{hostguard.PodProtectionFinalizer},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: row.VMName, UID: types.UID(row.VMUID), Controller: &yes}},
		Labels:          map[string]string{api.LabelEnableAutoscaling: "true", api.LabelEnableAutoMigration: "true", api.LabelTestingOnlyAlwaysMigrate: "true"},
		Annotations:     map[string]string{vmv1.VirtualMachineResourcesAnnotation: "{broken", api.InternalAnnotationResourcesRequested: "{broken", api.InternalAnnotationResourcesApproved: "{broken"}},
		Spec:   core.PodSpec{NodeName: binding.NodeName, SchedulerName: profile.SchedulerName, Containers: []core.Container{{Name: "neonvm-runner", Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("500m"), core.ResourceMemory: resource.MustParse("1Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("2"), core.ResourceMemory: resource.MustParse("4Gi")}}}}},
		Status: core.PodStatus{Phase: core.PodFailed, ContainerStatuses: []core.ContainerStatus{{Name: "neonvm-runner", State: core.ContainerState{Terminated: &core.ContainerStateTerminated{Reason: "ContainerStatusUnknown"}}}}}}
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[row.Namespace+"/"+row.PodName] = pod.DeepCopy() })
	metric := metrics.BuildPluginMetrics(prometheus.NewRegistry(), nil)
	calls := new(int)
	config := Config{SchedulerName: profile.SchedulerName}
	if ignored {
		config.IgnoredNamespaces = []string{row.Namespace}
	}
	s := &PluginState{config: config, host: host, hostContext: context.Background(), metrics: metric,
		nodes:                map[string]*nodeState{binding.NodeName: {node: state.NodeStateFromParams(binding.NodeName, 8000, 8<<30, 0.9, nil), requestedMigrations: map[types.UID]struct{}{pod.UID: {}}, podsVMPatchedAt: make(map[types.UID]time.Time)}},
		tentativelyScheduled: make(map[types.UID]string), requeueAfterStartup: make(map[types.UID]struct{}), requeueNode: func(string) error { return nil }, requeuePod: func(types.UID) error { return nil },
		patchVM:         func(util.NamespacedName, []patch.Operation) error { *calls++; return nil },
		createMigration: func(*zap.Logger, *vmv1.VirtualMachineMigration) error { *calls++; return nil }}
	return s, f, pod, row, calls
}

func TestRetainedManagedInitialEventCompletesWithoutReleasingUnknown(t *testing.T) {
	for _, ignored := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal-namespace", true: "ignored-namespace"}[ignored], func(t *testing.T) {
			s, f, pod, row, legacyCalls := retainedManagedPluginFixture(t, ignored)
			init := initevents.NewInitEventsMiddleware()
			init.AddRequired(pod)
			done := init.Done()
			select {
			case <-done:
				t.Fatal("未确认原占账不能提前完成初始事件")
			default:
			}
			var originalNode string
			var writes [3]int
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				originalNode = f.Node.Annotations[hostguard.LedgerAnnotation]
				writes = [3]int{f.NodeWrites, f.PodWrites, f.VMWrites}
			})
			params := reconcile.ObjectParams{GVK: core.SchemeGroupVersion.WithKind("Pod"), UID: pod.UID, Name: pod.Name, Namespace: pod.Namespace, EventKind: reconcile.EventKindAdded, Obj: pod}
			_, err := init.Call(zap.NewNop(), params, func(logger *zap.Logger, params reconcile.ObjectParams) (reconcile.Result, error) {
				result, err := s.HandlePodEvent(logger, params.EventKind, params.Obj.(*core.Pod))
				if result == nil {
					return reconcile.Result{}, err
				}
				return *result, err
			})
			require.NoError(t, err)
			select {
			case <-done:
			default:
				t.Fatal("已保留原完整成本的Unknown不能锁死整个Scheduler初始化")
			}
			if !ignored {
				view, ok := s.nodes[pod.Spec.NodeName].node.GetPod(pod.UID)
				require.True(t, ok)
				require.Equal(t, util.NamespacedName{Namespace: row.Namespace, Name: row.VMName}, view.VirtualMachine)
				require.EqualValues(t, 4<<30, view.Mem.Reserved)
				require.Equal(t, view.Mem.Reserved, view.Mem.Requested)
				require.EqualValues(t, 2000, view.CPU.Reserved)
				require.False(t, view.Migratable || view.Migrating || view.AlwaysMigrate)
				require.NotContains(t, s.nodes[pod.Spec.NodeName].requestedMigrations, pod.UID)
			}
			// startupDone以后再次事件也只能保留成本，不能恢复旧批准/迁移路径。
			s.startupDone = true
			_, err = s.HandlePodEvent(zap.NewNop(), reconcile.EventKindModified, pod)
			require.NoError(t, err)
			require.Zero(t, *legacyCalls)
			enforcer := &AutoscaleEnforcer{logger: zap.NewNop(), state: s, metrics: &s.metrics.Framework}
			require.False(t, enforcer.Reserve(context.Background(), framework.NewCycleState(), pod, pod.Spec.NodeName).IsSuccess(), "事件观察不得授予新的Framework Reserve")
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Equal(t, originalNode, f.Node.Annotations[hostguard.LedgerAnnotation])
				require.Equal(t, writes, [3]int{f.NodeWrites, f.PodWrites, f.VMWrites})
				require.Equal(t, pod.Finalizers, f.Pods[row.Namespace+"/"+row.PodName].Finalizers)
				require.Empty(t, f.Pods[row.Namespace+"/"+row.PodName].Annotations[hostguard.RetirementAnnotation])
			})
		})
	}
}

func TestRetainedManagedMissingRowStillBlocksInitialConfirmation(t *testing.T) {
	s, f, pod, _, _ := retainedManagedPluginFixture(t, false)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		var m hostguard.Model
		require.NoError(t, json.Unmarshal([]byte(f.Node.Annotations[hostguard.LedgerAnnotation]), &m))
		*m.Placements = []hostguard.Placement{}
		raw, err := json.Marshal(m)
		require.NoError(t, err)
		f.Node.Annotations[hostguard.LedgerAnnotation] = string(raw)
	})
	init := initevents.NewInitEventsMiddleware()
	init.AddRequired(pod)
	done := init.Done()
	_, err := init.Call(zap.NewNop(), reconcile.ObjectParams{GVK: core.SchemeGroupVersion.WithKind("Pod"), UID: pod.UID}, func(*zap.Logger, reconcile.ObjectParams) (reconcile.Result, error) {
		_, err := s.HandlePodEvent(zap.NewNop(), reconcile.EventKindAdded, pod)
		return reconcile.Result{}, err
	})
	require.Error(t, err)
	select {
	case <-done:
		t.Fatal("缺原行不能靠Failed状态跳过初始完整性检查")
	default:
	}
	require.Len(t, init.Remaining(), 1)
}
