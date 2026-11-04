package plugin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostscheduler"
	localstate "github.com/neondatabase/autoscaling/pkg/plugin/state"
)

// 用原公开Store执行原退休CAS，避免把缺行/终态直接当作fixture关闭证明。
func hostRetiredEventFixture(t *testing.T) (*PluginState, *dsgatest.SchedulerAPI, *core.Pod) {
	t.Helper()
	ctx := context.Background()
	holder := "original-holder"
	f, b := dsgatest.NewSchedulerAPI(t, holder)
	profile := hostguard.Profile{ID: "qualified-test-only", SchedulerName: "autoscale-scheduler", VMOverheadBytes: 64 << 20, PodOverheadBytes: 16 << 20}
	pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "retired-original", UID: "retired-original-UID", ResourceVersion: "1", Finalizers: []string{hostguard.PodProtectionFinalizer}}, Spec: core.PodSpec{NodeName: b.NodeName, SchedulerName: core.DefaultSchedulerName, Containers: []core.Container{{Name: "app", Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceMemory: resource.MustParse("1Mi")}, Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("1Gi")}}}}}}
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[pod.Namespace+"/"+pod.Name] = pod.DeepCopy() })
	store, err := hostguard.NewStore(f.Core.CoreV1(), f.Core.CoordinationV1(), b, holder)
	require.NoError(t, err)
	placement := hostguard.Placement{Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), MemoryBytes: control.Uint64(1 << 30), HostOverheadBytes: profile.PodOverheadBytes}
	snapshot, err := store.InitializeAllocations(ctx, "1", 1<<30, profile, []hostguard.Placement{placement})
	require.NoError(t, err)
	now := metav1.Now()
	pod.Status.Phase = core.PodSucceeded
	pod.Status.ContainerStatuses = []core.ContainerStatus{{Name: "app", ContainerID: "containerd://original-retired", State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: "containerd://original-retired", StartedAt: now, FinishedAt: now}}}}
	digest, err := hostguard.TerminalPodDigest(pod)
	require.NoError(t, err)
	retired := hostguard.Retirement{ProtocolVersion: 1, Binding: b, Origin: snapshot.State().Origin, BeforeSequence: snapshot.State().Sequence, Placement: placement, TerminalSHA256: digest}
	raw, err := json.Marshal(retired)
	require.NoError(t, err)
	pod.Annotations = map[string]string{hostguard.RetirementAnnotation: string(raw)}
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[pod.Namespace+"/"+pod.Name] = pod.DeepCopy() })
	snapshot, err = store.Retire(ctx, snapshot, retired)
	require.NoError(t, err)
	pod.Finalizers = nil
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[pod.Namespace+"/"+pod.Name] = pod.DeepCopy() })
	require.NoError(t, hostguard.InspectRetiredPod(snapshot.Node(), pod))
	config := hostscheduler.Config{Nodes: []hostscheduler.NodeConfig{{Binding: b, Profile: profile, SystemBytes: 1 << 30, InitializeAtResourceVersion: "1"}}}
	service, err := hostscheduler.New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, config, profile.SchedulerName)
	require.NoError(t, err)
	// 期望值也经同一typed API JSON回读，避免fixture内monotonic时钟与
	// HTTP去除monotonic后的时间值作结构相等比较；生产返回/完整断言不变。
	canonical, err := f.Core.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	return &PluginState{host: service}, f, canonical
}

func TestHostReserveRecognizesConfirmedRetiredOrdinaryWithoutZeroAllocation(t *testing.T) {
	s, f, p := hostRetiredEventFixture(t)
	ctx := context.Background()
	writes := [3]int{}
	f.Mutate(func(f *dsgatest.SchedulerAPI) { writes = [3]int{f.NodeWrites, f.PodWrites, f.VMWrites} })
	canonical, allocation, err := s.reserveHost(ctx, p, p.Spec.NodeName)
	require.NoError(t, err, "旧reserveHost实际签名须处理已退休初始事件")
	require.Equal(t, p, canonical)
	require.Nil(t, allocation, "不可返回伪零Placement让updatePod applyHostAllocation(0)")
	view, err := localstate.PodStateFromK8sObj(canonical)
	require.NoError(t, err)
	require.Positive(t, uint64(view.Mem.Reserved), "原normal requests本地视图保守保留，不伪造零额度")
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		require.Equal(t, writes, [3]int{f.NodeWrites, f.PodWrites, f.VMWrites})
	})
}

func TestHostReserveRetiredCallerRejectsUnknownOrForeignProof(t *testing.T) {
	for _, scenario := range []string{"no_proof", "unknown_termination", "VM_owner", "migration_owner"} {
		t.Run(scenario, func(t *testing.T) {
			s, f, p := hostRetiredEventFixture(t)
			switch scenario {
			case "no_proof":
				delete(p.Annotations, hostguard.RetirementAnnotation)
			case "unknown_termination":
				p.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.Time{}
			case "VM_owner":
				p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: "vm", UID: "vm-UID"}}
			case "migration_owner":
				p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachineMigration", Name: "migration", UID: "migration-UID"}}
			}
			writes := [3]int{}
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				f.Pods[p.Namespace+"/"+p.Name] = p.DeepCopy()
				writes = [3]int{f.NodeWrites, f.PodWrites, f.VMWrites}
			})
			canonical, allocation, err := s.reserveHost(context.Background(), p, p.Spec.NodeName)
			require.Error(t, err)
			require.Nil(t, canonical)
			require.Nil(t, allocation)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Equal(t, writes, [3]int{f.NodeWrites, f.PodWrites, f.VMWrites})
			})
		})
	}
}
