package plugin

import (
	"context"
	"fmt"
	"math"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/plugin/state"
	"github.com/neondatabase/autoscaling/pkg/util"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	kuberesource "k8s.io/kubernetes/pkg/api/v1/resource"
)

func closedManagedEvent(pod *core.Pod) bool {
	if pod == nil || pod.Spec.NodeName == "" || pod.DeletionTimestamp == nil && pod.Status.Phase != core.PodSucceeded && pod.Status.Phase != core.PodFailed {
		return false
	}
	_, vm := vmv1.VirtualMachineOwnerForPod(pod)
	return vm
}

// observeHostEvent与可执行Reserve显式分离。关闭受管Pod只恢复原成本视图，
// Framework的Filter/Reserve和旧ApproveLegacy继续使用原拒绝关闭对象的路径。
func (s *PluginState) observeHostEvent(ctx context.Context, pod *core.Pod) (*core.Pod, *hostguard.Placement, error) {
	if s.host != nil && s.host.HasNode(pod.Spec.NodeName) && closedManagedEvent(pod) {
		canonical, held, err := s.host.ObserveRetainedManaged(ctx, pod.Spec.NodeName, pod)
		if err != nil {
			return nil, nil, err
		}
		return canonical, &held, nil
	}
	return s.reserveHost(ctx, pod, pod.Spec.NodeName)
}

// retainedManagedPodState不消费可能过期/损坏的VM资源传播注解，也不冒充
// ordinary计费。内存固定为已验证原Placement完整Bytes（含Pod硬限floor），
// CPU保守使用Pod有效requests/limits较大值；两者只供本地视图，不签执行许可。
func retainedManagedPodState(pod *core.Pod, held hostguard.Placement) (state.Pod, error) {
	var none state.Pod
	if !closedManagedEvent(pod) || !held.Managed || held.PodUID != string(pod.UID) || held.Namespace != pod.Namespace || held.PodName != pod.Name || held.PodMemoryLimitBytes == 0 {
		return none, fmt.Errorf("retained managed planning requires original confirmed event")
	}
	charged, err := held.Bytes()
	if err != nil {
		return none, err
	}
	options := kuberesource.PodResourcesOptions{Reuse: nil, InPlacePodVerticalScalingEnabled: false, ExcludeOverhead: false, ContainerFn: nil, NonMissingContainerRequests: nil}
	requests := kuberesource.PodRequests(pod, options)
	limits := kuberesource.PodLimits(pod, options)
	cpu := max(requests.Cpu().MilliValue(), limits.Cpu().MilliValue())
	if cpu < 0 || cpu > math.MaxUint32 {
		return none, fmt.Errorf("retained managed Pod CPU accounting exceeds supported range")
	}
	return state.Pod{
		NamespacedName: util.GetNamespacedName(pod), UID: pod.UID, CreatedAt: pod.CreationTimestamp.Time,
		VirtualMachine: util.NamespacedName{Namespace: held.Namespace, Name: held.VMName},
		Migratable:     false, AlwaysMigrate: false, Migrating: false,
		CPU: state.PodResources[vmv1.MilliCPU]{Reserved: vmv1.MilliCPU(cpu), Requested: vmv1.MilliCPU(cpu), Factor: 0, Overcommit: resource.NewQuantity(1, resource.DecimalSI)},
		Mem: state.PodResources[api.Bytes]{Reserved: api.Bytes(charged), Requested: api.Bytes(charged), Factor: 0, Overcommit: resource.NewQuantity(1, resource.DecimalSI)},
	}, nil
}
