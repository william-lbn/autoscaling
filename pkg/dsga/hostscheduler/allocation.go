package hostscheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberesource "k8s.io/kubernetes/pkg/api/v1/resource"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

const observedVMVersion = "internal.autoscaling.neon.tech/dsga-observed-vm-rv"

// allocation重新GET原Pod及其原VM。传播到Pod的资源/批准注解只是缓存，不能
// 用于容量授予；这里只在返回的内存副本中重建它们，不PATCH Pod。Pod UID固定，
// VM owner必须是唯一controller，迁移target不得按普通Pod的微小requests入账。
func (s *Service) allocation(ctx context.Context, n NodeConfig, expected *core.Pod, target control.Uint64) (*core.Pod, hostguard.Placement, *vmv1.VirtualMachine, error) {
	var none hostguard.Placement
	if expected == nil || expected.UID == "" || expected.ResourceVersion == "" {
		return nil, none, nil, fmt.Errorf("original Pod UID/resourceVersion is required")
	}
	pod, err := s.core.Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return nil, none, nil, err
	}
	if pod.UID != expected.UID || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Status.Phase == core.PodSucceeded || pod.Status.Phase == core.PodFailed || pod.Spec.NodeName != "" && pod.Spec.NodeName != n.Binding.NodeName || pod.Spec.SchedulerName != n.Profile.SchedulerName {
		return nil, none, nil, fmt.Errorf("original Pod identity/Node/scheduler changed")
	}
	if _, _, migrating := vmv1.MigrationOwnerForPod(pod); migrating {
		return nil, none, nil, fmt.Errorf("migration requires an independent complete Host reservation protocol")
	}
	p := hostguard.Placement{Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), VMName: "", VMUID: "", Incarnation: "", Managed: false, MemoryBytes: 0, HostOverheadBytes: n.Profile.PodOverheadBytes, Settled: nil}
	owner, isVM := vmv1.VirtualMachineOwnerForPod(pod)
	if !isVM {
		if target != 0 {
			return nil, none, nil, fmt.Errorf("ordinary Pod cannot request VM memory approval")
		}
		// 完整硬limit须覆盖普通容器、init/原生sidecar、ephemeral；官方
		// PodLimits处理init峰值与Pod overhead。只用requests不能约束实际内存。
		if len(pod.Spec.Containers) == 0 || len(pod.Spec.EphemeralContainers) != 0 {
			return nil, none, nil, fmt.Errorf("pod has no bounded supported container set")
		}
		for _, containers := range [][]core.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
			for _, c := range containers {
				v, exact := c.Resources.Limits.Memory().AsInt64()
				if !exact || v <= 0 {
					return nil, none, nil, fmt.Errorf("all containers require exact positive hard memory limits")
				}
			}
		}
		limits := kuberesource.PodLimits(pod, kuberesource.PodResourcesOptions{Reuse: nil, InPlacePodVerticalScalingEnabled: false, ExcludeOverhead: false, ContainerFn: nil, NonMissingContainerRequests: nil})
		v, exact := limits.Memory().AsInt64()
		if !exact || v <= 0 {
			return nil, none, nil, fmt.Errorf("complete Pod memory limit is invalid or overflows")
		}
		p.MemoryBytes = control.Uint64(v)
		return pod.DeepCopy(), p, nil, nil
	}
	controller := metav1.GetControllerOf(pod)
	// GetControllerOf返回深拷贝，其中*bool地址不同；比较API身份与值，
	// 不能比较OwnerReference结构中的指针地址而拒绝合法原VM。
	if controller == nil || controller.APIVersion != owner.APIVersion || controller.Kind != owner.Kind || controller.Name != owner.Name || controller.UID != owner.UID || owner.UID == "" || len(pod.OwnerReferences) != 1 {
		return nil, none, nil, fmt.Errorf("runner does not have its unique original VM controller")
	}
	vm, err := s.vm.NeonvmV1().VirtualMachines(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return nil, none, nil, err
	}
	if vm.UID != owner.UID || vm.Name != owner.Name || vm.Namespace != pod.Namespace || vm.ResourceVersion == "" || vm.DeletionTimestamp != nil || vm.Status.Node != "" && vm.Status.Node != n.Binding.NodeName {
		return nil, none, nil, fmt.Errorf("original VM UID/lifecycle changed")
	}
	held, err := vm.DSGABootstrapHeld()
	if err != nil || held {
		return nil, none, nil, fmt.Errorf("original VM bootstrap is held or malformed")
	}
	slot, exact := vm.Spec.Guest.MemorySlotSize.AsInt64()
	slots := vm.Spec.Guest.MemorySlots
	if !exact || slot <= 0 || slots.Min == 0 || slots.Use < slots.Min || slots.Max < slots.Use || uint64(slots.Max) > uint64(math.MaxInt64)/uint64(slot) {
		return nil, none, nil, fmt.Errorf("original VM memory geometry is invalid")
	}
	p.VMName, p.VMUID, p.HostOverheadBytes = vm.Name, string(vm.UID), n.Profile.VMOverheadBytes
	p.MemoryBytes = control.Uint64(uint64(slot) * uint64(slots.Use))
	currentManaged := false
	if vm.Spec.DSGABootstrap != nil {
		p.Managed, p.Incarnation = true, vm.Spec.DSGABootstrap.Incarnation
		if target != 0 {
			return nil, none, nil, fmt.Errorf("managed VM never uses legacy resource approval")
		}
		binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: p.Incarnation}
		snapshot, err := journal.Inspect(vm, binding)
		if err != nil {
			return nil, none, nil, err
		}
		if snapshot.State().BudgetHeld != nil {
			closed, err := hostguard.SettlementFor(snapshot.State())
			if err != nil {
				return nil, none, nil, err
			}
			_, _, publication, err := vmguard.ReadPublished(ctx, s.core, binding, &closed.Runner.BirthClaim)
			if err != nil {
				return nil, none, nil, err
			}
			if publication.Identity.RunnerPodUID != string(pod.UID) || publication.Identity.NodeUID != string(n.Binding.NodeUID) {
				return nil, none, nil, fmt.Errorf("managed allocation original Runner/Node changed")
			}
			// DSGA动作不改写旧spec.Use/传播批准。永久预算存在后必须取
			// 原Runner实际值与当前预算，不能在结清后一轮又用旧Use重抬
			// 峰值。仍有pending时只保留最大值，绝不据此释放容量。
			p.MemoryBytes = max(publication.Actual.RequestedBytes, publication.Actual.ActualBytes, closed.Request.TargetVMMemoryBytes)
			currentManaged = true
		}
	}
	if !currentManaged && vm.Status.MemorySize != nil {
		actual, exact := vm.Status.MemorySize.AsInt64()
		if !exact || actual <= 0 {
			return nil, none, nil, fmt.Errorf("original VM actual memory is invalid")
		}
		p.MemoryBytes = max(p.MemoryBytes, control.Uint64(actual))
	}
	approved, err := api.ExtractApprovedScaling(vm)
	if err != nil {
		return nil, none, nil, err
	}
	if !currentManaged && approved != nil {
		p.MemoryBytes = max(p.MemoryBytes, control.Uint64(approved.Mem))
	}
	p.MemoryBytes = max(p.MemoryBytes, target)
	if p.MemoryBytes > control.Uint64(uint64(slot)*uint64(slots.Max)) {
		return nil, none, nil, fmt.Errorf("VM allocation exceeds original maximum memory")
	}
	copyPod := pod.DeepCopy()
	if copyPod.Annotations == nil {
		copyPod.Annotations = make(map[string]string)
	}
	if copyPod.Labels == nil {
		copyPod.Labels = make(map[string]string)
	}
	for _, k := range []string{api.AnnotationAutoscalingUnit, api.InternalAnnotationResourcesRequested, api.InternalAnnotationResourcesApproved} {
		delete(copyPod.Annotations, k)
		if v, ok := vm.Annotations[k]; ok {
			copyPod.Annotations[k] = v
		}
	}
	for _, k := range []string{api.LabelEnableAutoscaling, api.LabelEnableAutoMigration, api.LabelTestingOnlyAlwaysMigrate} {
		copyPod.Labels[k] = vm.Labels[k]
		if p.Managed {
			// DSGA只有新协议可授予/迁移，不得再走旧批准注解旁路。
			copyPod.Labels[k] = "false"
		}
	}
	raw, err := json.Marshal(vm.Spec.Resources())
	if err != nil {
		return nil, none, nil, err
	}
	copyPod.Annotations[vmv1.VirtualMachineResourcesAnnotation] = string(raw)
	copyPod.Annotations[observedVMVersion] = vm.ResourceVersion
	return copyPod, p, vm, nil
}
