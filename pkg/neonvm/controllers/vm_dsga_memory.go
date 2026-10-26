package controllers

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// dsgaMemory只读取原Runner发布的私有QMP事实，不发送原生QMP写，不按spec.Use
// 推导实际完成。状态展示也核真实Pod/Node；Agent做预算结算时仍须原UID回执、
// 当前floor/无pending和PG/Root/Host来源，不得拿VM.Status单独发缩容许可。
func (r *VMReconciler) dsgaMemory(ctx context.Context, vm *vmv1.VirtualMachine) (*resource.Quantity, bool, error) {
	if r.APIReader == nil || vm.Spec.DSGABootstrap == nil {
		return nil, false, fmt.Errorf("DSGA memory requires direct APIReader and original bootstrap")
	}
	binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation}
	current := new(vmv1.VirtualMachine)
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(vm), current); err != nil {
		return nil, false, err
	}
	snapshot, err := journal.Inspect(current, binding)
	if err != nil {
		return nil, false, err
	}
	cm := new(core.ConfigMap)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: vmguard.BindingName(binding)}, cm); err != nil {
		return nil, false, err
	}
	birth, ref, err := vmguard.InspectBirth(cm, binding, nil)
	if err != nil {
		return nil, false, err
	}
	stateCM := new(core.ConfigMap)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: birth.StateClaim.Name}, stateCM); err != nil {
		return nil, false, err
	}
	p, err := vmguard.InspectPublication(stateCM, binding, birth, ref)
	if err != nil {
		return nil, false, err
	}
	pod := new(core.Pod)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: current.Status.PodName}, pod); err != nil {
		return nil, false, err
	}
	owner := metav1.GetControllerOf(pod)
	if string(pod.UID) != birth.Identity.RunnerPodUID || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Status.Phase != core.PodRunning || pod.Spec.NodeName == "" || current.Status.Node != "" && current.Status.Node != pod.Spec.NodeName || current.Status.PodIP != "" && current.Status.PodIP != pod.Status.PodIP || owner == nil || owner.UID != vm.UID || owner.Name != vm.Name || owner.Kind != "VirtualMachine" || owner.APIVersion != vmv1.SchemeGroupVersion.String() || current.Status.RestartCount != 0 {
		return nil, false, fmt.Errorf("original DSGA Runner Pod changed")
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "neonvm-runner" && status.RestartCount != 0 {
			return nil, false, fmt.Errorf("original DSGA Runner container restarted")
		}
	}
	node := new(core.Node)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node); err != nil {
		return nil, false, err
	}
	if string(node.UID) != birth.Identity.NodeUID || node.DeletionTimestamp != nil || p.State.Floor != snapshot.State().Owner {
		return nil, false, fmt.Errorf("DSGA Runner Node or durable owner floor differs")
	}
	return resource.NewQuantity(int64(p.Actual.ActualBytes), resource.BinarySI), p.State.Pending != nil, nil
}

// managedScaling接管旧VmScaling分支：内存只消费最终Runner事实，CPU保留受支持
// 的sysfs路径。QMP CPU模式在受管出生前拒绝，不能先产生旧副作用再报内存失败。
func (r *VMReconciler) managedScaling(ctx context.Context, vm *vmv1.VirtualMachine) error {
	if vm.Spec.CpuScalingMode == nil || *vm.Spec.CpuScalingMode != vmv1.CpuScalingModeSysfs {
		return fmt.Errorf("DSGA Runner requires sysfs CPU mode")
	}
	memory, pending, err := r.dsgaMemory(ctx, vm)
	if err != nil {
		return err
	}
	pod := new(core.Pod)
	if err := r.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: vm.Status.PodName}, pod); err != nil {
		return err
	}
	cpuDone, err := r.handleCPUScaling(ctx, vm, pod)
	if err != nil {
		return err
	}
	r.updateVMStatusMemory(ctx, vm, memory)
	if cpuDone && !pending {
		vm.Status.Phase = vmv1.VmRunning
	}
	return nil
}
