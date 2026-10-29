package controllers

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// dsgaMemory只读取原Runner发布的私有QMP事实，不发送原生QMP写，不按spec.Use
// 推导实际完成。状态展示也核真实Pod/Node；Agent做预算结算时仍须原UID回执、
// 当前floor/无pending和PG/Root/Host来源，不得拿VM.Status单独发缩容许可。
func (r *VMReconciler) dsgaMemory(ctx context.Context, vm *vmv1.VirtualMachine) (*resource.Quantity, bool, error) {
	memory, pending, _, err := r.dsgaMemoryObservation(ctx, vm)
	return memory, pending, err
}

// dsgaMemoryObservation额外返回同一次直接读取且已核原出生的Pod，供CPU状态消费。
// 不重新使用cache里的Pod/IP作为后续HTTP目标，也不以CPU成功替代内存发布来源。
func (r *VMReconciler) dsgaMemoryObservation(ctx context.Context, vm *vmv1.VirtualMachine) (*resource.Quantity, bool, *core.Pod, error) {
	if r.APIReader == nil || vm.Spec.DSGABootstrap == nil {
		return nil, false, nil, fmt.Errorf("DSGA memory requires direct APIReader and original bootstrap")
	}
	binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation}
	current := new(vmv1.VirtualMachine)
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(vm), current); err != nil {
		return nil, false, nil, err
	}
	snapshot, err := journal.Inspect(current, binding)
	if err != nil {
		return nil, false, nil, err
	}
	cm := new(core.ConfigMap)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: vmguard.BindingName(binding)}, cm); err != nil {
		return nil, false, nil, err
	}
	birth, ref, err := vmguard.InspectBirth(cm, binding, nil)
	if err != nil {
		return nil, false, nil, err
	}
	exitCM := new(core.ConfigMap)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: vmguard.ExitClaimName(binding, birth)}, exitCM); err == nil {
		if _, err := vmguard.InspectProcessExit(exitCM, binding, birth, ref); err != nil {
			return nil, false, nil, err
		}
		return nil, false, nil, vmguard.ErrStopped
	} else if !apierrors.IsNotFound(err) {
		return nil, false, nil, err
	}
	stoppedCM := new(core.ConfigMap)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: vmguard.StoppedPodClaimName(binding, birth)}, stoppedCM); err == nil {
		if _, err := vmguard.InspectStoppedPodSource(stoppedCM, binding, birth, ref); err != nil {
			return nil, false, nil, err
		}
		return nil, false, nil, vmguard.ErrStopped
	} else if !apierrors.IsNotFound(err) {
		return nil, false, nil, err
	}
	stateCM := new(core.ConfigMap)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: birth.StateClaim.Name}, stateCM); err != nil {
		return nil, false, nil, err
	}
	p, err := vmguard.InspectPublication(stateCM, binding, birth, ref)
	if err != nil {
		return nil, false, nil, err
	}
	pod := new(core.Pod)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: current.Status.PodName}, pod); err != nil {
		return nil, false, nil, err
	}
	owner := metav1.GetControllerOf(pod)
	if string(pod.UID) != birth.Identity.RunnerPodUID || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Status.Phase != core.PodRunning || pod.Spec.NodeName == "" || current.Status.Node != "" && current.Status.Node != pod.Spec.NodeName || current.Status.PodIP != "" && current.Status.PodIP != pod.Status.PodIP || owner == nil || owner.UID != vm.UID || owner.Name != vm.Name || owner.Kind != "VirtualMachine" || owner.APIVersion != vmv1.SchemeGroupVersion.String() || current.Status.RestartCount != 0 {
		return nil, false, nil, fmt.Errorf("original DSGA Runner Pod changed")
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "neonvm-runner" && status.RestartCount != 0 {
			return nil, false, nil, fmt.Errorf("original DSGA Runner container restarted")
		}
	}
	node := new(core.Node)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName, Namespace: ""}, node); err != nil {
		return nil, false, nil, err
	}
	if string(node.UID) != birth.Identity.NodeUID || node.DeletionTimestamp != nil || p.State.Floor != snapshot.State().Owner {
		return nil, false, nil, fmt.Errorf("DSGA Runner Node or durable owner floor differs")
	}
	return resource.NewQuantity(int64(p.Actual.ActualBytes), resource.BinarySI), p.State.Pending != nil, pod, nil
}

const typeDSGACPUReady = "DSGACPUReady"

// managedScaling同时处理Running和Scaling的受管状态。内存只消费原Runner发布，
// CPU仅进入既有sysfs通道；两域没有共同事务，CPU HTTP失败不能阻止已核内存事实
// 落盘，否则Agent会永远等旧VM.Status。只有内存来源有效时才隔离CPU错误，来源
// 未知/身份变化仍整体失败，不把旧值或spec目标写成当前内存。
//
// CPU错误通过专属Condition及日志保留，VM继续Scaling并由正常1秒周期重查；
// 返回nil仅允许Reconcile提交已确认的状态，不表示CPU动作或整个伸缩成功。
// CPU仍未确认或Runner有pending时不得进入Running，也不结清预算/Host/原WAL。
func (r *VMReconciler) managedScaling(ctx context.Context, vm *vmv1.VirtualMachine) error {
	if vm.Spec.CpuScalingMode == nil || *vm.Spec.CpuScalingMode != vmv1.CpuScalingModeSysfs {
		return fmt.Errorf("DSGA Runner requires sysfs CPU mode")
	}
	memory, pending, pod, err := r.dsgaMemoryObservation(ctx, vm)
	if err != nil {
		return err
	}
	// HTTP地址只能是本次原出生校验的同一Pod。输入VM若已被并发替换地址，
	// 先拒绝而不是向cache/旧IP发CPU请求；后续资源版本CAS仍由Reconcile执行。
	if vm.Status.PodIP != pod.Status.PodIP || vm.Status.PodName != pod.Name || vm.Status.Node != pod.Spec.NodeName {
		return fmt.Errorf("CPU endpoint differs from original DSGA Runner")
	}
	r.updateVMStatusMemory(ctx, vm, memory)
	vm.Status.Phase = vmv1.VmScaling
	// CPU也必须按读回确认：POST接受目标不等于下一次GET已确认。先展示
	// 当前实际CPU，差异时只请求目标并保持Scaling，下一轮读回匹配才Ready。
	// 该分支不调用任何公开QMP、内存setter或修改内存账本。
	cpu, err := getRunnerCPULimits(ctx, vm)
	cpuDone := false
	if err == nil {
		r.updateVMStatusCPU(ctx, vm, pod, cpu.VCPUs.RoundedUp(), cpu)
		cpuDone = cpu.VCPUs == vm.Spec.Guest.CPUs.Use
		if !cpuDone {
			err = setRunnerCPULimits(ctx, vm, vm.Spec.Guest.CPUs.Use)
		}
	}
	condition := metav1.Condition{Type: typeDSGACPUReady, Status: metav1.ConditionFalse, ObservedGeneration: vm.Generation, Reason: "Reconciling", Message: "CPU state is not yet confirmed"}
	if err != nil {
		log.FromContext(ctx).Error(err, "DSGA CPU state remains unconfirmed; retaining verified memory observation")
		condition.Reason = "CPURequestFailed"
		meta.SetStatusCondition(&vm.Status.Conditions, condition)
		return nil
	}
	if cpuDone {
		condition.Status, condition.Reason, condition.Message = metav1.ConditionTrue, "Confirmed", "CPU state is confirmed"
	}
	meta.SetStatusCondition(&vm.Status.Conditions, condition)
	if cpuDone && !pending {
		vm.Status.Phase = vmv1.VmRunning
	}
	return nil
}
