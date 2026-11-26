package controllers

import (
	"context"
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"github.com/neondatabase/autoscaling/pkg/dsga/cpureader"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// dsgaMemory只读取原Runner发布的私有QMP事实，不发送原生QMP写，不按spec.Use
// 推导实际完成。状态展示也核真实Pod/Node；Agent做预算结算时仍须原UID回执、
// 当前floor/无pending和PG/Root/Host来源，不得拿VM.Status单独发缩容许可。
func (r *VMReconciler) dsgaMemory(ctx context.Context, vm *vmv1.VirtualMachine) (*resource.Quantity, bool, error) {
	memory, pending, _, _, err := r.dsgaMemoryObservation(ctx, vm)
	return memory, pending, err
}

// dsgaMemoryObservation额外返回同一次直接读取且已核原出生的Pod，供CPU状态消费。
// 不重新使用cache里的Pod/IP作为后续HTTP目标，也不以CPU成功替代内存发布来源。
func (r *VMReconciler) dsgaMemoryObservation(ctx context.Context, vm *vmv1.VirtualMachine) (*resource.Quantity, bool, *core.Pod, *vmguard.Identity, error) {
	if r.APIReader == nil || vm.Spec.DSGABootstrap == nil {
		return nil, false, nil, nil, fmt.Errorf("DSGA memory requires direct APIReader and original bootstrap")
	}
	binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation}
	current := new(vmv1.VirtualMachine)
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(vm), current); err != nil {
		return nil, false, nil, nil, err
	}
	snapshot, err := journal.Inspect(current, binding)
	if err != nil {
		return nil, false, nil, nil, err
	}
	cm := new(core.ConfigMap)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: vmguard.BindingName(binding)}, cm); err != nil {
		return nil, false, nil, nil, err
	}
	birth, ref, err := vmguard.InspectBirth(cm, binding, nil)
	if err != nil {
		return nil, false, nil, nil, err
	}
	exitCM := new(core.ConfigMap)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: vmguard.ExitClaimName(binding, birth)}, exitCM); err == nil {
		if _, err := vmguard.InspectProcessExit(exitCM, binding, birth, ref); err != nil {
			return nil, false, nil, nil, err
		}
		return nil, false, nil, nil, vmguard.ErrStopped
	} else if !apierrors.IsNotFound(err) {
		return nil, false, nil, nil, err
	}
	stoppedCM := new(core.ConfigMap)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: vmguard.StoppedPodClaimName(binding, birth)}, stoppedCM); err == nil {
		if _, err := vmguard.InspectStoppedPodSource(stoppedCM, binding, birth, ref); err != nil {
			return nil, false, nil, nil, err
		}
		return nil, false, nil, nil, vmguard.ErrStopped
	} else if !apierrors.IsNotFound(err) {
		return nil, false, nil, nil, err
	}
	stateCM := new(core.ConfigMap)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: birth.StateClaim.Name}, stateCM); err != nil {
		return nil, false, nil, nil, err
	}
	p, err := vmguard.InspectPublication(stateCM, binding, birth, ref)
	if err != nil {
		return nil, false, nil, nil, err
	}
	pod := new(core.Pod)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: current.Status.PodName}, pod); err != nil {
		return nil, false, nil, nil, err
	}
	owner := metav1.GetControllerOf(pod)
	if string(pod.UID) != birth.Identity.RunnerPodUID || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Status.Phase != core.PodRunning || pod.Spec.NodeName == "" || current.Status.Node != "" && current.Status.Node != pod.Spec.NodeName || current.Status.PodIP != "" && current.Status.PodIP != pod.Status.PodIP || owner == nil || owner.UID != vm.UID || owner.Name != vm.Name || owner.Kind != "VirtualMachine" || owner.APIVersion != vmv1.SchemeGroupVersion.String() || current.Status.RestartCount != 0 {
		return nil, false, nil, nil, fmt.Errorf("original DSGA Runner Pod changed")
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "neonvm-runner" && status.RestartCount != 0 {
			return nil, false, nil, nil, fmt.Errorf("original DSGA Runner container restarted")
		}
	}
	node := new(core.Node)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName, Namespace: ""}, node); err != nil {
		return nil, false, nil, nil, err
	}
	if string(node.UID) != birth.Identity.NodeUID || node.DeletionTimestamp != nil || p.State.Floor != snapshot.State().Owner {
		return nil, false, nil, nil, fmt.Errorf("DSGA Runner Node or durable owner floor differs")
	}
	return resource.NewQuantity(int64(p.Actual.ActualBytes), resource.BinarySI), p.State.Pending != nil, pod, &birth.Identity, nil
}

const typeDSGACPUReady = "DSGACPUReady"
const typeDSGACPUEntitlementReady = "DSGACPUEntitlementReady"

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
	memory, pending, pod, original, err := r.dsgaMemoryObservation(ctx, vm)
	if err != nil {
		return err
	}
	if vm.Status.PodIP != pod.Status.PodIP || vm.Status.PodName != pod.Name || vm.Status.Node != pod.Spec.NodeName {
		return fmt.Errorf("CPU endpoint differs from original DSGA Runner")
	}
	previousCPU := meta.FindStatusCondition(vm.Status.Conditions, typeDSGACPUReady)
	historicalContinuity := vm.Status.Phase == vmv1.VmRunning && previousCPU != nil &&
		previousCPU.ObservedGeneration == vm.Generation &&
		(previousCPU.Status == metav1.ConditionTrue || previousCPU.Reason == "HistoricalTopologyUnobserved")
	r.updateVMStatusMemory(ctx, vm, memory)
	vm.Status.Phase = vmv1.VmScaling
	cpuDone := false
	strict := vm.Spec.DSGABootstrap.RuntimeContractVersion == vmv1.RuntimeContractFixedCPU
	var cpu *api.VCPUCgroup
	if strict {
		reader, readerErr := cpureader.NewForAPIReader(r.APIReader)
		if readerErr != nil {
			err = readerErr
		} else {
			observed, readErr := reader.Observe(ctx, journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID,
				Incarnation: vm.Spec.DSGABootstrap.Incarnation}, vm.Spec.DSGABootstrap.ComputeInstanceID)
			if observed != nil && (readErr == nil || errors.Is(readErr, cpureader.ErrPending)) {
				current, currentErr := observed.Millicores()
				if currentErr != nil {
					err = currentErr
				} else {
					cpu = &api.VCPUCgroup{VCPUs: vmv1.MilliCPU(current), Managed: observed}
					err = nil
				}
			} else {
				err = readErr
			}
		}
	} else {
		cpu, err = getRunnerCPULimits(ctx, vm)
	}
	expected := originalRunnerCPUIdentity(vm, string(pod.UID), original.RunnerBootID, original.NodeUID)
	if err == nil && strict {
		if cpu.Managed.Identity != expected || cpu.Managed.Generation > vm.Generation {
			err = fmt.Errorf("CPU observation differs from original Runner Birth")
		} else if verifyErr := r.verifyCPUNodeCommitment(ctx, vm, pod, expected, cpu.Managed.NodeCommitment); verifyErr != nil {
			err = verifyErr
		} else {
			r.updateVMStatusCPU(ctx, vm, pod, cpu.Managed.OnlineVCPUs, cpu)
			cpuDone = cpu.VCPUs == vm.Spec.Guest.CPUs.Use && !cpu.Managed.Pending &&
				cpu.Managed.Generation == vm.Generation &&
				cpu.Managed.OnlineVCPUs == vm.Spec.Guest.CPUs.Use.RoundedUp()
			if !cpuDone {
				target, generation := vm.Spec.Guest.CPUs.Use, vm.Generation
				if cpu.Managed.Pending {
					// 原未知动作必须先逐字完成，下一轮才能采纳当前新generation。
					target, generation = vmv1.MilliCPU(cpu.Managed.TargetMillicores), cpu.Managed.Generation
				}
				if generation <= 0 {
					err = fmt.Errorf("original initial CPU birth remains pending before its generation is confirmed")
				} else {
					err = sendRunnerCPUChange(ctx, vm, api.VCPUChange{VCPUs: target,
						Managed: &runnercpu.Command{ProtocolVersion: runnercpu.ProtocolVersion, Identity: expected, Generation: generation}})
				}
			}
		}
	} else if err == nil {
		if cpu.HistoricalGuest == nil || cpu.HistoricalGuest.Identity != expected {
			err = fmt.Errorf("historical Guest CPU observation differs from original Runner Birth")
		} else {
			r.updateVMStatusCPU(ctx, vm, pod, cpu.HistoricalGuest.OnlineVCPUs, cpu)
			cpuDone = cpu.HistoricalGuest.OnlineVCPUs == vm.Spec.Guest.CPUs.Use.RoundedUp()
		}
	}
	condition := metav1.Condition{Type: typeDSGACPUReady, Status: metav1.ConditionFalse, ObservedGeneration: vm.Generation,
		Reason: "Reconciling", Message: "CPU state is not yet confirmed"}
	entitlement := metav1.Condition{Type: typeDSGACPUEntitlementReady, Status: metav1.ConditionFalse, ObservedGeneration: vm.Generation,
		Reason: "Reconciling", Message: "Original CPU quota and fixed Pod commitment are not confirmed"}
	if !strict {
		entitlement.Reason, entitlement.Message = "HistoricalContract", "Historical Guest topology does not qualify fractional CPU entitlement or new scheduling commitments"
	}
	if err != nil {
		log.FromContext(ctx).Error(err, "DSGA CPU state remains unconfirmed; retaining verified memory observation")
		condition.Reason = "CPURequestFailed"
		if !strict && errors.Is(err, errHistoricalCPUUnobserved) {
			condition.Reason = "HistoricalTopologyUnobserved"
			condition.Message = "Original historical service continuity retained; actual Guest topology and new CPU entitlement are not observed"
			// 只保留同一已确认generation的既有Running；新增目标/原pending仍未完成。
			if historicalContinuity && !pending {
				vm.Status.Phase = vmv1.VmRunning
			}
		}
		meta.SetStatusCondition(&vm.Status.Conditions, condition)
		meta.SetStatusCondition(&vm.Status.Conditions, entitlement)
		return nil
	}
	if cpuDone {
		condition.Status, condition.Reason, condition.Message = metav1.ConditionTrue, "Confirmed", "CPU state is confirmed"
		if strict {
			entitlement.Status, entitlement.Reason, entitlement.Message = metav1.ConditionTrue, "Confirmed", "Original CPU cap, fixed Pod commitment and Guest integer topology are confirmed"
		} else {
			condition.Reason, condition.Message = "HistoricalGuestTopologyConfirmed", "Original Guest integer topology is confirmed; fractional entitlement is unqualified"
		}
	}
	meta.SetStatusCondition(&vm.Status.Conditions, condition)
	meta.SetStatusCondition(&vm.Status.Conditions, entitlement)
	if cpuDone && !pending {
		vm.Status.Phase = vmv1.VmRunning
	}
	return nil
}
