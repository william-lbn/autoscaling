package controllers

import (
	"context"
	"encoding/json"
	"fmt"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// 消费Scheduler独立的未绑定锚点，不将它当QEMU退出或删除Host占账的许可。
// 直接重读全部原Node及birth/state的真实404，未知/残留来源禁止收养关闭。
func (r *VMReconciler) inspectUnboundClosure(ctx context.Context, vm *vmv1.VirtualMachine, pod *core.Pod) error {
	var closed hostguard.UnboundRetirement
	raw := vm.Annotations[hostguard.UnboundRetirementAnnotation]
	actual, err := r.readOriginalUnbornVM(ctx, vm)
	if err != nil {
		return err
	}
	if actual.Annotations[hostguard.UnboundRetirementAnnotation] != raw {
		return fmt.Errorf("original never-bound anchor changed on direct reread")
	}
	vm = actual
	if json.Unmarshal([]byte(raw), &closed) != nil || closed.Validate(true) != nil || closed.InspectVM(vm) != nil || vm.Annotations[hostguard.VMRetirementAnnotation] != "" {
		return fmt.Errorf("original never-bound VM closure changed")
	}
	if pod != nil && (closed.InspectPod(pod) != nil || pod.Annotations[hostguard.UnboundRetirementAnnotation] != raw) {
		return fmt.Errorf("original never-bound Pod closure changed")
	}
	for _, proof := range closed.Confirmed {
		node := new(core.Node)
		if err := r.APIReader.Get(ctx, client.ObjectKey{Name: proof.Binding.NodeName}, node); err != nil {
			return err
		}
		if err := hostguard.InspectUnboundNode(node, closed, proof); err != nil {
			return err
		}
	}

	return r.inspectRunnerSourcesAbsent(ctx, vm)
}

// Pod可能已经GC，关闭锚点也可能来自旧缓存；恢复读者必须重新核对当前原VM
// 日志的冻结摘要及真正缺失的birth/state，不能把新来源当可收集的旧垃圾。
func (r *VMReconciler) inspectUnpublishedClosure(ctx context.Context, vm *vmv1.VirtualMachine, closed hostguard.Retirement) error {
	actual, err := r.readOriginalUnbornVM(ctx, vm)
	if err != nil {
		return err
	}
	raw, marshalErr := json.Marshal(closed)
	if marshalErr != nil {
		return marshalErr
	}
	if actual.Annotations[hostguard.VMRetirementAnnotation] != string(raw) || actual.Annotations[hostguard.UnboundRetirementAnnotation] != "" {
		return fmt.Errorf("original pre-birth closing anchor changed on direct reread")
	}
	if !closed.UnpublishedBootOnly() || closed.Validate() != nil || closed.Placement.Namespace != actual.Namespace || closed.Placement.VMName != actual.Name || closed.Placement.VMUID != string(actual.UID) || closed.Placement.Incarnation != actual.Spec.DSGABootstrap.Incarnation || vmguard.SourceDigest([]byte(actual.Annotations[control.JournalAnnotation])) != closed.UnpublishedJournalDigest() {
		return fmt.Errorf("original pre-birth VM journal or identity changed on direct reread")
	}
	if closed.ProtocolVersion == 5 {
		return r.inspectRegisteredRunnerCollection(ctx, actual, closed)
	}
	return r.inspectRunnerSourcesAbsent(ctx, actual)
}

func (r *VMReconciler) inspectRegisteredRunnerCollection(ctx context.Context, vm *vmv1.VirtualMachine, closed hostguard.Retirement) error {
	binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation}
	names := vmguard.BirthSourceNames(binding)
	state := closed.UnpublishedBoot
	if closed.RunnerBirth.Name != names[0] || state.State.Name != names[1] {
		return fmt.Errorf("closed Runner collection source names differ")
	}
	birth := new(core.ConfigMap)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: names[0]}, birth); err == nil {
		finalizer, err := memorySourceFinalizer(birth, vm)
		var original vmguard.Birth
		if err != nil || finalizer != vmguard.ProtectionFinalizer || string(birth.UID) != closed.RunnerBirth.UID ||
			birth.Labels[vmguard.ClaimLabel] != "binding-v1" || birth.Immutable == nil || !*birth.Immutable ||
			json.Unmarshal([]byte(birth.Data[vmguard.ClaimKey]), &original) != nil ||
			original.Identity.VMUID != string(vm.UID) || original.Identity.Incarnation != binding.Incarnation ||
			original.StateClaim != state.State || original.Identity.RunnerPodUID != closed.Placement.PodUID ||
			original.Identity.NodeUID != string(closed.Binding.NodeUID) {
			return fmt.Errorf("closed Runner birth differs during collection")
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	current := new(core.ConfigMap)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: names[1]}, current); err == nil {
		finalizer, err := memorySourceFinalizer(current, vm)
		if err != nil || finalizer != vmguard.ProtectionFinalizer || string(current.UID) != state.State.UID ||
			current.Labels[vmguard.ClaimLabel] != "state-v1" ||
			vmguard.SourceDigest([]byte(current.Data[vmguard.ClaimKey])) != state.StateSHA256 {
			return fmt.Errorf("closed Runner empty state differs during collection")
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *VMReconciler) readOriginalUnbornVM(ctx context.Context, expected *vmv1.VirtualMachine) (*vmv1.VirtualMachine, error) {
	if expected == nil || expected.UID == "" || expected.Spec.DSGABootstrap == nil {
		return nil, fmt.Errorf("unborn closure lacks original VM")
	}
	actual := new(vmv1.VirtualMachine)
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), actual); err != nil {
		return nil, err
	}
	if actual.UID != expected.UID || actual.ResourceVersion == "" || actual.DeletionTimestamp == nil || actual.Spec.DSGABootstrap == nil || actual.Spec.DSGABootstrap.Incarnation != expected.Spec.DSGABootstrap.Incarnation {
		return nil, fmt.Errorf("original unborn VM changed on direct reread")
	}
	return actual, nil
}

func (r *VMReconciler) inspectRunnerSourcesAbsent(ctx context.Context, vm *vmv1.VirtualMachine) error {
	binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation}
	for _, name := range vmguard.BirthSourceNames(binding) {
		err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: name}, new(core.ConfigMap))
		if !apierrors.IsNotFound(err) {
			if err != nil {
				return err
			}
			return fmt.Errorf("unborn VM retains an original Runner source")
		}
	}
	return nil
}
