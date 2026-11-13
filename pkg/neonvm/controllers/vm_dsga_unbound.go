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
func (r *VMReconciler) inspectPreBirthClosure(ctx context.Context, vm *vmv1.VirtualMachine, closed hostguard.Retirement) error {
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
	if closed.ProtocolVersion != 3 || closed.Validate() != nil || closed.PreBirth == nil || closed.Placement.Namespace != actual.Namespace || closed.Placement.VMName != actual.Name || closed.Placement.VMUID != string(actual.UID) || closed.Placement.Incarnation != actual.Spec.DSGABootstrap.Incarnation || vmguard.SourceDigest([]byte(actual.Annotations[control.JournalAnnotation])) != closed.PreBirth.VMJournalSHA256 {
		return fmt.Errorf("original pre-birth VM journal or identity changed on direct reread")
	}
	return r.inspectRunnerSourcesAbsent(ctx, actual)
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
