package hostscheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/neondatabase/autoscaling/pkg/api"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/util/patch"
)

// ApproveLegacy只服务同域旧VM，不接管DSGA原操作。先从实际VM核本次来源和
// 请求，再持久保留目标上界，最后以该原VM UID/RV批准。VM PATCH冲突/未知时
// 已保留的Node高水位仍存在；不能撤销容量后再让迟到批准生效。此方法不持
// plugin mutex，且不将Host开销混入公开的Guest批准数值。
func (s *Service) ApproveLegacy(ctx context.Context, expected *core.Pod, resources api.Resources) error {
	if expected == nil {
		return fmt.Errorf("original Pod is required for legacy approval")
	}
	d, ok := s.domains[expected.Spec.NodeName]
	if !ok || resources.Mem == 0 || resources.VCPU == 0 {
		return fmt.Errorf("legacy approval requires original Host domain and positive resources")
	}
	canonical, _, vm, err := s.allocation(ctx, d.config, expected, control.Uint64(resources.Mem))
	if err != nil {
		return err
	}
	if vm == nil || vm.Spec.DSGABootstrap != nil || expected.Annotations[observedVMVersion] != vm.ResourceVersion {
		return fmt.Errorf("legacy approval VM source changed or is managed")
	}
	requested, err := api.ExtractRequestedScaling(vm)
	if err != nil || requested == nil || resources.Mem > requested.Mem || resources.VCPU > requested.VCPU || resources.VCPU < vm.Spec.Guest.CPUs.Min || resources.VCPU > vm.Spec.Guest.CPUs.Max {
		return fmt.Errorf("legacy approval exceeds original VM request or CPU bounds")
	}
	slot, _ := vm.Spec.Guest.MemorySlotSize.AsInt64()
	if uint64(resources.Mem)%uint64(slot) != 0 || uint64(resources.Mem)/uint64(slot) < uint64(vm.Spec.Guest.MemorySlots.Min) {
		return fmt.Errorf("legacy approval violates original memory geometry")
	}
	if _, _, err := s.Reserve(ctx, d.config.Binding.NodeName, canonical, control.Uint64(resources.Mem)); err != nil {
		return err
	}
	annotations := maps.Clone(vm.Annotations)
	if annotations == nil {
		annotations = make(map[string]string)
	}
	raw, err := json.Marshal(resources)
	if err != nil {
		return err
	}
	annotations[api.InternalAnnotationResourcesApproved] = string(raw)
	ops := []patch.Operation{
		{Op: patch.OpTest, Path: "/metadata/uid", Value: vm.UID},
		{Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: vm.ResourceVersion},
		{Op: patch.OpAdd, Path: "/metadata/annotations", Value: annotations},
	}
	payload, err := json.Marshal(ops)
	if err != nil {
		return err
	}
	got, err := s.vm.NeonvmV1().VirtualMachines(vm.Namespace).Patch(ctx, vm.Name, types.JSONPatchType, payload, metav1.PatchOptions{})
	if err != nil {
		return err
	}
	if got == nil || got.UID != vm.UID || got.ResourceVersion == "" || got.Annotations[api.InternalAnnotationResourcesApproved] != string(raw) {
		return fmt.Errorf("legacy VM approval response is unconfirmed; Host hold retained")
	}
	return nil
}
