package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/typed/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/util/patch"
)

// 身份取自启动Runner的watch事件，不取自可重用名称的最新GET。这里保护Kubernetes
// 资源写入，尚不构成DSGA持久issuer所有权、内存预算或已发QMP的最终执行栅栏。
type vmPatchIdentity struct {
	UID       types.UID
	Namespace string
	Name      string
	PodName   string
	PodIP     string
	SlotBytes api.Bytes
}

func patchVMResources(ctx context.Context, client vmclient.VirtualMachineInterface, identity vmPatchIdentity,
	current, target api.Resources, revision vmv1.RevisionWithTime,
) error {
	if identity.UID == "" || identity.Namespace == "" || identity.Name == "" || identity.PodName == "" || identity.PodIP == "" || identity.SlotBytes == 0 {
		return fmt.Errorf("VM resource request lacks fixed execution identity")
	}
	vm, err := client.Get(ctx, identity.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read VM before conditional resource update: %w", err)
	}
	payload, unchanged, err := resourcePatch(vm, identity, current, target, revision)
	if err != nil || unchanged {
		return err
	}
	updated, err := client.Patch(ctx, identity.Name, types.JSONPatchType, payload, metav1.PatchOptions{})
	if err != nil {
		// 超时/传输/服务器错误均不能证明没写入。调用方下一次先GET对账；禁止在本函数
		// 内无条件重试。若已写入同revision/目标，resourcePatch会原样确认而不再次写。
		return fmt.Errorf("conditional VM update is unconfirmed; reconcile before retry: %w", err)
	}
	if updated == nil || updated.UID != identity.UID || updated.Name != identity.Name || updated.Namespace != identity.Namespace ||
		updated.Status.PodName != identity.PodName || updated.Status.PodIP != identity.PodIP ||
		updated.Spec.Guest.CPUs.Use != target.VCPU || updated.Spec.Guest.MemorySlotSize.Value() != int64(identity.SlotBytes) ||
		int64(updated.Spec.Guest.MemorySlots.Use) != int64(target.Mem/identity.SlotBytes) ||
		updated.Spec.TargetRevision == nil || updated.Spec.TargetRevision.Revision != revision.Revision {
		return fmt.Errorf("VM update response differs from fixed identity or requested resources; result unconfirmed")
	}
	return nil
}

func resourcePatch(vm *vmv1.VirtualMachine, identity vmPatchIdentity, currentResources, target api.Resources,
	revision vmv1.RevisionWithTime,
) ([]byte, bool, error) {
	if vm == nil || vm.UID != identity.UID || vm.Name != identity.Name || vm.Namespace != identity.Namespace || vm.ResourceVersion == "" ||
		vm.DeletionTimestamp != nil || vm.Status.PodName != identity.PodName || vm.Status.PodIP != identity.PodIP ||
		(vm.Status.Phase != vmv1.VmRunning && vm.Status.Phase != vmv1.VmScaling) {
		return nil, false, fmt.Errorf("VM or runner identity/lifecycle changed; refusing stale resource update")
	}
	guest := &vm.Spec.Guest
	if identity.SlotBytes == 0 || uint64(identity.SlotBytes) > math.MaxInt64 ||
		guest.MemorySlotSize.Value() != int64(identity.SlotBytes) || target.Mem%identity.SlotBytes != 0 {
		return nil, false, fmt.Errorf("VM memory target is not an exact current slot multiple")
	}
	slots := uint64(target.Mem / identity.SlotBytes)
	if guest.MemorySlots.Min <= 0 || guest.MemorySlots.Max < guest.MemorySlots.Min ||
		guest.MemorySlots.Use < guest.MemorySlots.Min || guest.MemorySlots.Use > guest.MemorySlots.Max ||
		slots > math.MaxInt32 || slots < uint64(guest.MemorySlots.Min) || slots > uint64(guest.MemorySlots.Max) ||
		uint64(guest.MemorySlots.Use) > math.MaxInt64/uint64(identity.SlotBytes) || slots > math.MaxInt64/uint64(identity.SlotBytes) ||
		guest.CPUs.Min == 0 || guest.CPUs.Max < guest.CPUs.Min ||
		target.VCPU < guest.CPUs.Min || target.VCPU > guest.CPUs.Max {
		return nil, false, fmt.Errorf("VM target exceeds current resource bounds")
	}
	if revision.Value < 0 {
		return nil, false, fmt.Errorf("negative VM revision")
	}
	if current := vm.Spec.TargetRevision; current != nil {
		if revision.Value < current.Value || (revision.Value == current.Value && revision.Flags != current.Flags) {
			return nil, false, fmt.Errorf("VM target revision is stale or conflicts")
		}
		if revision.Revision == current.Revision && guest.CPUs.Use == target.VCPU && uint64(guest.MemorySlots.Use) == slots {
			// 以实际GET结果确认同revision/目标的写入，避免响应丢失后的重发；时间仅为
			// 既有传播时延观测，不改已持久记录。不把spec确认当作QMP/Guest已经完成。
			return nil, true, nil
		}
	}
	actual := api.Resources{VCPU: guest.CPUs.Use, Mem: api.Bytes(guest.MemorySlots.Use) * identity.SlotBytes}
	if actual != currentResources && actual != target {
		return nil, false, fmt.Errorf("VM resources changed since the action was planned; refusing stale intent")
	}
	// 官方Revision是传播进度，同一次目标的部分批准可在同revision下继续变更资源；
	// 不能把Revision直接当不可变operation ID。陈旧动作以原资源快照检查，再由RV CAS
	// 覆盖GET之后的竞态。真正DSGA的epoch/operation ID/预算需独立持久协议。
	// Revision=0是官方未启用Revision的旧模式，保留其行为但仍做UID/RV条件更新；
	// 它没有单调执行权，后续DSGA模式绝不能用0或此兼容路径绕过epoch/预算证明。
	operations := []patch.Operation{
		{Op: patch.OpTest, Path: "/metadata/uid", Value: identity.UID},
		{Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: vm.ResourceVersion},
		{Op: patch.OpReplace, Path: "/spec/guest/cpus/use", Value: target.VCPU.ToResourceQuantity()},
		{Op: patch.OpReplace, Path: "/spec/guest/memorySlots/use", Value: int32(slots)},
		// 可选targetRevision在旧VM中可能缺失；RFC6902 add对存在/缺失的对象字段均可用。
		{Op: patch.OpAdd, Path: "/spec/targetRevision", Value: revision},
	}
	payload, err := json.Marshal(operations)
	return payload, false, err
}
