package plugin

import (
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostscheduler"
	"github.com/neondatabase/autoscaling/pkg/plugin/state"
	"math"
)

func applyHostCPUPreview(node *state.Node, candidate state.Pod, preview *hostscheduler.PlacementPreview) bool {
	if preview == nil || preview.Allocation.CPUAccountingVersion == 0 {
		return true
	}
	if preview.Allocation.CPUAccountingVersion != hostguard.CPUAccountingVersion ||
		preview.AllocatableCPUMillicores > control.Uint64(math.MaxUint32) {
		return false
	}
	held := make(map[string]hostguard.Placement, len(preview.Held))
	for _, row := range preview.Held {
		if row.CPUAccountingVersion != hostguard.CPUAccountingVersion || row.CommittedCPUMillicores > control.Uint64(math.MaxUint32) {
			return false
		}
		held[row.PodUID] = row
	}
	for uid, pod := range node.Pods() {
		row, ok := held[string(uid)]
		if !ok {
			return false
		} // cache中的未记账来源不可据此授新容量，等原Host对账。
		copy := pod
		copy.HostCPUAccountingVersion = hostguard.CPUAccountingVersion
		copy.HostCommittedCPU = vmv1.MilliCPU(row.CommittedCPUMillicores)
		node.UpdatePod(pod, copy)
		delete(held, string(uid))
	}
	// 所有未出现在本地proposal的旧持有仍扣在可用预算内；候选本行仅在AddPod算一次。
	usedOutside := preview.SystemCPUMillicores
	for uid, row := range held {
		if uid == string(candidate.UID) {
			continue
		}
		if row.CommittedCPUMillicores > control.Uint64(math.MaxUint64)-usedOutside {
			return false
		}
		usedOutside += row.CommittedCPUMillicores
	}
	if usedOutside > preview.AllocatableCPUMillicores {
		return false
	}
	available := vmv1.MilliCPU(preview.AllocatableCPUMillicores - usedOutside)
	node.CPU.Total = min(node.CPU.Total, available)
	node.CPU.Watermark = min(node.CPU.Watermark, node.CPU.Total)
	return true
}
