package hostguard

import (
	"context"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"math"
)

// UpgradeCPUAccounting只允许原Node owner在完整重读每个旧Placement后做CPU轴CAS。
// 原Origin/holds/Claim/Memory/Settlement/Barrier逐字保留；0不能代表丢失的来源。
// VM最大CPU仍由调用它的原Service用可信VM客户端复核，本Store不认证请求自报VM。
func (s *Store) UpgradeCPUAccounting(ctx context.Context, snapshot *Snapshot, profile Profile, placements []Placement) (*Snapshot, error) {
	if snapshot == nil || snapshot.model.Binding != s.binding || snapshot.model.ProtocolVersion != 10 ||
		snapshot.model.Profile == nil || snapshot.model.Profile.CPUAccountingVersion != 0 ||
		profile.CPUAccountingVersion != CPUAccountingVersion || profile.Validate() != nil ||
		placements == nil || snapshot.model.Placements == nil || len(placements) != len(*snapshot.model.Placements) {
		return nil, fmt.Errorf("CPU upgrade requires explicit original v10 complete placement snapshot")
	}
	oldProfile := profile
	oldProfile.CPUAccountingVersion, oldProfile.SystemCPUMillicores = 0, 0
	if oldProfile != *snapshot.model.Profile {
		return nil, fmt.Errorf("CPU upgrade cannot change the original memory/profile domain")
	}
	actor, err := s.authorize(ctx, snapshot.node)
	if err != nil || actor != snapshot.model.LeaseFloor {
		return nil, fmt.Errorf("CPU upgrade requires original current fenced Node owner")
	}
	next := snapshot.model.Clone()
	for i, old := range *next.Placements {
		var candidate *Placement
		for j := range placements {
			if placements[j].PodUID == old.PodUID {
				if candidate != nil {
					return nil, fmt.Errorf("duplicate CPU upgrade source")
				}
				candidate = &placements[j]
			}
		}
		if candidate == nil || candidate.CPUAccountingVersion != CPUAccountingVersion {
			return nil, fmt.Errorf("CPU upgrade is missing an original placement source")
		}
		compare := *candidate
		compare.CPUAccountingVersion, compare.CommittedCPUMillicores = 0, 0
		if !old.sameOriginal(compare) {
			return nil, fmt.Errorf("CPU upgrade changed original Pod/VM/memory floor binding")
		}
		pod, err := s.core.Pods(old.Namespace).Get(ctx, old.PodName, metav1.GetOptions{})
		if err != nil || string(pod.UID) != old.PodUID || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil ||
			pod.Spec.NodeName != "" && pod.Spec.NodeName != s.binding.NodeName {
			return nil, fmt.Errorf("CPU upgrade original Pod source is missing, replaced or unknown")
		}
		if old.VMUID != "" {
			owner := metav1.GetControllerOf(pod)
			if len(pod.OwnerReferences) != 1 || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" ||
				owner.Name != old.VMName || string(owner.UID) != old.VMUID {
				return nil, fmt.Errorf("CPU upgrade original VM-Pod owner changed")
			}
		}
		request, err := PodCPURequest(pod)
		if err != nil || candidate.CommittedCPUMillicores < request ||
			old.VMUID != "" && candidate.CommittedCPUMillicores == 0 {
			return nil, fmt.Errorf("CPU upgrade cannot cover original full Pod CPU request")
		}
		(*next.Placements)[i].CPUAccountingVersion = CPUAccountingVersion
		(*next.Placements)[i].CommittedCPUMillicores = candidate.CommittedCPUMillicores
	}
	if next.Sequence == control.Uint64(math.MaxUint64) {
		return nil, fmt.Errorf("original Node sequence exhausted")
	}
	next.Profile = &profile
	next.ProtocolVersion = 11
	next.Sequence++
	if err = next.Validate(); err != nil {
		return nil, err
	}
	if err = next.CheckCPUCapacity(snapshot.node); err != nil {
		return nil, err
	}
	return s.write(ctx, snapshot.node, next, false)
}
