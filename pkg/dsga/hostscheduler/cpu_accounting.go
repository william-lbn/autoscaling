package hostscheduler

import (
	"context"
	"fmt"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"math"
)

func CPUPlacementCommitment(n NodeConfig, pod *core.Pod, vm *vmv1.VirtualMachine) (control.Uint64, error) {
	if n.Profile.CPUAccountingVersion == 0 {
		return 0, nil
	}
	if n.Profile.CPUAccountingVersion != hostguard.CPUAccountingVersion {
		return 0, fmt.Errorf("unsupported CPU accounting profile")
	}
	requested, err := hostguard.PodCPURequest(pod)
	if err != nil {
		return 0, err
	}
	if vm == nil {
		return requested, nil
	}
	maximum := control.Uint64(vm.Spec.Guest.CPUs.Max)
	if maximum == 0 {
		return 0, fmt.Errorf("original VM maximum CPU is unknown")
	}
	if vm.Spec.DSGABootstrap != nil && vm.Spec.DSGABootstrap.RuntimeContractVersion == vmv1.RuntimeContractFixedCPU {
		frozen, committed, err := vm.Spec.DSGARuntimeCPUBudget()
		if err != nil || control.Uint64(frozen) != maximum {
			return 0, fmt.Errorf("original strict CPU contract is incomplete")
		}
		found := false
		for _, container := range pod.Spec.Containers {
			if container.Name != "neonvm-runner" {
				continue
			}
			found = true
			request, reqErr := vmv1.ExactRuntimeMillicores(*container.Resources.Requests.Cpu())
			limit, limitErr := vmv1.ExactRuntimeMillicores(*container.Resources.Limits.Cpu())
			if reqErr != nil || limitErr != nil || request != committed || limit != committed {
				return 0, fmt.Errorf("original actual Runner CPU resources differ from frozen contract")
			}
		}
		if !found {
			return 0, fmt.Errorf("original Runner CPU container is missing")
		}
		requested = max(requested, control.Uint64(committed))
	}
	result := max(maximum, requested)
	if result > control.Uint64(math.MaxUint32) {
		return 0, fmt.Errorf("Host CPU commitment exceeds supported planner integer")
	}
	return result, nil
}

// 原Owner显式升级只恢复CPU轴；任一旧Pod/VM来源未知均不改旧账，不补零。
func (s *Service) upgradeCPUAccounting(ctx context.Context, d domain, snapshot *hostguard.Snapshot) (*hostguard.Snapshot, error) {
	state := snapshot.State()
	if d.config.Profile.CPUAccountingVersion == 0 || state.ProtocolVersion == 11 {
		return snapshot, nil
	}
	if d.config.Profile.CPUAccountingVersion != hostguard.CPUAccountingVersion ||
		d.config.CPUAccountingUpgradeMode != CPUAccountingUpgradeSnapshot || state.ProtocolVersion != 10 ||
		state.Profile == nil || state.Placements == nil {
		return nil, fmt.Errorf("CPU domain requires explicit complete original v10 placement upgrade")
	}
	oldProfile := d.config.Profile
	oldProfile.CPUAccountingVersion, oldProfile.SystemCPUMillicores = 0, 0
	if oldProfile != *state.Profile || state.OutsideBytes != d.config.SystemBytes || state.Binding != d.config.Binding {
		return nil, fmt.Errorf("CPU upgrade cannot replace original memory domain")
	}
	if d.config.KubernetesBudget == nil {
		return nil, fmt.Errorf("CPU upgrade lacks original Kubernetes budget")
	}
	if err := d.config.KubernetesBudget.Inspect(snapshot.Node(), d.config.Profile, d.config.SystemBytes); err != nil {
		return nil, err
	}
	// 先按原来源抬唯一节点Owner floor，仍不改任何旧持有或给出CPU资格。
	fenced, err := d.store.Fence(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	state = fenced.State()
	rows := make([]hostguard.Placement, 0, len(*state.Placements))
	for _, old := range *state.Placements {
		pod, err := s.core.Pods(old.Namespace).Get(ctx, old.PodName, metav1.GetOptions{})
		if err != nil || string(pod.UID) != old.PodUID {
			return nil, fmt.Errorf("CPU upgrade original Pod source unavailable")
		}
		canonical, derived, vm, err := s.allocation(ctx, d.config, pod, 0)
		if err != nil {
			return nil, err
		}
		// VM/Pod跨网络重读不得换UID或RV；旧memory/Settlement/Barrier仍由Store原样保留。
		latestPod, err := s.core.Pods(old.Namespace).Get(ctx, old.PodName, metav1.GetOptions{})
		if err != nil || latestPod.UID != canonical.UID || latestPod.ResourceVersion != canonical.ResourceVersion {
			return nil, fmt.Errorf("CPU upgrade original Pod changed during source verification")
		}
		if vm != nil {
			latestVM, err := s.vm.NeonvmV1().VirtualMachines(vm.Namespace).Get(ctx, vm.Name, metav1.GetOptions{})
			if err != nil || latestVM.UID != vm.UID || latestVM.ResourceVersion != vm.ResourceVersion {
				return nil, fmt.Errorf("CPU upgrade original VM changed during source verification")
			}
		}
		rows = append(rows, derived)
	}
	return d.store.UpgradeCPUAccounting(ctx, fenced, d.config.Profile, rows)
}
