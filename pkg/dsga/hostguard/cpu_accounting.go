package hostguard

import (
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	kuberesource "k8s.io/kubernetes/pkg/api/v1/resource"
	"math"
)

const CPUAccountingVersion uint32 = 1

func (p Profile) validateCPU() error {
	switch p.CPUAccountingVersion {
	case 0:
		if p.SystemCPUMillicores != 0 {
			return fmt.Errorf("historical Host profile cannot acquire CPU accounting fields")
		}
	case CPUAccountingVersion:
		if p.SystemCPUMillicores == 0 {
			return fmt.Errorf("explicit CPU profile requires its fixed in-allocatable system commitment")
		}
	default:
		return fmt.Errorf("unsupported Host CPU accounting version")
	}
	return nil
}

func (m Model) validateCPUAccounting() error {
	enabled := m.Profile != nil && m.Profile.CPUAccountingVersion == CPUAccountingVersion
	if enabled && m.ProtocolVersion < 11 || m.ProtocolVersion == 11 && !enabled {
		return fmt.Errorf("fixed CPU accounting requires explicit Host model v11")
	}
	if m.Placements != nil {
		for _, p := range *m.Placements {
			if enabled {
				if p.CPUAccountingVersion != CPUAccountingVersion || p.VMUID != "" && p.CommittedCPUMillicores == 0 {
					return fmt.Errorf("CPU-accounted original placement is missing its complete fixed commitment")
				}
			} else if p.CPUAccountingVersion != 0 || p.CommittedCPUMillicores != 0 {
				return fmt.Errorf("historical Host placement cannot silently acquire CPU accounting")
			}
		}
	}
	_, err := m.UsedCPUMillicores()
	return err
}

// CPU只按完整原Pod行持有。PG/内存Resize的多个Hold不会重复叠加CPU，shrink不释放。
func (m Model) UsedCPUMillicores() (control.Uint64, error) {
	if m.Profile == nil || m.Profile.CPUAccountingVersion == 0 {
		return 0, nil
	}
	if m.Profile.CPUAccountingVersion != CPUAccountingVersion || m.Placements == nil {
		return 0, fmt.Errorf("complete original CPU accounting model required")
	}
	used := m.Profile.SystemCPUMillicores
	for _, p := range *m.Placements {
		if p.CPUAccountingVersion != CPUAccountingVersion || p.CommittedCPUMillicores > control.Uint64(math.MaxUint64)-used {
			return 0, fmt.Errorf("Host CPU commitment sum is unknown or overflows")
		}
		used += p.CommittedCPUMillicores
	}
	return used, nil
}

// 精确millicore资源，Node/Pod允许高于单Runner的64CPU但不允许溢出或亚millicore。
func exactCPUMillicores(q resource.Quantity) (control.Uint64, error) {
	n := q.MilliValue()
	if n < 0 || q.Cmp(*resource.NewMilliQuantity(n, resource.DecimalSI)) != 0 {
		return 0, fmt.Errorf("CPU resource is negative, inexact or overflows")
	}
	return control.Uint64(n), nil
}

func PodCPURequest(pod *core.Pod) (control.Uint64, error) {
	if pod == nil || len(pod.Spec.Containers) == 0 || len(pod.Spec.EphemeralContainers) != 0 {
		return 0, fmt.Errorf("complete original Pod CPU request source required")
	}
	// 使用锁定Kubernetes的完整init/native sidecar/Pod overhead聚合，不手加普通containers。
	requests := kuberesource.PodRequests(pod, kuberesource.PodResourcesOptions{Reuse: nil, InPlacePodVerticalScalingEnabled: false,
		ExcludeOverhead: false, ContainerFn: nil, NonMissingContainerRequests: nil})
	return exactCPUMillicores(*requests.Cpu())
}

func nodeCPUCapacity(node *core.Node) (control.Uint64, control.Uint64, error) {
	if node == nil || node.UID == "" || node.ResourceVersion == "" || node.DeletionTimestamp != nil {
		return 0, 0, fmt.Errorf("original Node CPU capacity source unavailable")
	}
	capacity, err := exactCPUMillicores(*node.Status.Capacity.Cpu())
	if err != nil {
		return 0, 0, err
	}
	allocatable, err := exactCPUMillicores(*node.Status.Allocatable.Cpu())
	if err != nil || capacity == 0 || allocatable == 0 || allocatable > capacity {
		return 0, 0, fmt.Errorf("original Node has no exact positive allocatable CPU")
	}
	return capacity, allocatable, nil
}

// CheckCPUCapacity只试算本次同Node RV的固定CPU轴；不是新的Owner或分配账。
func (m Model) CheckCPUCapacity(node *core.Node) error {
	if m.Profile == nil || m.Profile.CPUAccountingVersion == 0 {
		return nil
	}
	if m.Profile.CPUAccountingVersion != CPUAccountingVersion || node == nil ||
		node.UID != m.Binding.NodeUID || node.Name != m.Binding.NodeName {
		return fmt.Errorf("original Host CPU domain changed")
	}
	_, available, err := nodeCPUCapacity(node)
	if err != nil {
		return err
	}
	used, err := m.UsedCPUMillicores()
	if err != nil || used > available {
		return fmt.Errorf("complete original Node CPU commitment exceeds allocatable")
	}
	return nil
}

// InspectCPUCommitment只有本次原Node读取中的完整v11行才可给出事实；不依赖PG已初始化。
func InspectCPUCommitment(node *core.Node, vm journal.Binding, podName, podUID string, maximum, committed uint32) (runnercpu.NodeCommitment, error) {
	var none runnercpu.NodeCommitment
	if node == nil || maximum == 0 || committed < maximum {
		return none, fmt.Errorf("original Node-Pod CPU maximum required")
	}
	var model Model
	if err := json.Unmarshal([]byte(node.Annotations[LedgerAnnotation]), &model); err != nil {
		return none, err
	}
	snapshot, err := inspect(node, model.Binding)
	if err != nil || model.ProtocolVersion != 11 || model.Profile == nil || model.Profile.CPUAccountingVersion != CPUAccountingVersion {
		return none, fmt.Errorf("original protected v11 CPU domain is not qualified")
	}
	if err = model.CheckCPUCapacity(node); err != nil {
		return none, err
	}
	row, err := snapshot.model.Placement(podUID)
	if err != nil || !row.Managed || row.CPUAccountingVersion != CPUAccountingVersion || row.Namespace != vm.Namespace ||
		row.PodName != podName || row.VMName != vm.Name || row.VMUID != string(vm.VMUID) || row.Incarnation != vm.Incarnation ||
		row.CommittedCPUMillicores < control.Uint64(committed) {
		return none, fmt.Errorf("original Node does not retain the complete same-Pod CPU commitment")
	}
	fact := runnercpu.NodeCommitment{ProtocolVersion: 1, Scope: runnercpu.NodeCommitmentScope,
		LedgerOrigin: model.Origin, LedgerSequence: uint64(model.Sequence), NodeResourceVersion: node.ResourceVersion,
		CommittedMillicores: uint64(row.CommittedCPUMillicores)}
	return fact, fact.Validate()
}
