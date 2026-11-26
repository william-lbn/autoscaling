package v1

import (
	"fmt"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// RuntimeContractFixedCPU 显式冻结整个 Runner/QEMU/控制/I/O 的 Pod CPU 承诺。
// 0 是原历史合同；不存在“运行新镜像就在线升级”的隐式迁移。
const RuntimeContractFixedCPU uint32 = 1
const RuntimeMaximumMillicores int64 = 64000

// ExactRuntimeMillicores 拒绝溢出、分数millicore、零及超出本发行固定拓扑的资源量。
func ExactRuntimeMillicores(q resource.Quantity) (uint32, error) {
	milli := q.MilliValue()
	if milli <= 0 || milli > RuntimeMaximumMillicores || q.Cmp(*resource.NewMilliQuantity(milli, resource.DecimalSI)) != 0 {
		return 0, fmt.Errorf("Runtime CPU quantity is outside exact supported millicores")
	}
	return uint32(milli), nil
}

// DSGARuntimeCPUBudget 返回原冻结最大合法权益和固定Pod承诺；它不访问内核或授予CPU。
func (s *VirtualMachineSpec) DSGARuntimeCPUBudget() (uint32, uint32, error) {
	if s == nil || s.DSGABootstrap == nil || s.DSGABootstrap.RuntimeContractVersion != RuntimeContractFixedCPU {
		return 0, 0, fmt.Errorf("explicit fixed CPU Runtime contract is not selected")
	}
	maximum := uint32(s.Guest.CPUs.Max)
	if maximum < 10 || s.Guest.CPUs.Min < 10 || int64(maximum) > RuntimeMaximumMillicores || s.Guest.CPUs.Use < 10 ||
		s.Guest.CPUs.Use > s.Guest.CPUs.Max || s.Guest.CPUs.Min > s.Guest.CPUs.Use {
		return 0, 0, fmt.Errorf("Runtime CPU target is outside the frozen declared range")
	}
	request, err := ExactRuntimeMillicores(s.PodResources.Requests[core.ResourceCPU])
	if err != nil {
		return 0, 0, err
	}
	limit, err := ExactRuntimeMillicores(s.PodResources.Limits[core.ResourceCPU])
	if err != nil || request != limit || limit < maximum {
		return 0, 0, fmt.Errorf("Runtime CPU requires equal Pod request/limit covering the full declared maximum")
	}
	return maximum, request, nil
}

func (s *VirtualMachineSpec) validateDSGARuntimeContract() error {
	if s == nil || s.DSGABootstrap == nil || s.DSGABootstrap.RuntimeContractVersion == 0 {
		return nil
	}
	if s.DSGABootstrap.RuntimeContractVersion != RuntimeContractFixedCPU {
		return fmt.Errorf("unsupported explicit DSGA Runtime contract version")
	}
	_, _, err := s.DSGARuntimeCPUBudget()
	return err
}
