package state

import (
	"fmt"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func (p Pod) hostCPU() PodResources[vmv1.MilliCPU] {
	switch p.HostCPUAccountingVersion {
	case 0:
		return p.CPU
	case 1:
		return PodResources[vmv1.MilliCPU]{Reserved: p.HostCommittedCPU, Requested: p.HostCommittedCPU,
			Factor: 0, Overcommit: resource.NewQuantity(1, resource.DecimalSI)}
	default:
		panic(fmt.Sprintf("unknown internal Host CPU view version %d", p.HostCPUAccountingVersion))
	}
}
