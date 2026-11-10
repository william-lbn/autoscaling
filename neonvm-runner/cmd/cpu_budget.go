package main

import (
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"sync/atomic"
)

// commitCPUBudget never acknowledges a logical limit whose kernel update failed.
// Callers serialize changes; reapplying a quota is safe after a lost response.
// The existing fourfold QEMU overhead factor remains in setCgroupLimit.
func commitCPUBudget(requested vmv1.MilliCPU, current *atomic.Uint32, apply func(vmv1.MilliCPU) error) error {
	if err := apply(requested); err != nil {
		return err
	}
	current.Store(uint32(requested))
	return nil
}
