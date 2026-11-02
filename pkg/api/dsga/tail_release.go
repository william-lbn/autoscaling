package dsga

import (
	"fmt"
	"math"
)

// ValidateReleasedTail只核DsgRegionReleaseTail回执的结构与完整尾部方向。
// 请求是logical maximum，实际尾端可以包含本region独占的最后半页padding；
// 起点向上取整会保留活跃边界页。Root仍必须用原Page/Element/Maximum/Mapped
// 精确核完整页，不能把此结构校验当来源认证或normal/VM许可。
func (p ProviderFact) ValidateReleasedTail() error {
	if !p.HasResult || p.SystemErrno != 0 || p.AllocationMayBePartial || p.ReleaseMayBePartial ||
		uint64(p.RequestedOffset) > math.MaxUint64-uint64(p.RequestedBytes) || uint64(p.AffectedOffset) > math.MaxUint64-uint64(p.AffectedBytes) {
		return fmt.Errorf("tail release is partial, failed or overflows")
	}
	switch p.Status {
	case ProviderNoop:
		// C的已知尾部fast path全0；低层ReleaseTail在同一最后页内还可以保留
		// 非零logical request，但实际完整页必须为空，由Root原几何再核。
		if p.PhysicalReleaseVerified || p.AffectedOffset != 0 || p.AffectedBytes != 0 || p.RequestedBytes == 0 && p.RequestedOffset != 0 {
			return fmt.Errorf("tail noop contains physical effects")
		}
		return nil
	case ProviderOK:
		if !p.PhysicalReleaseVerified || p.RequestedBytes == 0 || p.AffectedBytes == 0 || p.AffectedOffset < p.RequestedOffset || p.AffectedOffset+p.AffectedBytes < p.RequestedOffset+p.RequestedBytes {
			return fmt.Errorf("tail release does not cover original logical maximum")
		}
		return nil
	default:
		return fmt.Errorf("tail release is not verified OK or noop")
	}
}
