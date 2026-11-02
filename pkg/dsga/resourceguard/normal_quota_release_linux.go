//go:build linux

package resourceguard

import (
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"math"
)

// 只有caller已经绑定同原PG/Key/4FD后才可消费；本函数只核登记原几何的精确
// 整段尾回收，不重开路径、不猜RSS，不认证一个从JSON单独构造的array。
func normalQuotaReleasedArrayUpper(a control.ArrayFact, span arrayRegistryWireRange, target uint64) (uint64, error) {
	fail := func() (uint64, error) {
		return 0, fmt.Errorf("normal quota array tail differs from original page geometry")
	}
	page := span.Page
	if a.AllocationUnknown || a.ReleaseState != control.ReleaseVerified || page == 0 || page&(page-1) != 0 || span.Element == 0 || target == 0 || target > math.MaxUint64/span.Element || span.Maximum == 0 || span.Maximum > math.MaxUint64-(page-1) || span.Mapped != (span.Maximum+page-1)&^(page-1) {
		return fail()
	}
	offset := target * span.Element
	if offset > span.Maximum || offset > math.MaxUint64-(page-1) {
		return fail()
	}
	upper := (offset + page - 1) &^ (page - 1)
	if uint64(a.CommittedUpperBytes) != upper || upper > span.Mapped || a.Provider.ValidateReleasedTail() != nil {
		return fail()
	}
	p := a.Provider
	if p.Status == control.ProviderNoop && p.RequestedBytes == 0 {
		return upper, nil
	} // C已知回收前缀fast path，全0回执。
	if uint64(p.RequestedOffset) != offset || uint64(p.RequestedBytes) != span.Maximum-offset {
		return fail()
	}
	if p.Status == control.ProviderNoop {
		if upper != span.Mapped {
			return fail()
		} // 低层C logical非零NOOP必须没有任何完整尾页。
		return upper, nil
	}
	if upper == span.Mapped || uint64(p.AffectedOffset) != upper || uint64(p.AffectedBytes) != span.Mapped-upper {
		return fail()
	}
	return upper, nil
}
