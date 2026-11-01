package resourceguard

import (
	"errors"
	"fmt"
	"reflect"
)

// 这是原normal profile的拒绝门，不产生normal/VM许可。生产linux实现只从原
// 完整只读ISO选择；模型内核没有该能力时保留旧协议，不把它补签成normal来源。
type normalImmutableBoundsKernel interface{ normalImmutableBounds(Limits) error }

func normalImmutableBoundsPreflight(k kernel, target Limits) error {
	if original, ok := k.(normalImmutableBoundsKernel); ok {
		return original.normalImmutableBounds(target)
	}
	return nil
}

// plan是完整原raw/实际五资产派生的私有能力，不能从请求填入。N和Outside随受控
// 预算协议变化；hstate全0、任务/System/AS则必须逐字保持原创建值。原record-v3
// 之前也不能开放HugeTLB池；事后Observe发现漂移不能替代写前阻止。
func normalImmutableBoundsTarget(plan RuntimeBootRequirements, source SourceConfig, target Limits) error {
	if plan.ProfileID() != NormalSplitV1PhysicalKernelProfileID {
		return nil
	}
	fail := func(s string) error { return errors.Join(ErrUnknown, fmt.Errorf("immutable normal bounds: %s", s)) }
	if !plan.Enabled() || source.Validate() != nil || len(source.Limits.HugePools) == 0 {
		return fail("original normal source incomplete")
	}
	for _, h := range source.Limits.HugePools {
		if h.Pages != 0 {
			return fail("original HugeTLB target is not zero")
		}
	}
	if !reflect.DeepEqual(target.HugePools, source.Limits.HugePools) {
		return fail("HugeTLB target differs from original complete zero hstates")
	}
	if target.PidsMaxTasks != source.Limits.PidsMaxTasks || target.SystemMemoryMaxBytes != source.Limits.SystemMemoryMaxBytes || target.AddressSpaceMaxBytes != source.Limits.AddressSpaceMaxBytes {
		return fail("task/System/address-space target differs from original creation")
	}
	return nil
}
