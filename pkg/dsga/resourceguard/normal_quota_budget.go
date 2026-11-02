package resourceguard

import control "github.com/neondatabase/autoscaling/pkg/api/dsga"

// 仅原受信控制器的真实来源工厂生产，不是HTTP/JSON预算或当前RSS的别名。
// sourceSHA256只包含出生、固定几何与固定策略，交易中A/N变化不能改变原来源。
type normalQuotaBudget struct {
	forceUpperBytes    control.Uint64
	counterGapBytes    control.Uint64
	outsideUpperBytes  control.Uint64
	physicalUpperBytes control.Uint64
	sourceSHA256       string
}
