package budgetclaims

import (
	"context"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// ReadActiveHeld只认证新Active仍占用的旧永久预算来源。新Start已撤销全局
// closed-through证明，不等于原预算/Host成本消失；不能用要求Active=nil的
// ReadCurrent来判这些合法来源失效。返回原承诺的只读观察，不恢复旧执行权，
// 不能交给Host当作当前操作已闭合或可释放的证明。初始头另走其独立读者。
func (r *Reader) ReadActiveHeld(ctx context.Context, s *journal.Snapshot) (*Current, error) {
	if s == nil || !s.Dispatchable() || s.VirtualMachine().UID != r.binding.VMUID || s.State().Incarnation != r.binding.Incarnation {
		return nil, fmt.Errorf("active retained budget requires original running VM snapshot")
	}
	if _, err := activeHeldSettlement(s.State()); err != nil {
		return nil, err
	}
	return r.readCurrent(ctx, s, currentActiveHold)
}

// RecheckActiveHeld在其它领域认证后重读同一旧永久预算及原Host来源。
// 继续使用Active的原关闭锚，不借普通ReadCurrent恢复全局闭合或结清资格；
// 原UID、保护或完整声明变化即拒绝，仍保留当前操作与原峰值。
func (r *Reader) RecheckActiveHeld(ctx context.Context, s *journal.Snapshot, original *Current) error {
	return r.recheckCurrent(ctx, s, original, currentActiveHold)
}

// 只能选择原永久头和Start前已存在的完整关闭链。未派发时用顶层边界，
// 已Start时用同CAS捕获的PriorBudgetBoundary；绝不把它写回Model来复活
// 全局结清资格。原摘要/落账序列逐项匹配，Through只引用旧Key及旧序列。
func activeHeldSettlement(m journal.Model) (hostguard.Settlement, error) {
	var none hostguard.Settlement
	if m.Validate() != nil || m.Active == nil || m.Active.Cleanup != nil {
		return none, fmt.Errorf("active retained source requires original live operation")
	}
	closed, err := hostguard.SettlementFor(m)
	if err != nil {
		return none, err
	}
	b := m.BudgetBoundary
	if b == nil {
		b = m.Active.PriorBudgetBoundary
	}
	if b == nil || b.BudgetKey != closed.Request.Key || b.FinalizationSHA256 != closed.FinalizationSHA || b.RecordedSequence != closed.BudgetSequence || m.Active.Request.Key == closed.Request.Key {
		return none, fmt.Errorf("active retained source lost original budget closure anchor")
	}
	if b.ThroughKey != closed.Request.Key {
		closed.ThroughKey, closed.ThroughSequence = b.ThroughKey, b.ThroughSequence
	}
	return closed, closed.Validate()
}

// 从原永久头严格取得实际预算，再共用原CM/Pod/Node/Host读取；本
// 路径不放宽ReadCurrent的全局闭合条件，也不以当前VM值补造原容量。
func (r *Reader) readActiveCapacity(ctx context.Context, s *journal.Snapshot) (*Current, error) {
	m := s.State()
	closed, err := activeHeldSettlement(m)
	if err != nil {
		return nil, err
	}
	c, err := m.CurrentCapacity()
	if err != nil {
		return nil, err
	}
	observed, err := r.readReconciledSources(ctx, s, c, &closed)
	if err != nil {
		return nil, err
	}
	return &Current{Closed: closed, Host: observed.Host, outside: observed.outside, host: observed.host, pod: observed.pod, node: observed.node, initial: observed.initial}, nil
}
