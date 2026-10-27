package journal

import (
	"encoding/hex"
	"fmt"
)

// 原字段只说明真实Start之前已有的关闭链；Start之后顶层证明仍必须撤销。
// 历史记录可能已对应新的当前预算，因此通用验证只核单调关系，真正恢复时
// 再严格匹配当前Held预算/摘要/序列及所有真实来源。旧版本不能投影新增语义。
func (model Model) validatePriorBoundary(op Operation) error {
	b := op.PriorBudgetBoundary
	if b == nil {
		return nil
	}
	dispatched := false
	for _, step := range op.Steps {
		if step.Phase != Planned {
			dispatched = true
		}
	}
	digest, hashErr := hex.DecodeString(b.FinalizationSHA256)
	if !dispatched || model.ProtocolVersion < 7 || b.ProtocolVersion != 1 || b.BudgetKey.Validate() != nil || b.ThroughKey.Validate() != nil || b.BudgetKey.Instance != op.Request.Key.Instance || b.ThroughKey.Instance != b.BudgetKey.Instance ||
		b.RecordedSequence == 0 || b.SucceededSequence <= b.RecordedSequence || b.ThroughSequence < b.SucceededSequence || b.ThroughSequence >= model.Sequence || b.ThroughKey.IssuerEpoch > op.Origin.Epoch || compareKeys(b.ThroughKey, b.BudgetKey) < 0 || compareKeys(b.ThroughKey, op.Request.Key) >= 0 || hashErr != nil || len(digest) != 32 || hex.EncodeToString(digest) != b.FinalizationSHA256 {
		return fmt.Errorf("invalid original pre-dispatch budget boundary")
	}
	if compareKeys(b.ThroughKey, b.BudgetKey) == 0 && (b.ThroughKey != b.BudgetKey || b.ThroughSequence != b.SucceededSequence) {
		return fmt.Errorf("original pre-dispatch boundary changed initial closure")
	}
	return nil
}
