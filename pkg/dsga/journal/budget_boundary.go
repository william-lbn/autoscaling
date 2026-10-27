package journal

import (
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// ClosedBudgetBoundary是原WAL生命周期证明，不是Root/PG/QMP或物理空闲回执。
// 它绑定当前完整预算的命令摘要、首次落账和成功关闭序列；Through只可被全部
// Planned的取消推进。Start先撤销此证明，预算替换也清空，因此不能跨过任何
// 已派发/部分完成/拒绝的后续动作。所有变化都复用原VM UID/RV CAS，不另建锁。
// v6的SucceededSequence也可指所有容量已完成、原预算准入清理已闭合的取消；
// 它证明容量结清资格，不把业务结果AbortedAfterReconcile改成Succeeded。
//
// 该永久水位解决结果历史淘汰和后续零派发取消导致Host峰值无法结清的问题。
// Host仍须认证原Runner实际值、当前预算来源与原Node，不能仅凭本结构释放容量。
type ClosedBudgetBoundary struct {
	ProtocolVersion    uint32         `json:"protocol_version"`
	BudgetKey          control.Key    `json:"budget_key"`
	FinalizationSHA256 string         `json:"finalization_sha256"`
	RecordedSequence   control.Uint64 `json:"recorded_sequence"`
	SucceededSequence  control.Uint64 `json:"succeeded_sequence"`
	ThroughKey         control.Key    `json:"through_key"`
	ThroughSequence    control.Uint64 `json:"through_sequence"`
}

func (b *ClosedBudgetBoundary) UnmarshalJSON(data []byte) error {
	type plain ClosedBudgetBoundary
	var decoded plain
	if err := control.UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*b = ClosedBudgetBoundary(decoded)
	return nil
}

func (model Model) retainedRequest() (control.Request, error) {
	if model.CapacityHeld != nil {
		var c budget.ReconciledAbortCommand
		if json.Unmarshal(model.CapacityHeld.Cleanup.Command, &c) != nil {
			return control.Request{}, fmt.Errorf("original reconciled budget command unavailable")
		}
		return c.Capacity.Request, nil
	}
	var command budget.FinalizationCommand
	if model.BudgetHeld == nil || json.Unmarshal(model.BudgetHeld.Finalization.Command, &command) != nil {
		return control.Request{}, fmt.Errorf("original retained budget command unavailable")
	}
	return command.Finalized.Request, nil
}

func (model Model) budgetRecord() (string, control.Uint64) {
	if model.CapacityHeld != nil {
		return model.CapacityHeld.Cleanup.CommandSHA256, model.CapacityHeld.RecordedSequence
	}
	if model.BudgetHeld != nil {
		return model.BudgetHeld.Finalization.CommandSHA256, model.BudgetHeld.RecordedSequence
	}
	return "", 0
}

func (model Model) boundaryFor(request control.Request, sequence control.Uint64) *ClosedBudgetBoundary {
	hash, recorded := model.budgetRecord()
	return &ClosedBudgetBoundary{ProtocolVersion: 1, BudgetKey: request.Key, FinalizationSHA256: hash, RecordedSequence: recorded, SucceededSequence: sequence, ThroughKey: request.Key, ThroughSequence: sequence}
}

// legacyBudgetBoundary仅在显式Finish的同一原快照上升级v4：原成功记录必须仍
// 完整存在，随后每条都为零派发取消。历史已过期或中间有别的结果便不猜测。
// Load/普通读取不补字段；v5被Start清空的证明也不能用历史自动复活。
func (model Model) legacyBudgetBoundary(request control.Request) *ClosedBudgetBoundary {
	var boundary *ClosedBudgetBoundary
	for _, record := range model.History {
		if record.Operation.Request == request && record.Outcome == Succeeded && record.ClosedSequence > model.BudgetHeld.RecordedSequence {
			boundary = model.boundaryFor(request, record.ClosedSequence)
			continue
		}
		if boundary != nil {
			if record.Outcome != AbortedBeforeDispatch {
				return nil
			}
			boundary.ThroughKey, boundary.ThroughSequence = record.Operation.Request.Key, record.ClosedSequence
		}
	}
	return boundary
}

func (model *Model) closeBudgetBoundary(outcome Outcome) error {
	if model.ProtocolVersion < 4 || model.BudgetHeld == nil && model.CapacityHeld == nil {
		return nil
	}
	request, err := model.retainedRequest()
	if err != nil {
		return err
	}
	switch outcome {
	case Succeeded:
		if request != model.Active.Request {
			return fmt.Errorf("successful closure does not match current complete budget")
		}
		model.BudgetBoundary = model.boundaryFor(request, model.Sequence)
	case AbortedBeforeDispatch:
		if model.ProtocolVersion == 4 && model.BudgetBoundary == nil {
			model.BudgetBoundary = model.legacyBudgetBoundary(request)
		}
		if model.BudgetBoundary != nil {
			model.BudgetBoundary.ThroughKey, model.BudgetBoundary.ThroughSequence = model.Active.Request.Key, model.Sequence
		}
	case AbortedAfterReconcile:
		model.BudgetBoundary = nil
		// 业务取消不回滚已成功容量。只有同原完整预算、所有容量步骤已
		// 完成且准入恢复原审计闭合，才保留容量关闭资格；其他部分取消
		// 仍保留峰值。v6防止旧v5在历史淘汰后误解新增的关闭语义。
		if model.validateCommittedAbort(*model.Active) == nil {
			model.BudgetBoundary = model.boundaryFor(request, model.Sequence)
			model.ProtocolVersion = max(model.ProtocolVersion, 6)
		} else if model.validateUnchangedAbort(*model.Active) == nil {
			// 只有完整清理和捕获的原关闭链均核实，才恢复同一原预算的
			// 无容量效果证明。不是由NoEffect标签或当前数值自动补造。
			prior := *model.Active.PriorBudgetBoundary
			prior.ThroughKey, prior.ThroughSequence = model.Active.Request.Key, model.Sequence
			model.BudgetBoundary = &prior
		} else if model.validateReconciledAbort(*model.Active) == nil {
			model.BudgetBoundary = model.boundaryFor(request, model.Sequence)
		}
	}
	if model.BudgetBoundary != nil {
		model.ProtocolVersion = max(model.ProtocolVersion, 5)
	}
	return nil
}

func (model Model) validateBudgetBoundary() error {
	b := model.BudgetBoundary
	if b == nil {
		return nil
	}
	if model.ProtocolVersion < 5 || model.BudgetHeld == nil && model.CapacityHeld == nil || model.HighWater == nil || b.ProtocolVersion != 1 || b.BudgetKey.Validate() != nil || b.ThroughKey.Validate() != nil {
		return fmt.Errorf("invalid closed budget boundary version or binding")
	}
	request, err := model.retainedRequest()
	hash, recorded := model.budgetRecord()
	if err != nil || b.BudgetKey != request.Key || b.FinalizationSHA256 != hash || b.RecordedSequence != recorded ||
		b.SucceededSequence <= b.RecordedSequence || b.SucceededSequence > b.ThroughSequence || b.ThroughSequence > model.Sequence || b.ThroughKey.Instance != b.BudgetKey.Instance || b.ThroughKey.IssuerEpoch > model.Owner.Epoch ||
		compareKeys(b.ThroughKey, b.BudgetKey) < 0 || compareKeys(b.ThroughKey, *model.HighWater) > 0 {
		return fmt.Errorf("closed budget boundary changed original budget or monotonic sequence")
	}
	if compareKeys(b.ThroughKey, b.BudgetKey) == 0 && (b.ThroughKey != b.BudgetKey || b.ThroughSequence != b.SucceededSequence) {
		return fmt.Errorf("initial closed budget boundary differs from original success")
	}
	if compareKeys(b.ThroughKey, *model.HighWater) == 0 && b.ThroughKey != *model.HighWater {
		return fmt.Errorf("closed budget boundary conflicts with high water identity")
	}
	if model.Active != nil && compareKeys(model.Active.Request.Key, b.ThroughKey) <= 0 {
		return fmt.Errorf("closed budget boundary includes an active operation")
	}
	for _, record := range model.History {
		key := record.Operation.Request.Key
		if compareKeys(key, b.BudgetKey) < 0 || compareKeys(key, b.ThroughKey) > 0 {
			continue
		}
		if key == b.BudgetKey {
			capacityClosed := record.Outcome == Succeeded || (model.ProtocolVersion >= 6 && record.Outcome == AbortedAfterReconcile && model.validateCommittedAbort(record.Operation) == nil) || (model.ProtocolVersion >= 8 && record.Outcome == AbortedAfterReconcile && model.validateReconciledAbort(record.Operation) == nil)
			if record.Operation.Request != request || !capacityClosed || record.ClosedSequence != b.SucceededSequence {
				return fmt.Errorf("closed budget boundary contradicts original successful history")
			}
		} else if (record.Outcome != AbortedBeforeDispatch && (record.Outcome != AbortedAfterReconcile || model.validateUnchangedAbort(record.Operation) != nil)) || record.ClosedSequence <= b.SucceededSequence || record.ClosedSequence > b.ThroughSequence {
			return fmt.Errorf("closed budget boundary crosses a dispatched or contradictory history")
		}
		if compareKeys(key, b.ThroughKey) == 0 && (key != b.ThroughKey || record.ClosedSequence != b.ThroughSequence) {
			return fmt.Errorf("closed budget boundary differs from terminal high water")
		}
	}
	return nil
}

// CheckClosedBudget只证明当前完整预算之后、直到当前HWM没有尚未覆盖的派发。
// 新v5使用永久水位，历史淘汰不阻塞结清；旧v4仅接受原HWM仍等于原成功预算且
// 成功记录未过期的原行为。它不认证实时来源、不发新VM许可或释放预算对象。
func (model Model) CheckClosedBudget(request control.Request) error {
	if _, err := model.BudgetFor(request); err != nil {
		return err
	}
	if model.Active != nil || model.HighWater == nil {
		return fmt.Errorf("current budget still has an active or unbound operation")
	}
	if model.ProtocolVersion >= 5 {
		if model.BudgetBoundary == nil || model.BudgetBoundary.ThroughKey != *model.HighWater {
			return fmt.Errorf("current budget has no complete persistent closed-through boundary")
		}
		return nil
	}
	if *model.HighWater == request.Key {
		for _, record := range model.History {
			if record.Operation.Request == request && record.Outcome == Succeeded && record.ClosedSequence > model.BudgetHeld.RecordedSequence {
				return nil
			}
		}
	}
	return fmt.Errorf("legacy current budget has no original successful terminal operation")
}
