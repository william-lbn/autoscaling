package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

func unchangedCapacity(op Operation) (*Step, bool) {
	if !op.AbortRequested || op.Request.Kind != control.Grow || op.PriorBudgetBoundary == nil {
		return nil, false
	}
	var cancelled *Step
	for i := range op.Steps {
		step := &op.Steps[i]
		switch step.Action {
		case PrepareBudget, ReserveHost:
			if step.Phase != Settled || step.Observation == nil || step.Observation.Result != Completed {
				return nil, false
			}
		case ResizeVM:
			if step.Phase != Settled || step.Observation == nil || step.Observation.Result != NoEffect {
				return nil, false
			}
			cancelled = step
		default:
			// 原Guest/PG/预算/开放只要曾派发，就不能借原完整预算说容量未变。
			if step.Phase != Planned {
				return nil, false
			}
		}
	}
	return cancelled, cancelled != nil
}

func (model Model) unchangedAbortBudget(op Operation) (budget.Finalized, error) {
	var f budget.Finalized
	if _, ok := unchangedCapacity(op); !ok || model.ProtocolVersion < 7 || model.BudgetHeld == nil || model.BudgetHeld.Finalization.Observation == nil || model.validatePriorBoundary(op) != nil {
		return f, fmt.Errorf("unchanged abort requires captured original closure and no later capacity effect")
	}
	b, held := op.PriorBudgetBoundary, model.BudgetHeld
	if json.Unmarshal(held.Finalization.Observation.Data, &f) != nil || b.BudgetKey != f.Request.Key || b.RecordedSequence != held.RecordedSequence || b.FinalizationSHA256 != held.Finalization.CommandSHA256 {
		return f, fmt.Errorf("unchanged abort original complete budget was replaced or lost")
	}
	return f, nil
}

// UnchangedAbortBudget只识别可走原预算恢复的计划结构，不能把VM NoEffect标签
// 当来源认证。生产执行器仍须核原VM取消不可变回执、原PG/Root及全部预算来源。
func (model Model) UnchangedAbortBudget() (budget.Finalized, error) {
	if err := model.Validate(); err != nil {
		return budget.Finalized{}, err
	}
	if model.Active == nil {
		return budget.Finalized{}, fmt.Errorf("original unchanged abort is not active")
	}
	return model.unchangedAbortBudget(*model.Active)
}

func (model Model) validateUnchangedAbort(op Operation) error {
	f, err := model.unchangedAbortBudget(op)
	if err != nil {
		return err
	}
	step, _ := unchangedCapacity(op)
	cleanup := op.Cleanup
	if cleanup == nil || cleanup.Phase != Settled || cleanup.Observation == nil || cleanup.Observation.Result != Completed || cleanup.DispatchedBy == nil || cleanup.TargetID != op.Request.Key.Instance.ComputeInstanceID {
		return fmt.Errorf("unchanged abort has no complete original cleanup")
	}
	var command budget.UnchangedAbortCommand
	var receipt budget.UnchangedAbortReceipt
	digest := sha256.Sum256(step.Observation.Data)
	if json.Unmarshal(cleanup.Command, &command) != nil || json.Unmarshal(cleanup.Observation.Data, &receipt) != nil || command.Request != op.Request || command.FinalizationSHA256 != model.BudgetHeld.Finalization.CommandSHA256 || command.CancelledVMSHA256 != hex.EncodeToString(digest[:]) || command.CancellationSources.JournalSHA256 != step.CommandSHA256 {
		return fmt.Errorf("unchanged abort changed original cancelled VM or retained budget audit")
	}
	return receipt.ValidateFor(command, f, cleanup.DispatchedBy.Epoch, model.Owner.Epoch)
}

func (model Model) CheckUnchangedAbort() error {
	if err := model.Validate(); err != nil {
		return err
	}
	if model.Active == nil {
		return fmt.Errorf("original unchanged abort is not active")
	}
	return model.validateUnchangedAbort(*model.Active)
}
