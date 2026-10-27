package journal

import (
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// CheckCommittedAbort供生产协调器在Finish之前校验原已落账清理，不能拿通用
// Cleanup Completed当作完整预算/准入闭合。它不重算或替换历史审计原文。
func (model Model) CheckCommittedAbort() error {
	if err := model.Validate(); err != nil {
		return err
	}
	if model.Active == nil {
		return fmt.Errorf("original committed abort is not active")
	}
	return model.validateCommittedAbort(*model.Active)
}

// validateCommittedAbort只认证本VM WAL的完整结构和相互绑定，不认证网络来源。
// 它区分“业务取消”与“容量已成功并恢复工作”：所有容量步骤必须Completed，
// 当前预算必须是同一次原子Finalize，Cleanup须包含原完整准入/预算审计。
// 任意通用Cleanup、NoEffect/部分效果或旧预算均不能取得Host峰值结清资格。
func (model Model) validateCommittedAbort(op Operation) error {
	if !op.AbortRequested || model.ProtocolVersion < 4 || model.BudgetHeld == nil || op.Cleanup == nil || op.Cleanup.Phase != Settled || op.Cleanup.Observation == nil || op.Cleanup.Observation.Result != Completed || op.Cleanup.DispatchedBy == nil || op.Cleanup.TargetID != op.Request.Key.Instance.ComputeInstanceID {
		return fmt.Errorf("committed abort requires original complete capacity and cleanup")
	}
	var final, resume *Step
	for i := range op.Steps {
		step := &op.Steps[i]
		if step.Action == ResumeAdmission {
			resume = step
			continue
		}
		if step.Phase != Settled || step.Observation == nil || step.Observation.Result != Completed {
			return fmt.Errorf("committed abort cannot cover partial or unconfirmed capacity")
		}
		if step.Action == FinalizeBudget {
			final = step
		}
	}
	if final == nil || resume == nil || !reflect.DeepEqual(*final, model.BudgetHeld.Finalization) {
		return fmt.Errorf("committed abort changed atomically retained finalization")
	}
	f, err := finalizedBudget(op, *final)
	if err != nil {
		return err
	}
	var command budget.CommittedAbortCommand
	var receipt budget.CommittedAbortReceipt
	if json.Unmarshal(op.Cleanup.Command, &command) != nil || json.Unmarshal(op.Cleanup.Observation.Data, &receipt) != nil || command.FinalizationSHA256 != final.CommandSHA256 {
		return fmt.Errorf("committed abort cleanup changed original complete command")
	}
	if resume.Phase == Planned {
		if !command.MaySend {
			return fmt.Errorf("committed abort has no prior original admission to query")
		}
	} else {
		var original control.AdmissionRequest
		if resume.Phase != Settled || resume.Observation == nil || resume.Observation.Result != Completed || command.MaySend || json.Unmarshal(resume.Command, &original) != nil || original != command.Admission {
			return fmt.Errorf("committed abort cannot replace original settled admission")
		}
	}
	return receipt.ValidateFor(command, f, op.Cleanup.DispatchedBy.Epoch, model.Owner.Epoch)
}
