package owner

import (
	"context"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// RecoverUnchangedAdmission是与原命令重放不同的受信恢复入口。协调器必须
// 先确认暂停/运维关闭意图已撤销，再明确选择此原Key；默认轮询不会开放它。
func (worker *PGWorker) RecoverUnchangedAdmission(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if err := worker.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	return worker.admission.RecoverUnchangedAdmission(callCtx, key)
}

// 两种Cleanup共用独立准入栅栏，但预算语义不同：完整容量后取消用本请求的
// 新预算；VM未发送且容量未变则只用捕获的旧完整预算。恢复不能因解码失败
// 当作没有pending，也不能用新目标预算替换旧来源。真正认证在执行器中重做。
func originalAbortAdmission(m journal.Model, cleanup journal.Step, instance control.Instance) (*control.AdmissionRequest, error) {
	if cleanup.DispatchedBy == nil || cleanup.TargetID != instance.ComputeInstanceID || m.Active == nil {
		return nil, fmt.Errorf("original abort admission lacks complete original dispatch context")
	}
	var reconciled budget.ReconciledAbortCommand
	if json.Unmarshal(cleanup.Command, &reconciled) == nil {
		// 首次/部分容量可能没有旧Finalized。原独立实际预算及计划核完
		// 后恢复同一原准入，不借构造空SDK丢掉旧命令，也不转换原目标。
		if reconciled.ValidateFor(cleanup.DispatchedBy.Epoch) != nil || reconciled.Capacity.Request.Key.Instance != instance || m.CheckReconciledPlan(reconciled) != nil {
			return nil, fmt.Errorf("original reconciled admission differs from actual capacity plan")
		}
		return reconciled.Admission, nil
	}
	if m.BudgetHeld == nil {
		return nil, fmt.Errorf("original abort admission lacks retained normal budget")
	}
	var committed budget.CommittedAbortCommand
	if json.Unmarshal(cleanup.Command, &committed) == nil {
		held, err := m.BudgetFor(m.Active.Request)
		if err != nil || committed.ValidateFor(held, cleanup.DispatchedBy.Epoch) != nil || committed.FinalizationSHA256 != m.BudgetHeld.Finalization.CommandSHA256 {
			return nil, fmt.Errorf("original committed abort admission differs from retained budget")
		}
		if committed.MaySend {
			return &committed.Admission, nil
		}
		return nil, nil
	}
	var unchanged budget.UnchangedAbortCommand
	held, err := m.UnchangedAbortBudget()
	if err != nil || json.Unmarshal(cleanup.Command, &unchanged) != nil || unchanged.Request != m.Active.Request || unchanged.ValidateFor(held, cleanup.DispatchedBy.Epoch) != nil || unchanged.FinalizationSHA256 != m.BudgetHeld.Finalization.CommandSHA256 {
		return nil, fmt.Errorf("original unchanged abort admission differs from captured prior budget")
	}
	return unchanged.Admission, nil
}
