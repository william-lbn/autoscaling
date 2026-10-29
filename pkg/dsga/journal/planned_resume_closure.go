package journal

import (
	"encoding/json"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// plannedResumeBudget只解释已通过Model校验的最终稳定边界，不递归Validate。
// 所有资源效果均已Completed且完整Finalize原文已永久持有，只有Resume尚未
// Start；因此可审计新预算，但不能把普通Resize中间态按旧预算误判为异常。
// Planned没有命令/派发者/未知；关闭时必须保留这一事实，不能补造原开放。
func (model Model) plannedResumeBudget() (budget.Finalized, error) {
	var f budget.Finalized
	op, held := model.Active, model.BudgetHeld
	if op == nil || op.Cleanup != nil || held == nil || held.Finalization.Observation == nil || len(op.Steps) == 0 {
		return f, fmt.Errorf("planned resume lacks retained finalized operation")
	}
	if json.Unmarshal(held.Finalization.Observation.Data, &f) != nil || f.Request != op.Request {
		return f, fmt.Errorf("planned resume changed original finalized budget")
	}
	for i, step := range op.Steps {
		if i == len(op.Steps)-1 {
			if step.Action != ResumeAdmission || step.Phase != Planned {
				return f, fmt.Errorf("final admission was already dispatched")
			}
		} else if step.Phase != Settled || step.Observation == nil || step.Observation.Result != Completed {
			return f, fmt.Errorf("planned resume cannot adopt unfinished prior domains")
		}
	}
	return f, nil
}

// 返回完整预算的值副本供独立关闭核验容量；不授予开放、Finish或释放许可。
func (model Model) PlannedResumeBudgetForClosure() (budget.Finalized, error) {
	if err := model.Validate(); err != nil {
		return budget.Finalized{}, err
	}
	return model.plannedResumeBudget()
}

func (model Model) validatePlannedResumeServiceClosure() error {
	c := model.ServiceClosure
	f, err := model.plannedResumeBudget()
	digest, hashErr := operationClosureDigest(model.Active)
	if err != nil || hashErr != nil || c.Reason != "planned_resume_source_unconfirmed" || c.OperationSHA256 != digest || c.Original != nil || c.ClosureReceipt != nil || c.Request.ExpectedCapacityGeneration != f.Shared.Observed.CapacityGeneration {
		return fmt.Errorf("planned resume closure lost exact unstarted operation or finalized capacity")
	}
	return nil
}
