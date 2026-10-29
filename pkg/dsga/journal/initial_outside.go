package journal

import (
	"encoding/json"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// retainedInitialOutside用于模型内部校验，不能递归调用Model.Validate。
// 来源只能是尚持有的原Initial，或已完成Cleanup永久保存的同一完整Initial；
// 不能从历史、调用者配置或同数值声明重新发现。原始预算没有Resize Key。
func (model Model) retainedInitialOutside() (budget.Initial, error) {
	var i budget.Initial
	if h := model.InitialHeld; h != nil {
		if !model.InitialAdmissionReady() || json.Unmarshal(h.Budget, &i) != nil {
			return i, fmt.Errorf("initial outside has no settled original first admission")
		}
		return i, nil
	}
	if h := model.CapacityHeld; h != nil {
		var c budget.ReconciledAbortCommand
		if h.Cleanup.Phase == Settled && json.Unmarshal(h.Cleanup.Command, &c) == nil && c.Capacity.InitialOutside != nil {
			return *c.Capacity.InitialOutside, nil
		}
	}
	return i, fmt.Errorf("no complete original initial outside budget is retained")
}

// RetainedInitialOutside只返回通过整个模型校验后的深解码副本。原初始头
// 被实际预算同CAS替换后，仍可认证其永久持有的启动资格；并不放宽当前Root/
// PG/VM必须仍符合该原预算的要求，也不提供派发或来源释放票据。
func (model Model) RetainedInitialOutside() (budget.Initial, error) {
	if err := model.Validate(); err != nil {
		return budget.Initial{}, err
	}
	return model.retainedInitialOutside()
}

func (model Model) checkInitialOutside(c budget.ReconciledCapacity) error {
	if c.InitialOutside == nil {
		return nil
	}
	i := c.InitialOutside
	if i.Incarnation != model.Incarnation || model.ResourceIdentity == nil || i.PGAggregate.Identity != *model.ResourceIdentity {
		return fmt.Errorf("reconciled initial outside changed original durable Guest")
	}
	retained, err := model.retainedInitialOutside()
	if err != nil {
		return err
	}
	a, err := retained.Digest()
	if err != nil {
		return err
	}
	b, err := i.Digest()
	if err != nil || a != b {
		return fmt.Errorf("reconciled initial outside replaced original complete initial budget")
	}
	return nil
}
