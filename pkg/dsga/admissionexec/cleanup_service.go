package admissionexec

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// CheckCleanupService在查询原开放之前核全部原预算来源，避免NotFound或
// 丢回复掩盖已经失效的服务。只复用三类原生产审计，不发开放、Record、
// Finish、资源反向动作或释放；SDK原未知保持。网络不持本地锁，最终关闭
// 仍由owner另做原生fence、固定容量和UID/RV写前CAS，不能把本次读当票据。
func (e *Executor) CheckCleanupService(ctx context.Context, s *journal.Snapshot) error {
	m := s.State()
	if e.claims == nil || e.runner == nil || e.resources == nil || !s.Dispatchable() || m.Owner != e.actor || m.ServiceClosure != nil {
		return fmt.Errorf("cleanup service check requires original live production domains")
	}
	if _, err := m.CleanupServiceForClosure(); err != nil {
		return err
	}
	step := m.Active.Cleanup
	var actual budget.ReconciledAbortCommand
	if json.Unmarshal(step.Command, &actual) == nil {
		var p budget.Prepared
		for _, prior := range m.Active.Steps {
			if prior.Action == journal.PrepareBudget && prior.Observation != nil {
				if err := json.Unmarshal(prior.Observation.Data, &p); err != nil {
					return err
				}
			}
		}
		_, _, _, _, err := e.checkReconciled(ctx, s, p, &actual.Capacity)
		return err
	}
	var committed budget.CommittedAbortCommand
	if json.Unmarshal(step.Command, &committed) == nil {
		f, err := m.BudgetFor(m.Active.Request)
		if err != nil {
			return err
		}
		_, _, _, err = e.checkCommittedBudget(ctx, s, f)
		return err
	}
	f, err := m.UnchangedAbortBudget()
	if err != nil {
		return err
	}
	vm := unchangedVM(m.Active)
	var native vmguard.Command
	var cancelled vmexec.CancelledVM
	if vm == nil || vm.Observation == nil || json.Unmarshal(vm.Command, &native) != nil || native.Request != m.Active.Request || json.Unmarshal(vm.Observation.Data, &cancelled) != nil || cancelled.ValidateFor(native, vm.CommandSHA256) != nil {
		return fmt.Errorf("cleanup service lost original native VM cancellation")
	}
	_, _, _, err = e.checkUnchangedBudget(ctx, s, f, native, cancelled)
	return err
}
