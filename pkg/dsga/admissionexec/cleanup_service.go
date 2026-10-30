package admissionexec

import (
	"context"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
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
	_, _, _, err := e.checkCleanupService(ctx, s, false)
	return err
}

// 恢复只复用原三类预算审计，不查询/重放旧开放，不清SDK未知。来源必须
// 在新开放写前、发送前后和最终完成前再次认证，旧Proof不是永久活性许可。
func (e *Executor) CheckCleanupServiceRecovery(ctx context.Context, s *journal.Snapshot) error {
	m := s.State()
	if m.ServiceClosure == nil || m.ServiceClosure.CleanupRecovery == nil || m.CheckStableServiceRecovery() != nil {
		return fmt.Errorf("cleanup service recovery lacks exact retained independent proof")
	}
	_, _, _, err := e.checkCleanupService(ctx, s, true)
	return err
}

func (e *Executor) RecoverServiceCleanup(ctx context.Context, s *journal.Snapshot, selector string) error {
	fence, shared, root, err := e.checkCleanupService(ctx, s, true)
	if err != nil {
		return err
	}
	_, err = e.store.RecordServiceCleanupRecovery(ctx, s, e.actor, selector, fence, shared, root)
	return err
}

func (e *Executor) checkCleanupService(ctx context.Context, s *journal.Snapshot, recovery bool) (control.FenceResponse, control.SharedCostResponse, budget.RootAggregate, error) {
	var fence control.FenceResponse
	var shared control.SharedCostResponse
	var root budget.RootAggregate
	m := s.State()
	if e.claims == nil || e.runner == nil || e.resources == nil || !s.Dispatchable() || m.Owner != e.actor || !recovery && m.ServiceClosure != nil || recovery && (m.ServiceClosure == nil || m.ServiceClosure.Reason != "cleanup_source_unconfirmed" || m.ServiceClosure.Pending()) {
		return fence, shared, root, fmt.Errorf("cleanup service check requires original live production domains")
	}
	if _, err := m.CleanupServiceForClosure(); err != nil {
		return fence, shared, root, err
	}
	step := m.Active.Cleanup
	var actual budget.ReconciledAbortCommand
	if json.Unmarshal(step.Command, &actual) == nil {
		var p budget.Prepared
		for _, prior := range m.Active.Steps {
			if prior.Action == journal.PrepareBudget && prior.Observation != nil {
				if err := json.Unmarshal(prior.Observation.Data, &p); err != nil {
					return fence, shared, root, err
				}
			}
		}
		_, fence, shared, root, err := e.checkReconciled(ctx, s, p, &actual.Capacity)
		return fence, shared, root, err
	}
	var committed budget.CommittedAbortCommand
	if json.Unmarshal(step.Command, &committed) == nil {
		f, err := m.BudgetFor(m.Active.Request)
		if err != nil {
			return fence, shared, root, err
		}
		return e.checkCommittedBudget(ctx, s, f)
	}
	f, err := m.UnchangedAbortBudget()
	if err != nil {
		return fence, shared, root, err
	}
	vm := unchangedVM(m.Active)
	var native vmguard.Command
	var cancelled vmexec.CancelledVM
	if vm == nil || vm.Observation == nil || json.Unmarshal(vm.Command, &native) != nil || native.Request != m.Active.Request || json.Unmarshal(vm.Observation.Data, &cancelled) != nil || cancelled.ValidateFor(native, vm.CommandSHA256) != nil {
		return fence, shared, root, fmt.Errorf("cleanup service lost original native VM cancellation")
	}
	return e.checkUnchangedBudget(ctx, s, f, native, cancelled)
}
