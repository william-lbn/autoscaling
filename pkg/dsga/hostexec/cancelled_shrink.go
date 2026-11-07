package hostexec

import (
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// CancelledShrinkNeedsAccounting 只选中已明确取消、PG 已回收、Guest/VM 从未派发的原操作。
// 原操作的取消不可逆。普通超时、部分/未知的物理效果和业务关闭均不能触发记账。
func CancelledShrinkNeedsAccounting(m journal.Model) bool {
	op := m.Active
	if op == nil || !op.AbortRequested || op.Request.Kind != control.Shrink || op.Cleanup != nil ||
		m.ServiceClosure != nil || op.PriorBudgetBoundary == nil || m.HighWater == nil || *m.HighWater != op.Request.Key {
		return false
	}
	actions := [...]journal.Action{journal.PrepareBudget, journal.Quiesce, journal.ResizePG, journal.ReleasePG, journal.GuestReady, journal.ResizeVM, journal.ObserveVM, journal.FinalizeBudget, journal.ResumeAdmission}
	if len(op.Steps) != len(actions) {
		return false
	}
	for i, step := range op.Steps {
		if step.Action != actions[i] {
			return false
		}
		if i < 4 {
			if step.Phase != journal.Settled || step.Observation == nil || step.Observation.Result != journal.Completed ||
				step.DispatchedBy == nil || *step.DispatchedBy != op.Origin || step.TargetID != op.Request.Key.Instance.ComputeInstanceID {
				return false
			}
		} else if step.Phase != journal.Planned || step.DispatchedBy != nil || step.Observation != nil {
			return false
		}
	}
	var prepared budget.Prepared
	var checkpoint budget.Quiesced
	if m.ResourceIdentity == nil || json.Unmarshal(op.Steps[0].Observation.Data, &prepared) != nil ||
		prepared.ValidateFor(op.Request, *m.ResourceIdentity) != nil || prepared.OnlineShrinkProtocolVersion != 1 ||
		prepared.OutsideClaim == nil || op.Request.TargetVMMemoryBytes >= prepared.VMBeforeMemoryBytes ||
		json.Unmarshal(op.Steps[1].Observation.Data, &checkpoint) != nil {
		return false
	}
	return checkpoint.ProtocolVersion == 2 && checkpoint.ValidateFor(prepared, op.Origin.Epoch) == nil
}

// checkPaidShrink 检查本次写入只把同一 Pod 已付且受原水位保护的额度关联到取消账本。
// 调用者在 Node Fence 后对同一快照做 Reserve CAS；并发变更使 CAS 失败并重新核对。
func checkPaidShrink(m journal.Model, node hostguard.Model, plan hostguard.Plan) error {
	op := m.Active
	if op == nil || op.PriorBudgetBoundary == nil || plan.Request != op.Request || plan.Request.Kind != control.Shrink ||
		plan.Request.TargetVMMemoryBytes >= plan.BeforeBytes {
		return fmt.Errorf("cancelled shrink lost the original paid request")
	}
	prior, err := hostguard.SettlementFor(m)
	if err != nil {
		return err
	}
	b := op.PriorBudgetBoundary
	if b.BudgetKey != prior.Request.Key || b.FinalizationSHA256 != prior.FinalizationSHA || b.RecordedSequence != prior.BudgetSequence {
		return fmt.Errorf("cancelled shrink lost the previous permanent VM budget")
	}
	if b.ThroughKey != prior.Request.Key {
		prior.ThroughKey, prior.ThroughSequence = b.ThroughKey, b.ThroughSequence
	}
	row, err := node.Placement(plan.RunnerPodUID)
	if err != nil {
		return err
	}
	paid, err := row.Bytes()
	peak, planErr := plan.Bytes()
	if err != nil || planErr != nil || peak != paid || !row.Managed || row.Settled == nil || *row.Settled != prior ||
		row.Namespace != plan.Namespace || row.VMName != plan.VMName || row.VMUID != plan.Request.Key.Instance.VMUID ||
		row.Incarnation != plan.Incarnation || row.HostOverheadBytes != plan.HostOverheadBytes ||
		row.PodMemoryLimitBytes != plan.PodMemoryLimitBytes || row.MemoryBytes != plan.BeforeBytes || prior.MemoryBytes() != plan.BeforeBytes {
		return fmt.Errorf("cancelled shrink does not match its original paid Pod placement")
	}
	return nil
}
