package admissionexec

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 实际恢复只接已知的原效果；任何领域仍Unknown时交原执行器先查询。
// Finalize/Resume一旦派发便属于原完整预算路径，不能用新Cleanup掩盖。
// 本函数仅选路径；原Prepare/来源真实性、当前PG/Root和最终CAS另行核验。
func reconciledEligible(op *journal.Operation) bool {
	if op == nil || !op.AbortRequested {
		return false
	}
	prepared := false
	for _, step := range op.Steps {
		if step.Phase == journal.Unconfirmed || (step.Action == journal.FinalizeBudget || step.Action == journal.ResumeAdmission) && step.Phase != journal.Planned {
			return false
		}
		if step.Action == journal.PrepareBudget {
			prepared = step.Phase == journal.Settled && step.Observation != nil && step.Observation.Result == journal.Completed
		}
	}
	return prepared
}

// 仅按持久格式选择解析器；真实原命令仍需完整严格校验。已有实际Cleanup
// 优先于可能仍满足旧“容量未变”形状的原步骤，避免被旧解析器误报损坏。
func reconciledCleanup(op *journal.Operation) bool {
	if op == nil || op.Cleanup == nil {
		return false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(op.Cleanup.Command, &fields) != nil {
		return false
	}
	_, exists := fields["capacity"]
	return exists
}

// Outside只选原目标的Prepare，或永久预算中准确覆盖当前VM的来源。VM仍大
// 时不能借已缩小目标的系统资格；旧来源仍须真实Reader重读和持续保护。
func reconciledOutside(m journal.Model, p budget.Prepared, memory control.Uint64) (budget.ObjectRef, error) {
	if p.Request.TargetVMMemoryBytes == memory && p.OutsideClaim != nil {
		return *p.OutsideClaim, p.OutsideClaim.Validate()
	}
	if m.CapacityHeld != nil && m.CapacityHeld.Cleanup.DispatchedBy != nil {
		var c budget.ReconciledAbortCommand
		if json.Unmarshal(m.CapacityHeld.Cleanup.Command, &c) == nil && c.ValidateFor(m.CapacityHeld.Cleanup.DispatchedBy.Epoch) == nil && c.Capacity.Request.Key.Instance == p.Request.Key.Instance && c.Capacity.VMMemoryBytes == memory && c.Capacity.InitialOutside == nil {
			return c.Capacity.OutsideClaim, nil
		}
	}
	if m.BudgetHeld != nil {
		var c budget.FinalizationCommand
		if json.Unmarshal(m.BudgetHeld.Finalization.Command, &c) == nil && c.Validate() == nil && c.Finalized.Request.Key.Instance == p.Request.Key.Instance && c.Finalized.Request.TargetVMMemoryBytes == memory && c.Finalized.Sources != nil {
			return c.Finalized.Sources.OutsideClaim, nil
		}
	}
	return budget.ObjectRef{}, fmt.Errorf("no original outside qualification covers actual VM capacity")
}

// 普通取消仅能恢复自己仍有效的原Quiesce；Grow没有自己的关门，外部更高
// revision关闭也不属于本操作。其它关闭必须由受信协调器独立选择，不能按
// Abort/超时猜测服务意图。旧owner原关闭可查询，新的开放只用当前owner代次。
func reconciledOwnClose(op *journal.Operation, p budget.Prepared, shared control.SharedCostResponse) bool {
	if op.Request.Kind != control.Shrink || shared.Observed.AdmissionOpen {
		return false
	}
	for _, step := range op.Steps {
		if step.Action != journal.Quiesce || step.Phase != journal.Settled || step.Observation == nil || step.Observation.Result != journal.Completed || step.DispatchedBy == nil || step.TargetID != p.Request.Key.Instance.ComputeInstanceID {
			continue
		}
		var command budget.QuiesceCommand
		var receipt budget.Quiesced
		if json.Unmarshal(step.Command, &command) != nil || command.ValidateFor(p, step.DispatchedBy.Epoch) != nil || command.Close.IssuerEpoch != step.DispatchedBy.Epoch || json.Unmarshal(step.Observation.Data, &receipt) != nil || receipt.ValidateFor(p, step.DispatchedBy.Epoch) != nil || receipt.Close != command.Close || !reflect.DeepEqual(receipt.PGAggregate, command.PGAggregate) || shared.Observed.AdmissionRevision != command.Close.Revision {
			return false
		}
		return true
	}
	return false
}

func reconciledOpening(op *journal.Operation, p budget.Prepared, c budget.ReconciledCapacity, shared control.SharedCostResponse, epoch control.Uint64, selected bool) (*control.AdmissionRequest, error) {
	if shared.Observed.AdmissionOpen {
		return nil, nil
	}
	if !selected && !reconciledOwnClose(op, p, shared) {
		return nil, ErrRecoverySelectionRequired
	}
	if shared.Observed.AdmissionRevision == control.Uint64(math.MaxUint64) {
		return nil, fmt.Errorf("actual budget admission revision exhausted")
	}
	return &control.AdmissionRequest{ProtocolVersion: 1, Instance: op.Request.Key.Instance, IssuerEpoch: epoch, Revision: shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: c.CapacityGeneration, Open: true, BudgetCommitmentID: op.Request.BudgetCommitmentID}, nil
}
