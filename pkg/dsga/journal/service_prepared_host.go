package journal

import (
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// PreparedHostClosure只是原WAL的只读投影，不发Host/PG/VM命令。Node生产者
// 与固定读者共用此选择，避免另写一套关闭/Prepare摘要而漏掉Continuation。
type PreparedHostClosure struct {
	Prepared        budget.Prepared
	PreparedSHA256  string
	OperationSHA256 string
	ClosedSHA256    string
	ClosedSequence  control.Uint64
}

func completedPreparedOperation(op Operation) (budget.Prepared, string, error) {
	var prepared budget.Prepared
	if op.Validate() != nil || op.Cleanup != nil || len(op.Steps) == 0 {
		return prepared, "", fmt.Errorf("completed Prepare recovery lacks original early operation")
	}
	var raw []byte
	for _, step := range op.Steps {
		if step.Action == PrepareBudget {
			if raw != nil || step.Phase != Settled || step.Observation == nil || step.Observation.Result != Completed || json.Unmarshal(step.Observation.Data, &prepared) != nil || prepared.ValidateFor(op.Request, prepared.Identity) != nil || prepared.OutsideClaim == nil {
				return prepared, "", fmt.Errorf("completed Prepare recovery lost exact original full observation")
			}
			raw = step.Observation.Data
		} else if step.Phase != Planned {
			return prepared, "", fmt.Errorf("completed Prepare recovery crossed dispatched resource boundary")
		}
	}
	if raw == nil {
		return prepared, "", fmt.Errorf("completed Prepare recovery has no original Prepare")
	}
	// rawDigest复用Journal生产摘要，只哈希原Observation.Data字节；绝不从
	// 重新marshal的Prepared投影冒充原WAL原文。
	return prepared, rawDigest(raw), nil
}

func retainedOperationCandidate(op Operation) error {
	if earlyRetainedOperation(op) == nil {
		return nil
	}
	_, _, err := completedPreparedOperation(op)
	return err
}

func (m Model) preparedHostClosure() (PreparedHostClosure, error) {
	var out PreparedHostClosure
	c := m.ServiceClosure
	if c == nil || c.Pending() || m.serviceClosureVersion() != 16 || m.Active == nil || m.ResourceIdentity == nil || c.ActualRecovery != nil || c.Finalization != nil || c.CleanupRecovery != nil || c.ResourceRecovery != nil || c.PGRecovery != nil || c.PGRejection != nil || c.VMRecovery != nil {
		return out, fmt.Errorf("prepared Host closure requires confirmed original category16 boundary")
	}
	prepared, digest, err := completedPreparedOperation(*m.Active)
	if err != nil || prepared.Identity != *m.ResourceIdentity {
		return out, fmt.Errorf("prepared Host closure changed original Prepare/resource identity")
	}
	_, _, _, closedDigest := c.Selection()
	closedAt := c.RecordedSequence
	if c.Continuation != nil {
		closedAt = c.Continuation.RecordedSequence
	}
	return PreparedHostClosure{Prepared: prepared, PreparedSHA256: digest, OperationSHA256: c.OperationSHA256, ClosedSHA256: closedDigest, ClosedSequence: closedAt}, nil
}

func (m Model) PreparedHostClosure() (PreparedHostClosure, error) {
	if err := m.Validate(); err != nil {
		return PreparedHostClosure{}, err
	}
	return m.preparedHostClosure()
}

// CheckPreparedHostBarrier只核原模型与Node证明的交叉合同，不认证网络来源。
// 当前VM可以在Proof/rearm/reopen时单调前进；原封存的VM序列/RV不被重写。
func (m Model) CheckPreparedHostBarrier(b budget.HostReservationBarrier) error {
	if err := m.Validate(); err != nil {
		return err
	}
	return m.checkPreparedHostBarrier(b)
}

// checkPreparedHostBarrier仅供已经整模校验中的Journal使用。
func (m Model) checkPreparedHostBarrier(b budget.HostReservationBarrier) error {
	f, err := m.preparedHostClosure()
	if err != nil || b.ValidateFor(f.Prepared, m.Incarnation) != nil || b.PreparedSHA256 != f.PreparedSHA256 || b.OperationSHA256 != f.OperationSHA256 || b.ClosedSHA256 != f.ClosedSHA256 || b.ClosedSequence != f.ClosedSequence || b.VMSequence > m.Sequence || b.VMSequence < b.ClosedSequence || !recoveryOwnerAt(m.Owner, Owner{HolderID: b.VMOwner.HolderID, Epoch: b.VMOwner.Epoch}, m.Owner) {
		return fmt.Errorf("Host barrier differs from exact original Prepare/operation/closed selection")
	}
	return nil
}

func (p ServiceRetainedRecovery) validatePreparedHost(op Operation, leaf retainedBudgetLeaf) error {
	if earlyRetainedOperation(op) == nil {
		if p.PreparedHostBarrier != nil || p.ClosedCommandSHA256 != "" {
			return fmt.Errorf("uncompleted Prepare cannot borrow completed Host barrier proof")
		}
		return nil
	}
	prepared, digest, err := completedPreparedOperation(op)
	b := p.PreparedHostBarrier
	if err != nil || b == nil || b.ValidateFor(prepared, p.Incarnation) != nil || prepared.Identity != p.Root.Identity || prepared.VMBeforeMemoryBytes != leaf.facts.memory || b.PreparedSHA256 != digest || b.OperationSHA256 != p.OperationSHA256 || !lifecycleDigest(p.ClosedCommandSHA256) || b.ClosedSHA256 != p.ClosedCommandSHA256 || b.ClosedSequence != p.ClosedSequence || b.VMSequence < b.ClosedSequence || b.VMSequence >= p.RecordedSequence || !recoveryOwnerAt(p.Origin, Owner{HolderID: b.VMOwner.HolderID, Epoch: b.VMOwner.Epoch}, p.Origin) {
		return fmt.Errorf("completed Prepare retained recovery requires independent exact Node barrier")
	}
	return nil
}
