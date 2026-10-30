package journal

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

func stableClosureVersion(version uint32) bool {
	return version >= 10 && version <= 15 || version == 18
}

func cloneRecoveredCapacity(p *RecoveredCapacityOrigin) *RecoveredCapacityOrigin {
	if p == nil {
		return nil
	}
	return &RecoveredCapacityOrigin{Original: cloneOperation(p.Original), Proof: *cloneServiceCleanupRecovery(&p.Proof)}
}

func (p ServiceCleanupRecovery) retainedHash(op Operation) string {
	if p.Kind == "actual" && op.Cleanup != nil {
		return op.Cleanup.CommandSHA256
	}
	if p.Finalization != nil {
		return p.Finalization.CommandSHA256
	}
	return ""
}

// 原命令与原相位逐字保留，独立预算不使用虚构的Cleanup Completed。
// Proof只包含有界资源事实与原Step，Original没有旧Model或旧永久头，
// 后续生命周期可替换当前Proof而不会递归累计历史预算。
func (p ServiceCleanupRecovery) held(op Operation) (*HeldCapacity, error) {
	if p.Kind != "actual" || p.validate(op) != nil || op.Cleanup == nil {
		return nil, fmt.Errorf("recovered actual header requires exact independently audited cancellation")
	}
	return &HeldCapacity{ProtocolVersion: 2, Identity: p.PGAggregate.Identity, Origin: op.Origin, RecordedSequence: p.RecordedSequence, Cleanup: cloneStep(*op.Cleanup), Recovery: &RecoveredCapacityOrigin{Original: cloneOperation(op), Proof: *cloneServiceCleanupRecovery(&p)}}, nil
}

func (m Model) validateRecoveredCapacity() error {
	h := m.CapacityHeld
	if h == nil || h.ProtocolVersion != 2 || h.Recovery == nil || m.ResourceIdentity == nil || h.Identity != *m.ResourceIdentity || m.PGInstance == nil || m.HighWater == nil || h.RecordedSequence > m.Sequence || h.Recovery.Proof.RecordedSequence != h.RecordedSequence || h.Recovery.Proof.Incarnation != m.Incarnation || h.Recovery.Proof.Origin.Epoch > m.Owner.Epoch || h.Recovery.Proof.Origin.Epoch == m.Owner.Epoch && h.Recovery.Proof.Origin != m.Owner || h.Recovery.Original.Request.Key.Instance != *m.PGInstance || compareKeys(h.Recovery.Original.Request.Key, *m.HighWater) > 0 {
		return fmt.Errorf("recovered actual header lost original VM, owner or sequence")
	}
	want, err := h.Recovery.Proof.held(h.Recovery.Original)
	if err != nil || !reflect.DeepEqual(want, h) {
		return fmt.Errorf("recovered actual header rewrote original cancellation or proof")
	}
	key := h.Recovery.Original.Request.Key
	check := func(op Operation, closed control.Uint64, coverage *CommittedServiceCoverage) error {
		order := compareKeys(op.Request.Key, key)
		if order == 0 && (closed <= h.RecordedSequence || !reflect.DeepEqual(op, h.Recovery.Original) || coverage == nil || !reflect.DeepEqual(coverage.CleanupRecovery, &h.Recovery.Proof)) {
			return fmt.Errorf("recovered actual budget differs from complete original cancellation coverage")
		}
		if closed != 0 && (order < 0 && closed >= h.RecordedSequence || order > 0 && closed <= h.RecordedSequence) {
			return fmt.Errorf("recovered actual budget sequence contradicts terminal history")
		}
		if err := rejectNewerFinalization(op, key); err != nil {
			return err
		}
		if order > 0 {
			var actual budget.ReconciledAbortCommand
			if op.Cleanup != nil && successful(*op.Cleanup) && json.Unmarshal(op.Cleanup.Command, &actual) == nil || coverage != nil && coverage.CleanupRecovery != nil && coverage.CleanupRecovery.Kind == "actual" {
				return fmt.Errorf("new actual settlement has no atomic replacement of recovered budget")
			}
		}
		return nil
	}
	for _, r := range m.History {
		if err := check(r.Operation, r.ClosedSequence, r.ServiceCoverage); err != nil {
			return err
		}
	}
	if m.Active != nil {
		if compareKeys(m.Active.Request.Key, key) <= 0 {
			return fmt.Errorf("recovered actual operation must already be independently closed")
		}
		return check(*m.Active, 0, nil)
	}
	return nil
}

// 期限观察携带当前数组快照，重新计算其保守成本，不能只按容量代次不变
// 就让未知尾部/成本上涨穿过完成CAS。这里仍只核结构，认证读由执行器负责。
func (p ServiceCleanupRecovery) validateCoveredCurrent(op Operation, current control.Snapshot, epoch control.Uint64) error {
	shared := p.Current
	shared.Observed = current
	total := uint64(shared.StartupMainUpperBytes)
	for _, array := range current.Arrays {
		if uint64(array.CommittedUpperBytes) > math.MaxUint64-total {
			return fmt.Errorf("cancelled completion current PG cost overflows")
		}
		total += uint64(array.CommittedUpperBytes)
	}
	shared.CurrentMainAndArraysUpperBytes = control.Uint64(total)
	if p.Kind == "actual" {
		var c budget.ReconciledAbortCommand
		if op.Cleanup == nil || json.Unmarshal(op.Cleanup.Command, &c) != nil {
			return fmt.Errorf("cancelled completion lost original actual command")
		}
		return c.Capacity.ValidateCurrent(shared, epoch, c.Capacity.VMMemoryBytes)
	}
	var f budget.Finalized
	if p.Finalization == nil || p.Finalization.Observation == nil || json.Unmarshal(p.Finalization.Observation.Data, &f) != nil {
		return fmt.Errorf("cancelled completion lost retained complete budget")
	}
	return f.ValidateFor(f.Request, shared, epoch, f.RunnerPodName, f.RunnerRestartCount, uint64(f.Request.TargetVMMemoryBytes))
}

func (r Record) cancelledClosingOutcome() (Outcome, error) {
	c := r.ServiceCoverage
	p := c.CleanupRecovery
	q := c.Request
	if p == nil || c.Finalization != nil || r.Outcome != RecoveredAfterCancellation || p.validate(r.Operation) != nil || p.RecordedSequence >= r.ClosedSequence || c.Origin.Validate() != nil || p.Origin.Epoch > c.Origin.Epoch || p.Origin.Epoch == c.Origin.Epoch && p.Origin != c.Origin || q.Validate() != nil || !q.Open || q.Instance != r.Operation.Request.Key.Instance || q.IssuerEpoch < p.Origin.Epoch || q.IssuerEpoch > c.Origin.Epoch || q.ExpectedCapacityGeneration != p.Closed.ExpectedCapacityGeneration || q.BudgetCommitmentID != p.Closed.BudgetCommitmentID || q.Revision <= p.Closed.Revision || c.FinalizationSHA256 != p.retainedHash(r.Operation) || !c.Receipt.Confirms() || c.Receipt.ValidateFor(q, q.Instance, c.Query) != nil || !validLiveReopen(c.Current, q, c.Origin) || c.Receipt.Observed.IssuerFloorEpoch > c.Origin.Epoch || c.Receipt.Observed.CapacityGeneration != q.ExpectedCapacityGeneration {
		return "", fmt.Errorf("cancelled completion lacks independent exact live budget coverage")
	}
	if err := p.validateCoveredCurrent(r.Operation, c.Current.Observed, c.Origin.Epoch); err != nil {
		return "", err
	}
	return RecoveredAfterCancellation, nil
}

func (m Model) cancelledCompletionRecord(actor Owner, current control.AdmissionLeaseResponse, sequence control.Uint64) (*Record, error) {
	c := m.ServiceClosure
	if c == nil || c.CleanupRecovery == nil || c.Reopen == nil || c.Reopen.Phase != Settled || c.Reopen.Receipt == nil || c.Reopen.Closing != nil || m.Active == nil {
		return nil, fmt.Errorf("cancelled completion requires original confirmed independent reopen")
	}
	p, opening := c.CleanupRecovery, c.Reopen
	op := cloneOperation(*m.Active)
	coverage := &CommittedServiceCoverage{Origin: actor, FinalizationSHA256: p.retainedHash(op), Request: opening.Request, Receipt: *cloneAdmissionResponse(opening.Receipt), Query: opening.Query, Current: cloneLeaseResponse(current), Finalization: nil, CleanupRecovery: cloneServiceCleanupRecovery(p)}
	r := &Record{Operation: op, Outcome: RecoveredAfterCancellation, ClosedSequence: sequence, ServiceCoverage: coverage}
	_, err := r.closingOutcome()
	return r, err
}

func (m *Model) closeCancelledBudget(p ServiceCleanupRecovery) error {
	if m.Active == nil || p.validate(*m.Active) != nil {
		return fmt.Errorf("cancelled budget closure requires unchanged original operation")
	}
	if p.Kind == "unchanged" {
		if m.BudgetHeld == nil || m.Active.PriorBudgetBoundary == nil || !reflect.DeepEqual(p.Finalization, &m.BudgetHeld.Finalization) {
			return fmt.Errorf("cancelled old budget closure changed retained complete header")
		}
		prior := *m.Active.PriorBudgetBoundary
		prior.ThroughKey, prior.ThroughSequence = m.Active.Request.Key, m.Sequence
		m.BudgetBoundary = &prior
		return nil
	}
	request, err := m.retainedRequest()
	hash, _ := m.budgetRecord()
	if err != nil || request != m.Active.Request || p.retainedHash(*m.Active) != hash {
		return fmt.Errorf("cancelled complete budget closure lost exact retained request")
	}
	m.BudgetBoundary = m.boundaryFor(request, m.Sequence)
	return nil
}
