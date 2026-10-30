package journal

import (
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func sharedFromCoveredSnapshot(prior control.SharedCostResponse, current control.Snapshot) (control.SharedCostResponse, error) {
	prior.Observed = current
	total := uint64(prior.StartupMainUpperBytes)
	for _, a := range current.Arrays {
		if uint64(a.CommittedUpperBytes) > math.MaxUint64-total {
			return prior, fmt.Errorf("covered native shared cost overflows")
		}
		total += uint64(a.CommittedUpperBytes)
	}
	prior.CurrentMainAndArraysUpperBytes = control.Uint64(total)
	return prior, nil
}

func (p ServiceActualRecovery) held(op Operation) (*HeldCapacity, error) {
	if err := p.validate(op); err != nil {
		return nil, err
	}
	var noCleanup Step
	return &HeldCapacity{ProtocolVersion: 3, Identity: p.Root.Identity, Origin: op.Origin, RecordedSequence: p.RecordedSequence, Cleanup: noCleanup, Recovery: nil, ActiveRecovery: &RecoveredResourceOrigin{Original: cloneOperation(op), Proof: *cloneServiceActualRecovery(&p)}}, nil
}

func (m Model) validateActiveRecoveredCapacity() error {
	h := m.CapacityHeld
	var noCleanup Step
	if h == nil || h.ProtocolVersion != 3 || h.ActiveRecovery == nil || h.Recovery != nil || !reflect.DeepEqual(h.Cleanup, noCleanup) || m.InitialHeld != nil || m.BudgetHeld != nil || m.ResourceIdentity == nil || h.Identity != *m.ResourceIdentity || m.PGInstance == nil || m.HighWater == nil || h.RecordedSequence > m.Sequence || h.ActiveRecovery.Proof.RecordedSequence != h.RecordedSequence || h.ActiveRecovery.Proof.Incarnation != m.Incarnation || !recoveryOwnerAt(m.Owner, h.ActiveRecovery.Proof.Origin, m.Owner) || h.ActiveRecovery.Original.Request.Key.Instance != *m.PGInstance || compareKeys(h.ActiveRecovery.Original.Request.Key, *m.HighWater) > 0 {
		return fmt.Errorf("independent actual header lost exact original VM, owner or budget source")
	}
	want, err := h.ActiveRecovery.Proof.held(h.ActiveRecovery.Original)
	if err != nil || !reflect.DeepEqual(want, h) {
		return fmt.Errorf("independent actual header rewrote original resource effects or proof")
	}
	key := h.ActiveRecovery.Original.Request.Key
	check := func(op Operation, at control.Uint64, covered *CommittedServiceCoverage) error {
		order := compareKeys(op.Request.Key, key)
		if order == 0 && (at <= h.RecordedSequence || !reflect.DeepEqual(op, h.ActiveRecovery.Original) || covered == nil || !reflect.DeepEqual(covered.ActualRecovery, &h.ActiveRecovery.Proof)) {
			return fmt.Errorf("independent actual header lost original unknown operation coverage")
		}
		if at != 0 && (order < 0 && at >= h.RecordedSequence || order > 0 && at <= h.RecordedSequence) {
			return fmt.Errorf("independent actual header contradicts terminal sequence")
		}
		if err := rejectNewerFinalization(op, key); err != nil {
			return err
		}
		if order > 0 && covered != nil && (covered.ActualRecovery != nil || covered.CleanupRecovery != nil && covered.CleanupRecovery.Kind == "actual") {
			return fmt.Errorf("new actual settlement did not replace previous independent header")
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
			return fmt.Errorf("independent actual operation must already have closed coverage")
		}
		return check(*m.Active, 0, nil)
	}
	return nil
}

// 只从独立实际预算及刚认证的当前期限派生恢复终态。原业务目标可能只有
// 部分效果，因此用独立Outcome；原Resize/Guest/VM Unknown逐字留在Record。
func (r Record) actualClosingOutcome() (Outcome, error) {
	c := r.ServiceCoverage
	p := c.ActualRecovery
	q := c.Request
	if p == nil || c.Finalization != nil || c.CleanupRecovery != nil || r.Outcome != RecoveredWithActualBudget || p.validate(r.Operation) != nil || p.RecordedSequence >= r.ClosedSequence || !recoveryOwnerAt(c.Origin, p.Origin, c.Origin) || q.Validate() != nil || !q.Open || q.Instance != r.Operation.Request.Key.Instance || q.IssuerEpoch < p.Origin.Epoch || q.IssuerEpoch > c.Origin.Epoch || q.ExpectedCapacityGeneration != p.Capacity.CapacityGeneration || q.BudgetCommitmentID != r.Operation.Request.BudgetCommitmentID || q.Revision <= p.Closed.Revision || c.FinalizationSHA256 != p.CapacitySHA256 || !c.Receipt.Confirms() || c.Receipt.ValidateFor(q, q.Instance, c.Query) != nil || !validLiveReopen(c.Current, q, c.Origin) || c.Receipt.Observed.IssuerFloorEpoch > c.Origin.Epoch || c.Receipt.Observed.CapacityGeneration != q.ExpectedCapacityGeneration {
		return "", fmt.Errorf("actual resource recovery lacks exact independent live admission coverage")
	}
	shared, err := sharedFromCoveredSnapshot(p.Current, c.Current.Observed)
	if err != nil || p.Capacity.ValidateCurrent(shared, c.Origin.Epoch, p.Capacity.VMMemoryBytes) != nil {
		return "", fmt.Errorf("actual resource completion exceeds retained native budget")
	}
	return RecoveredWithActualBudget, nil
}

func (m Model) actualCompletionRecord(actor Owner, current control.AdmissionLeaseResponse, sequence control.Uint64) (*Record, error) {
	c := m.ServiceClosure
	if c == nil || c.ActualRecovery == nil || c.Reopen == nil || c.Reopen.Phase != Settled || c.Reopen.Receipt == nil || c.Reopen.Closing != nil || m.Active == nil {
		return nil, fmt.Errorf("actual completion requires original confirmed independent reopen")
	}
	p, q := c.ActualRecovery, c.Reopen
	coverage := &CommittedServiceCoverage{Origin: actor, FinalizationSHA256: p.CapacitySHA256, Request: q.Request, Receipt: *cloneAdmissionResponse(q.Receipt), Query: q.Query, Current: cloneLeaseResponse(current), Finalization: nil, CleanupRecovery: nil, ActualRecovery: cloneServiceActualRecovery(p)}
	r := &Record{Operation: cloneOperation(*m.Active), Outcome: RecoveredWithActualBudget, ClosedSequence: sequence, ServiceCoverage: coverage}
	_, err := r.closingOutcome()
	return r, err
}

// EffectiveServiceCapacity只派生当前预算的准确容量代，不改旧关闭的expected。
// 保存实际预算前仍取原关闭；保存后晚到PG提交的下一代必须被完整预算覆盖。
func (m Model) effectiveServiceCapacity() control.Uint64 {
	if m.ServiceClosure == nil {
		return 0
	}
	if p := m.ServiceClosure.ActualRecovery; p != nil {
		return p.Capacity.CapacityGeneration
	}
	_, _, q, _ := m.ServiceClosure.Selection()
	return q.ExpectedCapacityGeneration
}

func (m Model) EffectiveServiceCapacity() (control.Uint64, error) {
	if err := m.Validate(); err != nil {
		return 0, err
	}
	if m.ServiceClosure == nil {
		return 0, fmt.Errorf("no retained service closure")
	}
	return m.effectiveServiceCapacity(), nil
}
