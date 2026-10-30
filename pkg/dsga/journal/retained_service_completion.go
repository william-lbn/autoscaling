package journal

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// RetainedServiceCoverage永久保留最近早期操作的完整Unknown及独立覆盖。
// 不含旧Model/生命周期；下一次只保Previous单层退休边界，因此连续恢复
// 不会递归累积Context。普通预算替换不删除它，只解除其当前来源身份。
type RetainedServiceCoverage struct {
	Cycle    control.Uint64             `json:"cycle"`
	Record   Record                     `json:"record"`
	Previous *RetainedServiceRetirement `json:"previous"`
}

type RetainedServiceRetirement struct {
	Cycle          control.Uint64           `json:"cycle"`
	Key            control.Key              `json:"key"`
	ClosedSequence control.Uint64           `json:"closed_sequence"`
	EvidenceSHA256 string                   `json:"evidence_sha256"`
	Open           control.AdmissionRequest `json:"open"`
}

func cloneRetainedCoverage(p *RetainedServiceCoverage) *RetainedServiceCoverage {
	if p == nil {
		return nil
	}
	n := *p
	n.Record.Operation = cloneOperation(p.Record.Operation)
	n.Record.ServiceCoverage = cloneCommittedCoverage(p.Record.ServiceCoverage)
	if p.Previous != nil {
		v := *p.Previous
		n.Previous = &v
	}
	return &n
}

func (r Record) retainedClosingOutcome() (Outcome, error) {
	c := r.ServiceCoverage
	p := c.RetainedRecovery
	q := c.Request
	if p == nil || c.Finalization != nil || c.ActualRecovery != nil || c.CleanupRecovery != nil || r.Outcome != RecoveredWithRetainedBudget || p.ValidateFor(r.Operation) != nil || p.RecordedSequence >= r.ClosedSequence || !recoveryOwnerAt(c.Origin, p.Origin, c.Origin) {
		return "", fmt.Errorf("retained completion lost independent original operation coverage")
	}
	facts, err := p.Source.facts()
	if err != nil || q.Validate() != nil || !q.Open || q.Instance != facts.instance || q.IssuerEpoch < p.Origin.Epoch || q.IssuerEpoch > c.Origin.Epoch || q.BudgetCommitmentID != facts.budgetID || q.ExpectedCapacityGeneration != facts.generation || q.Revision <= p.Closed.Revision || c.FinalizationSHA256 != p.Source.SourceSHA256 || !c.Receipt.Confirms() || c.Receipt.ValidateFor(q, q.Instance, c.Query) != nil || !validLiveReopen(c.Current, q, c.Origin) || c.Receipt.Observed.IssuerFloorEpoch > c.Origin.Epoch {
		return "", fmt.Errorf("retained completion lacks exact live old-budget reopen")
	}
	shared, err := sharedFromCoveredSnapshot(p.Current, c.Current.Observed)
	if err != nil || p.ValidateCurrent(shared, p.Root, c.Origin, facts.memory, facts.usable) != nil {
		return "", fmt.Errorf("retained completion current resources exceed original permanent budget")
	}
	return RecoveredWithRetainedBudget, nil
}

func (m Model) retainedCompletionRecord(actor Owner, current control.AdmissionLeaseResponse, sequence control.Uint64) (*Record, error) {
	c := m.ServiceClosure
	if c == nil || c.RetainedRecovery == nil || c.Reopen == nil || c.Reopen.Phase != Settled || c.Reopen.Receipt == nil || c.Reopen.Closing != nil || m.Active == nil {
		return nil, fmt.Errorf("retained completion requires confirmed original reopen")
	}
	p, r := c.RetainedRecovery, c.Reopen
	covered := &CommittedServiceCoverage{Origin: actor, FinalizationSHA256: p.Source.SourceSHA256, Request: r.Request, Receipt: *cloneAdmissionResponse(r.Receipt), Query: r.Query, Current: cloneLeaseResponse(current), Finalization: nil, CleanupRecovery: nil, ActualRecovery: nil, RetainedRecovery: cloneServiceRetainedRecovery(p)}
	out := &Record{Operation: cloneOperation(*m.Active), Outcome: RecoveredWithRetainedBudget, ClosedSequence: sequence, ServiceCoverage: covered}
	_, err := out.closingOutcome()
	return out, err
}

func (m *Model) closeRetainedBudget(p ServiceRetainedRecovery) error {
	if m.Active == nil || p.ValidateFor(*m.Active) != nil || m.checkRetainedSource(p.Source) != nil {
		return fmt.Errorf("retained completion lost unchanged original permanent header")
	}
	if p.Source.Initial != nil {
		if m.BudgetBoundary != nil {
			return fmt.Errorf("retained initial completion cannot invent resize closure")
		}
		return nil
	}
	if p.Anchor == nil {
		return fmt.Errorf("retained completion lost original closed anchor")
	}
	b := *p.Anchor
	b.ThroughKey, b.ThroughSequence = m.Active.Request.Key, m.Sequence
	m.BudgetBoundary = &b
	return nil
}

func (m *Model) installRetainedCoverage(record Record) error {
	next := &RetainedServiceCoverage{Cycle: 1, Record: record, Previous: nil}
	if old := m.RetainedCoverage; old != nil {
		if old.Cycle == control.Uint64(math.MaxUint64) {
			return fmt.Errorf("retained coverage counter exhausted")
		}
		raw, err := json.Marshal(old)
		if err != nil {
			return err
		}
		next.Cycle = old.Cycle + 1
		next.Previous = &RetainedServiceRetirement{Cycle: old.Cycle, Key: old.Record.Operation.Request.Key, ClosedSequence: old.Record.ClosedSequence, EvidenceSHA256: rawDigest(raw), Open: old.Record.ServiceCoverage.Request}
	}
	m.RetainedCoverage = cloneRetainedCoverage(next)
	return nil
}

func (m Model) validateRetainedCoverage() error {
	h := m.RetainedCoverage
	if h == nil {
		if m.ProtocolVersion == 22 {
			return fmt.Errorf("retained journal lost required permanent original coverage")
		}
		return nil
	}
	if m.ProtocolVersion != 22 || m.ServiceLifecycle == nil || m.PGInstance == nil || m.ResourceIdentity == nil || h.Cycle == 0 || h.Record.ServiceCoverage == nil || h.Record.ServiceCoverage.RetainedRecovery == nil {
		return fmt.Errorf("retained permanent coverage requires explicit journal format")
	}
	r := h.Record
	p := r.ServiceCoverage.RetainedRecovery
	if _, err := r.closingOutcome(); err != nil {
		return err
	}
	if r.ClosedSequence > m.Sequence || r.ClosedSequence > m.ServiceLifecycle.RecordedSequence || m.HighWater == nil || r.Operation.Request.Key.Instance != *m.PGInstance || compareKeys(r.Operation.Request.Key, *m.HighWater) > 0 || compareKeys(r.Operation.Request.Key, *m.HighWater) == 0 && r.Operation.Request.Key != *m.HighWater || !recoveryOwnerAt(m.Owner, r.ServiceCoverage.Origin, m.Owner) || p.Incarnation != m.Incarnation || p.Root.Identity != *m.ResourceIdentity {
		return fmt.Errorf("retained coverage lost original VM, high water or completion sequence")
	}
	source, err := m.currentRetainedSource()
	if err != nil {
		return err
	}
	if source.RecordedSequence == p.Source.RecordedSequence {
		if !reflect.DeepEqual(source, p.Source) {
			return fmt.Errorf("retained coverage changed exact still-current original header")
		}
	} else if source.RecordedSequence <= r.ClosedSequence {
		return fmt.Errorf("retained source replacement lacks strictly later permanent settlement")
	}
	if prev := h.Previous; prev != nil {
		if prev.Cycle == 0 || prev.Cycle == control.Uint64(math.MaxUint64) || h.Cycle != prev.Cycle+1 || prev.Key.Validate() != nil || prev.Key.Instance != r.Operation.Request.Key.Instance || compareKeys(prev.Key, r.Operation.Request.Key) >= 0 || prev.ClosedSequence == 0 || prev.ClosedSequence >= p.ClosedStartedSequence || !lifecycleDigest(prev.EvidenceSHA256) || prev.Open.Validate() != nil || !prev.Open.Open || prev.Open.Instance != r.Operation.Request.Key.Instance || prev.Open.IssuerEpoch < prev.Key.IssuerEpoch || prev.Open.IssuerEpoch > p.ClosedOrigin.Epoch || prev.Open.Revision >= p.Closed.Revision || prev.Open.ExpectedCapacityGeneration > p.Closed.ExpectedCapacityGeneration {
			return fmt.Errorf("retained coverage retirement lacks strict superseding native boundary")
		}
	} else if h.Cycle != 1 {
		return fmt.Errorf("retained coverage lost required previous retirement")
	}
	found := false
	for _, history := range m.History {
		if history.Operation.Request.Key == r.Operation.Request.Key {
			found = true
			if !reflect.DeepEqual(history, r) {
				return fmt.Errorf("retained permanent coverage differs from complete original history")
			}
		}
	}
	if !found && (m.ExpiredResults == 0 || compareKeys(*m.HighWater, r.Operation.Request.Key) <= 0) {
		return fmt.Errorf("retained coverage lost original terminal before actual history eviction")
	}
	return nil
}

func (m Model) retainedServiceSource() (RetainedBudgetSource, error) {
	var none RetainedBudgetSource
	c := m.ServiceClosure
	if c == nil || m.serviceClosureVersion() != 16 || m.Active == nil || earlyRetainedOperation(*m.Active) != nil || c.ActualRecovery != nil || c.Finalization != nil || c.CleanupRecovery != nil || c.ResourceRecovery != nil || c.PGRecovery != nil || c.PGRejection != nil || c.VMRecovery != nil {
		return none, fmt.Errorf("retained recovery requires original early category16 closure")
	}
	return m.currentRetainedSource()
}

// RetainedServiceSource统一collector资格，不清除Prepare未知或Abort，不派发。
func (m Model) RetainedServiceSource() (RetainedBudgetSource, error) {
	var none RetainedBudgetSource
	if err := m.Validate(); err != nil {
		return none, err
	}
	return m.retainedServiceSource()
}

func (h *RetainedServiceCoverage) UnmarshalJSON(raw []byte) error {
	type plain RetainedServiceCoverage
	var v plain
	if err := control.UnmarshalObjectBounded(raw, &v, MaxRawBytes/2, 16384); err != nil {
		return err
	}
	*h = RetainedServiceCoverage(v)
	return nil
}

func (h *RetainedServiceRetirement) UnmarshalJSON(raw []byte) error {
	type plain RetainedServiceRetirement
	var v plain
	if err := control.UnmarshalObject(raw, &v); err != nil {
		return err
	}
	*h = RetainedServiceRetirement(v)
	return nil
}

// 生命周期冻结不嵌旧slot，但必须显式保留其摘要；新独立恢复以Previous
// 退休，普通后继生命周期必须继续持有完全相同slot，不能静默删Unknown。
func (m Model) validateContextRetained(l *ServiceLifecycle) error {
	h := m.RetainedCoverage
	if l.ContextRetainedSHA256 == "" {
		if h != nil && (h.Cycle != 1 || h.Record.ClosedSequence != l.RecordedSequence) {
			return fmt.Errorf("lifecycle omitted retained original coverage without retirement")
		}
		return nil
	}
	if h == nil || !lifecycleDigest(l.ContextRetainedSHA256) {
		return fmt.Errorf("lifecycle lost original retained coverage digest")
	}
	raw, err := json.Marshal(h)
	if err != nil {
		return err
	}
	if rawDigest(raw) == l.ContextRetainedSHA256 {
		return nil
	}
	if h.Previous == nil || h.Record.ClosedSequence != l.RecordedSequence || h.Previous.EvidenceSHA256 != l.ContextRetainedSHA256 {
		return fmt.Errorf("lifecycle changed retained coverage without explicit previous retirement")
	}
	return nil
}
