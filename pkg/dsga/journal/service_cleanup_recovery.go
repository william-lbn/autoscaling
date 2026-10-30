package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// ServiceCleanupRecovery只复核原取消清理中的资源/预算，不补写原Cleanup成功。
// 三类取消使用原实际容量、原完整目标或原旧容量，不能把取消目标当作实际值。
// 原Active/旧永久头/原开放未知保持不变；后继独立开放完整完成CAS才可结案。
// Finalization只保存旧完整预算的原Step，不嵌套旧Model/Lifecycle/恢复永久头，
// 因此连续取消不会把之前所有恢复上下文递归塞进新证明。
type ServiceCleanupRecovery struct {
	Origin           Owner                      `json:"origin"`
	RecordedSequence control.Uint64             `json:"recorded_sequence"`
	Incarnation      string                     `json:"incarnation"`
	OperationSHA256  string                     `json:"operation_sha256"`
	Kind             string                     `json:"kind"`
	Closed           control.AdmissionRequest   `json:"closed"`
	ClosedSequence   control.Uint64             `json:"closed_sequence"`
	Fence            control.FenceResponse      `json:"issuer_fence"`
	Current          control.SharedCostResponse `json:"current"`
	PGAggregate      budget.RootAggregate       `json:"pg_aggregate"`
	Finalization     *Step                      `json:"finalization,omitempty"`
}

// 实际容量恢复的永久头保留原完整操作及原Cleanup（包括Unknown），独立证明
// 与原步骤分开。它不是业务目标成功，也不提供重发旧开放/资源动作的权限。
type RecoveredCapacityOrigin struct {
	Original Operation              `json:"original"`
	Proof    ServiceCleanupRecovery `json:"proof"`
}

func cleanupKind(s Step) string {
	var actual budget.ReconciledAbortCommand
	if json.Unmarshal(s.Command, &actual) == nil {
		return "actual"
	}
	var committed budget.CommittedAbortCommand
	if json.Unmarshal(s.Command, &committed) == nil {
		return "committed"
	}
	return "unchanged"
}

// basis仅用于历史证明的结构核验，不是可派发Model，不调用Validate或网络。
// 预算Step来自恢复时的真实永久头；不制造原步骤Completed，不带旧恢复头，
// 外部来源真实性仍由生产执行器在保存/开放/完成前分别重新认证。
func (p ServiceCleanupRecovery) basis(op Operation) Model {
	id, instance, key := p.PGAggregate.Identity, op.Request.Key.Instance, op.Request.Key
	var m Model
	m.ProtocolVersion, m.VMUID, m.Incarnation, m.Sequence = 18, instance.VMUID, p.Incarnation, p.RecordedSequence
	m.Owner, m.HighWater, m.Active, m.ResourceIdentity, m.PGInstance = p.Origin, &key, &op, &id, &instance
	if p.Finalization != nil {
		at := control.Uint64(1)
		if op.PriorBudgetBoundary != nil {
			at = op.PriorBudgetBoundary.RecordedSequence
		}
		m.BudgetHeld = &HeldBudget{ProtocolVersion: 1, Identity: id, RecordedSequence: at, Finalization: cloneStep(*p.Finalization), Recovery: nil}
	}
	return m
}

func (p ServiceCleanupRecovery) boundary(op Operation) (CleanupServiceBoundary, error) {
	m := p.basis(op)
	if p.Kind != "actual" {
		return m.cleanupServiceBoundary()
	}
	var c budget.ReconciledAbortCommand
	s := op.Cleanup
	if p.Finalization != nil || s == nil || s.DispatchedBy == nil || json.Unmarshal(s.Command, &c) != nil || c.ValidateFor(s.DispatchedBy.Epoch) != nil || m.reconciledResourcePattern(op, c) != nil {
		return CleanupServiceBoundary{}, fmt.Errorf("cleanup recovery lost original actual-capacity plan")
	}
	// InitialOutside已逐字位于原Cleanup命令中。保存Proof时还须与当时真实
	// 永久头匹配；历史结构核验只核其原身份/完整值，不能向当前配置找替身。
	if i := c.Capacity.InitialOutside; i != nil && (i.Incarnation != p.Incarnation || i.PGAggregate.Identity != p.PGAggregate.Identity) {
		return CleanupServiceBoundary{}, fmt.Errorf("cleanup recovery changed original initial outside")
	}
	if s.Phase == Settled {
		var r budget.ReconciledAbortReceipt
		if s.Observation == nil || s.Observation.Result != Completed || json.Unmarshal(s.Observation.Data, &r) != nil || r.PGAggregate.Identity != p.PGAggregate.Identity || r.ValidateFor(c, s.DispatchedBy.Epoch, p.Origin.Epoch) != nil {
			return CleanupServiceBoundary{}, fmt.Errorf("cleanup recovery changed original settled actual receipt")
		}
	} else if s.Phase != Unconfirmed {
		return CleanupServiceBoundary{}, fmt.Errorf("cleanup recovery cannot cover rejected actual cleanup")
	}
	return CleanupServiceBoundary{Admission: c.Admission, CapacityGeneration: c.Capacity.CapacityGeneration, BudgetCommitmentID: c.Capacity.Request.BudgetCommitmentID}, nil
}

func (p ServiceCleanupRecovery) validate(op Operation) error {
	digest, err := operationClosureDigest(&op)
	if op.Validate() != nil || op.Cleanup == nil || op.Cleanup.DispatchedBy == nil || !op.AbortRequested || p.Origin.Validate() != nil || !validIncarnation(p.Incarnation) || p.RecordedSequence == 0 || p.Kind != cleanupKind(*op.Cleanup) || err != nil || p.OperationSHA256 != digest || p.basis(op).validateOwners(op) != nil || p.PGAggregate.Validate() != nil {
		return fmt.Errorf("cleanup recovery changed original cancellation or authenticated resource binding")
	}
	b, err := p.boundary(op)
	q, current := p.Closed, p.Current
	if err != nil || q.Validate() != nil || q.Open || q.Instance != op.Request.Key.Instance || q.IssuerEpoch > p.Origin.Epoch || q.ExpectedCapacityGeneration != b.CapacityGeneration || q.BudgetCommitmentID != b.BudgetCommitmentID || p.ClosedSequence == 0 || p.ClosedSequence >= p.RecordedSequence || current.ValidateFor(q.Instance) != nil || current.Observed.AdmissionOpen || current.Observed.AdmissionRevision != q.Revision || current.Observed.CapacityGeneration != q.ExpectedCapacityGeneration || p.Fence.Disposition != control.Fenced || p.Fence.ValidateFor(control.FenceRequest{ProtocolVersion: 1, Instance: q.Instance, IssuerEpoch: p.Origin.Epoch}, q.Instance) != nil || current.Observed.CapacityGeneration < p.Fence.Observed.CapacityGeneration || p.PGAggregate.Identity.VMUID != q.Instance.VMUID || p.PGAggregate.Identity.ComputeInstanceID != q.Instance.ComputeInstanceID {
		return fmt.Errorf("cleanup recovery lacks exact original closed budget and current owner fence")
	}
	if b.Admission != nil && q.ClosesOver(*b.Admission) != nil {
		return fmt.Errorf("cleanup recovery does not fence late original admission")
	}
	if p.Kind == "actual" {
		var c budget.ReconciledAbortCommand
		if json.Unmarshal(op.Cleanup.Command, &c) != nil || c.Capacity.ValidateCurrent(current, p.Origin.Epoch, c.Capacity.VMMemoryBytes) != nil || c.Capacity.PGAggregate != p.PGAggregate.Bound {
			return fmt.Errorf("cleanup recovery no longer covers exact original actual capacity")
		}
		hash, err := budget.RootDigest(p.PGAggregate)
		if err != nil || hash != c.Capacity.RootSHA256 {
			return fmt.Errorf("cleanup recovery replaced original actual root source")
		}
		return nil
	}
	if p.Finalization == nil || p.Finalization.Observation == nil {
		return fmt.Errorf("cleanup recovery lost exact retained complete budget")
	}
	var c budget.FinalizationCommand
	var f budget.Finalized
	if json.Unmarshal(p.Finalization.Command, &c) != nil || c.Validate() != nil || json.Unmarshal(p.Finalization.Observation.Data, &f) != nil || !reflect.DeepEqual(c.Finalized, f) || p.Finalization.Phase != Settled || p.Finalization.Observation.Result != Completed || f.Aggregate == nil || *f.Aggregate != p.PGAggregate.Bound || f.ValidateFor(f.Request, current, p.Origin.Epoch, f.RunnerPodName, f.RunnerRestartCount, uint64(f.Request.TargetVMMemoryBytes)) != nil {
		return fmt.Errorf("cleanup recovery changed retained finalization or current PG budget")
	}
	s := *p.Finalization
	hash, err := RequestDigest(f.Request)
	if err != nil || s.DispatchedBy == nil || s.DispatchedBy.Epoch > p.Origin.Epoch || s.DispatchedBy.Epoch < f.IssuerEpoch || s.Action != FinalizeBudget && s.Action != RecoverFinalization || s.Action == FinalizeBudget && s.DispatchedBy.Epoch != f.IssuerEpoch || s.TargetID != f.Request.Key.Instance.ComputeInstanceID {
		return fmt.Errorf("cleanup recovery lost exact original finalization owner")
	}
	original := Operation{Request: f.Request, RequestSHA256: hash, Origin: *s.DispatchedBy, AbortRequested: false, Steps: nil, Cleanup: nil, PriorBudgetBoundary: nil}
	if err := s.validate(original, len(plan(f.Request.Kind))-1, s.Action); err != nil {
		return err
	}
	return nil
}

func cloneServiceCleanupRecovery(p *ServiceCleanupRecovery) *ServiceCleanupRecovery {
	if p == nil {
		return nil
	}
	n := *p
	var shared control.AdmissionResponse
	shared.Observed = p.Fence.Observed
	n.Fence.Observed = cloneAdmissionResponse(&shared).Observed
	shared.Observed = p.Current.Observed
	n.Current.Observed = cloneAdmissionResponse(&shared).Observed
	n.PGAggregate.Bootstrap.Limits.HugePools = append(n.PGAggregate.Bootstrap.Limits.HugePools[:0:0], p.PGAggregate.Bootstrap.Limits.HugePools...)
	if p.PGAggregate.Held != nil {
		h := *p.PGAggregate.Held
		h.HugePools = append(h.HugePools[:0:0], p.PGAggregate.Held.HugePools...)
		n.PGAggregate.Held = &h
	}
	if p.Finalization != nil {
		s := cloneStep(*p.Finalization)
		n.Finalization = &s
	}
	return &n
}

func (m Model) validateServiceCleanupRecovery() error {
	p := m.ServiceClosure.CleanupRecovery
	if p == nil {
		return nil
	}
	c := m.ServiceClosure
	if m.serviceClosureVersion() != 18 || m.Active == nil || c.Pending() || p.RecordedSequence > m.Sequence || p.Origin.Epoch > m.Owner.Epoch || p.Origin.Epoch == m.Owner.Epoch && p.Origin != m.Owner || p.Incarnation != m.Incarnation || p.OperationSHA256 != c.OperationSHA256 || m.ResourceIdentity == nil || p.PGAggregate.Identity != *m.ResourceIdentity {
		return fmt.Errorf("cleanup recovery lost current original closure or owner")
	}
	_, _, closed, _ := c.Selection()
	at := c.RecordedSequence
	if c.Continuation != nil {
		at = c.Continuation.RecordedSequence
	}
	if p.Closed != closed || p.ClosedSequence != at || p.Kind != "actual" && (m.BudgetHeld == nil || !reflect.DeepEqual(p.Finalization, &m.BudgetHeld.Finalization)) {
		return fmt.Errorf("cleanup recovery replaced original retained budget or closure")
	}
	return p.validate(*m.Active)
}

// 只有显式管理恢复可保存此只读证明；普通Run不会自行选择恢复/开放。
// 同一UID/RV CAS固定完整原操作摘要、原资源来源、当前关闭和原预算。失败
// 保留输入；丢回复后重读原Proof，不重新结算数值或改原Cleanup的phase。
func (m Model) RecordServiceCleanupRecovery(actor Owner, selector string, fence control.FenceResponse, current control.SharedCostResponse, root budget.RootAggregate) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || m.serviceClosureVersion() != 18 || c.Pending() || c.CleanupRecovery != nil || c.Rearm != nil || c.Reopen != nil || m.Active == nil || m.Active.Cleanup == nil {
		return m, fmt.Errorf("cleanup recovery requires confirmed unchanged original cancellation closure")
	}
	_, _, closed, selected := c.Selection()
	if selector != selected {
		return m, fmt.Errorf("cleanup recovery selector differs from original latest closure")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	at := c.RecordedSequence
	if c.Continuation != nil {
		at = c.Continuation.RecordedSequence
	}
	p := &ServiceCleanupRecovery{Origin: actor, RecordedSequence: next.Sequence, Incarnation: m.Incarnation, OperationSHA256: c.OperationSHA256, Kind: cleanupKind(*m.Active.Cleanup), Closed: closed, ClosedSequence: at, Fence: fence, Current: current, PGAggregate: root, Finalization: nil}
	if p.Kind != "actual" && m.BudgetHeld != nil {
		s := cloneStep(m.BudgetHeld.Finalization)
		p.Finalization = &s
	}
	next.ServiceClosure.CleanupRecovery = cloneServiceCleanupRecovery(p)
	return next, next.Validate()
}

func (s *Store) RecordServiceCleanupRecovery(ctx context.Context, snapshot *Snapshot, actor Owner, selector string, fence control.FenceResponse, current control.SharedCostResponse, root budget.RootAggregate) (*Snapshot, error) {
	if err := s.checked(snapshot); err != nil {
		return nil, err
	}
	if !snapshot.Dispatchable() {
		return nil, fmt.Errorf("cleanup recovery requires original running VM")
	}
	next, err := snapshot.model.RecordServiceCleanupRecovery(actor, selector, fence, current, root)
	if err != nil {
		return nil, err
	}
	return s.apply(ctx, snapshot, next)
}

func (p *ServiceCleanupRecovery) UnmarshalJSON(raw []byte) error {
	type plain ServiceCleanupRecovery
	var next plain
	if err := control.UnmarshalObjectBounded(raw, &next, MaxRawBytes/4, 16384); err != nil {
		return err
	}
	*p = ServiceCleanupRecovery(next)
	return nil
}

func (p *RecoveredCapacityOrigin) UnmarshalJSON(raw []byte) error {
	type plain RecoveredCapacityOrigin
	var next plain
	if err := control.UnmarshalObjectBounded(raw, &next, MaxRawBytes/2, 16384); err != nil {
		return err
	}
	*p = RecoveredCapacityOrigin(next)
	return nil
}
