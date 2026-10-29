package journal

import (
	"context"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ServiceReopenClosure 独立保存新开放之后来源异常或失租接管的安全关闭。
// Close 复用原生完整原意图关闭及接续合同；它自己的 Rearm/Reopen/资源
// Active 必须不存在，不能形成递归服务生命周期或收养别的资源未知。
// Before 固定首次选择，LatestBefore 仅固定最近更高 owner 接续观察。
// 原开放无论 Unconfirmed/Settled 都逐字保留，关门不等于排空或预算释放。
type ServiceReopenClosure struct {
	Before       control.AdmissionLeaseResponse  `json:"before"`
	Close        *ServiceClosure                 `json:"close"`
	LatestBefore *control.AdmissionLeaseResponse `json:"latest_before"`
}

func cloneServiceReopenClosure(original *ServiceReopenClosure) *ServiceReopenClosure {
	if original == nil {
		return nil
	}
	cloned := *original
	cloned.Before = cloneLeaseResponse(original.Before)
	if original.LatestBefore != nil {
		before := cloneLeaseResponse(*original.LatestBefore)
		cloned.LatestBefore = &before
	}
	if original.Close != nil {
		// 借已存在的完整关闭深拷贝，载体只有该字段，无预算/历史复制。
		// 合法子关闭没有 Reopen，再关闭不能递归生长；Validate 在 next
		// 和 decode 返回前先核此限制，生产不会对未校验来源调用本函数。
		var carrier Model
		carrier.ServiceClosure = original.Close
		cloned.Close = carrier.Clone().ServiceClosure
	}
	return &cloned
}

func reopenCloseBefore(b control.AdmissionLeaseResponse, owner Owner, original control.AdmissionRequest) error {
	if b.ValidateFor(b.Request, original.Instance) != nil || b.Request.Action != "observe" || !b.Armed || b.Request.IssuerEpoch != owner.Epoch || b.Observed.IssuerFloorEpoch != owner.Epoch || b.LeaseEpoch > owner.Epoch || b.Observed.CapacityGeneration != original.ExpectedCapacityGeneration || max(b.Observed.AdmissionRevision, original.Revision) == control.Uint64(math.MaxUint64) {
		return fmt.Errorf("reopen closure lacks original native boot, floor or capacity")
	}
	return nil
}

func (m Model) validateServiceReopenClosure() error {
	r := m.ServiceClosure.Reopen
	stage := r.SafetyStage
	if r.Rearm != nil {
		if stage != "rearming" || r.Closing == nil || r.Closing.Close == nil || r.Closing.Close.Pending() {
			return fmt.Errorf("reopen lease recovery lost complete reclosure or required safety stage")
		}
		// 期限恢复叠加在已完成的关闭上；局部判别符只复用原关闭校验，
		// 不改原记录，更不把 rearming 投影为可开放的 opening。
		stage = "closing"
		if r.Closing.Close.Continuation != nil {
			stage = "continuing"
		}
	}
	if r.Closing == nil {
		if stage != "opening" {
			return fmt.Errorf("reopen recovery lost selected safety closure")
		}
		return nil
	}
	a, c := r.Closing, r.Closing.Close
	if c == nil || stage != "closing" && stage != "continuing" || c.Rearm != nil || c.Reopen != nil || c.OperationSHA256 != "" || c.Original == nil || *c.Original != r.Request || c.Reason != "reopen_source_unconfirmed" || c.Origin.Validate() != nil || c.Origin.Epoch < r.Origin.Epoch || c.Origin.Epoch == r.Origin.Epoch && c.Origin != r.Origin || c.Origin.Epoch > m.Owner.Epoch || c.Origin.Epoch == m.Owner.Epoch && c.Origin != m.Owner || c.StartedSequence <= max(r.StartedSequence, r.RecordedSequence) || c.StartedSequence > m.Sequence || c.Request.ClosesOver(r.Request) != nil || c.Request.IssuerEpoch != c.Origin.Epoch || c.Request.ExpectedCapacityGeneration != r.Request.ExpectedCapacityGeneration || c.Request.BudgetCommitmentID != r.Request.BudgetCommitmentID || c.Receipt != nil {
		return fmt.Errorf("reopen closure changed original opening or stable service boundary")
	}
	digest, err := c.Request.ContextSHA256()
	if err != nil || digest != c.CommandSHA256 || reopenCloseBefore(a.Before, c.Origin, r.Request) != nil || c.Request.Revision != max(a.Before.Observed.AdmissionRevision, r.Request.Revision)+1 {
		return fmt.Errorf("reopen closure changed original close selection")
	}
	switch c.Phase {
	case Unconfirmed:
		if c.RecordedSequence != 0 || c.Query || c.ClosureReceipt != nil {
			return fmt.Errorf("unknown reopen closure contains completion")
		}
	case Settled:
		request := control.AdmissionCloseRequest{ProtocolVersion: 1, Original: r.Request, Close: c.Request}
		if c.RecordedSequence <= c.StartedSequence || c.RecordedSequence > m.Sequence || c.ClosureReceipt == nil || c.ClosureReceipt.ValidateFor(request, *m.PGInstance, c.Query) != nil || c.ClosureReceipt.ClosedBy.Observed.IssuerFloorEpoch > m.Owner.Epoch || c.ClosureReceipt.ClosedBy.Observed.CapacityGeneration != r.Request.ExpectedCapacityGeneration {
			return fmt.Errorf("reopen closure lacks exact original closed proof")
		}
	default:
		return fmt.Errorf("invalid reopen closure phase")
	}
	if c.Continuation == nil {
		if a.LatestBefore != nil || stage != "closing" {
			return fmt.Errorf("reopen closure invented continuation observation")
		}
		return nil
	}
	n := c.Continuation
	if stage != "continuing" || a.LatestBefore == nil || reopenCloseBefore(*a.LatestBefore, n.Origin, r.Request) != nil || max(a.LatestBefore.Observed.AdmissionRevision, n.Request.PreviousClose.Revision) == control.Uint64(math.MaxUint64) || n.Request.Close.Revision != max(a.LatestBefore.Observed.AdmissionRevision, n.Request.PreviousClose.Revision)+1 || n.Receipt != nil && n.Receipt.ClosedBy.Observed.CapacityGeneration != r.Request.ExpectedCapacityGeneration {
		return fmt.Errorf("reopen closure continuation lost original native selection")
	}
	// 只复用独立接续校验器：它认证原意图、完整直接前驱、严格更高
	// owner/序列和完整回执，不调用首次业务/预算校验或临时模型 Validate。
	// 此处稳定同容量，绝不借 v17 跨容量许可扩宽新开放的资源边界。
	carrier := m
	carrier.ProtocolVersion, carrier.ServiceClosure = 12, c
	return carrier.validateServiceClosureContinuation()
}

func (m Model) StartServiceReopenClosure(actor Owner, selector string, before control.AdmissionLeaseResponse) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	if m.ServiceClosure == nil || m.ServiceClosure.Reopen == nil || m.ServiceClosure.Reopen.Closing != nil || m.ServiceClosure.Reopen.CommandSHA256 != selector {
		return m, fmt.Errorf("reopen closure requires exact original opening without another close")
	}
	r := m.ServiceClosure.Reopen
	if err := reopenCloseBefore(before, actor, r.Request); err != nil {
		return m, err
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	original := r.Request
	request := original
	request.Open, request.IssuerEpoch, request.Revision = false, actor.Epoch, max(before.Observed.AdmissionRevision, original.Revision)+1
	digest, err := request.ContextSHA256()
	if err != nil {
		return m, err
	}
	closing := &ServiceClosure{Origin: actor, StartedSequence: next.Sequence, Reason: "reopen_source_unconfirmed", Request: request, CommandSHA256: digest, Phase: Unconfirmed, RecordedSequence: 0, Query: false, Receipt: nil, Continuation: nil, Original: &original, ClosureReceipt: nil, OperationSHA256: "", Rearm: nil, Reopen: nil}
	next.ServiceClosure.Reopen.Closing = &ServiceReopenClosure{Before: cloneLeaseResponse(before), Close: closing, LatestBefore: nil}
	next.ServiceClosure.Reopen.SafetyStage = "closing"
	return next, next.Validate()
}

func (m Model) RecordServiceReopenClosure(actor Owner, fact control.AdmissionClosure, query bool) (Model, error) {
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	if next.ServiceClosure == nil || next.ServiceClosure.Reopen == nil || next.ServiceClosure.Reopen.Closing == nil {
		return m, fmt.Errorf("original reopen closure is absent")
	}
	c := next.ServiceClosure.Reopen.Closing.Close
	if c.Continuation != nil || c.Phase != Unconfirmed || !query && c.Origin != actor {
		return m, fmt.Errorf("reopen closure is not original unknown")
	}
	proof := fact
	proof.ClosedBy = *cloneAdmissionResponse(&fact.ClosedBy)
	c.Phase, c.Query, c.RecordedSequence, c.ClosureReceipt = Settled, query, next.Sequence, &proof
	return next, next.Validate()
}

// 失租后只允许明确更高 owner 的关闭接续。首次关闭和原开放永不改写，
// 最近/直接前驱保持有界。普通重试不能换 revision，须读回或原 owner
// 显式原重放；这里仍只返回模型，Store 确认之前没有发送许可。
func (m Model) ContinueServiceReopenClosure(actor Owner, selector string, before control.AdmissionLeaseResponse) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	if m.ServiceClosure == nil || m.ServiceClosure.Reopen == nil || m.ServiceClosure.Reopen.Closing == nil {
		return m, fmt.Errorf("original reopen closure is absent")
	}
	r, a := m.ServiceClosure.Reopen, m.ServiceClosure.Reopen.Closing
	c := a.Close
	origin, sequence, previous, selected := c.Selection()
	if !c.Pending() || actor.Epoch <= origin.Epoch || selector != selected || reopenCloseBefore(before, actor, r.Request) != nil || max(before.Observed.AdmissionRevision, previous.Revision) == control.Uint64(math.MaxUint64) {
		return m, fmt.Errorf("reopen closure continuation requires exact higher-owner native selection")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	closeRequest := previous
	closeRequest.IssuerEpoch, closeRequest.Revision = actor.Epoch, max(before.Observed.AdmissionRevision, previous.Revision)+1
	request := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: r.Request, PreviousClose: previous, Close: closeRequest}
	digest, err := request.ContextSHA256()
	if err != nil {
		return m, err
	}
	latest := cloneLeaseResponse(before)
	a = next.ServiceClosure.Reopen.Closing
	a.LatestBefore = &latest
	a.Close.Continuation = &ServiceClosureContinuation{Origin: actor, StartedSequence: next.Sequence, PreviousOrigin: origin, PreviousSequence: sequence, Request: request, CommandSHA256: digest, Phase: Unconfirmed, RecordedSequence: 0, Query: false, Receipt: nil}
	next.ServiceClosure.Reopen.SafetyStage = "continuing"
	return next, next.Validate()
}

func (m Model) RecordServiceReopenContinuation(actor Owner, fact control.AdmissionContinuationResponse, query bool) (Model, error) {
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	if next.ServiceClosure == nil || next.ServiceClosure.Reopen == nil || next.ServiceClosure.Reopen.Closing == nil || next.ServiceClosure.Reopen.Closing.Close.Continuation == nil {
		return m, fmt.Errorf("original reopen continuation is absent")
	}
	n := next.ServiceClosure.Reopen.Closing.Close.Continuation
	if n.Phase != Unconfirmed || !query && n.Origin != actor {
		return m, fmt.Errorf("reopen continuation is not original unknown")
	}
	proof := fact
	proof.ClosedBy = *cloneAdmissionResponse(&fact.ClosedBy)
	n.Phase, n.Query, n.RecordedSequence, n.Receipt = Settled, query, next.Sequence, &proof
	return next, next.Validate()
}

func (c *ServiceReopenClosure) UnmarshalJSON(raw []byte) error {
	type plain ServiceReopenClosure
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	*c = ServiceReopenClosure(next)
	return nil
}

func (store *Store) StartServiceReopenClosure(ctx context.Context, s *Snapshot, actor Owner, selector string, before control.AdmissionLeaseResponse) (*Snapshot, *ServiceReopenClosure, error) {
	return store.selectServiceReopenClosure(ctx, s, actor, selector, before, false)
}

func (store *Store) ContinueServiceReopenClosure(ctx context.Context, s *Snapshot, actor Owner, selector string, before control.AdmissionLeaseResponse) (*Snapshot, *ServiceReopenClosure, error) {
	return store.selectServiceReopenClosure(ctx, s, actor, selector, before, true)
}

// 关闭即使来源已坏也不能绕过原 VM UID/RV 和当前 owner。与新开放不同，
// 它不要求原生期限仍有效：已过期也要封住可能迟到的旧开放命令。
func (store *Store) selectServiceReopenClosure(ctx context.Context, s *Snapshot, actor Owner, selector string, before control.AdmissionLeaseResponse, continuation bool) (*Snapshot, *ServiceReopenClosure, error) {
	if err := store.checked(s); err != nil {
		return nil, nil, err
	}
	if !s.Dispatchable() {
		return nil, nil, fmt.Errorf("reopen closure requires original running VM")
	}
	var next Model
	var err error
	if continuation {
		next, err = s.model.ContinueServiceReopenClosure(actor, selector, before)
	} else {
		next, err = s.model.StartServiceReopenClosure(actor, selector, before)
	}
	if err != nil {
		return nil, nil, err
	}
	result, err := store.apply(ctx, s, next)
	if err != nil {
		return nil, nil, err
	}
	return result, result.State().ServiceClosure.Reopen.Closing, nil
}

func (store *Store) RecordServiceReopenClosure(ctx context.Context, s *Snapshot, actor Owner, fact control.AdmissionClosure, query bool) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	next, err := s.model.RecordServiceReopenClosure(actor, fact, query)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}

func (store *Store) RecordServiceReopenContinuation(ctx context.Context, s *Snapshot, actor Owner, fact control.AdmissionContinuationResponse, query bool) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	next, err := s.model.RecordServiceReopenContinuation(actor, fact, query)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}
