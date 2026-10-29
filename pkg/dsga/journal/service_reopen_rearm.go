package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ReopenLeaseRecovery只恢复最近完整再关闭后的有限原生期限。原开放、
// 独立关闭、首次未知及预算全部保留；不能用外层首次Rearm覆盖本轮历史。
// 再次过期须显式选择且严格提高owner：原生floor封住迟到旧CAS，旧未知
// 仍是未知。Retired保存完整直接前驱及累计摘要，去掉更早递归上下文，
// 空间不随故障次数增长。所有方法只构造/保存WAL，不开放、不续租。
type ReopenLeaseRecovery struct {
	Origin           Owner                          `json:"origin"`
	StartedSequence  control.Uint64                 `json:"started_sequence"`
	ClosingSHA256    string                         `json:"closing_sha256"`
	HeldSHA256       string                         `json:"held_sha256"`
	Before           control.AdmissionLeaseResponse `json:"before"`
	Request          control.AdmissionLeaseRequest  `json:"request"`
	CommandSHA256    string                         `json:"command_sha256"`
	Phase            Phase                          `json:"phase"`
	RecordedSequence control.Uint64                 `json:"recorded_sequence"`
	Receipt          *control.AdmissionRearmStatus  `json:"receipt"`
	Cycle            control.Uint64                 `json:"cycle"`
	Retired          *ServiceReopenRetirement       `json:"retired"`
}

func cloneReopenLeaseRecovery(r *ReopenLeaseRecovery) *ReopenLeaseRecovery {
	if r == nil {
		return nil
	}
	n := *r
	n.Before = cloneLeaseResponse(r.Before)
	if r.Receipt != nil {
		f := *r.Receipt
		f.Current = cloneLeaseResponse(f.Current)
		n.Receipt = &f
	}
	if r.Retired != nil {
		h := *r.Retired
		h.Context = append([]byte(nil), h.Context...)
		n.Retired = &h
	}
	return &n
}

func (m Model) validateReopenLeaseRecovery(r *ReopenLeaseRecovery, archived bool) error {
	if r == nil {
		return nil // 必填SafetyStage由完整再关闭校验器独立认证。
	}
	a := m.ServiceClosure.Reopen
	if a.Closing == nil || a.Closing.Close == nil || a.Closing.Close.Pending() || m.stableServiceRecovery() != nil || m.PGInstance == nil {
		return fmt.Errorf("reopen lease recovery requires complete stable reclosure")
	}
	c := a.Closing.Close
	closing, err := json.Marshal(a.Closing)
	held, heldErr := serviceRearmHeldDigest(m)
	raw, commandErr := json.Marshal(r.Request)
	_, _, closed, _ := c.Selection()
	closedAt := c.RecordedSequence
	if c.Continuation != nil {
		closedAt = c.Continuation.RecordedSequence
	}
	if err != nil || heldErr != nil || commandErr != nil || r.ClosingSHA256 != rawDigest(closing) || r.HeldSHA256 != held || r.CommandSHA256 != rawDigest(raw) || r.Origin.Validate() != nil || r.Origin.Epoch > m.Owner.Epoch || r.Origin.Epoch == m.Owner.Epoch && r.Origin != m.Owner || r.Origin.Epoch < closed.IssuerEpoch || r.StartedSequence <= closedAt || r.StartedSequence > m.Sequence || r.Cycle == 0 {
		return fmt.Errorf("reopen lease recovery changed original closed service, budget or owner")
	}
	b, q := r.Before, r.Request
	latestClose := a.Closing.Before
	if a.Closing.LatestBefore != nil {
		latestClose = *a.Closing.LatestBefore
	}
	// 同boot原生期限和lease epoch均不回退；原开放与最近关闭的观察
	// 都必须保留。不能用更早的expired快照绕过关闭期间的真实续租。
	if b.DeadlineNS < latestClose.DeadlineNS || b.LeaseEpoch < latestClose.LeaseEpoch {
		return fmt.Errorf("reopen rearm predates latest closed native lease")
	}
	if b.ValidateFor(b.Request, *m.PGInstance) != nil || b.Request.Action != "observe" || !b.Armed || !b.Expired || b.Observed.AdmissionOpen || b.Request.IssuerEpoch != r.Origin.Epoch || b.Observed.IssuerFloorEpoch != r.Origin.Epoch || b.LeaseEpoch >= r.Origin.Epoch || b.DeadlineNS < a.Before.DeadlineNS || b.LeaseEpoch < a.Before.LeaseEpoch || b.Observed.AdmissionRevision != closed.Revision || b.Observed.CapacityGeneration != closed.ExpectedCapacityGeneration || q.Validate() != nil || q.Action != "rearm" || q.Instance != *m.PGInstance || q.IssuerEpoch != r.Origin.Epoch || q.ExpectedCapacityGeneration != b.Observed.CapacityGeneration || q.ExpectedAdmissionRevision != b.Observed.AdmissionRevision || q.ExpectedDeadlineNS != b.DeadlineNS {
		return fmt.Errorf("reopen lease recovery lacks exact expired native CAS")
	}
	switch r.Phase {
	case Unconfirmed:
		if r.RecordedSequence != 0 || r.Receipt != nil {
			return fmt.Errorf("unknown reopen rearm contains completion")
		}
	case Settled:
		if r.RecordedSequence <= r.StartedSequence || r.RecordedSequence > m.Sequence || r.Receipt == nil || !r.Receipt.Retained || r.Receipt.ValidateFor(q, *m.PGInstance) != nil || r.Receipt.Current.Observed.IssuerFloorEpoch > m.Owner.Epoch || r.Receipt.Current.Observed.CapacityGeneration != closed.ExpectedCapacityGeneration || r.Receipt.Current.Observed.AdmissionRevision != closed.Revision || r.Receipt.Current.Observed.AdmissionOpen {
			return fmt.Errorf("reopen rearm lacks exact retained closed native fact")
		}
	default:
		return fmt.Errorf("invalid reopen rearm phase")
	}
	if archived || r.Cycle == 1 {
		if r.Retired != nil {
			return fmt.Errorf("first or archived rearm invented recursive predecessor")
		}
		return nil
	}
	h := r.Retired
	if h == nil || h.ContextSHA256 != rawDigest(h.Context) || h.EvidenceSHA256 != reopenRetirementDigest(h.Context, h.PreviousSHA256) || r.Cycle == 2 && h.PreviousSHA256 != "" || r.Cycle > 2 && !lifecycleDigest(h.PreviousSHA256) {
		return fmt.Errorf("reopen rearm lost bounded complete predecessor")
	}
	var previous ReopenLeaseRecovery
	if err := json.Unmarshal(h.Context, &previous); err != nil {
		return err
	}
	if previous.Cycle == control.Uint64(math.MaxUint64) || previous.Cycle+1 != r.Cycle || previous.Origin.Epoch >= r.Origin.Epoch || previous.StartedSequence >= r.StartedSequence || previous.RecordedSequence >= r.StartedSequence || previous.Before.DeadlineNS > b.DeadlineNS || previous.Before.LeaseEpoch > b.LeaseEpoch || previous.Phase == Settled && (previous.Receipt == nil || previous.Receipt.AppliedDeadlineNS > b.DeadlineNS || previous.Origin.Epoch > b.LeaseEpoch) {
		return fmt.Errorf("reopen rearm predecessor does not precede higher-owner expired choice")
	}
	return m.validateReopenLeaseRecovery(&previous, true)
}

// StartReopenLeaseRecovery以最近关闭或最近恢复的原摘要选择，不接受调用方
// 提供新deadline/命令。next先深拷贝；同owner不能覆盖原Unknown/回执。
// 尚未发送的原CAS同样必须在更高floor下退休，不能凭未找到宣称NoEffect。
func (m Model) StartReopenLeaseRecovery(actor Owner, selector string, before control.AdmissionLeaseResponse) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.Reopen == nil || c.Reopen.Closing == nil || c.Reopen.Closing.Close == nil || c.Reopen.Closing.Close.Pending() || m.stableServiceRecovery() != nil {
		return m, fmt.Errorf("reopen rearm requires complete original reclosure")
	}
	a := c.Reopen
	_, _, _, selected := a.Closing.Close.Selection()
	cycle := control.Uint64(1)
	var retired *ServiceReopenRetirement
	if old := a.Rearm; old != nil {
		selected = old.CommandSHA256
		if actor.Epoch <= old.Origin.Epoch || old.Cycle == control.Uint64(math.MaxUint64) {
			return m, fmt.Errorf("reopen rearm replacement requires strictly higher owner")
		}
		previous := *old
		previous.Retired = nil
		raw, err := json.Marshal(previous)
		if err != nil {
			return m, err
		}
		retired = &ServiceReopenRetirement{Context: raw, ContextSHA256: rawDigest(raw), PreviousSHA256: "", EvidenceSHA256: ""}
		if old.Retired != nil {
			retired.PreviousSHA256 = old.Retired.EvidenceSHA256
		}
		retired.EvidenceSHA256 = reopenRetirementDigest(raw, retired.PreviousSHA256)
		cycle = old.Cycle + 1
	}
	if selected != selector {
		return m, fmt.Errorf("reopen rearm selector is not latest original command")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	q := before.Request
	q.Action, q.ExpectedDeadlineNS = "rearm", before.DeadlineNS
	q.ExpectedCapacityGeneration, q.ExpectedAdmissionRevision = before.Observed.CapacityGeneration, before.Observed.AdmissionRevision
	raw, err := json.Marshal(q)
	if err != nil {
		return m, err
	}
	closing, err := json.Marshal(a.Closing)
	if err != nil {
		return m, err
	}
	held, err := serviceRearmHeldDigest(m)
	if err != nil {
		return m, err
	}
	next.ServiceClosure.Reopen.Rearm = &ReopenLeaseRecovery{Origin: actor, StartedSequence: next.Sequence, ClosingSHA256: rawDigest(closing), HeldSHA256: held, Before: cloneLeaseResponse(before), Request: q, CommandSHA256: rawDigest(raw), Phase: Unconfirmed, RecordedSequence: 0, Receipt: nil, Cycle: cycle, Retired: retired}
	next.ServiceClosure.Reopen.SafetyStage = "rearming"
	if err := next.reopenCompletionCapacity(); err != nil {
		return m, err
	}
	return next, next.Validate()
}

func (m Model) RecordReopenLeaseRecovery(actor Owner, fact control.AdmissionRearmStatus) (Model, error) {
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	if next.ServiceClosure == nil || next.ServiceClosure.Reopen == nil || next.ServiceClosure.Reopen.Rearm == nil || next.ServiceClosure.Reopen.Rearm.Phase != Unconfirmed {
		return m, fmt.Errorf("reopen rearm is not original unknown")
	}
	r := next.ServiceClosure.Reopen.Rearm
	fact.Current = cloneLeaseResponse(fact.Current)
	r.Phase, r.RecordedSequence, r.Receipt = Settled, next.Sequence, &fact
	return next, next.Validate()
}

// Store只在原VM UID/RV写前完整确认后返回派发票据；丢回复即无票据，
// 后继只能读原回执或原owner显式重放。网络期间不持本地锁，不释放预算。
func (store *Store) StartReopenLeaseRecovery(ctx context.Context, s *Snapshot, actor Owner, selector string, before control.AdmissionLeaseResponse) (*Snapshot, *ReopenLeaseRecovery, error) {
	if err := store.checked(s); err != nil {
		return nil, nil, err
	}
	if !s.Dispatchable() {
		return nil, nil, fmt.Errorf("reopen rearm requires original running VM")
	}
	next, err := s.model.StartReopenLeaseRecovery(actor, selector, before)
	if err != nil {
		return nil, nil, err
	}
	result, err := store.apply(ctx, s, next)
	if err != nil {
		return nil, nil, err
	}
	return result, result.State().ServiceClosure.Reopen.Rearm, nil
}

func (store *Store) RecordReopenLeaseRecovery(ctx context.Context, s *Snapshot, actor Owner, fact control.AdmissionRearmStatus) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	next, err := s.model.RecordReopenLeaseRecovery(actor, fact)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}

func (r *ReopenLeaseRecovery) UnmarshalJSON(raw []byte) error {
	type plain ReopenLeaseRecovery
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, MaxRawBytes/2, 16384); err != nil {
		return err
	}
	*r = ReopenLeaseRecovery(p)
	return nil
}
