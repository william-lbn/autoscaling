package journal

import (
	"context"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ServiceRearm固定已关闭空闲服务的一次原生期限恢复选择。外层版本19显式
// 拒绝旧读者，ClosureVersion保留原关闭格式，不能把所有旧类别投影为同一种。
// 期限恢复不等于业务重开：完整原关闭/预算/首次开放未知全部保留，Begin和
// 普通开放仍被原ServiceClosure拒绝。只保留一次原命令，失败不换deadline。
type ServiceRearm struct {
	ClosureVersion   uint32                         `json:"closure_version"`
	Origin           Owner                          `json:"origin"`
	StartedSequence  control.Uint64                 `json:"started_sequence"`
	ClosureSHA256    string                         `json:"closure_sha256"`
	HeldSHA256       string                         `json:"held_sha256"`
	Before           control.AdmissionLeaseResponse `json:"before"`
	Request          control.AdmissionLeaseRequest  `json:"request"`
	CommandSHA256    string                         `json:"command_sha256"`
	Phase            Phase                          `json:"phase"`
	RecordedSequence control.Uint64                 `json:"recorded_sequence"`
	Receipt          *control.AdmissionRearmStatus  `json:"receipt"`
}

func cloneLeaseResponse(original control.AdmissionLeaseResponse) control.AdmissionLeaseResponse {
	var temporary control.AdmissionResponse
	temporary.Observed = original.Observed
	original.Observed = cloneAdmissionResponse(&temporary).Observed
	return original
}

func serviceClosureDigest(c ServiceClosure) (string, error) {
	c.Rearm = nil
	// 后继开放不属于更早 rearm 选择时的关闭摘要，不能因此改写原摘要。
	c.Reopen = nil
	c.FormatVersion = 0
	raw, err := json.Marshal(c)
	return rawDigest(raw), err
}

func serviceRearmHeldDigest(m Model) (string, error) {
	// 固定完整永久来源/预算及关闭水位，不仅固定可复用ID。只允许无Active
	// 的稳定边界；既有终态历史仍按原规则淘汰，不能牵连永久预算/原未知。
	// owner/Sequence允许接管推进；任何资源/原未知变化都使此选择失效。
	raw, err := json.Marshal([]any{m.PGInstance, m.ResourceIdentity, m.InitialHeld, m.BudgetHeld, m.CapacityHeld, m.BudgetBoundary, m.HighWater})
	return rawDigest(raw), err
}

func (m Model) validateServiceRearm() error {
	c := m.ServiceClosure
	if c.Rearm == nil {
		if m.ClosureFormatVersion() == 19 {
			return fmt.Errorf("rearm journal lost original recovery selection")
		}
		return nil
	}
	r := c.Rearm
	closureHash, closureErr := serviceClosureDigest(*c)
	heldHash, heldErr := serviceRearmHeldDigest(m)
	raw, commandErr := json.Marshal(r.Request)
	_, _, closeRequest, _ := c.Selection()
	closedAt := c.RecordedSequence
	if c.Continuation != nil {
		closedAt = c.Continuation.RecordedSequence
	}
	rearmVersion := m.ClosureFormatVersion() == 19 || m.ClosureFormatVersion() == 20 && c.Reopen != nil && c.Reopen.ClosureVersion == 19
	if !rearmVersion || r.ClosureVersion < 10 || r.ClosureVersion > 12 || m.Active != nil || c.Pending() || closureErr != nil || heldErr != nil || commandErr != nil || r.ClosureSHA256 != closureHash || r.HeldSHA256 != heldHash || r.CommandSHA256 != rawDigest(raw) || r.Origin.Validate() != nil || r.Origin.Epoch > m.Owner.Epoch || r.Origin.Epoch == m.Owner.Epoch && r.Origin != m.Owner || r.StartedSequence <= closedAt || r.StartedSequence > m.Sequence {
		return fmt.Errorf("service rearm changed original closed service, held budget or owner")
	}
	b := r.Before
	q := r.Request
	if m.PGInstance == nil || b.ValidateFor(b.Request, *m.PGInstance) != nil || b.Request.Action != "observe" || !b.Armed || !b.Expired || b.Observed.AdmissionOpen || b.Observed.IssuerFloorEpoch != r.Origin.Epoch || b.Request.IssuerEpoch != r.Origin.Epoch || r.Origin.Epoch <= b.LeaseEpoch || b.Observed.CapacityGeneration != closeRequest.ExpectedCapacityGeneration || b.Observed.AdmissionRevision < closeRequest.Revision || q.Validate() != nil || q.Action != "rearm" || q.Instance != *m.PGInstance || q.IssuerEpoch != r.Origin.Epoch || q.ExpectedCapacityGeneration != b.Observed.CapacityGeneration || q.ExpectedAdmissionRevision != b.Observed.AdmissionRevision || q.ExpectedDeadlineNS != b.DeadlineNS {
		return fmt.Errorf("service rearm lacks original expired native CAS context")
	}
	switch r.Phase {
	case Unconfirmed:
		if r.RecordedSequence != 0 || r.Receipt != nil {
			return fmt.Errorf("unknown rearm contains completion")
		}
	case Settled:
		if r.RecordedSequence <= r.StartedSequence || r.RecordedSequence > m.Sequence || r.Receipt == nil || !r.Receipt.Retained || r.Receipt.ValidateFor(q, *m.PGInstance) != nil || r.Receipt.Current.Observed.IssuerFloorEpoch > m.Owner.Epoch {
			return fmt.Errorf("service rearm lacks exact retained native application")
		}
	default:
		return fmt.Errorf("invalid service rearm phase")
	}
	return nil
}

func (m Model) StartServiceRearm(actor Owner, selector string, before control.AdmissionLeaseResponse) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.Rearm != nil || c.Pending() || m.Active != nil || m.ClosureFormatVersion() < 10 || m.ClosureFormatVersion() > 12 {
		return m, fmt.Errorf("rearm requires original confirmed idle closure without another selection")
	}
	_, _, _, selected := c.Selection()
	if selector != selected {
		return m, fmt.Errorf("rearm selector differs from latest closed command")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	request := before.Request
	request.Action = "rearm"
	request.ExpectedDeadlineNS = before.DeadlineNS
	request.ExpectedCapacityGeneration = before.Observed.CapacityGeneration
	request.ExpectedAdmissionRevision = before.Observed.AdmissionRevision
	closureHash, err := serviceClosureDigest(*c)
	if err != nil {
		return m, err
	}
	heldHash, err := serviceRearmHeldDigest(m)
	if err != nil {
		return m, err
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return m, err
	}
	next.setClosureFormatVersion(19)
	next.ServiceClosure.Rearm = &ServiceRearm{ClosureVersion: m.ClosureFormatVersion(), Origin: actor, StartedSequence: next.Sequence, ClosureSHA256: closureHash, HeldSHA256: heldHash, Before: cloneLeaseResponse(before), Request: request, CommandSHA256: rawDigest(raw), Phase: Unconfirmed, RecordedSequence: 0, Receipt: nil}
	return next, next.Validate()
}

func (m Model) RecordServiceRearm(actor Owner, fact control.AdmissionRearmStatus) (Model, error) {
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	if next.ServiceClosure == nil || next.ServiceClosure.Rearm == nil || next.ServiceClosure.Rearm.Phase != Unconfirmed {
		return m, fmt.Errorf("service rearm is not original unknown")
	}
	r := next.ServiceClosure.Rearm
	fact.Current = cloneLeaseResponse(fact.Current)
	r.Phase, r.RecordedSequence, r.Receipt = Settled, next.Sequence, &fact
	return next, next.Validate()
}

// 写前CAS丢回复永不返回派发票据；只允许后继查询或原owner显式原重放。
// 此模块不发送网络，不释放预算，也不能使关闭消失。
func (store *Store) StartServiceRearm(ctx context.Context, s *Snapshot, actor Owner, selector string, before control.AdmissionLeaseResponse) (*Snapshot, *ServiceRearm, error) {
	if err := store.checked(s); err != nil {
		return nil, nil, err
	}
	if !s.Dispatchable() {
		return nil, nil, fmt.Errorf("rearm requires original running VM")
	}
	next, err := s.model.StartServiceRearm(actor, selector, before)
	if err != nil {
		return nil, nil, err
	}
	result, err := store.apply(ctx, s, next)
	if err != nil {
		return nil, nil, err
	}
	return result, result.State().ServiceClosure.Rearm, nil
}

func (store *Store) RecordServiceRearm(ctx context.Context, s *Snapshot, actor Owner, fact control.AdmissionRearmStatus) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	next, err := s.model.RecordServiceRearm(actor, fact)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}

func (r *ServiceRearm) UnmarshalJSON(raw []byte) error {
	type plain ServiceRearm
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	*r = ServiceRearm(next)
	return nil
}
