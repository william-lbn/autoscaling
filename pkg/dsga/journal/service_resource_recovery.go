package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// ServiceResourceRecovery是16/17关门后的独立资源对账，不修改Active的原
// Unknown、Abort标志或任何原命令。原关闭、预算、操作摘要先固定；每个领域
// 的真实效果另存，全部领域/预算闭合及独立重开前仍不允许新请求或普通Finish。
// 本阶段只接Guest；后继PG/VM须各自证明，不能由Guest成功补造其他领域完成。
type ServiceResourceRecovery struct {
	Origin          Owner                    `json:"origin"`
	StartedSequence control.Uint64           `json:"started_sequence"`
	OperationSHA256 string                   `json:"operation_sha256"`
	Closed          control.AdmissionRequest `json:"closed"`
	ClosedSequence  control.Uint64           `json:"closed_sequence"`
	Guest           *ServiceGuestRecovery    `json:"guest"`
}

// CancelUnreceived是显式写前选择，授权Root在自己的文件锁内核实并取消
// 从未收到的原命令；不是“未找到即NoEffect”。若Apply先获得锁，取消失败，
// 后继只能查询原Applied/未知，不反向资源、不换命令/epoch重发Apply。
// State保存完整Root原来源；ObservedBy与原选择者分开，允许更高owner只读落账。
type ServiceGuestRecovery struct {
	CommandSHA256    string              `json:"command_sha256"`
	CancelUnreceived bool                `json:"cancel_unreceived"`
	Phase            Phase               `json:"phase"`
	RecordedSequence control.Uint64      `json:"recorded_sequence"`
	ObservedBy       *Owner              `json:"observed_by"`
	Query            bool                `json:"query"`
	Effect           string              `json:"effect"`
	State            *resourceguard.View `json:"state"`
}

// OriginalServiceGuest返回副本，只选择原Unknown GuestReady及其完整Prepared。
// 它不是派发票据；生产者仍须核真实Root/出生/当前owner及相同VM UID/RV。
func (m Model) OriginalServiceGuest() (resourceguard.Command, string, error) {
	var command resourceguard.Command
	if m.Active == nil || m.ResourceIdentity == nil || m.PGInstance == nil {
		return command, "", fmt.Errorf("service resource recovery lacks original bound operation")
	}
	var prepared budget.Prepared
	found := false
	for _, step := range m.Active.Steps {
		if step.Action == PrepareBudget && step.Phase == Settled && step.Observation != nil && step.Observation.Result == Completed {
			found = json.Unmarshal(step.Observation.Data, &prepared) == nil && prepared.ValidateFor(m.Active.Request, *m.ResourceIdentity) == nil
		}
		if step.Action != GuestReady {
			continue
		}
		if !found || step.Phase != Unconfirmed || step.DispatchedBy == nil || step.TargetID != m.PGInstance.ComputeInstanceID || json.Unmarshal(step.Command, &command) != nil || command.Validate() != nil || command.IssuerEpoch != step.DispatchedBy.Epoch || command.Identity != *m.ResourceIdentity || command.BudgetCommitmentID != m.Active.Request.BudgetCommitmentID || command.MemoryMaxBytes != prepared.Limits.MemoryMaxBytes || command.PidsMaxTasks != prepared.Limits.PidsMaxTasks || command.SystemMemoryMaxBytes != prepared.Limits.SystemMemoryMaxBytes || command.AddressSpaceMaxBytes != prepared.Limits.AddressSpaceMaxBytes || command.OutsideUpperBytes != prepared.Limits.OutsideUpperBytes || !reflect.DeepEqual(command.HugePools, prepared.Limits.HugePools) {
			return resourceguard.Command{}, "", fmt.Errorf("service recovery lost complete original unknown Guest command")
		}
		return command, step.CommandSHA256, nil
	}
	return command, "", fmt.Errorf("service recovery has no original unknown GuestReady")
}

func (m Model) validateServiceResourceRecovery() error {
	p := m.ServiceClosure.ResourceRecovery
	if p == nil {
		return nil
	}
	c, version := m.ServiceClosure, m.serviceClosureVersion()
	command, commandHash, err := m.OriginalServiceGuest()
	// 选择必须晚于已经确认的关闭，且逐字固定当时的完整Active与原
	// Guest摘要。接管只增加观察者，不改变命令issuer或把旧owner换成新holder。
	if err != nil || version != 16 && version != 17 || c.Pending() || p.Guest == nil || p.Origin.Validate() != nil || p.Origin.Epoch > m.Owner.Epoch || p.Origin.Epoch == m.Owner.Epoch && p.Origin != m.Owner || p.OperationSHA256 != c.OperationSHA256 || p.StartedSequence > m.Sequence || p.StartedSequence <= p.ClosedSequence || p.ClosedSequence == 0 || command.IssuerEpoch > p.Origin.Epoch || p.Guest.CommandSHA256 != commandHash {
		return fmt.Errorf("service resource recovery changed original closed operation or owner")
	}
	_, _, closed, _ := c.Selection()
	at := c.RecordedSequence
	if c.Continuation != nil {
		at = c.Continuation.RecordedSequence
	}
	if p.Closed != closed || p.ClosedSequence != at {
		return fmt.Errorf("service resource recovery replaced latest complete closure")
	}
	g := p.Guest
	switch g.Phase {
	case Unconfirmed:
		// 写前选择不是完成；任何半份事实都不能附着在Unknown上被后继采信。
		if g.RecordedSequence != 0 || g.ObservedBy != nil || g.Query || g.Effect != "" || g.State != nil {
			return fmt.Errorf("unknown Guest recovery contains independent completion")
		}
	case Settled:
		if g.RecordedSequence <= p.StartedSequence || g.RecordedSequence > m.Sequence || g.ObservedBy == nil || g.ObservedBy.Validate() != nil || g.ObservedBy.Epoch < p.Origin.Epoch || g.ObservedBy.Epoch == p.Origin.Epoch && *g.ObservedBy != p.Origin || g.ObservedBy.Epoch > m.Owner.Epoch || g.ObservedBy.Epoch == m.Owner.Epoch && *g.ObservedBy != m.Owner || g.State == nil || g.State.Identity != command.Identity || g.State.IssuerFloorEpoch != g.ObservedBy.Epoch || g.State.Birth == nil || g.State.Birth.PostmasterPID != command.PostmasterPID || g.State.Birth.PostmasterStartTicks != command.PostmasterStartTicks || g.State.Birth.AddressSpaceMaxBytes != command.AddressSpaceMaxBytes {
			return fmt.Errorf("guest recovery lacks original source and independent observing owner")
		}
		// 同时核实整组限额、原出生和活PG计费域；当前占用不是未来上界。
		// 这里只保存Guest自己的事实，不能替PG/Runner/Outside结清预算。
		if _, err := budget.RootAggregateFromView(*g.State); err != nil {
			return err
		}
		switch g.Effect {
		case "applied":
			if !g.Query || !g.State.Applied || g.State.Original == nil || g.State.Held == nil || !reflect.DeepEqual(*g.State.Original, command) || !reflect.DeepEqual(*g.State.Held, command) || g.State.Actual.ValidateFor(command) != nil {
				return fmt.Errorf("guest recovery lacks complete original applied fact")
			}
		case "cancelled":
			proof := resourceguard.Cancellation{ProtocolVersion: 1, VMShrinkPermit: false, Command: command, State: *g.State}
			if !g.CancelUnreceived || proof.ValidateFor(command, g.ObservedBy.Epoch) != nil {
				return fmt.Errorf("guest recovery lacks formally sealed original unsent cancellation")
			}
		default:
			return fmt.Errorf("unknown independent Guest recovery effect")
		}
	default:
		return fmt.Errorf("invalid independent Guest recovery phase")
	}
	return nil
}

func cloneServiceResourceRecovery(p *ServiceResourceRecovery) *ServiceResourceRecovery {
	if p == nil {
		return nil
	}
	n := *p
	if p.Guest != nil {
		g := *p.Guest
		if g.ObservedBy != nil {
			owner := *g.ObservedBy
			g.ObservedBy = &owner
		}
		if g.State != nil {
			state := g.State.Clone()
			g.State = &state
		}
		n.Guest = &g
	}
	return &n
}

// StartServiceGuestRecovery只创建独立写前模型；没有外部Root写权限。原
// Active、Abort、关闭和旧预算均由next深拷贝保留，Store确认CAS后才能取消。
func (m Model) StartServiceGuestRecovery(actor Owner, selector string, cancelUnreceived bool) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.Pending() || c.ResourceRecovery != nil || c.Rearm != nil || c.Reopen != nil {
		return m, fmt.Errorf("guest recovery requires original unchanged confirmed closure")
	}
	_, commandHash, err := m.OriginalServiceGuest()
	if err != nil {
		return m, err
	}
	_, _, closed, selected := c.Selection()
	if selector != selected {
		return m, fmt.Errorf("guest recovery requires latest complete closure selector")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	at := c.RecordedSequence
	if c.Continuation != nil {
		at = c.Continuation.RecordedSequence
	}
	next.ServiceClosure.ResourceRecovery = &ServiceResourceRecovery{Origin: actor, StartedSequence: next.Sequence, OperationSHA256: c.OperationSHA256, Closed: closed, ClosedSequence: at, Guest: &ServiceGuestRecovery{CommandSHA256: commandHash, CancelUnreceived: cancelUnreceived, Phase: Unconfirmed, RecordedSequence: 0, ObservedBy: nil, Query: false, Effect: "", State: nil}}
	return next, next.Validate()
}

// RecordServiceGuestRecovery只接受选择后的完整原事实；不再覆盖已落账证明。
// 调用者负责真实Root身份认证及相同VM检查，Model验证命令/出生/floor和序列。
func (m Model) RecordServiceGuestRecovery(actor Owner, selector, effect string, view resourceguard.View, query bool) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.ResourceRecovery == nil || c.ResourceRecovery.Guest == nil || c.ResourceRecovery.Guest.Phase != Unconfirmed || c.Rearm != nil || c.Reopen != nil {
		return m, fmt.Errorf("guest recovery is not original independent unknown")
	}
	_, _, _, selected := c.Selection()
	if selector != selected {
		return m, fmt.Errorf("guest recovery record selected another closure")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	state := view.Clone()
	g := next.ServiceClosure.ResourceRecovery.Guest
	g.Phase, g.RecordedSequence, g.ObservedBy, g.Query, g.Effect, g.State = Settled, next.Sequence, &actor, query, effect, &state
	return next, next.Validate()
}

// CAS未确认不给取消票据；原关闭/未知/预算与本次选择同一UID/RV线性化。
// 后继查询可重复，新增Root取消只由显式管理且已确认原选择的执行器发出。
func (store *Store) StartServiceGuestRecovery(ctx context.Context, s *Snapshot, actor Owner, selector string, cancelUnreceived bool) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	if !s.Dispatchable() {
		return nil, fmt.Errorf("guest recovery requires original running VM")
	}
	next, err := s.model.StartServiceGuestRecovery(actor, selector, cancelUnreceived)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}

func (store *Store) RecordServiceGuestRecovery(ctx context.Context, s *Snapshot, actor Owner, selector, effect string, view resourceguard.View, query bool) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	if !s.Dispatchable() {
		return nil, fmt.Errorf("guest recovery requires original running VM")
	}
	next, err := s.model.RecordServiceGuestRecovery(actor, selector, effect, view, query)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}

func (p *ServiceResourceRecovery) UnmarshalJSON(raw []byte) error {
	type plain ServiceResourceRecovery
	var next plain
	if err := control.UnmarshalObjectBounded(raw, &next, MaxObservationBytes*2, 8192); err != nil {
		return err
	}
	*p = ServiceResourceRecovery(next)
	return nil
}

func (g *ServiceGuestRecovery) UnmarshalJSON(raw []byte) error {
	type plain ServiceGuestRecovery
	var next plain
	if err := control.UnmarshalObjectBounded(raw, &next, MaxObservationBytes*2, 8192); err != nil {
		return err
	}
	*g = ServiceGuestRecovery(next)
	return nil
}
