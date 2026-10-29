package journal

import (
	"bytes"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// HeldInitialBudget独立于Active/HWM/Resize结果，保存完整原启动预算。首次
// 持久化同时升VM v9，旧读者必须拒绝，不能把“无Resize”误当“没有预算”。
// Budget/命令/回执均有独立字节界限；未知开放期间预算不可清除或替换。
type HeldInitialBudget struct {
	ProtocolVersion  uint32            `json:"protocol_version"`
	Origin           Owner             `json:"origin"`
	RecordedSequence control.Uint64    `json:"recorded_sequence"`
	Budget           []byte            `json:"budget"`
	BudgetSHA256     string            `json:"budget_sha256"`
	Admission        *InitialAdmission `json:"admission"`
}

// InitialAdmission只有首次开放这一动作，不复用带Resize Key的通用Step。
// Unconfirmed保留完整原命令而无回执；新owner只读，不能替换原epoch重发。
type InitialAdmission struct {
	Origin           Owner          `json:"origin"`
	StartedSequence  control.Uint64 `json:"started_sequence"`
	Command          []byte         `json:"command"`
	CommandSHA256    string         `json:"command_sha256"`
	Phase            Phase          `json:"phase"`
	RecordedSequence control.Uint64 `json:"recorded_sequence"`
	Receipt          []byte         `json:"receipt"`
}

func cloneInitial(h *HeldInitialBudget) *HeldInitialBudget {
	if h == nil {
		return nil
	}
	cloned := *h
	cloned.Budget = append([]byte(nil), h.Budget...)
	if h.Admission != nil {
		a := *h.Admission
		a.Command = append([]byte(nil), a.Command...)
		a.Receipt = append([]byte(nil), a.Receipt...)
		cloned.Admission = &a
	}
	return &cloned
}

func (model Model) validateInitialBudget() error {
	h := model.InitialHeld
	if h == nil {
		return nil
	}
	if model.ProtocolVersion < 9 || model.BudgetHeld != nil || model.CapacityHeld != nil || h.ProtocolVersion != 1 || h.Origin.Validate() != nil || h.Origin.Epoch > model.Owner.Epoch || h.Origin.Epoch == model.Owner.Epoch && h.Origin != model.Owner || h.RecordedSequence == 0 || h.RecordedSequence > model.Sequence || model.PGInstance == nil || model.ResourceIdentity == nil || control.ValidateJSON(h.Budget, MaxObservationBytes, 2048) != nil || rawDigest(h.Budget) != h.BudgetSHA256 {
		return fmt.Errorf("invalid original permanent initial budget")
	}
	var i budget.Initial
	if json.Unmarshal(h.Budget, &i) != nil || i.Instance != *model.PGInstance || i.PGAggregate.Identity != *model.ResourceIdentity || i.Incarnation != model.Incarnation || i.IssuerEpoch != h.Origin.Epoch {
		return fmt.Errorf("initial budget differs from durable original PG/Guest/owner")
	}
	a := h.Admission
	if a == nil {
		if model.HighWater != nil || model.Active != nil || len(model.History) != 0 || model.ExpiredResults != 0 {
			return fmt.Errorf("resize history cannot precede first admission completion")
		}
		return nil
	}
	if a.Origin.Validate() != nil || a.Origin.Epoch < h.Origin.Epoch || a.Origin.Epoch > model.Owner.Epoch || a.Origin.Epoch == model.Owner.Epoch && a.Origin != model.Owner || a.Origin.Epoch == h.Origin.Epoch && a.Origin != h.Origin || a.StartedSequence <= h.RecordedSequence || a.StartedSequence > model.Sequence || control.ValidateJSON(a.Command, MaxCommandBytes, 2048) != nil || rawDigest(a.Command) != a.CommandSHA256 {
		return fmt.Errorf("invalid first admission original owner, sequence or command")
	}
	var c budget.InitialAdmissionCommand
	if json.Unmarshal(a.Command, &c) != nil || c.ValidateFor(i) != nil || c.Request.IssuerEpoch != a.Origin.Epoch {
		return fmt.Errorf("first admission command changed original initial budget")
	}
	switch a.Phase {
	case Unconfirmed:
		if a.RecordedSequence != 0 || a.Receipt != nil || model.HighWater != nil || model.Active != nil || len(model.History) != 0 || model.ExpiredResults != 0 {
			return fmt.Errorf("unknown first admission cannot contain receipt or resize history")
		}
	case Settled:
		var receipt budget.InitialAdmissionReceipt
		if a.RecordedSequence <= a.StartedSequence || a.RecordedSequence > model.Sequence || control.ValidateJSON(a.Receipt, MaxObservationBytes, 2048) != nil || json.Unmarshal(a.Receipt, &receipt) != nil || receipt.ResourceEpoch > model.Owner.Epoch || receipt.ValidateFor(i, c, receipt.ResourceEpoch) != nil {
			return fmt.Errorf("first admission lacks exact original completed audit")
		}
		check := func(op Operation) error {
			if op.Request.ExpectedCapacityGeneration != i.Shared.Observed.CapacityGeneration {
				return fmt.Errorf("operation changed retained initial capacity baseline")
			}
			for _, step := range op.Steps {
				if step.Action == FinalizeBudget && step.Phase == Settled && step.Observation.Result == Completed {
					return fmt.Errorf("completed normal budget did not atomically replace initial budget")
				}
			}
			return nil
		}
		for _, record := range model.History {
			if record.ClosedSequence <= a.RecordedSequence {
				return fmt.Errorf("resize history predates completed first admission")
			}
			if err := check(record.Operation); err != nil {
				return err
			}
		}
		if model.Active != nil {
			if err := check(*model.Active); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid first admission phase")
	}
	return nil
}

// InitialBudgetFor只返回原持久预算深解码副本，不认证当前来源或赋开放权。
func (model Model) InitialBudgetFor() (budget.Initial, error) {
	var i budget.Initial
	if model.Validate() != nil || model.InitialHeld == nil {
		return i, fmt.Errorf("original initial budget is not retained")
	}
	if err := json.Unmarshal(model.InitialHeld.Budget, &i); err != nil {
		return i, err
	}
	return i, nil
}

func (model Model) BindInitialBudget(actor Owner, i budget.Initial) (Model, error) {
	if err := model.Validate(); err != nil {
		return model, err
	}
	if actor != model.Owner || i.Validate() != nil || model.PGInstance == nil || i.Instance != *model.PGInstance || model.ResourceIdentity == nil || i.PGAggregate.Identity != *model.ResourceIdentity || i.Incarnation != model.Incarnation {
		return model, fmt.Errorf("invalid initial budget original owner/instance")
	}
	raw, err := json.Marshal(i)
	if err != nil {
		return model, err
	}
	if model.InitialHeld != nil {
		if !bytes.Equal(raw, model.InitialHeld.Budget) {
			return model, fmt.Errorf("original initial budget cannot be rebound")
		}
		return model.Clone(), nil
	}
	if i.IssuerEpoch != actor.Epoch || model.BudgetHeld != nil || model.CapacityHeld != nil || model.HighWater != nil || model.Active != nil || len(model.History) != 0 || model.ExpiredResults != 0 {
		return model, fmt.Errorf("initial budget requires original pristine bound PG/Guest")
	}
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	next.ProtocolVersion = 9
	next.InitialHeld = &HeldInitialBudget{ProtocolVersion: 1, Origin: actor, RecordedSequence: next.Sequence, Budget: raw, BudgetSHA256: rawDigest(raw), Admission: nil}
	return next, next.Validate()
}

// StartInitialAdmission与原预算同VM UID/RV持久化；仅保存命令，不发native
// setter。已派发命令不能由普通重试覆盖，即使相同数值也须走原查询/显式重放。
func (model Model) StartInitialAdmission(actor Owner, c budget.InitialAdmissionCommand) (Model, error) {
	i, err := model.InitialBudgetFor()
	if err != nil {
		return model, err
	}
	if actor != model.Owner || model.ServiceClosure != nil || model.InitialHeld.Admission != nil || c.ValidateFor(i) != nil || c.Request.IssuerEpoch != actor.Epoch || model.Active != nil || model.HighWater != nil {
		return model, fmt.Errorf("first admission is not eligible for original dispatch")
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return model, err
	}
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	next.InitialHeld.Admission = &InitialAdmission{Origin: actor, StartedSequence: next.Sequence, Command: raw, CommandSHA256: rawDigest(raw), Phase: Unconfirmed, RecordedSequence: 0, Receipt: nil}
	return next, next.Validate()
}

func (model Model) RecordInitialAdmission(actor Owner, receipt budget.InitialAdmissionReceipt) (Model, error) {
	i, err := model.InitialBudgetFor()
	if err != nil {
		return model, err
	}
	a := model.InitialHeld.Admission
	var c budget.InitialAdmissionCommand
	if actor != model.Owner || a == nil || a.Phase != Unconfirmed || json.Unmarshal(a.Command, &c) != nil || receipt.ResourceEpoch != actor.Epoch || receipt.ValidateFor(i, c, actor.Epoch) != nil {
		return model, fmt.Errorf("first admission receipt differs from original unknown command")
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return model, err
	}
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	next.InitialHeld.Admission.Phase, next.InitialHeld.Admission.RecordedSequence, next.InitialHeld.Admission.Receipt = Settled, next.Sequence, raw
	return next, next.Validate()
}

func (h *HeldInitialBudget) UnmarshalJSON(raw []byte) error {
	type plain HeldInitialBudget
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	*h = HeldInitialBudget(p)
	return nil
}

func (a *InitialAdmission) UnmarshalJSON(raw []byte) error {
	type plain InitialAdmission
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	*a = InitialAdmission(p)
	return nil
}
