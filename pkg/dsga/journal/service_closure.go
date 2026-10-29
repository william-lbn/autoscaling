package journal

import (
	"context"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// ServiceClosure是空闲服务发现来源异常后的独立、不可撤销关闭WAL。它只
// 阻止新连接；原预算、历史及已存在的连接全部保留。没有受控重开协议前，
// 不允许因来源重新可达就清除此字段。旧版本必须因v10拒绝，不能忽略门禁。
// 当前只覆盖无Active、无首次开放未知的边界；在途异常须另行取消/对账。
type ServiceClosure struct {
	Origin           Owner                      `json:"origin"`
	StartedSequence  control.Uint64             `json:"started_sequence"`
	Reason           string                     `json:"reason"`
	Request          control.AdmissionRequest   `json:"request"`
	CommandSHA256    string                     `json:"command_sha256"`
	Phase            Phase                      `json:"phase"`
	RecordedSequence control.Uint64             `json:"recorded_sequence"`
	Query            bool                       `json:"query"`
	Receipt          *control.AdmissionResponse `json:"receipt"`
	// v11只添加接续，不改写上面的原始意图/派发者/未知。旧v10读者必须
	// 因版本或未知字段拒绝，不能把新关闭投影成原命令已成功或重新开放。
	Continuation *ServiceClosureContinuation `json:"continuation,omitempty"`
}

func cloneAdmissionResponse(original *control.AdmissionResponse) *control.AdmissionResponse {
	cloned := *original
	cloned.Observed.Arrays = append([]control.ArrayFact(nil), original.Observed.Arrays...)
	for i := range cloned.Observed.Arrays {
		if key := cloned.Observed.Arrays[i].ReleaseKey; key != nil {
			value := *key
			cloned.Observed.Arrays[i].ReleaseKey = &value
		}
	}
	if op := cloned.Observed.Operation; op != nil {
		value := *op
		cloned.Observed.Operation = &value
	}
	return &cloned
}

// 只引用永久完整预算的原ID，不能由错误文字、RSS或当前资格派生新承诺。
// 本函数不认证外部对象，也不向Root/Host发出释放许可。
func (model Model) ServiceBudgetID() (string, error) {
	switch {
	case model.InitialHeld != nil:
		var initial budget.Initial
		if json.Unmarshal(model.InitialHeld.Budget, &initial) == nil {
			return initial.PGAggregate.Bootstrap.BudgetCommitmentID, nil
		}
	case model.BudgetHeld != nil:
		var finalized budget.Finalized
		if o := model.BudgetHeld.Finalization.Observation; o != nil && json.Unmarshal(o.Data, &finalized) == nil {
			return finalized.Request.BudgetCommitmentID, nil
		}
	case model.CapacityHeld != nil:
		var command budget.ReconciledAbortCommand
		if json.Unmarshal(model.CapacityHeld.Cleanup.Command, &command) == nil {
			return command.Capacity.Request.BudgetCommitmentID, nil
		}
	}
	return "", fmt.Errorf("service closure lacks original retained budget")
}

func (model Model) validateServiceClosure() error {
	c := model.ServiceClosure
	if c == nil {
		if model.ProtocolVersion >= 10 {
			return fmt.Errorf("service closure cannot disappear from v10 journal")
		}
		return nil
	}
	budgetID, err := model.ServiceBudgetID()
	digest, hashErr := c.Request.ContextSHA256()
	if model.ProtocolVersion < 10 || model.ProtocolVersion > 11 || (model.ProtocolVersion == 11) != (c.Continuation != nil) || model.Active != nil || model.PGInstance == nil || err != nil || hashErr != nil || c.Request.Open || c.Request.Instance != *model.PGInstance || c.Request.BudgetCommitmentID != budgetID || c.CommandSHA256 != digest || c.Reason != "idle_source_unconfirmed" || c.Origin.Validate() != nil || c.Request.IssuerEpoch != c.Origin.Epoch || c.Origin.Epoch > model.Owner.Epoch || c.Origin.Epoch == model.Owner.Epoch && c.Origin != model.Owner || c.StartedSequence == 0 || c.StartedSequence > model.Sequence {
		return fmt.Errorf("service closure differs from original idle PG, budget or owner")
	}
	if h := model.InitialHeld; h != nil && (h.Admission == nil || h.Admission.Phase != Settled) {
		return fmt.Errorf("idle service closure cannot hide first admission unknown")
	}
	// StartedSequence必须严格晚于受保护的持久边界。不能只核非零/小于
	// 当前Sequence：旧预算、首次开放及最近零派发取消各自都可能合法，
	// 但把关闭写前点放到它们之前会伪造“先关闭再开放”的历史。永久
	// BudgetBoundary独立于可淘汰历史保留同一限制，接管只推进外层序列。
	var protectedSequence control.Uint64
	if h := model.InitialHeld; h != nil {
		protectedSequence = max(h.RecordedSequence, h.Admission.RecordedSequence)
	}
	if h := model.BudgetHeld; h != nil {
		protectedSequence = max(protectedSequence, h.RecordedSequence)
	}
	if h := model.CapacityHeld; h != nil {
		protectedSequence = max(protectedSequence, h.RecordedSequence)
	}
	if b := model.BudgetBoundary; b != nil {
		protectedSequence = max(protectedSequence, b.ThroughSequence)
	}
	if n := len(model.History); n != 0 {
		protectedSequence = max(protectedSequence, model.History[n-1].ClosedSequence)
	}
	if c.StartedSequence <= protectedSequence {
		return fmt.Errorf("service closure predates retained budget or completed admission boundary")
	}
	switch c.Phase {
	case Unconfirmed:
		if c.RecordedSequence != 0 || c.Receipt != nil || c.Query {
			return fmt.Errorf("unknown service closure contains completion")
		}
	case Settled:
		if c.RecordedSequence <= c.StartedSequence || c.RecordedSequence > model.Sequence || c.Receipt == nil || c.Receipt.Observed.IssuerFloorEpoch > model.Owner.Epoch || !c.Receipt.Confirms() || c.Receipt.ValidateFor(c.Request, *model.PGInstance, c.Query) != nil {
			return fmt.Errorf("service closure lacks exact native closed receipt")
		}
	default:
		return fmt.Errorf("invalid service closure phase")
	}
	return model.validateServiceClosureContinuation()
}

func (model Model) StartServiceClosure(actor Owner, request control.AdmissionRequest) (Model, error) {
	if err := model.Validate(); err != nil {
		return model, err
	}
	if model.ServiceClosure != nil {
		return model, fmt.Errorf("service closure already retained; query original")
	}
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	digest, err := request.ContextSHA256()
	if err != nil {
		return model, err
	}
	next.ProtocolVersion = 10
	next.ServiceClosure = &ServiceClosure{Origin: actor, StartedSequence: next.Sequence, Reason: "idle_source_unconfirmed", Request: request, CommandSHA256: digest, Phase: Unconfirmed, RecordedSequence: 0, Query: false, Receipt: nil, Continuation: nil}
	return next, next.Validate()
}

func (model Model) RecordServiceClosure(actor Owner, fact control.AdmissionResponse, query bool) (Model, error) {
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	c := next.ServiceClosure
	if c == nil || c.Continuation != nil || c.Phase != Unconfirmed || !query && c.Origin != actor {
		return model, fmt.Errorf("service closure is not original unknown")
	}
	c.Phase, c.Query, c.RecordedSequence, c.Receipt = Settled, query, next.Sequence, cloneAdmissionResponse(&fact)
	return next, next.Validate()
}

// 只有UID/RV写前CAS获得完整确认后返回命令票据。CAS丢回复不能向PG发送；
// 下次只能读取原WAL查询，不把当前布尔门禁补成原命令已完成。
func (store *Store) StartServiceClosure(ctx context.Context, s *Snapshot, actor Owner, request control.AdmissionRequest) (*Snapshot, *ServiceClosure, error) {
	if err := store.checked(s); err != nil {
		return nil, nil, err
	}
	if !s.Dispatchable() {
		return nil, nil, fmt.Errorf("service closure requires original running VM")
	}
	next, err := s.model.StartServiceClosure(actor, request)
	if err != nil {
		return nil, nil, err
	}
	result, err := store.apply(ctx, s, next)
	if err != nil {
		return nil, nil, err
	}
	return result, result.State().ServiceClosure, nil
}

func (store *Store) RecordServiceClosure(ctx context.Context, s *Snapshot, actor Owner, fact control.AdmissionResponse, query bool) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	next, err := s.model.RecordServiceClosure(actor, fact, query)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}

func (c *ServiceClosure) UnmarshalJSON(raw []byte) error {
	type plain ServiceClosure
	var value plain
	if err := control.UnmarshalObject(raw, &value); err != nil {
		return err
	}
	*c = ServiceClosure(value)
	return nil
}
