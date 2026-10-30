package journal

import (
	"context"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// ServiceClosure是空闲服务发现来源异常后的独立关闭WAL。它只
// 阻止新连接；原预算、历史及已存在的连接全部保留。没有受控重开协议前，
// 不允许因来源重新可达就清除此字段；完整业务完成另以CAS归档全部原文。
// 旧版本必须因v10拒绝，不能忽略门禁。
// v12另支持无Active的首次开放未知：Original逐字引用原开放，关闭回执
// 单独保存，绝不把首次开放改成Settled/NoEffect。v13保护已完整结算但
// 尚未结案的最终开放，以完整Active摘要冻结所有原领域事实；其他在途
// Resize阶段仍须独立关闭/对账，不能用本终段规则收养合法中间态。
// v14支持同一完整结算边界上尚未Start的Resume：只记录真实关闭，Original
// 保持nil，Active仍保留空Planned；旧v13读者不得误读成无Active空闲关闭。
// v15保护资源前驱已完成但Finalize仍未落账的稳定边界。预算ID仅引用
// Active的原目标承诺，不代表它已成为当前永久头；原永久预算全部保留。
// v16固定PG未派发的资源在途；v17保护完整原PG已派发的在途操作，只允许
// 原容量/提交后下一代。新代关闭不结清原Resize，也不授予原资源动作重发。
// v18固定原取消Cleanup及其完整服务边界；原实际/目标/旧容量预算分别认证，
// 不因关闭把原Cleanup的未知、完成或永久头改写成另一次恢复。
type ServiceClosure struct {
	// 外层21以后独立保留当前关闭类别；旧账本此字段必须为零。它不会
	// 随预算/历史变化推测，关闭与重开写前CAS只能按各自严格规则推进。
	FormatVersion    uint32                     `json:"format_version,omitempty"`
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
	Continuation   *ServiceClosureContinuation `json:"continuation,omitempty"`
	Original       *control.AdmissionRequest   `json:"original,omitempty"`
	ClosureReceipt *control.AdmissionClosure   `json:"closure_receipt,omitempty"`
	// v13～v18只保存完整原Active的SHA256；完整原文仍在Active中，防止关闭
	// 后借普通Record/RequestAbort/Finish重写原步骤或丢失未知效果。
	OperationSHA256 string `json:"operation_sha256,omitempty"`
	// 原生期限恢复与业务重开分域；该记录不能撤销本关闭或改变旧开放未知。
	Rearm *ServiceRearm `json:"rearm,omitempty"`
	// 新开放有独立写前/回执，不改旧关闭或任何原开放未知；完成该原生
	// 事实仍须后继来源复核及生命周期归档，本字段本身不解除关闭。
	Reopen *ServiceReopen `json:"reopen,omitempty"`
	// 类别15恢复后独立保留完整结算；原Active/Finalize未知及旧预算仍不改。
	// 只有后继开放完成CAS可以把它安装为当前永久预算。
	Finalization *ServiceFinalization `json:"finalization,omitempty"`
	// 类别18只读复核的独立取消预算；原Cleanup仍保持原相位/完整原文。
	CleanupRecovery *ServiceCleanupRecovery `json:"cleanup_recovery,omitempty"`
	// 原明确拒绝缺原生终态时的独立高floor屏障；不改原Step，不开放。
	PGRejection *ServicePGRejection `json:"pg_rejection,omitempty"`
	// 在途资源独立对账；原Active未知保留，不单独授予开放或预算释放。
	ResourceRecovery *ServiceResourceRecovery `json:"resource_recovery,omitempty"`
	// 原PG在途/终态淘汰的独立对账；隔离模式保留原Unknown，不制造NoEffect。
	PGRecovery *ServicePGRecovery `json:"pg_recovery,omitempty"`
}

// ProtectedAdmission返回本关闭保护的最初意图。v10/11原意图就是关闭；
// v12/v13为原首次/最终开放；v14/v15为没有原开放的真实关闭。返回副本，
// 不能由后继选择覆盖持久原意图。
func (c *ServiceClosure) ProtectedAdmission() control.AdmissionRequest {
	if c.Original != nil {
		return *c.Original
	}
	return c.Request
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

// 通常只引用永久完整预算的原ID；v15关闭则只引用完整Active的原目标ID，
// 不将其补成已结算预算。不能由错误文字、RSS或当前资格派生新承诺。
// 本函数不认证外部对象，也不向Root/Host发出释放许可。
func (model Model) ServiceBudgetID() (string, error) {
	// rearm/reopen是外层恢复阶段，不能把类别18的旧预算ID改成新Active目标。
	if model.serviceClosureVersion() == 18 {
		boundary, err := model.cleanupServiceBoundary()
		return boundary.BudgetCommitmentID, err
	}
	if model.ClosureFormatVersion() >= 15 && model.Active != nil {
		return model.Active.Request.BudgetCommitmentID, nil
	}
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
	version := model.serviceClosureVersion()
	if c == nil {
		if model.ProtocolVersion >= 10 && model.ProtocolVersion != 21 {
			return fmt.Errorf("service closure cannot disappear from v10 journal")
		}
		return nil
	}
	budgetID, err := model.ServiceBudgetID()
	digest, hashErr := c.Request.ContextSHA256()
	if version < 10 || version > 18 || version < 12 && ((version == 11) != (c.Continuation != nil)) || model.PGInstance == nil || err != nil || hashErr != nil || c.Request.Open || c.Request.Instance != *model.PGInstance || c.Request.BudgetCommitmentID != budgetID || c.CommandSHA256 != digest || c.Origin.Validate() != nil || c.Request.IssuerEpoch != c.Origin.Epoch || c.Origin.Epoch > model.Owner.Epoch || c.Origin.Epoch == model.Owner.Epoch && c.Origin != model.Owner || c.StartedSequence == 0 || c.StartedSequence > model.Sequence {
		return fmt.Errorf("service closure differs from original retained PG, budget or owner")
	}
	switch {
	case version == 18:
		if err := model.validateCleanupServiceClosure(); err != nil {
			return err
		}
	case c.Original != nil && version == 12:
		if model.Active != nil || c.OperationSHA256 != "" {
			return fmt.Errorf("initial service closure cannot adopt active resize")
		}
		if err := model.validateInitialServiceClosure(); err != nil {
			return err
		}
	case c.Original != nil && version == 13:
		if err := model.validateResumeServiceClosure(); err != nil {
			return err
		}
	case c.Original == nil && version == 14:
		if err := model.validatePlannedResumeServiceClosure(); err != nil {
			return err
		}
	case c.Original == nil && version == 15:
		if err := model.validatePendingFinalizationServiceClosure(); err != nil {
			return err
		}
	case version == 16 || version == 17:
		if err := model.validateActiveServiceClosure(); err != nil {
			return err
		}
	case c.Original == nil && version < 12:
		if model.Active != nil || c.OperationSHA256 != "" || c.Reason != "idle_source_unconfirmed" || c.ClosureReceipt != nil {
			return fmt.Errorf("idle service closure changed reason or original proof")
		}
		if model.InitialHeld != nil && !model.InitialAdmissionReady() {
			return fmt.Errorf("idle service closure cannot hide first admission unknown")
		}
	default:
		return fmt.Errorf("service closure lost original admission or required version")
	}
	// StartedSequence必须严格晚于受保护的持久边界。不能只核非零/小于
	// 当前Sequence：旧预算、首次开放及最近零派发取消各自都可能合法，
	// 但把关闭写前点放到它们之前会伪造“先关闭再开放”的历史。永久
	// BudgetBoundary独立于可淘汰历史保留同一限制，接管只推进外层序列。
	var protectedSequence control.Uint64
	if h := model.InitialHeld; h != nil {
		protectedSequence = max(h.RecordedSequence, h.Admission.RecordedSequence, h.Admission.StartedSequence)
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
		if c.RecordedSequence != 0 || c.Receipt != nil || c.ClosureReceipt != nil || c.Query {
			return fmt.Errorf("unknown service closure contains completion")
		}
	case Settled:
		fact := c.Receipt
		if c.Original != nil && c.ClosureReceipt != nil {
			fact = &c.ClosureReceipt.ClosedBy
		}
		if c.RecordedSequence <= c.StartedSequence || c.RecordedSequence > model.Sequence || fact == nil || fact.Observed.IssuerFloorEpoch > model.Owner.Epoch || !fact.Confirms() || fact.ValidateFor(c.Request, *model.PGInstance, c.Query) != nil {
			return fmt.Errorf("service closure lacks exact native closed receipt")
		}
	default:
		return fmt.Errorf("invalid service closure phase")
	}
	if err := model.validateServiceClosureContinuation(); err != nil {
		return err
	}
	if err := model.validateServiceFinalization(); err != nil {
		return err
	}
	if err := model.validateServiceResourceRecovery(); err != nil {
		return err
	}
	if err := model.validateServicePGRecovery(); err != nil {
		return err
	}
	if err := model.validateServicePGRejection(); err != nil {
		return err
	}
	if err := model.validateServiceCleanupRecovery(); err != nil {
		return err
	}
	if err := model.validateServiceRearm(); err != nil {
		return err
	}
	return model.validateServiceReopen()
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
	next.ServiceClosure = &ServiceClosure{Origin: actor, StartedSequence: next.Sequence, Reason: "idle_source_unconfirmed", Request: request, CommandSHA256: digest, Phase: Unconfirmed, RecordedSequence: 0, Query: false, Receipt: nil, Continuation: nil, Original: nil, ClosureReceipt: nil, OperationSHA256: ""}
	if model.Active != nil {
		// 完整Active不变地纳入摘要；普通Record/取消/Finish会因摘要或
		// 稳定边界校验失败而拒绝。这里只生成模型，Store仍须写前CAS。
		opDigest, err := operationClosureDigest(model.Active)
		if err != nil {
			return model, err
		}
		next.ProtocolVersion = 14
		next.ServiceClosure.Reason = "planned_resume_source_unconfirmed"
		next.ServiceClosure.OperationSHA256 = opDigest
		if _, err := model.plannedResumeBudget(); err != nil {
			next.ProtocolVersion = 15
			next.ServiceClosure.Reason = "finalize_source_unconfirmed"
		}
	}
	next.setClosureFormatVersion(next.ProtocolVersion)
	return next, next.Validate()
}

func (model Model) RecordServiceClosure(actor Owner, fact control.AdmissionResponse, query bool) (Model, error) {
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	c := next.ServiceClosure
	if c == nil || c.Original != nil || c.Continuation != nil || c.Phase != Unconfirmed || !query && c.Origin != actor {
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
	if err := control.UnmarshalObjectBounded(raw, &value, MaxRawBytes/2, 16384); err != nil {
		return err
	}
	*c = ServiceClosure(value)
	return nil
}
