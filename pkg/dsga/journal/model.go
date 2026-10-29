// Package journal 保存DSGA完整意图、owner代次及外部动作的不确定性。
// 本包是有界恢复账本，不认证租约/回执来源，也不发放VM许可；调用方必须是已认证
// 的协调器，并在记录结论前核验实际组件事实。最终Runner仍须独立fence和串行执行。
package journal

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

const (
	HistoryLimit        = 16
	MaxObservationBytes = 32 * 1024
	MaxCommandBytes     = 8 * 1024
	// 只有v2/v3实际预算Cleanup携带完整原Initial时需要双份来源上下文。
	// 此独立上限不能用于普通命令，选择前先核有界JSON及完整typed合同；
	// 整体VM注解/Observation/值节点上限不变，旧读者明确拒绝新capacity版本。
	MaxInitialCapacityCommandBytes = 16 * 1024
	MaxRawBytes                    = 1024 * 1024
)

type Owner struct {
	// HolderID必须由上层为每个协调器进程生成独立身份，不能使用Pod名等可复用值。
	// Epoch在同VM incarnation内只增不减；这里记录已认证的决定，不证明租约归属。
	HolderID string         `json:"holder_id"`
	Epoch    control.Uint64 `json:"epoch"`
}

type Action string

const (
	PrepareBudget   Action = "prepare_budget"
	ReserveHost     Action = "reserve_host"
	Quiesce         Action = "quiesce"
	ResizePG        Action = "resize_pg"
	ReleasePG       Action = "release_pg"
	ResizeVM        Action = "resize_vm"
	GuestReady      Action = "guest_ready"
	ObserveVM       Action = "observe_vm"
	FinalizeBudget  Action = "finalize_budget"
	ResumeAdmission Action = "resume_admission"
	AbortReconcile  Action = "abort_reconcile"
)

type Phase string

const (
	Planned     Phase = "planned"
	Unconfirmed Phase = "unconfirmed"
	Settled     Phase = "settled"
)

type Result string

const (
	Completed Result = "completed"
	Rejected  Result = "rejected"
	NoEffect  Result = "no_effect"
)

type Outcome string

const (
	Succeeded             Outcome = "succeeded"
	AbortedBeforeDispatch Outcome = "aborted_before_dispatch"
	AbortedAfterReconcile Outcome = "aborted_after_reconcile"
)

type Disposition string

const (
	Accepted        Disposition = "accepted"
	Replayed        Disposition = "replayed"
	Busy            Disposition = "busy"
	Conflict        Disposition = "conflict"
	ResultExpired   Disposition = "result_expired"
	Stale           Disposition = "stale"
	DeadlineExpired Disposition = "deadline_expired"
)

// Observation只存储上层核验后的结论和完整审计body。不能从旧Ok、HTTP200、查询
// not-found或单次RSS采样构造Completed/NoEffect；来源真实性与领域约束由适配器负责。
// Data不得含Bearer/密码等凭据。32KiB与正式HTTP响应同界，完整回执不得截断；
// 命令另限8KiB，不能用任意大日志填满VM对象。总持久化字节预算仍独立核验。
type Observation struct {
	Key           control.Key    `json:"key"`
	RequestSHA256 string         `json:"request_sha256"`
	CommandSHA256 string         `json:"command_sha256"`
	Ordinal       control.Uint64 `json:"ordinal"`
	Action        Action         `json:"action"`
	Result        Result         `json:"result"`
	SourceID      string         `json:"source_id"`
	Data          []byte         `json:"data"`
}

type Step struct {
	Ordinal       control.Uint64 `json:"ordinal"`
	Action        Action         `json:"action"`
	TargetID      string         `json:"target_id"`
	Command       []byte         `json:"command"`
	CommandSHA256 string         `json:"command_sha256"`
	Phase         Phase          `json:"phase"`
	DispatchedBy  *Owner         `json:"dispatched_by"`
	Observation   *Observation   `json:"observation"`
}

type Operation struct {
	Request        control.Request `json:"request"`
	RequestSHA256  string          `json:"request_sha256"`
	Origin         Owner           `json:"origin"`
	AbortRequested bool            `json:"abort_requested"`
	Steps          []Step          `json:"steps"`
	Cleanup        *Step           `json:"cleanup"`
	// 首次Start与撤销顶层关闭证明同CAS保存。它不是当前许可；只供后续
	// 完整取消核验“本操作之前”的原预算关闭链，历史淘汰后也不能猜测补造。
	PriorBudgetBoundary *ClosedBudgetBoundary `json:"prior_budget_boundary,omitempty"`
}

type Record struct {
	Operation      Operation      `json:"operation"`
	Outcome        Outcome        `json:"outcome"`
	ClosedSequence control.Uint64 `json:"closed_sequence"`
}

type Model struct {
	ProtocolVersion uint32         `json:"protocol_version"`
	VMUID           string         `json:"vm_uid"`
	Incarnation     string         `json:"incarnation"`
	Sequence        control.Uint64 `json:"sequence"`
	Owner           Owner          `json:"owner"`
	HighWater       *control.Key   `json:"high_water"`
	Active          *Operation     `json:"active"`
	History         []Record       `json:"history"`
	ExpiredResults  control.Uint64 `json:"expired_results"`
	// Guest资源生命周期独立于PG boot。首次只读发现后永久绑定本incarnation；
	// 历史淘汰/PG重启/接管不能清空。缺字段兼容旧未启用账本，不允许带旧操作
	// 的账本自动收养当前Guest；跨Guest恢复须显式证明旧资源/VM效果已经关闭。
	ResourceIdentity *resourceguard.Identity `json:"resource_identity,omitempty"`
	// PG首次发现后、任何执行器可用之前即固定完整实例，不等待首个Resize/HWM。
	// owner接管、空Active和历史淘汰均不能使它消失；否则首次准入之后、尚未
	// 伸缩时重建worker会把另一个boot收养为新实例。旧缺字段日志只按原HWM
	// 恢复，不在线补写猜测。受控跨boot须另行关闭旧未知/预算，不能调用BindPG替换。
	PGInstance *control.Instance `json:"pg_instance,omitempty"`
	// 当前完整预算有独立于Active/终态历史的寿命。结算Completed与替换此字段
	// 必须同一次VM UID/RV CAS；历史淘汰、取消及owner接管均不能清除。保留完整
	// 原命令/回执，不能只留下数字、可复用SourceID或当前RSS。本字段首次写入
	// 同时升级Model v3，旧读者必须拒绝，控制面后续移植时再统一升级读者。
	BudgetHeld *HeldBudget `json:"budget_held,omitempty"`
	// 部分效果取消使用独立实际预算，和普通目标成功预算互斥。Cleanup
	// 完整落账与长期持有同CAS升级v8，禁止旧读者投影为目标已成功。
	CapacityHeld *HeldCapacity `json:"capacity_held,omitempty"`
	// 首次服务预算独立于任何Resize/HWM。v9显式保留完整预算及首次开放
	// 原WAL；未知开放不能因为Active为空而消失，普通预算完成后同CAS替换。
	InitialHeld *HeldInitialBudget `json:"initial_held,omitempty"`
	// BudgetBoundary永久记录当前预算成功关闭后，直到哪条零派发取消仍没有
	// 后续派发效果。它与Finish同CAS，独立于16条结果缓存；任何Start先撤销
	// 此证明，新预算也必须重新成功关闭。首次生成升v5，旧读者必须拒绝。
	BudgetBoundary *ClosedBudgetBoundary `json:"budget_boundary,omitempty"`
	// 异常服务关闭独立于Resize和预算终态，保留原命令及未知。v10旧
	// 读者必须拒绝；只有完整业务完成CAS才能将它移入ServiceLifecycle，
	// 关门本身不证明旧连接排空，也不解除任何资源持有。
	ServiceClosure *ServiceClosure `json:"service_closure,omitempty"`
	// 完成一次受控重开后永久保留最近完整恢复证据和退休水位。外层格式21
	// 必须包含此字段；当前关闭类别独立保存，不能删除关闭后降回旧格式。
	ServiceLifecycle *ServiceLifecycle `json:"service_lifecycle,omitempty"`
}

func New(vmUID, incarnation, holder string) (Model, error) {
	model := Model{
		ProtocolVersion: 1, VMUID: vmUID, Incarnation: incarnation, Sequence: 1,
		Owner: Owner{HolderID: holder, Epoch: 1}, HighWater: nil, Active: nil, History: []Record{}, ExpiredResults: 0,
		ResourceIdentity: nil, PGInstance: nil, BudgetHeld: nil, CapacityHeld: nil, InitialHeld: nil, BudgetBoundary: nil, ServiceClosure: nil, ServiceLifecycle: nil,
	}
	return model, model.Validate()
}

func (owner Owner) Validate() error {
	if err := control.ValidateID(owner.HolderID, "owner process"); err != nil {
		return err
	}
	if owner.Epoch == 0 {
		return fmt.Errorf("zero owner epoch")
	}
	return nil
}

func validIncarnation(value string) bool {
	return control.ValidateIncarnation(value) == nil
}

// 这是账本自己的领域分离摘要，不是PG jsonb canonical context_sha256；不能混用。
func RequestDigest(request control.Request) (string, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	data, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(append([]byte("neon-dsga-journal-request-v1\x00"), data...))
	return hex.EncodeToString(hash[:]), nil
}

func plan(kind control.Kind) []Action {
	if kind == control.Grow {
		return []Action{PrepareBudget, ReserveHost, ResizeVM, GuestReady, ResizePG, FinalizeBudget, ResumeAdmission}
	}
	return []Action{PrepareBudget, Quiesce, ResizePG, ReleasePG, GuestReady, ResizeVM, ObserveVM, FinalizeBudget, ResumeAdmission}
}

func (observation Observation) Validate() error {
	if err := observation.Key.Validate(); err != nil {
		return err
	}
	if observation.Ordinal == 0 || len(observation.RequestSHA256) != 64 || len(observation.CommandSHA256) != 64 {
		return fmt.Errorf("invalid observation identity")
	}
	if _, err := hex.DecodeString(observation.RequestSHA256); err != nil {
		return err
	}
	if _, err := hex.DecodeString(observation.CommandSHA256); err != nil {
		return err
	}
	if observation.Result != Completed && observation.Result != Rejected && observation.Result != NoEffect {
		return fmt.Errorf("invalid observation result")
	}
	if err := control.ValidateID(observation.SourceID, "observation source"); err != nil {
		return err
	}
	if err := control.ValidateJSON(observation.Data, MaxObservationBytes, 2048); err != nil {
		return err
	}
	if data := bytes.TrimSpace(observation.Data); len(data) == 0 || data[0] != '{' {
		return fmt.Errorf("observation requires an object body")
	}
	return nil
}

func (step Step) validate(operation Operation, ordinal int, action Action) error {
	if step.Ordinal != control.Uint64(ordinal) || step.Action != action {
		return fmt.Errorf("step order/action changed")
	}
	switch step.Phase {
	case Planned:
		if step.DispatchedBy != nil || step.Observation != nil || step.TargetID != "" || len(step.Command) != 0 || step.CommandSHA256 != "" {
			return fmt.Errorf("undispatched step contains execution facts")
		}
	case Unconfirmed, Settled:
		if err := control.ValidateID(step.TargetID, "effect target"); err != nil {
			return err
		}
		if step.DispatchedBy == nil {
			return fmt.Errorf("dispatched step lacks owner")
		}
		if err := step.DispatchedBy.Validate(); err != nil {
			return err
		}
		hash, err := commandDigest(operation.RequestSHA256, step)
		if err != nil {
			return err
		}
		if hash != step.CommandSHA256 {
			return fmt.Errorf("immutable command digest differs")
		}
		if action != AbortReconcile && *step.DispatchedBy != operation.Origin {
			return fmt.Errorf("new owner cannot dispatch an old operation")
		}
		if step.Phase == Unconfirmed && step.Observation != nil {
			return fmt.Errorf("unconfirmed step contains a terminal observation")
		}
		if step.Phase == Settled {
			if step.Observation == nil {
				return fmt.Errorf("settled step lacks observation")
			}
			if err := step.Observation.Validate(); err != nil {
				return err
			}
			if step.Observation.Key != operation.Request.Key || step.Observation.RequestSHA256 != operation.RequestSHA256 ||
				step.Observation.Ordinal != step.Ordinal || step.Observation.Action != step.Action || step.Observation.SourceID != step.TargetID || step.Observation.CommandSHA256 != step.CommandSHA256 {
				return fmt.Errorf("observation belongs to another step/intent")
			}
		}
	default:
		return fmt.Errorf("invalid step phase")
	}
	return nil
}

func successful(step Step) bool {
	return step.Phase == Settled && step.Observation != nil && step.Observation.Result != Rejected
}

func (operation Operation) Validate() error {
	hash, err := RequestDigest(operation.Request)
	if err != nil {
		return err
	}
	if hash != operation.RequestSHA256 {
		return fmt.Errorf("immutable operation digest differs")
	}
	if err := operation.Origin.Validate(); err != nil {
		return err
	}
	if operation.Origin.Epoch != operation.Request.Key.IssuerEpoch {
		return fmt.Errorf("operation epoch differs from original owner")
	}
	actions := plan(operation.Request.Kind)
	if len(operation.Steps) != len(actions) {
		return fmt.Errorf("operation plan changed")
	}
	priorSuccessful := true
	for i, step := range operation.Steps {
		if err := step.validate(operation, i+1, actions[i]); err != nil {
			return err
		}
		if step.Phase != Planned && !priorSuccessful {
			return fmt.Errorf("step dispatched before predecessor settled successfully")
		}
		priorSuccessful = priorSuccessful && successful(step)
	}
	if operation.Cleanup != nil {
		if !operation.AbortRequested {
			return fmt.Errorf("cleanup requires an abort intent")
		}
		for _, step := range operation.Steps {
			if step.Phase == Unconfirmed {
				return fmt.Errorf("cleanup crosses an unconfirmed effect")
			}
		}
		if err := operation.Cleanup.validate(operation, len(actions)+1, AbortReconcile); err != nil {
			return err
		}
	}
	return nil
}

func (operation Operation) closingOutcome() (Outcome, error) {
	if !operation.AbortRequested {
		for _, step := range operation.Steps {
			if !successful(step) {
				return "", fmt.Errorf("operation still has incomplete/rejected steps")
			}
		}
		if operation.Cleanup != nil {
			return "", fmt.Errorf("success cannot contain cleanup")
		}
		return Succeeded, nil
	}
	started := false
	for _, step := range operation.Steps {
		if step.Phase == Unconfirmed {
			return "", fmt.Errorf("cannot close an unconfirmed effect")
		}
		started = started || step.Phase != Planned
	}
	if !started && operation.Cleanup == nil {
		return AbortedBeforeDispatch, nil
	}
	if operation.Cleanup == nil || !successful(*operation.Cleanup) {
		return "", fmt.Errorf("abort requires completed budget/admission reconciliation")
	}
	return AbortedAfterReconcile, nil
}

func compareKeys(left, right control.Key) int {
	if left.IssuerEpoch < right.IssuerEpoch {
		return -1
	}
	if left.IssuerEpoch > right.IssuerEpoch {
		return 1
	}
	if left.Revision < right.Revision {
		return -1
	}
	if left.Revision > right.Revision {
		return 1
	}
	return 0
}

func (model Model) Validate() error {
	if err := model.validateServiceLifecycle(); err != nil {
		return err
	}
	return model.validateCore()
}

// validateCore只供顶层和已去除生命周期嵌套的冻结上下文调用。上下文不赋
// 当前许可；其字节摘要、归属和有界性必须先由validateServiceLifecycle核验。
func (model Model) validateCore() error {
	if (model.ProtocolVersion < 1 || model.ProtocolVersion > 21) || model.Sequence == 0 || !validIncarnation(model.Incarnation) {
		return fmt.Errorf("invalid journal version/sequence/incarnation")
	}
	// 旧Model已有严格字段解码，会拒绝pg_instance；这里另用v2明确标识
	// 必须保存首次PG身份的新语义。首次固定与格式升级在同一次CAS，旧
	// 读者即便只收到旧字段投影也仍拒绝版本。创建仍是v1，v2不可缺PG。
	if (model.ProtocolVersion == 1 && model.PGInstance != nil) || (model.ProtocolVersion >= 2 && model.PGInstance == nil) {
		return fmt.Errorf("journal version does not preserve required PG identity")
	}
	// 旧v2实际容量Cleanup可能尚无永久头。v18仅允许其独立安全关闭：完整
	// 原Cleanup及实际预算由validateCleanupServiceClosure继续严格验证，
	// 不把无头状态升级成已持有预算，也不放宽其他格式的永久头要求。
	cleanupClosureOnly := model.ClosureFormatVersion() == 18 && model.ServiceClosure != nil
	if (model.ProtocolVersion < 3 && model.BudgetHeld != nil) || (model.CapacityHeld != nil && (model.ProtocolVersion < 8 || model.BudgetHeld != nil || model.InitialHeld != nil)) || (model.InitialHeld != nil && (model.ProtocolVersion < 9 || model.BudgetHeld != nil)) || (model.ProtocolVersion >= 3 && ((model.BudgetHeld == nil && model.CapacityHeld == nil && model.InitialHeld == nil && !cleanupClosureOnly) || model.ResourceIdentity == nil)) {
		return fmt.Errorf("journal version does not preserve required held budget")
	}
	if err := control.ValidateID(model.VMUID, "VM"); err != nil {
		return err
	}
	if err := model.Owner.Validate(); err != nil {
		return err
	}
	if model.ResourceIdentity != nil {
		if model.Sequence < 2 || model.ResourceIdentity.Validate() != nil || model.ResourceIdentity.VMUID != model.VMUID || (model.HighWater != nil && model.ResourceIdentity.ComputeInstanceID != model.HighWater.Instance.ComputeInstanceID) {
			return fmt.Errorf("journal resource identity differs from original Compute")
		}
	}
	if model.PGInstance != nil {
		if model.Sequence < 2 || model.PGInstance.Validate() != nil || model.PGInstance.VMUID != model.VMUID ||
			(model.ResourceIdentity != nil && model.PGInstance.ComputeInstanceID != model.ResourceIdentity.ComputeInstanceID) ||
			(model.HighWater != nil && model.HighWater.Instance != *model.PGInstance) {
			return fmt.Errorf("journal PG identity differs from original resource/high water")
		}
	}
	if model.History == nil || len(model.History) > HistoryLimit {
		return fmt.Errorf("invalid journal history")
	}
	if model.HighWater != nil {
		if err := model.HighWater.Validate(); err != nil {
			return err
		}
		if model.HighWater.Instance.VMUID != model.VMUID || model.HighWater.IssuerEpoch > model.Owner.Epoch {
			return fmt.Errorf("invalid journal high water")
		}
	}
	var previous *Record
	for i := range model.History {
		record := &model.History[i]
		if err := model.validateOwners(record.Operation); err != nil {
			return err
		}
		if err := record.Operation.Validate(); err != nil {
			return err
		}
		outcome, err := record.Operation.closingOutcome()
		if err != nil {
			return err
		}
		if record.Outcome != outcome || record.ClosedSequence == 0 || record.ClosedSequence > model.Sequence || record.Operation.PriorBudgetBoundary != nil && record.Operation.PriorBudgetBoundary.ThroughSequence >= record.ClosedSequence ||
			record.Operation.Request.Key.Instance.VMUID != model.VMUID || model.HighWater == nil || compareKeys(record.Operation.Request.Key, *model.HighWater) > 0 {
			return fmt.Errorf("invalid journal terminal record")
		}
		if model.PGInstance != nil && record.Operation.Request.Key.Instance != *model.PGInstance {
			return fmt.Errorf("journal history differs from retained PG identity")
		}
		if previous != nil && (previous.ClosedSequence >= record.ClosedSequence || compareKeys(previous.Operation.Request.Key, record.Operation.Request.Key) >= 0) {
			return fmt.Errorf("journal history is not monotonic")
		}
		previous = record
	}
	if model.Active != nil {
		if err := model.validateOwners(*model.Active); err != nil {
			return err
		}
		if err := model.Active.Validate(); err != nil {
			return err
		}
		if model.HighWater == nil || model.Active.Request.Key != *model.HighWater || model.Active.Request.Key.Instance.VMUID != model.VMUID {
			return fmt.Errorf("active operation differs from high water")
		}
		if previous != nil && compareKeys(previous.Operation.Request.Key, model.Active.Request.Key) >= 0 {
			return fmt.Errorf("active operation reuses a retired revision")
		}
	}
	if err := model.validateHeldBudget(); err != nil {
		return err
	}
	if err := model.validateHeldCapacity(); err != nil {
		return err
	}
	if err := model.validateInitialBudget(); err != nil {
		return err
	}
	if err := model.validateServiceClosure(); err != nil {
		return err
	}
	return model.validateBudgetBoundary()
}

func (model Model) validateOwners(operation Operation) error {
	if err := model.validatePriorBoundary(operation); err != nil {
		return err
	}
	if operation.Origin.Epoch > model.Owner.Epoch || (operation.Origin.Epoch == model.Owner.Epoch && operation.Origin != model.Owner) {
		return fmt.Errorf("operation owner conflicts with journal epoch")
	}
	if operation.Cleanup != nil && operation.Cleanup.DispatchedBy != nil {
		owner := *operation.Cleanup.DispatchedBy
		if owner.Epoch < operation.Origin.Epoch || owner.Epoch > model.Owner.Epoch ||
			(owner.Epoch == model.Owner.Epoch && owner != model.Owner) || (owner.Epoch == operation.Origin.Epoch && owner != operation.Origin) {
			return fmt.Errorf("cleanup owner conflicts with journal epoch")
		}
	}
	return nil
}

func cloneStep(step Step) Step {
	step.Command = append([]byte(nil), step.Command...)
	if step.DispatchedBy != nil {
		owner := *step.DispatchedBy
		step.DispatchedBy = &owner
	}
	if step.Observation != nil {
		observation := *step.Observation
		observation.Data = append([]byte(nil), observation.Data...)
		step.Observation = &observation
	}
	return step
}

func cloneOperation(operation Operation) Operation {
	if operation.PriorBudgetBoundary != nil {
		prior := *operation.PriorBudgetBoundary
		operation.PriorBudgetBoundary = &prior
	}
	steps := make([]Step, len(operation.Steps))
	for i, step := range operation.Steps {
		steps[i] = cloneStep(step)
	}
	operation.Steps = steps
	if operation.Cleanup != nil {
		step := cloneStep(*operation.Cleanup)
		operation.Cleanup = &step
	}
	return operation
}

func (model Model) Clone() Model {
	if model.ServiceLifecycle != nil {
		lifecycle := *model.ServiceLifecycle
		lifecycle.Context = append([]byte(nil), lifecycle.Context...)
		lifecycle.Current = cloneLeaseResponse(lifecycle.Current)
		if lifecycle.Previous != nil {
			previous := *lifecycle.Previous
			previous.Current = cloneLeaseResponse(previous.Current)
			lifecycle.Previous = &previous
		}
		model.ServiceLifecycle = &lifecycle
	}
	if model.ServiceClosure != nil {
		closure := *model.ServiceClosure
		if closure.Reopen != nil {
			reopen := *closure.Reopen
			reopen.Before = cloneLeaseResponse(reopen.Before)
			if reopen.Receipt != nil {
				reopen.Receipt = cloneAdmissionResponse(reopen.Receipt)
			}
			reopen.Closing = cloneServiceReopenClosure(reopen.Closing)
			closure.Reopen = &reopen
		}
		if closure.Rearm != nil {
			rearm := *closure.Rearm
			rearm.Before = cloneLeaseResponse(rearm.Before)
			if rearm.Receipt != nil {
				receipt := *rearm.Receipt
				receipt.Current = cloneLeaseResponse(receipt.Current)
				rearm.Receipt = &receipt
			}
			closure.Rearm = &rearm
		}
		if closure.Original != nil {
			original := *closure.Original
			closure.Original = &original
		}
		if closure.ClosureReceipt != nil {
			proof := *closure.ClosureReceipt
			proof.ClosedBy = *cloneAdmissionResponse(&proof.ClosedBy)
			closure.ClosureReceipt = &proof
		}
		if closure.Continuation != nil {
			continuation := *closure.Continuation
			if continuation.Receipt != nil {
				fact := *continuation.Receipt
				fact.ClosedBy = *cloneAdmissionResponse(&fact.ClosedBy)
				continuation.Receipt = &fact
			}
			closure.Continuation = &continuation
		}
		if closure.Receipt != nil {
			// Snapshot含数组、release key和操作指针，逐层复制避免调用方
			// 修改快照内原生回执；不持锁，也不将指针共享给管理入口。
			closure.Receipt = cloneAdmissionResponse(closure.Receipt)
		}
		model.ServiceClosure = &closure
	}
	model.InitialHeld = cloneInitial(model.InitialHeld)
	if model.BudgetBoundary != nil {
		boundary := *model.BudgetBoundary
		model.BudgetBoundary = &boundary
	}
	if model.BudgetHeld != nil {
		copy := *model.BudgetHeld
		copy.Finalization = cloneStep(copy.Finalization)
		model.BudgetHeld = &copy
	}
	if model.CapacityHeld != nil {
		held := *model.CapacityHeld
		held.Cleanup = cloneStep(held.Cleanup)
		model.CapacityHeld = &held
	}
	if model.PGInstance != nil {
		copy := *model.PGInstance
		model.PGInstance = &copy
	}
	if model.ResourceIdentity != nil {
		copy := *model.ResourceIdentity
		model.ResourceIdentity = &copy
	}
	if model.HighWater != nil {
		key := *model.HighWater
		model.HighWater = &key
	}
	if model.Active != nil {
		operation := cloneOperation(*model.Active)
		model.Active = &operation
	}
	history := make([]Record, len(model.History))
	for i, record := range model.History {
		record.Operation = cloneOperation(record.Operation)
		history[i] = record
	}
	model.History = history
	return model
}

// BindResource只允许没有任何原操作/HWM的账本首次冻结；actor接管可已经推进
// Sequence，但不能借淘汰历史或新PG boot把有旧效果的账本当成初次创建。
func (model Model) BindResource(actor Owner, id resourceguard.Identity) (Model, error) {
	if err := model.Validate(); err != nil {
		return model, err
	}
	if actor != model.Owner || id.Validate() != nil || id.VMUID != model.VMUID || (model.PGInstance != nil && id.ComputeInstanceID != model.PGInstance.ComputeInstanceID) {
		return model, fmt.Errorf("invalid resource binding owner or Guest")
	}
	if model.ResourceIdentity != nil {
		if *model.ResourceIdentity != id {
			return model, fmt.Errorf("resource Guest cannot be rebound")
		}
		return model, nil
	}
	if model.HighWater != nil || model.Active != nil || len(model.History) != 0 || model.ExpiredResults != 0 {
		return model, fmt.Errorf("resource discovery requires no original operations")
	}
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	copy := id
	next.ResourceIdentity = &copy
	return next, next.Validate()
}

// BindPG仅在没有原操作/HWM/历史的账本冻结首次实例；相同值只幂等读取。
// 本函数不认证PG来源，调用者须经固定Compute的能力协商获得真实boot，随后
// Store以原VM UID/RV CAS落盘。旧有操作但缺此字段时不能从当前PG补齐；它们
// 已有HWM身份，恢复继续采用原HWM。失败返回原model，不分配新epoch/revision。
func (model Model) BindPG(actor Owner, instance control.Instance) (Model, error) {
	if err := model.Validate(); err != nil {
		return model, err
	}
	if actor != model.Owner || instance.Validate() != nil || instance.VMUID != model.VMUID ||
		(model.ResourceIdentity != nil && instance.ComputeInstanceID != model.ResourceIdentity.ComputeInstanceID) {
		return model, fmt.Errorf("invalid initial PG binding owner or instance")
	}
	if model.PGInstance != nil {
		if *model.PGInstance != instance {
			return model, fmt.Errorf("PG boot cannot be rebound without controlled recovery")
		}
		return model, nil
	}
	if model.HighWater != nil || model.Active != nil || len(model.History) != 0 || model.ExpiredResults != 0 {
		return model, fmt.Errorf("initial PG binding cannot adopt old operation history")
	}
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	copy := instance
	next.ProtocolVersion = 2
	next.PGInstance = &copy
	return next, next.Validate()
}

func (model Model) next(actor Owner) (Model, error) {
	if err := model.Validate(); err != nil {
		return model, err
	}
	if actor != model.Owner {
		return model, fmt.Errorf("journal owner is stale")
	}
	if model.Sequence == control.Uint64(math.MaxUint64) {
		return model, fmt.Errorf("journal sequence exhausted")
	}
	model = model.Clone()
	model.Sequence++
	return model, nil
}

// Takeover只记录上层已经认证的领导权决定，不验证调用者的租约。epoch递增但所有
// 在途动作/预算引用保持原样；新owner只能对账/取消旧操作，不能继续发出旧代次动作。
func (model Model) Takeover(actor Owner, nextHolder string) (Model, error) {
	if err := control.ValidateID(nextHolder, "owner process"); err != nil {
		return model, err
	}
	if actor != model.Owner {
		return model, fmt.Errorf("journal owner is stale")
	}
	if actor.HolderID == nextHolder {
		return model.Clone(), model.Validate()
	}
	if actor.Epoch == control.Uint64(math.MaxUint64) {
		return model, fmt.Errorf("owner epoch exhausted")
	}
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	next.Owner = Owner{HolderID: nextHolder, Epoch: actor.Epoch + 1}
	return next, next.Validate()
}

func (model Model) Lookup(request control.Request) Disposition {
	if model.Active != nil && model.Active.Request.Key == request.Key {
		if model.Active.Request == request {
			return Replayed
		}
		return Conflict
	}
	for _, record := range model.History {
		if record.Operation.Request.Key == request.Key {
			if record.Operation.Request == request {
				return Replayed
			}
			return Conflict
		}
	}
	if model.HighWater != nil && compareKeys(request.Key, *model.HighWater) <= 0 {
		if compareKeys(request.Key, *model.HighWater) == 0 && request.Key != *model.HighWater {
			return Conflict
		}
		return ResultExpired
	}
	return Stale
}

func (model Model) Begin(actor Owner, request control.Request, now uint64) (Model, Disposition, error) {
	if err := model.Validate(); err != nil {
		return model, Stale, err
	}
	if actor != model.Owner {
		return model, Stale, fmt.Errorf("journal owner is stale")
	}
	if err := request.Validate(); err != nil {
		return model, Stale, err
	}
	if request.Key.Instance.VMUID != model.VMUID {
		return model, Stale, fmt.Errorf("request targets another VM")
	}
	if model.PGInstance != nil && request.Key.Instance != *model.PGInstance {
		return model, Stale, fmt.Errorf("request differs from durably bound PG boot")
	}
	// 验证当前调用owner后，先查原意图，再校验请求期限/原issuer代次。跨owner重放
	// 只返回已保存事实，不给旧动作新派发权；actor身份仍需上层认证，不来自请求自报。
	if disposition := model.Lookup(request); disposition != Stale {
		return model.Clone(), disposition, nil
	}
	if model.ServiceClosure != nil {
		return model, Busy, nil
	}
	if request.Key.IssuerEpoch != actor.Epoch {
		return model, Stale, nil
	}
	if model.Active != nil {
		return model, Busy, nil
	}
	if model.InitialHeld != nil {
		// 首次门禁未知必须先在独立WAL对账，不能以没有Active Resize为由
		// 开始新动作。完成后首个请求仍固定原初始容量代次，不借过期请求
		// 或外部自报generation覆盖当前预算。
		a := model.InitialHeld.Admission
		if a == nil || !model.InitialAdmissionReady() {
			return model, Busy, nil
		}
		var initial budget.Initial
		if json.Unmarshal(model.InitialHeld.Budget, &initial) != nil || request.ExpectedCapacityGeneration != initial.Shared.Observed.CapacityGeneration {
			return model, Stale, fmt.Errorf("resize differs from retained initial capacity generation")
		}
	}
	// 旧v1没有独立PG字段，但已有HWM同样是原实例约束。可读原历史，
	// 新请求不能先改HWM再使NewPGWorker接受另一boot；受控跨boot关闭协议
	// 未完成前明确拒绝，既不丢原效果，也不偷偷把v1补成首次发现。
	if model.HighWater != nil && request.Key.Instance != model.HighWater.Instance {
		return model, Stale, fmt.Errorf("new request cannot replace retained PG high water")
	}
	if _, valid := request.RemainingWaitMillis(now); !valid {
		return model, DeadlineExpired, nil
	}
	next, err := model.next(actor)
	if err != nil {
		return model, Stale, err
	}
	hash, err := RequestDigest(request)
	if err != nil {
		return model, Stale, err
	}
	actions := plan(request.Kind)
	steps := make([]Step, len(actions))
	for i, action := range actions {
		steps[i] = Step{Ordinal: control.Uint64(i + 1), Action: action, TargetID: "", Command: nil, CommandSHA256: "", Phase: Planned, DispatchedBy: nil, Observation: nil}
	}
	next.Active = &Operation{Request: request, RequestSHA256: hash, Origin: actor, AbortRequested: false, Steps: steps, Cleanup: nil, PriorBudgetBoundary: nil}
	key := request.Key
	next.HighWater = &key
	return next, Accepted, next.Validate()
}

func (model Model) active(actor Owner, key control.Key) (Model, error) {
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	if next.Active == nil || next.Active.Request.Key != key {
		return model, fmt.Errorf("operation is not active")
	}
	return next, nil
}

// Start登记Unconfirmed必须先持久化成功，调用方才能发送外部命令。API超时没有
// 派发票据，必须读取对账，不能把内存里构造出的本Model当作已持久提交。
func (model Model) Start(actor Owner, key control.Key, action Action, targetID string, command []byte) (Model, error) {
	if err := control.ValidateID(targetID, "effect target"); err != nil {
		return model, err
	}
	next, err := model.active(actor, key)
	if err != nil {
		return model, err
	}
	operation := next.Active
	if operation.AbortRequested || operation.Origin != actor {
		return model, fmt.Errorf("operation cannot dispatch after abort/takeover")
	}
	for i := range operation.Steps {
		step := &operation.Steps[i]
		if step.Phase == Planned {
			if step.Action != action {
				return model, fmt.Errorf("action is out of order")
			}
			owner := actor
			step.Phase = Unconfirmed
			step.TargetID = targetID
			step.DispatchedBy = &owner
			step.Command = append([]byte(nil), command...)
			hash, err := commandDigest(operation.RequestSHA256, *step)
			if err != nil {
				return model, err
			}
			step.CommandSHA256 = hash
			// 写前票据一旦可能生效，旧预算的“之后无派发”证明立即在同CAS
			// 失效；不能等真正HTTP/QMP回复后再撤销。完整预算本身继续持有。
			if next.BudgetBoundary != nil && next.Active.PriorBudgetBoundary == nil {
				prior := *next.BudgetBoundary
				next.Active.PriorBudgetBoundary = &prior
				next.ProtocolVersion = max(next.ProtocolVersion, 7)
			}
			next.BudgetBoundary = nil
			return next, next.Validate()
		}
		if !successful(*step) {
			return model, fmt.Errorf("previous effect requires reconciliation")
		}
	}
	return model, fmt.Errorf("operation has no pending action")
}

func (model Model) Record(actor Owner, key control.Key, observation Observation) (Model, error) {
	next, err := model.active(actor, key)
	if err != nil {
		return model, err
	}
	if err := observation.Validate(); err != nil {
		return model, err
	}
	operation := next.Active
	var step *Step
	if observation.Ordinal > 0 && observation.Ordinal <= control.Uint64(len(operation.Steps)) {
		step = &operation.Steps[int(observation.Ordinal)-1]
	}
	if operation.Cleanup != nil && observation.Ordinal == operation.Cleanup.Ordinal {
		step = operation.Cleanup
	}
	if step == nil || step.Phase == Planned || observation.Key != key || observation.RequestSHA256 != operation.RequestSHA256 || observation.Action != step.Action || observation.SourceID != step.TargetID || observation.CommandSHA256 != step.CommandSHA256 {
		return model, fmt.Errorf("observation does not match dispatched intent")
	}
	if step.Phase == Settled {
		left, _ := json.Marshal(step.Observation)
		right, _ := json.Marshal(observation)
		if !bytes.Equal(left, right) {
			return model, fmt.Errorf("terminal observation changed")
		}
		return model.Clone(), nil
	}
	recorded := observation
	recorded.Data = append([]byte(nil), observation.Data...)
	step.Observation = &recorded
	step.Phase = Settled
	if step.Action == FinalizeBudget && recorded.Result == Completed && next.requiresHeldBudget(*step) {
		// 外部生产者的活性核验在执行器中完成；这里负责完整原WAL/回执的一致性
		// 和原子持久不变量。构造失败返回输入model，不能留下仅Completed的半状态。
		held, err := next.newHeldBudget(*step)
		if err != nil {
			return model, err
		}
		// Runner原UID/完整摘要成为当前预算永久来源时同时升级v4。旧v3读者
		// 即便收到删掉新字段的投影也必须拒绝版本，不能退回只验数值的旧VM事实。
		next.ProtocolVersion, next.BudgetHeld = max(next.ProtocolVersion, heldBudgetVersion(held)), held
		next.CapacityHeld = nil
		next.InitialHeld = nil
		next.BudgetBoundary = nil
	}
	if step.Action == AbortReconcile && recorded.Result == Completed {
		var marker map[string]json.RawMessage
		_ = json.Unmarshal(step.Command, &marker)
		if _, reconciled := marker["capacity"]; reconciled {
			held, err := next.heldCapacity(*operation, *step)
			if err != nil {
				return model, err
			}
			next.ProtocolVersion, next.CapacityHeld, next.BudgetHeld, next.BudgetBoundary = max(next.ProtocolVersion, 8), held, nil, nil
			next.InitialHeld = nil
		}
	}
	return next, next.Validate()
}

func (model Model) RequestAbort(actor Owner, key control.Key) (Model, error) {
	next, err := model.active(actor, key)
	if err != nil {
		return model, err
	}
	if next.Active.AbortRequested {
		return model.Clone(), nil
	}
	next.Active.AbortRequested = true
	return next, next.Validate()
}

// 已发出的动作都核验后，才可进入预算/准入清理。新的owner允许清理旧操作，但
// 不允许将旧操作剩余的VM/PG动作继续派发；清理的领域安全条件仍由协调器核验。
func (model Model) StartCleanup(actor Owner, key control.Key, targetID string, command []byte) (Model, error) {
	if err := control.ValidateID(targetID, "cleanup target"); err != nil {
		return model, err
	}
	next, err := model.active(actor, key)
	if err != nil {
		return model, err
	}
	if !next.Active.AbortRequested || next.Active.Cleanup != nil {
		return model, fmt.Errorf("cleanup is not eligible")
	}
	for _, step := range next.Active.Steps {
		if step.Phase == Unconfirmed {
			return model, fmt.Errorf("cleanup cannot erase an unconfirmed effect")
		}
	}
	var marker map[string]json.RawMessage
	_ = json.Unmarshal(command, &marker)
	if _, reconciled := marker["capacity"]; reconciled {
		var original budget.ReconciledAbortCommand
		if json.Unmarshal(command, &original) != nil || original.ValidateFor(actor.Epoch) != nil || targetID != key.Instance.ComputeInstanceID || next.reconciledPattern(*next.Active, original) != nil {
			return model, fmt.Errorf("reconciled Cleanup changed original actual capacity or plan")
		}
	}
	owner := actor
	next.Active.Cleanup = &Step{Ordinal: control.Uint64(len(next.Active.Steps) + 1), Action: AbortReconcile, TargetID: targetID, Command: append([]byte(nil), command...), CommandSHA256: "", Phase: Unconfirmed, DispatchedBy: &owner, Observation: nil}
	hash, err := commandDigest(next.Active.RequestSHA256, *next.Active.Cleanup)
	if err != nil {
		return model, err
	}
	next.Active.Cleanup.CommandSHA256 = hash
	next.BudgetBoundary = nil
	return next, next.Validate()
}

// 完整业务DTO（含CPU/目标/预算等实际动作参数）在派发前固化，不保存鉴权头。
// 原始字节不可改，来源实例和派发owner也绑定摘要。这里不认证业务DTO的领域语义。
func commandDigest(requestHash string, step Step) (string, error) {
	limit := MaxCommandBytes
	if len(step.Command) > MaxCommandBytes && len(step.Command) <= MaxInitialCapacityCommandBytes && step.Action == AbortReconcile && step.DispatchedBy != nil {
		// 先限字节/深度/节点/重复键，之后才能做typed解码；不因请求
		// 自报protocol_version就给任意大命令扩界。畸形或普通合同
		// 仍走8KiB拒绝路径，只有完整初始来源合同可使用16KiB。
		if err := control.ValidateJSON(step.Command, MaxInitialCapacityCommandBytes, 2048); err != nil {
			return "", err
		}
		var c budget.ReconciledAbortCommand
		if json.Unmarshal(step.Command, &c) == nil && (c.Capacity.ProtocolVersion == 2 || c.Capacity.ProtocolVersion == 3) && c.Capacity.InitialOutside != nil && c.ValidateFor(step.DispatchedBy.Epoch) == nil {
			limit = MaxInitialCapacityCommandBytes
		}
	}
	if err := control.ValidateJSON(step.Command, limit, 2048); err != nil {
		return "", err
	}
	if data := bytes.TrimSpace(step.Command); len(data) == 0 || data[0] != '{' {
		return "", fmt.Errorf("command requires an object body")
	}
	context := struct {
		Request  string         `json:"request"`
		Ordinal  control.Uint64 `json:"ordinal"`
		Action   Action         `json:"action"`
		TargetID string         `json:"target_id"`
		Owner    *Owner         `json:"owner"`
		Command  []byte         `json:"command"`
	}{Request: requestHash, Ordinal: step.Ordinal, Action: step.Action, TargetID: step.TargetID, Owner: step.DispatchedBy, Command: step.Command}
	data, err := json.Marshal(context)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(append([]byte("neon-dsga-journal-command-v1\x00"), data...))
	return hex.EncodeToString(hash[:]), nil
}

func (model Model) Finish(actor Owner, key control.Key) (Model, error) {
	next, err := model.active(actor, key)
	if err != nil {
		return model, err
	}
	for _, step := range next.Active.Steps {
		if step.Action == FinalizeBudget && step.Phase == Settled && step.Observation.Result == Completed && next.requiresHeldBudget(step) {
			// 旧v2生产结算也不能先关闭Active，再靠可淘汰历史推测当前预算。
			// 必须在原Active仍可认证时显式重核并升级；通用旧领域记录仍可读。
			if _, err := next.BudgetFor(next.Active.Request); err != nil {
				return model, err
			}
		}
	}
	outcome, err := next.Active.closingOutcome()
	if err != nil {
		return model, err
	}
	if err := next.closeBudgetBoundary(outcome); err != nil {
		return model, err
	}
	next.History = append(next.History, Record{Operation: cloneOperation(*next.Active), Outcome: outcome, ClosedSequence: next.Sequence})
	next.Active = nil
	for len(next.History) > HistoryLimit {
		if err := next.expireOldest(); err != nil {
			return model, err
		}
	}
	return next, next.Validate()
}

func (model *Model) expireOldest() error {
	if len(model.History) == 0 {
		return fmt.Errorf("active journal exceeds its byte budget")
	}
	if model.ExpiredResults == control.Uint64(math.MaxUint64) {
		return fmt.Errorf("expired-result counter exhausted")
	}
	model.History = append([]Record{}, model.History[1:]...)
	model.ExpiredResults++
	return nil
}
