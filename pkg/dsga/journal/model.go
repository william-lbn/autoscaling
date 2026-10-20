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
)

const (
	HistoryLimit        = 16
	MaxObservationBytes = 8 * 1024
	MaxRawBytes         = 1024 * 1024
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
// Data不得含Bearer/密码等凭据。8KiB限制完整协议事实，不能用任意大日志填满VM对象。
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
}

func New(vmUID, incarnation, holder string) (Model, error) {
	model := Model{
		ProtocolVersion: 1, VMUID: vmUID, Incarnation: incarnation, Sequence: 1,
		Owner: Owner{HolderID: holder, Epoch: 1}, HighWater: nil, Active: nil, History: []Record{}, ExpiredResults: 0,
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
	if len(value) != 32 {
		return false
	}
	nonzero := false
	for _, b := range []byte(value) {
		if (b < '0' || b > '9') && (b < 'a' || b > 'f') {
			return false
		}
		nonzero = nonzero || b != '0'
	}
	return nonzero
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
	if model.ProtocolVersion != 1 || model.Sequence == 0 || !validIncarnation(model.Incarnation) {
		return fmt.Errorf("invalid journal version/sequence/incarnation")
	}
	if err := control.ValidateID(model.VMUID, "VM"); err != nil {
		return err
	}
	if err := model.Owner.Validate(); err != nil {
		return err
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
		if record.Outcome != outcome || record.ClosedSequence == 0 || record.ClosedSequence > model.Sequence ||
			record.Operation.Request.Key.Instance.VMUID != model.VMUID || model.HighWater == nil || compareKeys(record.Operation.Request.Key, *model.HighWater) > 0 {
			return fmt.Errorf("invalid journal terminal record")
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
	return nil
}

func (model Model) validateOwners(operation Operation) error {
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
	// 验证当前调用owner后，先查原意图，再校验请求期限/原issuer代次。跨owner重放
	// 只返回已保存事实，不给旧动作新派发权；actor身份仍需上层认证，不来自请求自报。
	if disposition := model.Lookup(request); disposition != Stale {
		return model.Clone(), disposition, nil
	}
	if request.Key.IssuerEpoch != actor.Epoch {
		return model, Stale, nil
	}
	if model.Active != nil {
		return model, Busy, nil
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
	next.Active = &Operation{Request: request, RequestSHA256: hash, Origin: actor, AbortRequested: false, Steps: steps, Cleanup: nil}
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
	owner := actor
	next.Active.Cleanup = &Step{Ordinal: control.Uint64(len(next.Active.Steps) + 1), Action: AbortReconcile, TargetID: targetID, Command: append([]byte(nil), command...), CommandSHA256: "", Phase: Unconfirmed, DispatchedBy: &owner, Observation: nil}
	hash, err := commandDigest(next.Active.RequestSHA256, *next.Active.Cleanup)
	if err != nil {
		return model, err
	}
	next.Active.Cleanup.CommandSHA256 = hash
	return next, next.Validate()
}

// 完整业务DTO（含CPU/目标/预算等实际动作参数）在派发前固化，不保存鉴权头。
// 原始字节不可改，来源实例和派发owner也绑定摘要。这里不认证业务DTO的领域语义。
func commandDigest(requestHash string, step Step) (string, error) {
	if err := control.ValidateJSON(step.Command, MaxObservationBytes, 2048); err != nil {
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
	outcome, err := next.Active.closingOutcome()
	if err != nil {
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
