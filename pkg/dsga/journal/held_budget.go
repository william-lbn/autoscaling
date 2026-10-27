package journal

import (
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// HeldBudget保留当前已核验的完整预算，而非请求结果缓存。Finalization深拷贝
// 保存原命令、派发owner、摘要及完整Finalized回执；Active结束和16条历史淘汰
// 不影响其寿命。RecordedSequence是本账本原子落账序列，不是外部资源的版本。
// 替换只记录新完整预算已覆盖当前Guest；不删除或释放任何旧Host/Outside对象。
// 各可信生产者仍须持续保存原持有对象，直到它们自己证明可以显式结清。
type HeldBudget struct {
	ProtocolVersion  uint32                 `json:"protocol_version"`
	Identity         resourceguard.Identity `json:"identity"`
	RecordedSequence control.Uint64         `json:"recorded_sequence"`
	Finalization     Step                   `json:"finalization"`
}

func (h *HeldBudget) UnmarshalJSON(data []byte) error {
	type plain HeldBudget
	var p plain
	if err := control.UnmarshalObject(data, &p); err != nil {
		return err
	}
	*h = HeldBudget(p)
	return nil
}

// finalizedBudget校验持久数据自洽，不认证Host/Root/PG来源。严格核对完整原命令
// 与回执，不能把后读的另一代Shared/Source拼进原操作。命令摘要包含原owner；
// 接管后只读确认仍保留其旧epoch，不生成新的派发证明。
func finalizedBudget(op Operation, step Step) (budget.Finalized, error) {
	var c budget.FinalizationCommand
	var f budget.Finalized
	if step.Action != FinalizeBudget || step.Phase != Settled || step.Observation == nil || step.Observation.Result != Completed ||
		json.Unmarshal(step.Command, &c) != nil || c.Validate() != nil || json.Unmarshal(step.Observation.Data, &f) != nil || !reflect.DeepEqual(c.Finalized, f) || f.Request != op.Request || step.TargetID != op.Request.Key.Instance.ComputeInstanceID {
		return f, fmt.Errorf("held budget lacks exact complete original finalization")
	}
	ordinal := 0
	for i, action := range plan(op.Request.Kind) {
		if action == FinalizeBudget {
			ordinal = i + 1
		}
	}
	if err := step.validate(op, ordinal, FinalizeBudget); err != nil {
		return f, err
	}
	return f, nil
}

func (model Model) requiresHeldBudget(step Step) bool {
	if model.ProtocolVersion >= 3 {
		return true
	}
	var c budget.FinalizationCommand
	var f budget.Finalized
	// v1/v2历史及低层通用领域夹具仍可读；新生产命令或带Sources的结算必须
	// 使用严格永久持有语义，不能因某个字段错误退回旧通用JSON完成路径。
	return json.Unmarshal(step.Command, &c) == nil || (step.Observation != nil && json.Unmarshal(step.Observation.Data, &f) == nil && f.Sources != nil)
}

func (model Model) newHeldBudget(step Step) (*HeldBudget, error) {
	if model.Active == nil || model.PGInstance == nil || model.ResourceIdentity == nil || model.Active.Request.Key.Instance != *model.PGInstance {
		return nil, fmt.Errorf("held budget requires original durable PG and Guest bindings")
	}
	f, err := finalizedBudget(*model.Active, step)
	if err != nil {
		return nil, err
	}
	if model.ProtocolVersion >= 4 && (f.Sources == nil || f.Sources.Runner == nil) {
		return nil, fmt.Errorf("runner-backed budget cannot downgrade to legacy VM sources")
	}
	return &HeldBudget{ProtocolVersion: 1, Identity: *model.ResourceIdentity, RecordedSequence: model.Sequence, Finalization: cloneStep(step)}, nil
}

func (model Model) validateHeldBudget() error {
	if model.BudgetHeld == nil {
		return nil
	}
	h := model.BudgetHeld
	if h.ProtocolVersion != 1 || h.RecordedSequence == 0 || h.RecordedSequence > model.Sequence || h.Identity.Validate() != nil || model.ResourceIdentity == nil || h.Identity != *model.ResourceIdentity || model.PGInstance == nil || model.HighWater == nil || h.Finalization.DispatchedBy == nil || h.Finalization.Observation == nil {
		return fmt.Errorf("invalid retained budget identity or sequence")
	}
	var c budget.FinalizationCommand
	if json.Unmarshal(h.Finalization.Command, &c) != nil {
		return fmt.Errorf("invalid retained budget command")
	}
	if (model.ProtocolVersion >= 4 && (c.Finalized.Sources == nil || c.Finalized.Sources.Runner == nil)) || (model.ProtocolVersion == 3 && c.Finalized.Sources != nil && c.Finalized.Sources.Runner != nil) {
		return fmt.Errorf("journal version does not preserve original Runner budget sources")
	}
	request := c.Finalized.Request
	hash, err := RequestDigest(request)
	if err != nil || request.Key.Instance != *model.PGInstance || request.Key.Instance.ComputeInstanceID != h.Identity.ComputeInstanceID || request.Key.Instance.VMUID != h.Identity.VMUID || compareKeys(request.Key, *model.HighWater) > 0 || request.Key.IssuerEpoch != h.Finalization.DispatchedBy.Epoch {
		return fmt.Errorf("retained budget differs from original VM/PG/high water")
	}
	op := Operation{Request: request, RequestSHA256: hash, Origin: *h.Finalization.DispatchedBy}
	if err := model.validateOwners(op); err != nil {
		return err
	}
	if _, err := finalizedBudget(op, h.Finalization); err != nil {
		return err
	}
	// 仍有原Active/历史时要求逐字节同一命令/回执。较新操作可以尚未结算，
	// 却不能已经Completed而顶层仍保留旧预算。历史淘汰只移除这份交叉检查，
	// 顶层完整原WAL继续独立验证；不得从空历史补成“没有预算”。
	check := func(candidate Operation, closed control.Uint64) error {
		order := compareKeys(candidate.Request.Key, request.Key)
		for _, step := range candidate.Steps {
			if step.Action != FinalizeBudget {
				continue
			}
			if order == 0 && (!reflect.DeepEqual(step, h.Finalization) || candidate.Request != request || (closed != 0 && closed <= h.RecordedSequence)) {
				return fmt.Errorf("retained budget differs from original operation")
			}
			if order > 0 && step.Phase == Settled && step.Observation.Result == Completed {
				return fmt.Errorf("newer finalization has no atomic retained budget")
			}
		}
		if closed != 0 && ((order < 0 && closed >= h.RecordedSequence) || (order > 0 && closed <= h.RecordedSequence)) {
			return fmt.Errorf("retained budget sequence conflicts with terminal history")
		}
		return nil
	}
	for _, record := range model.History {
		if err := check(record.Operation, record.ClosedSequence); err != nil {
			return err
		}
	}
	if model.Active != nil {
		return check(*model.Active, 0)
	}
	return nil
}

// BudgetFor只返回同一完整原请求当前持有的预算。返回值重新严格解码，不与账本
// 共享指针；不能用较新/较旧请求或预算ID相同来借用别的原结算。调用者仍必须
// 复核各真实来源的持续活性，这个方法本身不生成准入或VM许可。
func (model Model) BudgetFor(request control.Request) (budget.Finalized, error) {
	var f budget.Finalized
	if model.Validate() != nil || model.ProtocolVersion < 3 || model.BudgetHeld == nil {
		return f, fmt.Errorf("complete current budget is not durably retained")
	}
	if err := json.Unmarshal(model.BudgetHeld.Finalization.Observation.Data, &f); err != nil {
		return f, err
	}
	if f.Request != request {
		return f, fmt.Errorf("retained budget belongs to another original request")
	}
	return f, nil
}

// RetainFinalizedBudget仅用于v2已完成但尚在Active中的原生产结算显式升级。
// 执行器必须先重新认证完整原来源/root/PG/VM，再在同UID/RV快照调用本方法；
// 不扫描历史、不推测当前预算、不修改旧Completed/原Shared，不能替换已有v3预算。
// 若升级CAS丢回复，重读已存在的同一原预算即可确认，不重复任何外部动作。
func (model Model) RetainFinalizedBudget(actor Owner, key control.Key) (Model, error) {
	next, err := model.active(actor, key)
	if err != nil {
		return model, err
	}
	if model.BudgetHeld != nil {
		if _, err := model.BudgetFor(next.Active.Request); err != nil {
			return model, err
		}
		return model.Clone(), nil
	}
	if model.ProtocolVersion != 2 {
		return model, fmt.Errorf("legacy budget upgrade requires durable PG binding")
	}
	for _, step := range next.Active.Steps {
		if step.Action != FinalizeBudget {
			continue
		}
		held, err := next.newHeldBudget(step)
		if err != nil {
			return model, err
		}
		next.ProtocolVersion, next.BudgetHeld = heldBudgetVersion(held), held
		if err := next.Validate(); err != nil {
			return model, err
		}
		return next, nil
	}
	return model, fmt.Errorf("legacy original finalization missing")
}

func heldBudgetVersion(held *HeldBudget) uint32 {
	var c budget.FinalizationCommand
	if held != nil && json.Unmarshal(held.Finalization.Command, &c) == nil && c.Finalized.Sources != nil && c.Finalized.Sources.Runner != nil {
		return 4
	}
	return 3
}
