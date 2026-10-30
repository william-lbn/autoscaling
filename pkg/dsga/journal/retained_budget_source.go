package journal

import (
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

const (
	RetainedInitialV1      = "initial-v1"
	RetainedFinalizationV1 = "finalization-v1"
	RetainedFinalizationV2 = "finalization-v2"
	RetainedCapacityV1     = "capacity-v1"
	RetainedCapacityV2     = "capacity-v2"
	RetainedCapacityV3     = "capacity-v3"
)

// RetainedBudgetSource固定当前旧永久头的格式、原摘要/落账序列和完整叶子。
// 恰好一个叶子，不包含旧Header.Recovery、Original、Lifecycle或整个Model，
// 因而连续恢复不会递归累积原账本。Initial没有Resize Request，绝不补造它。
//
// SourceSHA256保留旧头自身的摘要：Initial原BudgetSHA、Finalization原Step
// SHA、Capacity原Cleanup或独立预算SHA。后两类不总能仅从叶子重新推导；
// Validate只核自洽，真正身份必须由当前Model精确比对getter。它不是认证
// 网络回执、当前来源活性、开放许可、Host结清或新的资源执行权。
type RetainedBudgetSource struct {
	Kind             string                     `json:"kind"`
	RecordedSequence control.Uint64             `json:"recorded_sequence"`
	SourceSHA256     string                     `json:"source_sha256"`
	Initial          *budget.Initial            `json:"initial,omitempty"`
	Finalization     *Step                      `json:"finalization,omitempty"`
	Capacity         *budget.ReconciledCapacity `json:"capacity,omitempty"`
}

func (s RetainedBudgetSource) Validate() error {
	if s.RecordedSequence == 0 || !lifecycleDigest(s.SourceSHA256) {
		return fmt.Errorf("retained budget source lacks original digest or recorded sequence")
	}
	switch s.Kind {
	case RetainedInitialV1:
		if s.Initial == nil || s.Finalization != nil || s.Capacity != nil || s.Initial.Validate() != nil {
			return fmt.Errorf("retained initial source lost exclusive complete original budget")
		}
	case RetainedFinalizationV1, RetainedFinalizationV2:
		if s.Initial != nil || s.Finalization == nil || s.Capacity != nil {
			return fmt.Errorf("retained finalization source mixed another permanent header")
		}
		if _, err := s.finalizationCommand(); err != nil {
			return err
		}
	case RetainedCapacityV1, RetainedCapacityV2, RetainedCapacityV3:
		if s.Initial != nil || s.Finalization != nil || s.Capacity == nil || s.Capacity.Validate() != nil || (s.Kind == RetainedCapacityV3) != (s.Capacity.ProtocolVersion == 4) {
			return fmt.Errorf("retained actual capacity source lost original format or complete leaf")
		}
	default:
		return fmt.Errorf("unknown retained budget header format")
	}
	return nil
}

// finalizationCommand同时核完整原命令/回执及Step摘要域。独立结算Step保留
// 新结算owner；原Unknown的SettlementEpoch仍为0，新Planned结算才记录
// 其实际SettlementEpoch，原Request issuer不改。这个局部Operation
// 只供Step自洽校验，不是可派发原操作，也不补造已退休Original的结果。
func (s RetainedBudgetSource) finalizationCommand() (budget.FinalizationCommand, error) {
	var c budget.FinalizationCommand
	step := s.Finalization
	if step == nil || step.Phase != Settled || step.DispatchedBy == nil || step.Observation == nil || step.Observation.Result != Completed || step.CommandSHA256 != s.SourceSHA256 || json.Unmarshal(step.Command, &c) != nil || c.Validate() != nil {
		return c, fmt.Errorf("retained finalization lacks complete original command and terminal audit")
	}
	f := c.Finalized
	var observed budget.Finalized
	if json.Unmarshal(step.Observation.Data, &observed) != nil || !reflect.DeepEqual(f, observed) || step.TargetID != f.Request.Key.Instance.ComputeInstanceID {
		return c, fmt.Errorf("retained finalization changed exact original budget receipt")
	}
	action := FinalizeBudget
	if s.Kind == RetainedFinalizationV2 {
		action = RecoverFinalization
		if step.DispatchedBy.Epoch < f.IssuerEpoch || f.SettlementEpoch != 0 && f.SettlementEpoch != step.DispatchedBy.Epoch {
			return c, fmt.Errorf("retained recovered finalization lost independent settlement owner")
		}
	} else if s.Kind != RetainedFinalizationV1 || f.SettlementEpoch != 0 || step.DispatchedBy.Epoch != f.IssuerEpoch {
		return c, fmt.Errorf("retained ordinary finalization changed original issuing owner")
	}
	hash, err := RequestDigest(f.Request)
	if err != nil {
		return c, err
	}
	var local Operation
	local.Request, local.RequestSHA256, local.Origin = f.Request, hash, *step.DispatchedBy
	if err := step.validate(local, len(plan(f.Request.Kind))-1, action); err != nil {
		return c, err
	}
	return c, nil
}

func cloneRetainedBudgetSource(s *RetainedBudgetSource) *RetainedBudgetSource {
	if s == nil {
		return nil
	}
	n := *s
	if s.Initial != nil {
		i := *s.Initial
		i.PGAggregate = cloneActualRoot(i.PGAggregate)
		i.Shared.Observed = cloneActualSnapshot(i.Shared.Observed)
		n.Initial = &i
	}
	if s.Finalization != nil {
		step := cloneStep(*s.Finalization)
		n.Finalization = &step
	}
	if s.Capacity != nil {
		capacity := cloneActualCapacity(*s.Capacity)
		n.Capacity = &capacity
	}
	return &n
}

// currentRetainedSource不递归调用Model.Validate，供模型本身的交叉绑定使用。
// 只读取唯一原永久头，不扫描历史或借BudgetBoundary/PriorBoundary推导来源。
// 外层Model仍须验证完整旧Header.Recovery；这里不将该有界旧证明复制进新叶子。
func (m Model) currentRetainedSource() (RetainedBudgetSource, error) {
	var source RetainedBudgetSource
	count := 0
	if h := m.InitialHeld; h != nil {
		count++
		var initial budget.Initial
		if json.Unmarshal(h.Budget, &initial) != nil {
			return source, fmt.Errorf("retained source cannot decode original initial leaf")
		}
		source = RetainedBudgetSource{Kind: RetainedInitialV1, RecordedSequence: h.RecordedSequence, SourceSHA256: h.BudgetSHA256, Initial: &initial, Finalization: nil, Capacity: nil}
	}
	if h := m.BudgetHeld; h != nil {
		count++
		kind := RetainedFinalizationV1
		if h.ProtocolVersion == 2 {
			kind = RetainedFinalizationV2
		} else if h.ProtocolVersion != 1 {
			return source, fmt.Errorf("retained source has unknown finalized header version")
		}
		step := cloneStep(h.Finalization)
		source = RetainedBudgetSource{Kind: kind, RecordedSequence: h.RecordedSequence, SourceSHA256: step.CommandSHA256, Initial: nil, Finalization: &step, Capacity: nil}
	}
	if h := m.CapacityHeld; h != nil {
		count++
		capacity, digest, at, err := m.currentCapacityRecord()
		if err != nil {
			return source, err
		}
		kind := ""
		switch h.ProtocolVersion {
		case 1:
			kind = RetainedCapacityV1
		case 2:
			kind = RetainedCapacityV2
		case 3:
			kind = RetainedCapacityV3
		default:
			return source, fmt.Errorf("retained source has unknown actual-capacity header version")
		}
		source = RetainedBudgetSource{Kind: kind, RecordedSequence: at, SourceSHA256: digest, Initial: nil, Finalization: nil, Capacity: &capacity}
	}
	if count != 1 || source.RecordedSequence > m.Sequence {
		return source, fmt.Errorf("retained source requires one original current permanent header")
	}
	if err := source.Validate(); err != nil {
		return source, err
	}
	return *cloneRetainedBudgetSource(&source), nil
}

// CurrentRetainedSource只导出已校验当前永久头的深副本。不要求旧终态仍在
// 可淘汰缓存，也不要求关闭水位存在；这绝不替代当前PG/Root/Runner/Host
// 来源认证，更不使无水位的旧头获得资源释放或新请求派发资格。
func (m Model) CurrentRetainedSource() (RetainedBudgetSource, error) {
	if err := m.Validate(); err != nil {
		var none RetainedBudgetSource
		return none, err
	}
	return m.currentRetainedSource()
}

func (m Model) checkRetainedSource(source RetainedBudgetSource) error {
	if err := source.Validate(); err != nil {
		return err
	}
	current, err := m.currentRetainedSource()
	if err != nil || !reflect.DeepEqual(source, current) {
		return fmt.Errorf("retained budget source differs from exact original current header")
	}
	return nil
}

// CheckRetainedSource只比较原Kind、序列、摘要和完整叶子；允许旧头本身没有
// BudgetBoundary。来源网络读者须在前后分别调用并检查VM UID/RV，不得据此
// 构造Host结清水位、清旧Unknown或将Initial包装成一条不存在的Resize请求。
func (m Model) CheckRetainedSource(source RetainedBudgetSource) error {
	if err := m.Validate(); err != nil {
		return err
	}
	return m.checkRetainedSource(source)
}

type retainedBudgetFacts struct {
	instance                   control.Instance
	budgetID                   string
	generation, memory, usable control.Uint64
	runner                     string
	restarts                   int32
}

func (s RetainedBudgetSource) facts() (retainedBudgetFacts, error) {
	var facts retainedBudgetFacts
	if err := s.Validate(); err != nil {
		return facts, err
	}
	if i := s.Initial; i != nil {
		facts.instance, facts.budgetID = i.Instance, i.PGAggregate.Bootstrap.BudgetCommitmentID
		facts.generation, facts.memory, facts.usable = i.Shared.Observed.CapacityGeneration, i.VMMemoryBytes, i.GuestUsableMemoryBytes
		facts.runner, facts.restarts = i.RunnerPodName, i.RunnerRestartCount
		return facts, nil
	}
	if c := s.Capacity; c != nil {
		facts.instance, facts.budgetID = c.Request.Key.Instance, c.Request.BudgetCommitmentID
		facts.generation, facts.memory, facts.usable = c.CapacityGeneration, c.VMMemoryBytes, c.GuestUsableMemoryBytes
		facts.runner, facts.restarts = c.RunnerPodName, c.RunnerRestartCount
		return facts, nil
	}
	c, err := s.finalizationCommand()
	if err != nil {
		return facts, err
	}
	f := c.Finalized
	facts.instance, facts.budgetID = f.Request.Key.Instance, f.Request.BudgetCommitmentID
	facts.generation, facts.memory, facts.usable = f.Shared.Observed.CapacityGeneration, f.Request.TargetVMMemoryBytes, f.GuestUsableMemoryBytes
	facts.runner, facts.restarts = f.RunnerPodName, f.RunnerRestartCount
	return facts, nil
}

func (s RetainedBudgetSource) BudgetCommitmentID() (string, error) {
	f, err := s.facts()
	return f.budgetID, err
}

func (s RetainedBudgetSource) Instance() (control.Instance, error) {
	f, err := s.facts()
	return f.instance, err
}

func (s RetainedBudgetSource) CapacityGeneration() (control.Uint64, error) {
	f, err := s.facts()
	return f.generation, err
}

func (s RetainedBudgetSource) VMMemoryBytes() (control.Uint64, error) {
	f, err := s.facts()
	return f.memory, err
}

func (s RetainedBudgetSource) GuestUsableMemoryBytes() (control.Uint64, error) {
	f, err := s.facts()
	return f.usable, err
}

func (s RetainedBudgetSource) RunnerPodName() (string, error) {
	f, err := s.facts()
	return f.runner, err
}

func (s RetainedBudgetSource) RunnerRestartCount() (int32, error) {
	f, err := s.facts()
	return f.restarts, err
}

func (s *RetainedBudgetSource) UnmarshalJSON(raw []byte) error {
	type plain RetainedBudgetSource
	var value plain
	if err := control.UnmarshalObjectBounded(raw, &value, MaxRawBytes/2, 16384); err != nil {
		return err
	}
	next := RetainedBudgetSource(value)
	if err := next.Validate(); err != nil {
		return err
	}
	*s = next
	return nil
}
