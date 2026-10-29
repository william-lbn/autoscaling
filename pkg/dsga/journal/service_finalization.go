package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// ServiceFinalization是类别15关闭后重新认证全部原资源的独立结算。
// 没有外部资源写，不需要制造原Finalize的派发或Completed；一个UID/RV CAS
// 保存真实当前Shared及完整预算。原Active、原未知、旧永久头和服务关闭保持
// 不变。它不是当前准入或Host释放证明，开放前仍须重新认证全部来源。
// RecordedSequence记录本证据首次持久化，而非原Finalize发生时间。
type ServiceFinalization struct {
	Origin           Owner                      `json:"origin"`
	RecordedSequence control.Uint64             `json:"recorded_sequence"`
	OperationSHA256  string                     `json:"operation_sha256"`
	Closed           control.AdmissionRequest   `json:"closed"`
	ClosedSequence   control.Uint64             `json:"closed_sequence"`
	Command          budget.FinalizationCommand `json:"command"`
	Current          control.SharedCostResponse `json:"current"`
}

// RecoveredBudgetOrigin随当前预算永久保留原操作，结果缓存淘汰也不能把
// 原Finalize未知投影为成功。Proof的Command和Current只说明独立结算，
// 新步骤Action另为RecoverFinalization；后继正常Finalize才可替换本永久头。
type RecoveredBudgetOrigin struct {
	Original Operation           `json:"original"`
	Proof    ServiceFinalization `json:"proof"`
}

func (p ServiceFinalization) validate(op Operation) error {
	if err := op.Validate(); err != nil {
		return err
	}
	if op.AbortRequested || op.Cleanup != nil || len(op.Steps) < 2 || p.Origin.Validate() != nil || p.Origin.Epoch < op.Origin.Epoch || p.Origin.Epoch == op.Origin.Epoch && p.Origin != op.Origin || p.RecordedSequence == 0 {
		return fmt.Errorf("independent finalization requires original complete non-aborted resource prefix")
	}
	for i, s := range op.Steps {
		switch i {
		case len(op.Steps) - 1:
			if s.Action != ResumeAdmission || s.Phase != Planned {
				return fmt.Errorf("independent finalization cannot cover dispatched final admission")
			}
		case len(op.Steps) - 2:
			if s.Action != FinalizeBudget || s.Phase != Planned && s.Phase != Unconfirmed {
				return fmt.Errorf("independent finalization lost original unsettled boundary")
			}
		default:
			if s.Phase != Settled || s.Observation == nil || s.Observation.Result != Completed {
				return fmt.Errorf("independent finalization cannot retire unfinished resource effects")
			}
		}
	}
	digest, err := operationClosureDigest(&op)
	f := p.Command.Finalized
	if err != nil || digest != p.OperationSHA256 || p.Command.Validate() != nil || f.Request != op.Request || f.Sources == nil || f.Sources.Runner == nil || f.ValidateFor(op.Request, p.Current, p.Origin.Epoch, f.RunnerPodName, f.RunnerRestartCount, uint64(op.Request.TargetVMMemoryBytes)) != nil || p.Closed.Validate() != nil || p.Closed.Open || p.Closed.Instance != op.Request.Key.Instance || p.Closed.BudgetCommitmentID != op.Request.BudgetCommitmentID || p.Closed.IssuerEpoch > p.Origin.Epoch || p.ClosedSequence == 0 || p.ClosedSequence >= p.RecordedSequence || p.Current.Observed.AdmissionOpen || p.Current.Observed.AdmissionRevision != p.Closed.Revision || p.Current.Observed.CapacityGeneration != p.Closed.ExpectedCapacityGeneration {
		return fmt.Errorf("independent finalization changed original operation, current PG or complete budget")
	}
	// 完成时会生成独立Step；保存Proof之前就检查其原命令/回执边界，
	// 防止先开放，再因Step序列化超过既有上限而无法安装永久预算。
	commandRaw, err := json.Marshal(p.Command)
	if err != nil || control.ValidateJSON(commandRaw, MaxCommandBytes, 2048) != nil {
		return fmt.Errorf("independent finalization cannot retain bounded complete command")
	}
	factRaw, err := json.Marshal(f)
	if err != nil || control.ValidateJSON(factRaw, MaxObservationBytes, 2048) != nil {
		return fmt.Errorf("independent finalization cannot retain bounded complete receipt")
	}
	original := op.Steps[len(op.Steps)-2]
	if original.Phase == Unconfirmed {
		// 原未知没有外部资源效果，仍须逐字引用其完整原命令。新owner
		// 只能重新认证它，不重算较小成本、换来源或改旧命令的issuer。
		var command budget.FinalizationCommand
		if json.Unmarshal(original.Command, &command) != nil || !reflect.DeepEqual(command, p.Command) || command.Finalized.SettlementEpoch != 0 {
			return fmt.Errorf("independent finalization rewrote original unknown command")
		}
	} else if f.SettlementEpoch != p.Origin.Epoch {
		return fmt.Errorf("new independent settlement must preserve its actual observing owner")
	}
	for _, step := range op.Steps[:len(op.Steps)-2] {
		var wanted string
		switch step.Action {
		case PrepareBudget:
			wanted = p.Command.PreparedSHA256
		case GuestReady:
			wanted = p.Command.GuestSHA256
		case ResizeVM:
			wanted = p.Command.VMSHA256
		default:
			continue
		}
		if rawDigest(step.Observation.Data) != wanted {
			return fmt.Errorf("independent finalization replaced complete resource predecessor")
		}
	}
	return nil
}

func cloneServiceFinalization(p *ServiceFinalization) *ServiceFinalization {
	if p == nil {
		return nil
	}
	// 显式复制所有嵌套slice/指针；Clone也服务于负面校验夹具，不能
	// 通过严格解码或panic隐含要求输入已经有效。无网络、无全局锁。
	next := *p
	var current control.AdmissionResponse
	current.Observed = p.Current.Observed
	next.Current.Observed = cloneAdmissionResponse(&current).Observed
	f := &next.Command.Finalized
	var shared control.AdmissionResponse
	shared.Observed = f.Shared.Observed
	f.Shared.Observed = cloneAdmissionResponse(&shared).Observed
	for _, bound := range []**budget.Bound{&f.Aggregate, &f.PrivateAndDSM, &f.LFCOutsideShared, &f.HugePoolOutsideShared} {
		if *bound != nil {
			value := **bound
			*bound = &value
		}
	}
	if f.Sources != nil {
		sources := *f.Sources
		if sources.Runner != nil {
			runner := *sources.Runner
			sources.Runner = &runner
		}
		f.Sources = &sources
	}
	return &next
}

func cloneRecoveredBudget(p *RecoveredBudgetOrigin) *RecoveredBudgetOrigin {
	if p == nil {
		return nil
	}
	return &RecoveredBudgetOrigin{Original: cloneOperation(p.Original), Proof: *cloneServiceFinalization(&p.Proof)}
}

func (p ServiceFinalization) recoveredStep(op Operation) (Step, error) {
	if err := p.validate(op); err != nil {
		return Step{}, err
	}
	data, err := json.Marshal(p.Command)
	if err != nil {
		return Step{}, err
	}
	step := Step{Ordinal: op.Steps[len(op.Steps)-2].Ordinal, Action: RecoverFinalization, TargetID: op.Request.Key.Instance.ComputeInstanceID, Command: data, CommandSHA256: "", Phase: Settled, DispatchedBy: &p.Origin, Observation: nil}
	step.CommandSHA256, err = commandDigest(op.RequestSHA256, step)
	if err != nil {
		return Step{}, err
	}
	fact, err := json.Marshal(p.Command.Finalized)
	if err != nil {
		return Step{}, err
	}
	step.Observation = &Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: RecoverFinalization, Result: Completed, SourceID: step.TargetID, Data: fact}
	return step, nil
}

func (p ServiceFinalization) held(id Model) (*HeldBudget, error) {
	if id.Active == nil || id.ResourceIdentity == nil {
		return nil, fmt.Errorf("independent budget requires original active resource and identity")
	}
	op := *id.Active
	step, err := p.recoveredStep(op)
	if err != nil {
		return nil, err
	}
	return &HeldBudget{ProtocolVersion: 2, Identity: *id.ResourceIdentity, RecordedSequence: p.RecordedSequence, Finalization: step, Recovery: &RecoveredBudgetOrigin{Original: cloneOperation(op), Proof: *cloneServiceFinalization(&p)}}, nil
}

func (m Model) validateServiceFinalization() error {
	p := m.ServiceClosure.Finalization
	if p == nil {
		return nil
	}
	if m.serviceClosureVersion() != 15 || m.Active == nil || p.RecordedSequence > m.Sequence || p.Origin.Epoch > m.Owner.Epoch || p.Origin.Epoch == m.Owner.Epoch && p.Origin != m.Owner || p.OperationSHA256 != m.ServiceClosure.OperationSHA256 {
		return fmt.Errorf("service finalization lost original closure or current owner")
	}
	closedAt := m.ServiceClosure.RecordedSequence
	if c := m.ServiceClosure.Continuation; c != nil {
		closedAt = c.RecordedSequence
	}
	if m.ServiceClosure.Pending() || p.RecordedSequence <= closedAt {
		return fmt.Errorf("independent finalization must follow confirmed original closure")
	}
	_, _, closeRequest, _ := m.ServiceClosure.Selection()
	if p.Closed != closeRequest || p.ClosedSequence != closedAt {
		return fmt.Errorf("independent finalization changed original confirmed admission closure")
	}
	return p.validate(*m.Active)
}

// RecordServiceFinalization只有本地CAS，没有可重发的外部命令。丢回复重读
// 原证据即可；当前来源变化不能替换旧Proof。新的开放/期限恢复选择之后不再
// 允许增加/改写结算，否则已固定的完整关闭与预算摘要将失去写前含义。
func (m Model) RecordServiceFinalization(actor Owner, selector string, command budget.FinalizationCommand, current control.SharedCostResponse) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || m.serviceClosureVersion() != 15 || c.Finalization != nil || c.Rearm != nil || c.Reopen != nil || c.Pending() || m.Active == nil {
		return m, fmt.Errorf("independent finalization requires unchanged confirmed pending-finalize closure")
	}
	_, _, _, selected := c.Selection()
	if selector != selected {
		return m, fmt.Errorf("independent finalization must select latest complete closure")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	_, _, closeRequest, _ := c.Selection()
	closedAt := c.RecordedSequence
	if c.Continuation != nil {
		closedAt = c.Continuation.RecordedSequence
	}
	next.ServiceClosure.Finalization = cloneServiceFinalization(&ServiceFinalization{Origin: actor, RecordedSequence: next.Sequence, OperationSHA256: c.OperationSHA256, Closed: closeRequest, ClosedSequence: closedAt, Command: command, Current: current})
	return next, next.Validate()
}

func (s *Store) RecordServiceFinalization(ctx context.Context, snapshot *Snapshot, actor Owner, selector string, command budget.FinalizationCommand, current control.SharedCostResponse) (*Snapshot, error) {
	if err := s.checked(snapshot); err != nil {
		return nil, err
	}
	if !snapshot.Dispatchable() {
		return nil, fmt.Errorf("independent finalization requires original running VM")
	}
	next, err := snapshot.model.RecordServiceFinalization(actor, selector, command, current)
	if err != nil {
		return nil, err
	}
	return s.apply(ctx, snapshot, next)
}

func (m Model) validateRecoveredBudget() error {
	h := m.BudgetHeld
	if h == nil || h.ProtocolVersion != 2 || h.Recovery == nil || h.Recovery.Proof.RecordedSequence != h.RecordedSequence || h.RecordedSequence > m.Sequence || m.ResourceIdentity == nil || h.Identity != *m.ResourceIdentity || m.PGInstance == nil || h.Recovery.Original.Request.Key.Instance != *m.PGInstance || h.Recovery.Proof.Origin.Epoch > m.Owner.Epoch || h.Recovery.Proof.Origin.Epoch == m.Owner.Epoch && h.Recovery.Proof.Origin != m.Owner || m.HighWater == nil || compareKeys(h.Recovery.Original.Request.Key, *m.HighWater) > 0 {
		return fmt.Errorf("independent recovered budget lost original identity, owner or sequence")
	}
	// 重建期望值只需原操作与身份；不复制整个历史/生命周期，避免每次
	// 预算来源重审都重复分配完整大账本。held只读这两项且自行深拷贝。
	var basis Model
	basis.Active = &h.Recovery.Original
	basis.ResourceIdentity = m.ResourceIdentity
	want, err := h.Recovery.Proof.held(basis)
	if err != nil || !reflect.DeepEqual(want, h) {
		return fmt.Errorf("independent recovered budget differs from full original proof")
	}
	for _, r := range m.History {
		order := compareKeys(r.Operation.Request.Key, h.Recovery.Original.Request.Key)
		if r.Operation.Request.Key == h.Recovery.Original.Request.Key && (!reflect.DeepEqual(r.Operation, h.Recovery.Original) || r.ServiceCoverage == nil || !reflect.DeepEqual(r.ServiceCoverage.Finalization, &h.Recovery.Proof) || r.ClosedSequence <= h.RecordedSequence) {
			return fmt.Errorf("recovered budget rewrote original terminal operation")
		}
		if order < 0 && r.ClosedSequence >= h.RecordedSequence || order > 0 && r.ClosedSequence <= h.RecordedSequence {
			return fmt.Errorf("recovered budget sequence contradicts terminal history")
		}
		if order > 0 && r.ServiceCoverage != nil && r.ServiceCoverage.Finalization != nil {
			return fmt.Errorf("newer independent finalization has no atomic replacement of recovered budget")
		}
		if err := rejectNewerFinalization(r.Operation, h.Recovery.Original.Request.Key); err != nil {
			return err
		}
	}
	if m.Active != nil {
		if compareKeys(m.Active.Request.Key, h.Recovery.Original.Request.Key) <= 0 {
			return fmt.Errorf("recovered current budget cannot still have its original active operation")
		}
		return rejectNewerFinalization(*m.Active, h.Recovery.Original.Request.Key)
	}
	return nil
}

// 后继Finalize一旦Completed必须同CAS替换当前永久头；恢复预算也不能因
// 自带旧完整证明而跨过这条原子性不变量。Planned/Unknown后继仍保留旧预算。
func rejectNewerFinalization(op Operation, key control.Key) error {
	if compareKeys(op.Request.Key, key) <= 0 {
		return nil
	}
	for _, step := range op.Steps {
		if step.Action == FinalizeBudget && step.Phase == Settled && step.Observation != nil && step.Observation.Result == Completed {
			return fmt.Errorf("newer finalization has no atomic replacement of recovered budget")
		}
	}
	return nil
}

func (p *ServiceFinalization) UnmarshalJSON(raw []byte) error {
	type plain ServiceFinalization
	var next plain
	if err := control.UnmarshalObjectBounded(raw, &next, MaxObservationBytes*2, 16384); err != nil {
		return err
	}
	*p = ServiceFinalization(next)
	return nil
}

func (p *RecoveredBudgetOrigin) UnmarshalJSON(raw []byte) error {
	type plain RecoveredBudgetOrigin
	var next plain
	if err := control.UnmarshalObjectBounded(raw, &next, MaxRawBytes/2, 16384); err != nil {
		return err
	}
	*p = RecoveredBudgetOrigin(next)
	return nil
}
