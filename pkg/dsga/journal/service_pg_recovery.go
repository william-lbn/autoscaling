package journal

import (
	"context"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ServicePGRecovery保存原执行未知或原生终态淘汰后的独立PG事实。原Active/
// ResizePG相位、回执和预算永不改写；Effect=isolated_unknown只证明更高floor
// 下旧执行者退出、未来旧POST被拒绝以及当前稳定容量，不解释历史效果。
// 本证明不能单独开放、缩VM或释放Host；后继仍须认证全部资源和实际预算。
type ServicePGRecovery struct {
	Origin           Owner                        `json:"origin"`
	RecordedSequence control.Uint64               `json:"recorded_sequence"`
	OperationSHA256  string                       `json:"operation_sha256"`
	CommandSHA256    string                       `json:"command_sha256"`
	Closed           control.AdmissionRequest     `json:"closed"`
	ClosedSequence   control.Uint64               `json:"closed_sequence"`
	Effect           control.ResizeRecoveryEffect `json:"effect"`
	Fence            control.FenceResponse        `json:"issuer_fence"`
	Status           control.ControlResponse      `json:"status"`
}

// OriginalServicePG读取完整原派发。Unknown允许无回执；Settled必须保留真实
// 原生终态审计。没有终态的明确Busy/Rejected继续独立拒绝恢复，不能混入此域。
// 返回的完整回执来自解码副本，不向调用方泄露原WAL可变指针。
func (op Operation) OriginalServicePG() (string, *control.ControlResponse, error) {
	for _, step := range op.Steps {
		if step.Action != ResizePG {
			continue
		}
		command, err := control.DecodeRequest(step.Command)
		if err != nil || command != op.Request || step.DispatchedBy == nil || command.Key.IssuerEpoch != step.DispatchedBy.Epoch || step.TargetID != op.Request.Key.Instance.ComputeInstanceID {
			return "", nil, fmt.Errorf("service PG recovery lost original complete dispatch")
		}
		if step.Phase == Unconfirmed && step.Observation == nil {
			return step.CommandSHA256, nil, nil
		}
		var audit struct {
			ProtocolVersion uint32                  `json:"protocol_version"`
			Fence           control.FenceResponse   `json:"issuer_fence"`
			Response        control.ControlResponse `json:"response"`
		}
		if step.Phase != Settled || step.Observation == nil || control.UnmarshalObject(step.Observation.Data, &audit) != nil || audit.ProtocolVersion != 1 || audit.Response.ValidateResize(op.Request, op.Request.Key.Instance) != nil || audit.Response.Receipt.Result == nil {
			return "", nil, fmt.Errorf("service PG recovery lacks original native terminal audit")
		}
		q := control.FenceRequest{ProtocolVersion: 1, Instance: op.Request.Key.Instance, IssuerEpoch: audit.Fence.RequestedEpoch}
		phase := audit.Response.Receipt.Result.Operation.Phase
		nativeTerminal := (step.Observation.Result == Completed && phase == control.Committed) || (step.Observation.Result == Rejected && phase == control.Denied)
		if audit.Fence.ValidateFor(q, q.Instance) != nil || audit.Fence.Disposition != control.Fenced || q.IssuerEpoch < step.DispatchedBy.Epoch || audit.Response.Observed.IssuerFloorEpoch != q.IssuerEpoch || audit.Response.Observed.CapacityGeneration < audit.Fence.Observed.CapacityGeneration ||
			!nativeTerminal {
			return "", nil, fmt.Errorf("service PG recovery original terminal contradicts execution audit")
		}
		return step.CommandSHA256, &audit.Response, nil
	}
	return "", nil, fmt.Errorf("service PG recovery has no original dispatched resize")
}

func (p ServicePGRecovery) validate(op Operation) error {
	commandHash, original, err := op.OriginalServicePG()
	digest, hashErr := operationClosureDigest(&op)
	effect, proofErr := control.ValidateResizeRecovery(op.Request, p.Fence, p.Status)
	if err != nil || hashErr != nil || proofErr != nil || op.Validate() != nil || effect != p.Effect || digest != p.OperationSHA256 || commandHash != p.CommandSHA256 ||
		p.Origin.Validate() != nil || p.Fence.RequestedEpoch != p.Origin.Epoch || p.RecordedSequence <= p.ClosedSequence || p.ClosedSequence == 0 ||
		p.Closed.Validate() != nil || p.Closed.Open || p.Closed.Instance != op.Request.Key.Instance || p.Closed.IssuerEpoch > p.Origin.Epoch ||
		p.Status.Observed.AdmissionOpen || p.Fence.Observed.AdmissionOpen || p.Status.Observed.AdmissionRevision != p.Closed.Revision ||
		p.Fence.Observed.AdmissionRevision != p.Closed.Revision || p.Status.Observed.CapacityGeneration < p.Closed.ExpectedCapacityGeneration {
		return fmt.Errorf("independent PG recovery changed original dispatch, closed domain or serialized proof")
	}
	// 原关闭已确认后，网络中的原Resize仍可能在相同owner下晚到并提交。
	// 不改写旧关闭的expected代次：当前Fence/Status另外证明同revision仍
	// 关闭且实际容量在原操作有限前后代内；后继按这份实际容量重新预算。
	// 原生终态已在外层保存时，新终态必须仍是同一原逻辑事实；结果淘汰
	// 只能进入更高floor的独立隔离，不能改写原Completed/Denied或其容量。
	if original != nil {
		if !control.SameResizeRecoveryBoundary(*original.Receipt.Result, p.Status.Observed) ||
			p.Status.Receipt.Result != nil && !control.SameResizeRecoveryTerminal(*original.Receipt.Result, *p.Status.Receipt.Result) {
			return fmt.Errorf("independent PG recovery contradicts original retained terminal")
		}
	}
	return nil
}

func cloneServicePGRecovery(p *ServicePGRecovery) *ServicePGRecovery {
	if p == nil {
		return nil
	}
	n := *p
	// 复用已有完整快照深拷贝，包含数组ReleaseKey、Operation和历史Result。
	var response control.AdmissionResponse
	response.Observed = p.Fence.Observed
	n.Fence.Observed = cloneAdmissionResponse(&response).Observed
	response.Observed = p.Status.Observed
	n.Status.Observed = cloneAdmissionResponse(&response).Observed
	if p.Status.Receipt.Result != nil {
		response.Observed = *p.Status.Receipt.Result
		result := cloneAdmissionResponse(&response).Observed
		n.Status.Receipt.Result = &result
	}
	return &n
}

func (m Model) validateServicePGRecovery() error {
	p := m.ServiceClosure.PGRecovery
	if p == nil {
		return nil
	}
	c, version := m.ServiceClosure, m.serviceClosureVersion()
	if (version != 17 && version != 18) || m.Active == nil || c.Pending() || c.PGRejection != nil || p.RecordedSequence > m.Sequence ||
		p.Origin.Epoch > m.Owner.Epoch || p.Origin.Epoch == m.Owner.Epoch && p.Origin != m.Owner || p.OperationSHA256 != c.OperationSHA256 {
		return fmt.Errorf("independent PG recovery lost original complete active closure")
	}
	_, _, closed, _ := c.Selection()
	at := c.RecordedSequence
	if c.Continuation != nil {
		at = c.Continuation.RecordedSequence
	}
	if p.Closed != closed || p.ClosedSequence != at {
		return fmt.Errorf("independent PG recovery replaced latest confirmed closure")
	}
	return p.validate(*m.Active)
}

// RecordServicePGRecovery只新增独立事实，不覆盖任何原命令/Phase/预算。
// 来源认证和串行Session由生产执行器负责；Model固定相同关闭及单调owner，
// Store确认UID/RV CAS之后才允许SDK重新查询并清同一原Pending。
func (m Model) RecordServicePGRecovery(actor Owner, selector string, fence control.FenceResponse, status control.ControlResponse) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.Pending() || c.PGRecovery != nil || c.PGRejection != nil || c.Rearm != nil || c.Reopen != nil || c.CleanupRecovery != nil || m.Active == nil {
		return m, fmt.Errorf("PG recovery requires unchanged original confirmed closed operation")
	}
	_, _, closed, selected := c.Selection()
	if selector != selected {
		return m, fmt.Errorf("PG recovery selected another closure")
	}
	commandHash, _, err := m.Active.OriginalServicePG()
	if err != nil {
		return m, err
	}
	effect, err := control.ValidateResizeRecovery(m.Active.Request, fence, status)
	if err != nil {
		return m, err
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	at := c.RecordedSequence
	if c.Continuation != nil {
		at = c.Continuation.RecordedSequence
	}
	next.ServiceClosure.PGRecovery = cloneServicePGRecovery(&ServicePGRecovery{Origin: actor, RecordedSequence: next.Sequence, OperationSHA256: c.OperationSHA256, CommandSHA256: commandHash, Closed: closed, ClosedSequence: at, Effect: effect, Fence: fence, Status: status})
	return next, next.Validate()
}

func (store *Store) RecordServicePGRecovery(ctx context.Context, s *Snapshot, actor Owner, selector string, fence control.FenceResponse, status control.ControlResponse) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	if !s.Dispatchable() {
		return nil, fmt.Errorf("PG recovery requires original running VM")
	}
	next, err := s.model.RecordServicePGRecovery(actor, selector, fence, status)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}

func (p *ServicePGRecovery) UnmarshalJSON(raw []byte) error {
	type plain ServicePGRecovery
	var next plain
	if err := control.UnmarshalObjectBounded(raw, &next, MaxObservationBytes*2, 8192); err != nil {
		return err
	}
	*p = ServicePGRecovery(next)
	return nil
}
