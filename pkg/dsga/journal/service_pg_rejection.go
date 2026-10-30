package journal

import (
	"context"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ServicePGRejection保存缺原生终态的明确拒绝的独立隔离证明，不回写原Step。
// 原执行已经明确Busy/Rejected；只有实际接管后更高floor封住旧POST、串行
// 执行者退出、同boot容量未变且原Key无结果才可保存。它既不是原生Denied，
// 也不使16/17的Guest/VM在途变成完成。来源恢复/开放仍走完整预算审计。
type ServicePGRejection struct {
	Origin           Owner                    `json:"origin"`
	RecordedSequence control.Uint64           `json:"recorded_sequence"`
	OperationSHA256  string                   `json:"operation_sha256"`
	Closed           control.AdmissionRequest `json:"closed"`
	ClosedSequence   control.Uint64           `json:"closed_sequence"`
	Fence            control.FenceResponse    `json:"issuer_fence"`
	Status           control.ControlResponse  `json:"status"`
}

// OriginalPGRejection只读取完整原审计；Unknown、Completed或已有完整原生
// 终态不属于此路径。不接受只含Rejected布尔的旧记录，不能拼接当前快照。
func (op Operation) OriginalPGRejection() (control.ControlResponse, error) {
	for _, step := range op.Steps {
		if step.Action != ResizePG {
			continue
		}
		var audit struct {
			ProtocolVersion uint32                  `json:"protocol_version"`
			Fence           control.FenceResponse   `json:"issuer_fence"`
			Response        control.ControlResponse `json:"response"`
		}
		command, err := control.DecodeRequest(step.Command)
		if err != nil || command != op.Request || step.Phase != Settled || step.Observation == nil || step.Observation.Result != Rejected || step.DispatchedBy == nil || step.TargetID != op.Request.Key.Instance.ComputeInstanceID || control.UnmarshalObject(step.Observation.Data, &audit) != nil || audit.ProtocolVersion != 1 || audit.Response.ValidateResize(op.Request, op.Request.Key.Instance) != nil || audit.Response.Receipt.Result != nil || (audit.Response.Receipt.Disposition != control.ReceiptBusy && audit.Response.Receipt.Disposition != control.ReceiptRejected) {
			return control.ControlResponse{}, fmt.Errorf("original PG step lacks complete non-native rejection audit")
		}
		q := control.FenceRequest{ProtocolVersion: 1, Instance: op.Request.Key.Instance, IssuerEpoch: audit.Fence.RequestedEpoch}
		if audit.Fence.ValidateFor(q, q.Instance) != nil || audit.Fence.Disposition != control.Fenced || audit.Fence.RequestedEpoch < step.DispatchedBy.Epoch || audit.Response.Observed.IssuerFloorEpoch != audit.Fence.RequestedEpoch || audit.Response.Observed.CapacityGeneration < audit.Fence.Observed.CapacityGeneration {
			return control.ControlResponse{}, fmt.Errorf("original PG rejection lost authenticated execution fence")
		}
		return audit.Response, nil
	}
	return control.ControlResponse{}, fmt.Errorf("original operation has no dispatched PG rejection")
}

func (p ServicePGRejection) validate(op Operation) error {
	original, err := op.OriginalPGRejection()
	digest, hashErr := operationClosureDigest(&op)
	if err != nil || hashErr != nil || op.Validate() != nil || digest != p.OperationSHA256 || p.Origin.Validate() != nil || p.Origin.Epoch != p.Fence.RequestedEpoch || p.RecordedSequence <= p.ClosedSequence || p.ClosedSequence == 0 || p.Closed.Validate() != nil || p.Closed.Open || p.Closed.Instance != op.Request.Key.Instance || p.Closed.IssuerEpoch > p.Origin.Epoch || p.Status.Observed.AdmissionOpen || p.Status.Observed.AdmissionRevision != p.Closed.Revision || p.Status.Observed.CapacityGeneration != p.Closed.ExpectedCapacityGeneration || control.ValidateRejectedResizeBarrier(op.Request, original, p.Fence, p.Status) != nil {
		return fmt.Errorf("independent PG rejection changed original rejection, owner or closed domain")
	}
	return nil
}

func cloneServicePGRejection(p *ServicePGRejection) *ServicePGRejection {
	if p == nil {
		return nil
	}
	n := *p
	var temporary control.AdmissionResponse
	temporary.Observed = p.Fence.Observed
	n.Fence.Observed = cloneAdmissionResponse(&temporary).Observed
	temporary.Observed = p.Status.Observed
	n.Status.Observed = cloneAdmissionResponse(&temporary).Observed
	if p.Status.Receipt.Result != nil {
		temporary.Observed = *p.Status.Receipt.Result
		result := cloneAdmissionResponse(&temporary).Observed
		n.Status.Receipt.Result = &result
	}
	return &n
}

func (m Model) validateServicePGRejection() error {
	p := m.ServiceClosure.PGRejection
	if p == nil {
		return nil
	}
	c, v := m.ServiceClosure, m.serviceClosureVersion()
	if (v != 17 && v != 18) || m.Active == nil || c.Pending() || p.RecordedSequence > m.Sequence || p.Origin.Epoch > m.Owner.Epoch || p.Origin.Epoch == m.Owner.Epoch && p.Origin != m.Owner || p.OperationSHA256 != c.OperationSHA256 {
		return fmt.Errorf("independent PG rejection lost original active closure")
	}
	_, _, closed, _ := c.Selection()
	at := c.RecordedSequence
	if c.Continuation != nil {
		at = c.Continuation.RecordedSequence
	}
	if p.Closed != closed || p.ClosedSequence != at {
		return fmt.Errorf("independent PG rejection replaced latest confirmed closure")
	}
	return p.validate(*m.Active)
}

func (m Model) RecordServicePGRejection(actor Owner, selector string, fence control.FenceResponse, status control.ControlResponse) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.Pending() || c.PGRejection != nil || c.Rearm != nil || c.Reopen != nil || c.CleanupRecovery != nil || m.Active == nil {
		return m, fmt.Errorf("PG rejection barrier requires original confirmed closed operation")
	}
	_, _, closed, selected := c.Selection()
	if selector != selected {
		return m, fmt.Errorf("PG rejection barrier requires latest complete closure selector")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	at := c.RecordedSequence
	if c.Continuation != nil {
		at = c.Continuation.RecordedSequence
	}
	next.ServiceClosure.PGRejection = cloneServicePGRejection(&ServicePGRejection{Origin: actor, RecordedSequence: next.Sequence, OperationSHA256: c.OperationSHA256, Closed: closed, ClosedSequence: at, Fence: fence, Status: status})
	return next, next.Validate()
}

// 只读屏障与原未知/预算共同由UID/RV CAS固定。丢回复不返回已确认快照，
// SDK不得提前清Pending；重读后可以复用原证明，不生成资源或原生取消命令。
func (store *Store) RecordServicePGRejection(ctx context.Context, s *Snapshot, actor Owner, selector string, fence control.FenceResponse, status control.ControlResponse) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	if !s.Dispatchable() {
		return nil, fmt.Errorf("PG rejection barrier requires original running VM")
	}
	next, err := s.model.RecordServicePGRejection(actor, selector, fence, status)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}

func (p *ServicePGRejection) UnmarshalJSON(raw []byte) error {
	type plain ServicePGRejection
	var next plain
	if err := control.UnmarshalObjectBounded(raw, &next, MaxObservationBytes*2, 8192); err != nil {
		return err
	}
	*p = ServicePGRejection(next)
	return nil
}
