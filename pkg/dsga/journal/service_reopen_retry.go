package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ServiceReopenRetirement在业务重试时归档已被完整再关闭封住的新开放；
// 期限恢复亦复用此有界证据载体，其原Unknown不会因此被改成NoEffect。
// Context保存直接前驱的全部原意图、未知/回执、Before、独立关闭及接续，
// 去掉更早Retired以保持空间与故障次数无关。PreviousSHA256累积更早证据，
// 不把被退休Unknown改成成功或NoEffect；资源Active及原预算不能被退休。
// 首次开放未知仍永久留在InitialHeld，直到正常资源协议替换该预算。
type ServiceReopenRetirement struct {
	Context        []byte `json:"context"`
	ContextSHA256  string `json:"context_sha256"`
	PreviousSHA256 string `json:"previous_sha256"`
	EvidenceSHA256 string `json:"evidence_sha256"`
}

func reopenRetirementDigest(raw []byte, previous string) string {
	// JSON数组固定字节边界，避免简单拼接产生歧义；不接收调用方摘要
	// 来代替完整直接前驱，解码时仍逐字段验证原生关闭和所有领域绑定。
	encoded, _ := json.Marshal([]any{previous, raw})
	return rawDigest(encoded)
}

func (m Model) validateServiceReopenRetirement(archived bool) error {
	r := m.ServiceClosure.Reopen
	if r.Attempt == 0 {
		if r.PriorClose != nil || r.PriorClosedAt != 0 || r.Retired != nil {
			return fmt.Errorf("first reopen invented retry predecessor")
		}
		return nil
	}
	p := r.PriorClose
	_, _, originalClosed, _ := m.ServiceClosure.Selection()
	if p == nil || p.Validate() != nil || p.Open || p.Instance != r.Request.Instance || p.BudgetCommitmentID != r.Request.BudgetCommitmentID || p.BudgetCommitmentID != originalClosed.BudgetCommitmentID || p.ExpectedCapacityGeneration != r.Request.ExpectedCapacityGeneration || p.ExpectedCapacityGeneration != originalClosed.ExpectedCapacityGeneration || p.IssuerEpoch > r.Origin.Epoch || p.Revision <= originalClosed.Revision || r.PriorClosedAt <= m.ServiceClosure.StartedSequence || r.PriorClosedAt >= r.StartedSequence {
		return fmt.Errorf("reopen retry lost exact prior closed boundary")
	}
	if archived {
		if r.Retired != nil {
			return fmt.Errorf("reopen retirement cannot recursively embed history")
		}
		return nil
	}
	h := r.Retired
	if h == nil || h.ContextSHA256 != rawDigest(h.Context) || h.EvidenceSHA256 != reopenRetirementDigest(h.Context, h.PreviousSHA256) || r.Attempt == 1 && h.PreviousSHA256 != "" || r.Attempt > 1 && !lifecycleDigest(h.PreviousSHA256) {
		return fmt.Errorf("reopen retry lost complete bounded predecessor evidence")
	}
	type plain ServiceReopen
	var decoded plain
	if err := control.UnmarshalObjectBounded(h.Context, &decoded, MaxRawBytes/2, 16384); err != nil {
		return err
	}
	previous := ServiceReopen(decoded)
	if previous.Retired != nil || previous.Attempt == control.Uint64(math.MaxUint64) || previous.Attempt+1 != r.Attempt || previous.Closing == nil || previous.Closing.Close == nil || previous.Closing.Close.Pending() || previous.Origin.Epoch > r.Origin.Epoch || previous.Origin.Epoch == r.Origin.Epoch && previous.Origin != r.Origin || previous.Before.DeadlineNS > r.Before.DeadlineNS || previous.Before.LeaseEpoch > r.Before.LeaseEpoch {
		return fmt.Errorf("reopen retry predecessor is not exact completed closure")
	}
	// 不递归调用Model.Validate，避免重复读取生命周期上下文。只替换
	// 私有载体中的Reopen并校验完整前驱；不改当前模型或原关闭字段。
	carrier, closure := m, *m.ServiceClosure
	closure.Reopen, carrier.ServiceClosure = &previous, &closure
	if err := carrier.validateServiceReopenRecord(true); err != nil {
		return err
	}
	_, _, closed, _ := previous.Closing.Close.Selection()
	closedAt := previous.Closing.Close.RecordedSequence
	if n := previous.Closing.Close.Continuation; n != nil {
		closedAt = n.RecordedSequence
	}
	if closed != *p || closedAt != r.PriorClosedAt || previous.ClosureVersion != r.ClosureVersion || previous.ClosureSHA256 != r.ClosureSHA256 || previous.HeldSHA256 != r.HeldSHA256 {
		return fmt.Errorf("reopen retry differs from full original closure and held budget")
	}
	if err := liveAfterReopenRearm(previous.Rearm, r.Before, r.StartedSequence); err != nil {
		return err
	}
	return nil
}

// 新业务重试须严格晚于最近原生恢复的完整确认。有效布尔不能代替准确
// 应用回执；期限/epoch不可回退，原恢复未知也不能被后继开放隐式抹掉。
func liveAfterReopenRearm(rearm *ReopenLeaseRecovery, b control.AdmissionLeaseResponse, sequence control.Uint64) error {
	if rearm != nil && (rearm.Phase != Settled || rearm.Receipt == nil || b.DeadlineNS < rearm.Receipt.AppliedDeadlineNS || b.LeaseEpoch < rearm.Origin.Epoch || sequence <= rearm.RecordedSequence) {
		return fmt.Errorf("reopen retry predates exact confirmed latest rearm")
	}
	return nil
}

// RetryServiceReopen只选择已完整再关闭后的下一次受控开放。普通重试、
// 查询、当前closed布尔及回退期限都不能产生新revision。仍要求原VM、
// 同容量/预算、有效期限与当前owner；有Active仅接受13/14完整资源结算。
// 调用方必须前后审计来源，不能用新开放替代15～18的资源/取消结算。
// next先深拷贝，失败保持原模型，永久预算/首次未知/HWM全部不改。
func (m Model) RetryServiceReopen(actor Owner, selector string, before control.AdmissionLeaseResponse) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.Reopen == nil || c.Reopen.Closing == nil || c.Reopen.Closing.Close == nil || c.Reopen.Closing.Close.Pending() || m.stableServiceRecovery() != nil {
		return m, fmt.Errorf("reopen retry requires complete original reclosure with stable resources")
	}
	r := c.Reopen
	_, _, closed, selected := r.Closing.Close.Selection()
	if selector != selected || r.Attempt == control.Uint64(math.MaxUint64) {
		return m, fmt.Errorf("reopen retry selector or counter differs from latest closure")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	if err := liveAfterReopenRearm(r.Rearm, before, next.Sequence); err != nil {
		return m, err
	}
	// 旧Retired只累积摘要，完整当前前驱仍逐字冻结；它所有原命令/
	// 未知/回执及关闭都在Context，不将较新closed投影回旧开放成功。
	prior := *next.ServiceClosure.Reopen
	prior.Retired = nil
	raw, err := json.Marshal(prior)
	if err != nil {
		return m, err
	}
	retired := &ServiceReopenRetirement{Context: raw, ContextSHA256: rawDigest(raw), PreviousSHA256: "", EvidenceSHA256: ""}
	if r.Retired != nil {
		retired.PreviousSHA256 = r.Retired.EvidenceSHA256
	}
	retired.EvidenceSHA256 = reopenRetirementDigest(raw, retired.PreviousSHA256)
	request := r.Request
	request.IssuerEpoch, request.Revision = actor.Epoch, closed.Revision+1
	digest, err := request.ContextSHA256()
	if err != nil {
		return m, err
	}
	closedAt := r.Closing.Close.RecordedSequence
	if n := r.Closing.Close.Continuation; n != nil {
		closedAt = n.RecordedSequence
	}
	next.ServiceClosure.Reopen = &ServiceReopen{ClosureVersion: r.ClosureVersion, Origin: actor, StartedSequence: next.Sequence, ClosureSHA256: r.ClosureSHA256, HeldSHA256: r.HeldSHA256, Before: cloneLeaseResponse(before), Request: request, CommandSHA256: digest, Phase: Unconfirmed, RecordedSequence: 0, Query: false, Receipt: nil, SafetyStage: "opening", Closing: nil, Attempt: r.Attempt + 1, PriorClose: &closed, PriorClosedAt: closedAt, Retired: retired}
	if err := next.reopenCompletionCapacity(); err != nil {
		return m, err
	}
	return next, next.Validate()
}

// Store写前CAS只在同一running VM返回确认票据。丢回复不发送；重建后
// 只查询当前新选择，或原owner持原摘要明确重放。退役证据不授予执行权。
func (store *Store) RetryServiceReopen(ctx context.Context, s *Snapshot, actor Owner, selector string, before control.AdmissionLeaseResponse) (*Snapshot, *ServiceReopen, error) {
	if err := store.checked(s); err != nil {
		return nil, nil, err
	}
	if !s.Dispatchable() {
		return nil, nil, fmt.Errorf("reopen retry requires original running VM")
	}
	next, err := s.model.RetryServiceReopen(actor, selector, before)
	if err != nil {
		return nil, nil, err
	}
	result, err := store.apply(ctx, s, next)
	if err != nil {
		return nil, nil, err
	}
	return result, result.State().ServiceClosure.Reopen, nil
}

func (h *ServiceReopenRetirement) UnmarshalJSON(raw []byte) error {
	type plain ServiceReopenRetirement
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, MaxRawBytes, 16384); err != nil {
		return err
	}
	*h = ServiceReopenRetirement(p)
	return nil
}
