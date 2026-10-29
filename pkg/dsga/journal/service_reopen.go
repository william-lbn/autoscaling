package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ServiceReopen 保存稳定关闭边界后唯一的新开放意图。它不改写旧关闭、
// 旧首次开放未知或原 rearm；原命令完整身份和当前有效期限先与来源摘要
// 一起落入同一 UID/RV 写前 CAS，之后生产执行器才可能取得派发票据。
//
// 本记录只确认新开放的原生事实，不能自行解除外层关闭：后继还必须完成
// setter 后来源复核、异常再关闭和生命周期归档。这里绝不清 ServiceClosure，
// 不把 Settled 当成 Begin/VM/预算释放许可。无Active沿用原规则；有Active
// 仅接受13/14完整结算及永久预算，或15独立结算证明；16～18仍须恢复资源。
type ServiceReopen struct {
	ClosureVersion   uint32                         `json:"closure_version"`
	Origin           Owner                          `json:"origin"`
	StartedSequence  control.Uint64                 `json:"started_sequence"`
	ClosureSHA256    string                         `json:"closure_sha256"`
	HeldSHA256       string                         `json:"held_sha256"`
	Before           control.AdmissionLeaseResponse `json:"before"`
	Request          control.AdmissionRequest       `json:"request"`
	CommandSHA256    string                         `json:"command_sha256"`
	Phase            Phase                          `json:"phase"`
	RecordedSequence control.Uint64                 `json:"recorded_sequence"`
	Query            bool                           `json:"query"`
	Receipt          *control.AdmissionResponse     `json:"receipt"`
	// 必填判别符防止删除 Closing/Continuation 后把再关闭投影回开放。
	// 它只描述新恢复的安全阶段，不改原开放 Phase 或伪造其执行结果。
	SafetyStage string `json:"safety_stage"`
	// 重新审计失败时的独立关闭。原开放相位/回执不被改成失败或 NoEffect，
	// 原关闭选择亦不覆盖；该关闭只能阻止之后的新准入，不能释放资源。
	Closing *ServiceReopenClosure `json:"closing,omitempty"`
	// 每次重新选择必须以最近完整再关闭为前驱。零表示首次新开放；
	// 后继保存完整直接前驱与累积摘要，不能递归包含旧前驱或改旧Unknown。
	Attempt       control.Uint64            `json:"attempt,omitempty"`
	PriorClose    *control.AdmissionRequest `json:"prior_close,omitempty"`
	PriorClosedAt control.Uint64            `json:"prior_closed_at,omitempty"`
	Retired       *ServiceReopenRetirement  `json:"retired,omitempty"`
	// 再关闭后期限过期的独立恢复。它不覆盖外层首次 Rearm，也不改变
	// 原开放/关闭未知；SafetyStage 必须同时为 rearming，防止字段丢失。
	Rearm *ReopenLeaseRecovery `json:"rearm,omitempty"`
}

func serviceReopenClosureDigest(c ServiceClosure) (string, error) {
	c.Reopen = nil
	c.FormatVersion = 0
	raw, err := json.Marshal(c)
	return rawDigest(raw), err
}

func serviceReopenVersionAllowed(version uint32) bool {
	return version >= 10 && version <= 15 || version == 19
}

// serviceClosureVersion 只解开明确保存的外层恢复格式，不能降级外层
// ProtocolVersion。原关闭的类别继续由它自己的校验器认证，未知类别拒绝。
func (m Model) serviceClosureVersion() uint32 {
	version := m.ClosureFormatVersion()
	if c := m.ServiceClosure; c != nil {
		if version == 20 && c.Reopen != nil {
			version = c.Reopen.ClosureVersion
		}
		if version == 19 && c.Rearm != nil {
			version = c.Rearm.ClosureVersion
		}
	}
	return version
}

func (m Model) validateServiceReopen() error {
	return m.validateServiceReopenRecord(false)
}

// archived仅供已去除更早前驱的完整直接前驱原文校验。当前记录始终
// 要求完整Retired；归档仍认证原命令/预算/期限/关闭，不能获得派发许可。
func (m Model) validateServiceReopenRecord(archived bool) error {
	c := m.ServiceClosure
	if c.Reopen == nil {
		if m.ClosureFormatVersion() == 20 {
			return fmt.Errorf("reopen journal lost original opening selection")
		}
		return nil
	}
	r := c.Reopen
	closureHash, closureErr := serviceReopenClosureDigest(*c)
	heldHash, heldErr := serviceRearmHeldDigest(m)
	digest, commandErr := r.Request.ContextSHA256()
	_, _, closed, _ := c.Selection()
	closedAt := c.RecordedSequence
	if c.Continuation != nil {
		closedAt = c.Continuation.RecordedSequence
	}
	if c.Rearm != nil {
		closedAt = max(closedAt, c.Rearm.RecordedSequence)
	}
	if r.Attempt != 0 {
		closedAt = r.PriorClosedAt
	}
	version := r.ClosureVersion
	if m.ClosureFormatVersion() != 20 || !serviceReopenVersionAllowed(version) || (version == 19) != (c.Rearm != nil) || m.stableServiceRecovery() != nil || c.Pending() || c.Rearm != nil && c.Rearm.Phase != Settled || closureErr != nil || heldErr != nil || commandErr != nil || r.ClosureSHA256 != closureHash || r.HeldSHA256 != heldHash || r.CommandSHA256 != digest || r.Origin.Validate() != nil || r.Origin.Epoch > m.Owner.Epoch || r.Origin.Epoch == m.Owner.Epoch && r.Origin != m.Owner || r.StartedSequence <= closedAt || r.StartedSequence > m.Sequence {
		return fmt.Errorf("service reopen changed original stable closure, budget or owner")
	}
	b, q := r.Before, r.Request
	if r.Attempt != 0 && r.PriorClose != nil {
		closed = *r.PriorClose
	}
	if m.PGInstance == nil || b.ValidateFor(b.Request, *m.PGInstance) != nil || b.Request.Action != "observe" || !b.Armed || b.Expired || b.Observed.AdmissionOpen || b.LeaseEpoch > r.Origin.Epoch || b.Observed.IssuerFloorEpoch != r.Origin.Epoch || b.Request.IssuerEpoch != r.Origin.Epoch || b.Observed.CapacityGeneration != closed.ExpectedCapacityGeneration || b.Observed.AdmissionRevision != closed.Revision || b.Observed.AdmissionRevision == control.Uint64(math.MaxUint64) || !q.Open || q.Instance != *m.PGInstance || q.IssuerEpoch != r.Origin.Epoch || q.Revision != b.Observed.AdmissionRevision+1 || q.ExpectedCapacityGeneration != b.Observed.CapacityGeneration || q.BudgetCommitmentID != closed.BudgetCommitmentID {
		return fmt.Errorf("service reopen lacks exact live native lease and next admission context")
	}
	if c.Rearm != nil && (b.DeadlineNS < c.Rearm.Receipt.AppliedDeadlineNS || b.LeaseEpoch < c.Rearm.Origin.Epoch) {
		return fmt.Errorf("service reopen predates confirmed original rearm")
	}
	switch r.Phase {
	case Unconfirmed:
		if r.RecordedSequence != 0 || r.Query || r.Receipt != nil {
			return fmt.Errorf("unknown service reopen contains completion")
		}
	case Settled:
		// 历史完整原回执可由更高 owner 查询，但较新 revision 的当前 open
		// 或 expired/result_expired 均不能补成原开放成功。容量也必须仍是
		// 此稳定边界；这里只落原生事实，不声称当前 lease/来源继续有效。
		if r.RecordedSequence <= r.StartedSequence || r.RecordedSequence > m.Sequence || r.Receipt == nil || !r.Receipt.Confirms() || r.Receipt.ValidateFor(q, *m.PGInstance, r.Query) != nil || r.Receipt.Observed.IssuerFloorEpoch > m.Owner.Epoch || r.Receipt.Observed.CapacityGeneration != q.ExpectedCapacityGeneration {
			return fmt.Errorf("service reopen lacks original native open receipt")
		}
	default:
		return fmt.Errorf("invalid service reopen phase")
	}
	if err := m.validateServiceReopenClosure(); err != nil {
		return err
	}
	if err := m.validateReopenLeaseRecovery(r.Rearm, false); err != nil {
		return err
	}
	return m.validateServiceReopenRetirement(archived)
}

// StartServiceReopen 只构造写前模型，不发送网络。新请求由已校验的原生
// observe 和原预算唯一派生；调用方不能提交另一预算/revision。next 深拷贝
// 并核 actor/Sequence 溢出；任何失败保留原输入，不能将临时对象当派发票据。
func (m Model) StartServiceReopen(actor Owner, selector string, before control.AdmissionLeaseResponse) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.Reopen != nil || c.Pending() || m.stableServiceRecovery() != nil || !serviceReopenVersionAllowed(m.ClosureFormatVersion()) {
		return m, fmt.Errorf("reopen requires original confirmed idle closure without another selection")
	}
	_, _, closed, selected := c.Selection()
	if selector != selected {
		return m, fmt.Errorf("reopen selector differs from latest closed command")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	request := control.AdmissionRequest{ProtocolVersion: 1, Instance: closed.Instance, IssuerEpoch: actor.Epoch, Revision: before.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: before.Observed.CapacityGeneration, Open: true, BudgetCommitmentID: closed.BudgetCommitmentID}
	closureHash, err := serviceReopenClosureDigest(*c)
	if err != nil {
		return m, err
	}
	heldHash, err := serviceRearmHeldDigest(m)
	if err != nil {
		return m, err
	}
	digest, err := request.ContextSHA256()
	if err != nil {
		return m, err
	}
	next.setClosureFormatVersion(20)
	next.ServiceClosure.Reopen = &ServiceReopen{ClosureVersion: m.ClosureFormatVersion(), Origin: actor, StartedSequence: next.Sequence, ClosureSHA256: closureHash, HeldSHA256: heldHash, Before: cloneLeaseResponse(before), Request: request, CommandSHA256: digest, Phase: Unconfirmed, RecordedSequence: 0, Query: false, Receipt: nil, SafetyStage: "opening", Closing: nil, Attempt: 0, PriorClose: nil, PriorClosedAt: 0, Retired: nil}
	if err := next.reopenCompletionCapacity(); err != nil {
		return m, err
	}
	return next, next.Validate()
}

func (m Model) RecordServiceReopen(actor Owner, fact control.AdmissionResponse, query bool) (Model, error) {
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	if next.ServiceClosure == nil || next.ServiceClosure.Reopen == nil || next.ServiceClosure.Reopen.Closing != nil || next.ServiceClosure.Reopen.Phase != Unconfirmed || !query && next.ServiceClosure.Reopen.Origin != actor {
		return m, fmt.Errorf("service reopen is not original unknown")
	}
	r := next.ServiceClosure.Reopen
	r.Phase, r.RecordedSequence, r.Query, r.Receipt = Settled, next.Sequence, query, cloneAdmissionResponse(&fact)
	return next, next.Validate()
}

// Store 只在原 running VM 的 UID/RV CAS 获得完整确认后返回派发票据。
// 丢回复、重建、旧 RV 都不能返回可执行对象；后继只能读原 WAL 并查询。
// 这里不持本地互斥锁跨网络，串行性由已认证 Session/Store CAS 共同保证。
func (store *Store) StartServiceReopen(ctx context.Context, s *Snapshot, actor Owner, selector string, before control.AdmissionLeaseResponse) (*Snapshot, *ServiceReopen, error) {
	if err := store.checked(s); err != nil {
		return nil, nil, err
	}
	if !s.Dispatchable() {
		return nil, nil, fmt.Errorf("service reopen requires original running VM")
	}
	next, err := s.model.StartServiceReopen(actor, selector, before)
	if err != nil {
		return nil, nil, err
	}
	result, err := store.apply(ctx, s, next)
	if err != nil {
		return nil, nil, err
	}
	return result, result.State().ServiceClosure.Reopen, nil
}

func (store *Store) RecordServiceReopen(ctx context.Context, s *Snapshot, actor Owner, fact control.AdmissionResponse, query bool) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	next, err := s.model.RecordServiceReopen(actor, fact, query)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}

func (r *ServiceReopen) UnmarshalJSON(raw []byte) error {
	type plain ServiceReopen
	var next plain
	if err := control.UnmarshalObjectBounded(raw, &next, MaxRawBytes/2, 16384); err != nil {
		return err
	}
	*r = ServiceReopen(next)
	return nil
}
