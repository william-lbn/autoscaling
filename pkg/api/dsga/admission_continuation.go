package dsga

import "fmt"

// AdmissionContinuationRequest为接管者显式选择同一空闲服务关闭的后继命令。
// Original始终保留最初完整意图；PreviousClose保留最近一次关闭，不能将其
// 直接换成新owner后重放。v1固定同容量；v2允许同boot/预算的容量代次
// 单调前进，用于PG已派发后关闭撞上原Resize提交的恢复。子请求仍为v1。
// v2不授权任意Resize：外层Journal必须另核该原操作允许的有限代次集合。
//
// 该值只是写前WAL的协议，不授予派发权限。实际执行前仍须原UID/RV写前
// 确认、当前Session认证及PG原生fence；只有Close的完整关闭回执才能完成。
// floor、NotFound或当前布尔closed都不能代替回执，更不能释放预算/缩VM。
type AdmissionContinuationRequest struct {
	ProtocolVersion uint32           `json:"protocol_version"`
	Original        AdmissionRequest `json:"original"`
	PreviousClose   AdmissionRequest `json:"previous_close"`
	Close           AdmissionRequest `json:"close"`
}

func (r AdmissionContinuationRequest) Validate() error {
	if (r.ProtocolVersion != 1 && r.ProtocolVersion != 2) || r.Original.Validate() != nil || r.PreviousClose.Validate() != nil || r.Close.Validate() != nil {
		return fmt.Errorf("invalid admission continuation protocol or request")
	}
	o, p, n := r.Original, r.PreviousClose, r.Close
	// 初次接续可令PreviousClose逐字等于原关闭；已有后继时必须是更高
	// 的原关闭。准入revision在同boot内全局单调，换owner也不能倒退。
	if p.Open || (p != o && p.ClosesOver(o) != nil) || p.Revision < o.Revision ||
		n.ClosesOver(p) != nil || n.IssuerEpoch < p.IssuerEpoch || n.Revision < p.Revision ||
		o.BudgetCommitmentID != p.BudgetCommitmentID || o.BudgetCommitmentID != n.BudgetCommitmentID {
		return fmt.Errorf("admission continuation changes original boundary or fails to fence predecessor")
	}
	// 只比较不加一，避免最大u64回绕。容量前进不证明原资源动作完成、没有
	// 效果或预算可释放；关闭原生回执仍须精确匹配所选Close及当前boot。
	if r.ProtocolVersion == 1 && (o.ExpectedCapacityGeneration != p.ExpectedCapacityGeneration || p.ExpectedCapacityGeneration != n.ExpectedCapacityGeneration) ||
		r.ProtocolVersion == 2 && (o.ExpectedCapacityGeneration > p.ExpectedCapacityGeneration || p.ExpectedCapacityGeneration > n.ExpectedCapacityGeneration) {
		return fmt.Errorf("admission continuation violates selected capacity protocol")
	}
	// 同owner只在v2且容量确实前进时选择新关闭，ClosesOver同时要求
	// revision严格提高，封住迟到旧关闭。这是原Resize唯一的有限恢复机会，
	// 不是超时后任意换revision的重试；同代次仍必须更高owner接管。
	if n.IssuerEpoch == p.IssuerEpoch && (r.ProtocolVersion != 2 || n.ExpectedCapacityGeneration <= p.ExpectedCapacityGeneration) {
		return fmt.Errorf("same owner continuation requires capacity progress")
	}
	return nil
}

func (r AdmissionContinuationRequest) ContextSHA256() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	return contextSHA256(r)
}

// AdmissionContinuationResponse固定原始、前驱和后继的完整选择摘要。ClosedBy
// 必须是PG对Close的原生回执；此包装不合成PG事实，也不声称任一旧请求未执行。
// 任意先前开放或已开始工作继续占原预算，目标平台仍需独立排空/回收验收。
type AdmissionContinuationResponse struct {
	ProtocolVersion     uint32                       `json:"protocol_version"`
	VMShrinkPermit      bool                         `json:"vm_shrink_permit"`
	Request             AdmissionContinuationRequest `json:"request"`
	ContextSHA256       string                       `json:"context_sha256"`
	ClosedBy            AdmissionResponse            `json:"closed_by"`
	PriorMayHaveApplied bool                         `json:"prior_may_have_applied"`
}

func (r AdmissionContinuationResponse) ValidateFor(request AdmissionContinuationRequest, bound Instance, query bool) error {
	digest, err := request.ContextSHA256()
	if err != nil || r.ProtocolVersion != request.ProtocolVersion || r.VMShrinkPermit || !r.PriorMayHaveApplied || r.Request != request || r.ContextSHA256 != digest || !r.ClosedBy.Confirms() {
		return fmt.Errorf("admission continuation lacks exact retained selection or native close")
	}
	return r.ClosedBy.ValidateFor(request.Close, bound, query)
}

func (r *AdmissionContinuationRequest) UnmarshalJSON(raw []byte) error {
	type plain AdmissionContinuationRequest
	var value plain
	if len(raw) > 16*1024 {
		return fmt.Errorf("admission continuation exceeds request limit")
	}
	if err := UnmarshalObject(raw, &value); err != nil {
		return err
	}
	next := AdmissionContinuationRequest(value)
	if err := next.Validate(); err != nil {
		return err
	}
	*r = next
	return nil
}

func (r *AdmissionContinuationResponse) UnmarshalJSON(raw []byte) error {
	type plain AdmissionContinuationResponse
	var value plain
	if err := UnmarshalObject(raw, &value); err != nil {
		return err
	}
	*r = AdmissionContinuationResponse(value)
	return nil
}
