package journal

import (
	"context"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ServiceClosureContinuation是原关闭未知的独立后继WAL。原始ServiceClosure
// 的请求/owner/序列/未知永不改写；这里保存最新及直接前驱的完整身份。
// 通常只允许更高owner显式选择；v17原PG提交使容量严格前进时，同owner可
// 以严格更高revision接续一次。普通超时不能反复换revision或丢直接前驱。
// 三份请求固定boot/预算；原PG容量集合及更高epoch/revision封住更早前驱。
type ServiceClosureContinuation struct {
	Origin           Owner                                  `json:"origin"`
	StartedSequence  control.Uint64                         `json:"started_sequence"`
	PreviousOrigin   Owner                                  `json:"previous_origin"`
	PreviousSequence control.Uint64                         `json:"previous_sequence"`
	Request          control.AdmissionContinuationRequest   `json:"request"`
	CommandSHA256    string                                 `json:"command_sha256"`
	Phase            Phase                                  `json:"phase"`
	RecordedSequence control.Uint64                         `json:"recorded_sequence"`
	Query            bool                                   `json:"query"`
	Receipt          *control.AdmissionContinuationResponse `json:"receipt"`
}

// Pending只表示最近完整关闭选择尚未确认，不把原始未知投影为NoEffect。
// 后继已关闭后原始Phase仍Unconfirmed；是否允许重新服务另需独立重开协议。
func (c *ServiceClosure) Pending() bool {
	if c.Continuation != nil {
		return c.Continuation.Phase == Unconfirmed
	}
	return c.Phase == Unconfirmed
}

// Selection给管理端/执行器选择当前WAL，返回值副本；不授予新setter执行权。
func (c *ServiceClosure) Selection() (Owner, control.Uint64, control.AdmissionRequest, string) {
	if c.Continuation != nil {
		n := c.Continuation
		return n.Origin, n.StartedSequence, n.Request.Close, n.CommandSHA256
	}
	return c.Origin, c.StartedSequence, c.Request, c.CommandSHA256
}

func (model Model) validateServiceClosureContinuation() error {
	c := model.ServiceClosure
	if c.Continuation == nil {
		return nil
	}
	n := c.Continuation
	// 只有已派发PG的v17模型可消费跨容量协议。即使SDK接受单调多代次，
	// Journal仍只接受原Resize的前后两代，且原始关闭/直接前驱不能倒退。
	version := uint32(1)
	if model.ProtocolVersion == 17 {
		version = 2
	}
	if n.Request.ProtocolVersion != version || model.ProtocolVersion == 17 &&
		(!model.activeServiceCapacityAllowed(n.Request.PreviousClose.ExpectedCapacityGeneration) || !model.activeServiceCapacityAllowed(n.Request.Close.ExpectedCapacityGeneration) || n.Request.PreviousClose.ExpectedCapacityGeneration < c.Request.ExpectedCapacityGeneration) {
		return fmt.Errorf("service continuation changes original resize capacity boundary")
	}
	sameOwnerProgress := model.ProtocolVersion == 17 && n.PreviousOrigin == n.Origin && n.Request.Close.ExpectedCapacityGeneration > n.Request.PreviousClose.ExpectedCapacityGeneration
	// 身份、序列及完整请求共同固定直接前驱，不能只凭epoch大小收养
	// 任意关闭。原始WAL始终Unconfirmed，证明的是新关闭已确认，而
	// 非旧关闭未执行。所有序列均来自同一外层CAS，不借PG快照补写。
	digest, err := n.Request.ContextSHA256()
	if c.Phase != Unconfirmed || err != nil || n.Request.Original != c.ProtectedAdmission() || n.CommandSHA256 != digest ||
		n.Origin.Validate() != nil || n.PreviousOrigin.Validate() != nil || n.Request.Close.IssuerEpoch != n.Origin.Epoch ||
		n.Request.PreviousClose.IssuerEpoch != n.PreviousOrigin.Epoch || n.Origin.Epoch > model.Owner.Epoch ||
		n.Origin.Epoch == model.Owner.Epoch && n.Origin != model.Owner || n.PreviousOrigin.Epoch > n.Origin.Epoch || n.PreviousOrigin.Epoch == n.Origin.Epoch && !sameOwnerProgress ||
		n.PreviousSequence < c.StartedSequence || n.StartedSequence <= n.PreviousSequence || n.StartedSequence > model.Sequence {
		return fmt.Errorf("service continuation changed original boundary, predecessor or owner")
	}
	if n.Request.PreviousClose == c.Request {
		if n.PreviousOrigin != c.Origin || n.PreviousSequence != c.StartedSequence {
			return fmt.Errorf("first continuation lost original predecessor")
		}
	} else if n.PreviousOrigin.Epoch < c.Origin.Epoch || n.PreviousOrigin.Epoch == c.Origin.Epoch && (model.ProtocolVersion != 17 || n.PreviousOrigin != c.Origin) || n.PreviousSequence <= c.StartedSequence {
		return fmt.Errorf("later continuation lost higher predecessor")
	}
	switch n.Phase {
	case Unconfirmed:
		if n.RecordedSequence != 0 || n.Query || n.Receipt != nil {
			return fmt.Errorf("unknown continuation contains completion")
		}
	case Settled:
		if model.ProtocolVersion == 17 && n.Receipt != nil && !model.activeServiceCapacityAllowed(n.Receipt.ClosedBy.Observed.CapacityGeneration) {
			return fmt.Errorf("service continuation proof exceeds original resize capacity")
		}
		// 查询可由后来的owner完成，但原生floor不得超出外层当前
		// 已确认的owner；否则会借用尚未完成接管的未来事实来结清。
		if n.RecordedSequence <= n.StartedSequence || n.RecordedSequence > model.Sequence || n.Receipt == nil ||
			n.Receipt.ClosedBy.Observed.IssuerFloorEpoch > model.Owner.Epoch || n.Receipt.ValidateFor(n.Request, *model.PGInstance, n.Query) != nil {
			return fmt.Errorf("continuation lacks exact native closed receipt")
		}
	default:
		return fmt.Errorf("invalid continuation phase")
	}
	return nil
}

// StartServiceClosureContinuation只产生待持久模型，不发送PG。每次更高
// owner只可选择最近完整摘要；同owner仅v17原容量前进可新选，普通丢回复
// 必须查询/明确重放原选择。容量前进许可不改变原关闭、PG步骤或永久预算。
// next进行深拷贝并校验当前actor、Sequence溢出，失败不修改传入模型。
func (model Model) StartServiceClosureContinuation(actor Owner, selector string, request control.AdmissionContinuationRequest) (Model, error) {
	if err := model.Validate(); err != nil {
		return model, err
	}
	c := model.ServiceClosure
	if c == nil || !c.Pending() {
		return model, fmt.Errorf("original service closure is not unknown")
	}
	origin, sequence, previous, digest := c.Selection()
	sameOwnerProgress := model.ProtocolVersion == 17 && actor == origin && request.Close.ExpectedCapacityGeneration > previous.ExpectedCapacityGeneration
	if actor.Epoch < origin.Epoch || actor.Epoch == origin.Epoch && !sameOwnerProgress || selector != digest || request.Validate() != nil || request.Original != c.ProtectedAdmission() || request.PreviousClose != previous || request.Close.IssuerEpoch != actor.Epoch {
		return model, fmt.Errorf("continuation is not exact explicit successor selection")
	}
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	commandDigest, err := request.ContextSHA256()
	if err != nil {
		return model, err
	}
	next.ProtocolVersion = max(next.ProtocolVersion, 11)
	next.ServiceClosure.Continuation = &ServiceClosureContinuation{
		Origin: actor, StartedSequence: next.Sequence, PreviousOrigin: origin, PreviousSequence: sequence,
		Request: request, CommandSHA256: commandDigest, Phase: Unconfirmed, RecordedSequence: 0, Query: false, Receipt: nil,
	}
	return next, next.Validate()
}

// RecordServiceClosureContinuation不接受布尔closed、floor或旧回执替代品。
// 同owner可记录原setter结果；接管者只记录原完整选择的只读回执。这里
// 不认证网络来源，生产者必须使用固定实例的SDK并持有当前Session。
func (model Model) RecordServiceClosureContinuation(actor Owner, fact control.AdmissionContinuationResponse, query bool) (Model, error) {
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	c := next.ServiceClosure
	if c == nil || c.Continuation == nil || !c.Pending() || !query && c.Continuation.Origin != actor {
		return model, fmt.Errorf("continuation is not original unknown")
	}
	proof := fact
	proof.ClosedBy = *cloneAdmissionResponse(&fact.ClosedBy)
	n := c.Continuation
	n.Phase, n.RecordedSequence, n.Query, n.Receipt = Settled, next.Sequence, query, &proof
	return next, next.Validate()
}

// 写前CAS必须返回完整UID/RV确认才提供派发票据。丢回复仍保留新选择，下一轮
// 只查它的原生回执，不能退回重放旧owner关闭或从当前closed布尔推导已完成。
func (store *Store) StartServiceClosureContinuation(ctx context.Context, s *Snapshot, actor Owner, selector string, request control.AdmissionContinuationRequest) (*Snapshot, *ServiceClosureContinuation, error) {
	if err := store.checked(s); err != nil {
		return nil, nil, err
	}
	if !s.Dispatchable() {
		return nil, nil, fmt.Errorf("continuation requires original running VM")
	}
	next, err := s.model.StartServiceClosureContinuation(actor, selector, request)
	if err != nil {
		return nil, nil, err
	}
	result, err := store.apply(ctx, s, next)
	if err != nil {
		return nil, nil, err
	}
	return result, result.State().ServiceClosure.Continuation, nil
}

func (store *Store) RecordServiceClosureContinuation(ctx context.Context, s *Snapshot, actor Owner, fact control.AdmissionContinuationResponse, query bool) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	next, err := s.model.RecordServiceClosureContinuation(actor, fact, query)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}

func (n *ServiceClosureContinuation) UnmarshalJSON(raw []byte) error {
	type plain ServiceClosureContinuation
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	*n = ServiceClosureContinuation(next)
	return nil
}
