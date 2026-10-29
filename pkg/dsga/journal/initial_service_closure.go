package journal

import (
	"context"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// v12将“原首次开放未知”与“当前关闭未知/确认”作为两项独立事实。原
// 预算/命令/owner/回执永不回写，关闭不证明旧开放没有执行或旧连接排空。
// 不调用InitialBudgetFor：本函数位于Validate内部，避免递归校验模型。
func (model Model) validateInitialServiceClosure() error {
	c, h := model.ServiceClosure, model.InitialHeld
	if h == nil || h.Admission == nil || h.Admission.Phase != Unconfirmed || c.Reason != "initial_source_unconfirmed" || c.Receipt != nil {
		return fmt.Errorf("initial closure requires retained original first admission unknown")
	}
	var command budget.InitialAdmissionCommand
	if json.Unmarshal(h.Admission.Command, &command) != nil || *c.Original != command.Request || !c.Original.Open || c.Request.ClosesOver(*c.Original) != nil || c.Request.ExpectedCapacityGeneration != c.Original.ExpectedCapacityGeneration || c.Request.BudgetCommitmentID != c.Original.BudgetCommitmentID {
		return fmt.Errorf("initial closure changes original open, capacity or budget")
	}
	if c.Phase == Settled {
		selection := control.AdmissionCloseRequest{ProtocolVersion: 1, Original: *c.Original, Close: c.Request}
		if c.ClosureReceipt == nil || c.ClosureReceipt.ValidateFor(selection, *model.PGInstance, c.Query) != nil {
			return fmt.Errorf("initial closure lacks full original closed proof")
		}
	}
	return nil
}

// StartInitialServiceClosure只保存关闭写前WAL，不向PG派发。请求必须逐字
// 引用首次开放；同owner的关闭revision严格更高，允许原生关闭跨过未投递
// 的开放revision。旧owner迟到开放因此被水位拒绝，不能靠重发开放凑序列。
func (model Model) StartInitialServiceClosure(actor Owner, request control.AdmissionCloseRequest) (Model, error) {
	if err := model.Validate(); err != nil {
		return model, err
	}
	if model.ServiceClosure != nil || request.Validate() != nil {
		return model, fmt.Errorf("initial closure is already retained or invalid")
	}
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	digest, err := request.Close.ContextSHA256()
	if err != nil {
		return model, err
	}
	original := request.Original
	next.ProtocolVersion = 12
	next.ServiceClosure = &ServiceClosure{Origin: actor, StartedSequence: next.Sequence, Reason: "initial_source_unconfirmed", Request: request.Close, CommandSHA256: digest, Phase: Unconfirmed, Original: &original, RecordedSequence: 0, Query: false, Receipt: nil, Continuation: nil, ClosureReceipt: nil}
	return next, next.Validate()
}

func (model Model) RecordInitialServiceClosure(actor Owner, fact control.AdmissionClosure, query bool) (Model, error) {
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	c := next.ServiceClosure
	if c == nil || c.Original == nil || c.Continuation != nil || c.Phase != Unconfirmed || !query && c.Origin != actor {
		return model, fmt.Errorf("initial closure is not original unknown")
	}
	proof := fact
	proof.ClosedBy = *cloneAdmissionResponse(&fact.ClosedBy)
	c.Phase, c.Query, c.RecordedSequence, c.ClosureReceipt = Settled, query, next.Sequence, &proof
	return next, next.Validate()
}

// 只有原UID/RV的完整CAS确认才返回派发票据。任何丢回复/冲突均为零
// 票据；重建只查询WAL中的两份选择，不能由首次NotFound推导NoEffect。
func (store *Store) StartInitialServiceClosure(ctx context.Context, s *Snapshot, actor Owner, request control.AdmissionCloseRequest) (*Snapshot, *ServiceClosure, error) {
	if err := store.checked(s); err != nil {
		return nil, nil, err
	}
	if !s.Dispatchable() {
		return nil, nil, fmt.Errorf("initial closure requires original running VM")
	}
	next, err := s.model.StartInitialServiceClosure(actor, request)
	if err != nil {
		return nil, nil, err
	}
	result, err := store.apply(ctx, s, next)
	if err != nil {
		return nil, nil, err
	}
	return result, result.State().ServiceClosure, nil
}

func (store *Store) RecordInitialServiceClosure(ctx context.Context, s *Snapshot, actor Owner, fact control.AdmissionClosure, query bool) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	next, err := s.model.RecordInitialServiceClosure(actor, fact, query)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}
