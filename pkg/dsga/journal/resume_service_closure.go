package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// resumeAdmissionForClosure只解释已由Model.Validate核对的原步骤；不能在
// 此递归调用Validate。仅完整Finalize已永久持有、其余领域均Completed、
// 最终开放已Start的边界适用。未知PG/VM/Guest中间态不在此规则内。
func (model Model) resumeAdmissionForClosure() (control.AdmissionRequest, error) {
	var original control.AdmissionRequest
	op, held := model.Active, model.BudgetHeld
	if op == nil || op.Cleanup != nil || held == nil || held.Finalization.Observation == nil || len(op.Steps) == 0 {
		return original, fmt.Errorf("resume closure lacks original finalized active operation")
	}
	var f budget.Finalized
	if json.Unmarshal(held.Finalization.Observation.Data, &f) != nil || f.Request != op.Request {
		return original, fmt.Errorf("resume closure changed retained full budget")
	}
	for i, s := range op.Steps {
		if i != len(op.Steps)-1 {
			if s.Phase != Settled || s.Observation == nil || s.Observation.Result != Completed {
				return original, fmt.Errorf("resume closure cannot adopt unfinished prior domains")
			}
			continue
		}
		if s.Action != ResumeAdmission || s.Phase != Unconfirmed && s.Phase != Settled || s.Phase == Settled && (s.Observation == nil || s.Observation.Result != Completed) || json.Unmarshal(s.Command, &original) != nil || !original.Open || original.Instance != op.Request.Key.Instance || original.BudgetCommitmentID != op.Request.BudgetCommitmentID || original.ExpectedCapacityGeneration != f.Shared.Observed.CapacityGeneration || s.DispatchedBy == nil || original.IssuerEpoch != s.DispatchedBy.Epoch || s.TargetID != original.Instance.ComputeInstanceID {
			return original, fmt.Errorf("resume closure differs from original final admission")
		}
	}
	return original, nil
}

// 返回原请求的值副本，仅用于关闭选择，不授予原开放重放或新的预算许可。
func (model Model) ResumeAdmissionForClosure() (control.AdmissionRequest, error) {
	if err := model.Validate(); err != nil {
		return control.AdmissionRequest{}, err
	}
	return model.resumeAdmissionForClosure()
}

func operationClosureDigest(op *Operation) (string, error) {
	raw, err := json.Marshal(op)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (model Model) validateResumeServiceClosure() error {
	c := model.ServiceClosure
	original, err := model.resumeAdmissionForClosure()
	digest, hashErr := operationClosureDigest(model.Active)
	if err != nil || hashErr != nil || c.Reason != "resume_source_unconfirmed" || c.Receipt != nil || c.OperationSHA256 != digest || *c.Original != original || c.Request.ClosesOver(original) != nil || c.Request.ExpectedCapacityGeneration != original.ExpectedCapacityGeneration || c.Request.BudgetCommitmentID != original.BudgetCommitmentID {
		return fmt.Errorf("resume closure lost exact original operation, admission or budget")
	}
	if c.Phase == Settled {
		selection := control.AdmissionCloseRequest{ProtocolVersion: 1, Original: original, Close: c.Request}
		if c.ClosureReceipt == nil || c.ClosureReceipt.ValidateFor(selection, *model.PGInstance, c.Query) != nil {
			return fmt.Errorf("resume closure lacks full original closed proof")
		}
	}
	return nil
}

// v13只添加独立关闭，不自动取消/Finish原伸缩。原Active完整摘要将请求、
// 每一领域命令和回执一起固定；接管只改外层Owner，关闭后不能用普通
// Record/RequestAbort重写它。受控恢复须另有持久协议，不删除本WAL。
func (model Model) StartResumeServiceClosure(actor Owner, request control.AdmissionCloseRequest) (Model, error) {
	if err := model.Validate(); err != nil {
		return model, err
	}
	if model.ServiceClosure != nil || request.Validate() != nil {
		return model, fmt.Errorf("resume closure is already retained or invalid")
	}
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	digest, err := request.Close.ContextSHA256()
	if err != nil {
		return model, err
	}
	opDigest, err := operationClosureDigest(model.Active)
	if err != nil {
		return model, err
	}
	original := request.Original
	next.ProtocolVersion = 13
	next.ServiceClosure = &ServiceClosure{Origin: actor, StartedSequence: next.Sequence, Reason: "resume_source_unconfirmed", Request: request.Close, CommandSHA256: digest, Phase: Unconfirmed, Original: &original, RecordedSequence: 0, Query: false, Receipt: nil, Continuation: nil, ClosureReceipt: nil, OperationSHA256: opDigest}
	next.setClosureFormatVersion(next.ProtocolVersion)
	return next, next.Validate()
}

func (model Model) RecordResumeServiceClosure(actor Owner, fact control.AdmissionClosure, query bool) (Model, error) {
	return model.recordOriginalServiceClosure(actor, fact, query, "resume_source_unconfirmed")
}

// 原VM UID/RV写前确认之后才返回派发票据；丢回复/冲突返回nil票据，
// 原Active/预算仍保留，下一轮只查询已保存的完整原开放/关闭选择。
func (store *Store) StartResumeServiceClosure(ctx context.Context, s *Snapshot, actor Owner, request control.AdmissionCloseRequest) (*Snapshot, *ServiceClosure, error) {
	if err := store.checked(s); err != nil {
		return nil, nil, err
	}
	if !s.Dispatchable() {
		return nil, nil, fmt.Errorf("resume closure requires original running VM")
	}
	next, err := s.model.StartResumeServiceClosure(actor, request)
	if err != nil {
		return nil, nil, err
	}
	result, err := store.apply(ctx, s, next)
	if err != nil {
		return nil, nil, err
	}
	return result, result.State().ServiceClosure, nil
}

func (store *Store) RecordResumeServiceClosure(ctx context.Context, s *Snapshot, actor Owner, fact control.AdmissionClosure, query bool) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	next, err := s.model.RecordResumeServiceClosure(actor, fact, query)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}
