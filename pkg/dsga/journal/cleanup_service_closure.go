package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// CleanupServiceBoundary只描述原Cleanup已写前固定的服务边界，不是预算已
// 完成或可重开的许可。实际容量取消、完整目标取消、旧容量取消分别核原合同；
// 尤其旧容量取消必须引用旧预算ID，不能借新Active目标偷换仍在服务的承诺。
// Admission为nil表示原Cleanup只读，仍可能有更早已开放的连接准入需要关闭。
type CleanupServiceBoundary struct {
	Admission          *control.AdmissionRequest
	CapacityGeneration control.Uint64
	BudgetCommitmentID string
}

// 位于Model.Validate内部，不能调用公开的BudgetFor/CheckReconciledPlan等
// 再次Validate。只核持久结构与原文摘要；PG/Root/Host/Runner认证由生产执行器
// 独立完成。任何原资源未知均不能借Cleanup消失，失败不修改传入Model。
func (model Model) cleanupServiceBoundary() (CleanupServiceBoundary, error) {
	var none CleanupServiceBoundary
	op := model.Active
	if op == nil || !op.AbortRequested || op.Cleanup == nil || op.Cleanup.DispatchedBy == nil || op.Cleanup.TargetID != op.Request.Key.Instance.ComputeInstanceID {
		return none, fmt.Errorf("cleanup closure lacks original dispatched cancellation")
	}
	s := op.Cleanup
	if s.Phase != Unconfirmed && (s.Phase != Settled || s.Observation == nil || s.Observation.Result != Completed) {
		return none, fmt.Errorf("cleanup closure cannot adopt rejected cleanup")
	}
	var actual budget.ReconciledAbortCommand
	if json.Unmarshal(s.Command, &actual) == nil {
		if actual.ValidateFor(s.DispatchedBy.Epoch) != nil || model.reconciledPattern(*op, actual) != nil || s.Phase == Settled && model.validateReconciledAbort(*op) != nil {
			return none, fmt.Errorf("cleanup closure changed original actual-capacity budget")
		}
		return CleanupServiceBoundary{Admission: actual.Admission, CapacityGeneration: actual.Capacity.CapacityGeneration, BudgetCommitmentID: actual.Capacity.Request.BudgetCommitmentID}, nil
	}
	var committed budget.CommittedAbortCommand
	if json.Unmarshal(s.Command, &committed) == nil {
		if model.BudgetHeld == nil {
			return none, fmt.Errorf("cleanup closure lost original complete budget")
		}
		var final, resume *Step
		for i := range op.Steps {
			step := &op.Steps[i]
			if step.Action == ResumeAdmission {
				resume = step
				continue
			}
			if !successful(*step) {
				return none, fmt.Errorf("complete cleanup closure crosses unfinished resource effect")
			}
			if step.Action == FinalizeBudget {
				final = step
			}
		}
		if final == nil || resume == nil || !reflect.DeepEqual(*final, model.BudgetHeld.Finalization) {
			return none, fmt.Errorf("cleanup closure changed atomically retained finalization")
		}
		f, err := finalizedBudget(*op, *final)
		if err != nil || committed.ValidateFor(f, s.DispatchedBy.Epoch) != nil || committed.FinalizationSHA256 != final.CommandSHA256 {
			return none, fmt.Errorf("cleanup closure changed complete cleanup command")
		}
		if resume.Phase == Planned {
			if !committed.MaySend {
				return none, fmt.Errorf("cleanup closure invented prior final admission")
			}
		} else {
			var original control.AdmissionRequest
			if !successful(*resume) || committed.MaySend || json.Unmarshal(resume.Command, &original) != nil || original != committed.Admission {
				return none, fmt.Errorf("cleanup closure replaced original final admission")
			}
		}
		if s.Phase == Settled && model.validateCommittedAbort(*op) != nil {
			return none, fmt.Errorf("cleanup closure lost complete original cleanup receipt")
		}
		return CleanupServiceBoundary{Admission: &committed.Admission, CapacityGeneration: f.Shared.Observed.CapacityGeneration, BudgetCommitmentID: f.Request.BudgetCommitmentID}, nil
	}
	var unchanged budget.UnchangedAbortCommand
	f, err := model.unchangedAbortBudget(*op)
	if err != nil || json.Unmarshal(s.Command, &unchanged) != nil || unchanged.Request != op.Request || unchanged.ValidateFor(f, s.DispatchedBy.Epoch) != nil || unchanged.FinalizationSHA256 != model.BudgetHeld.Finalization.CommandSHA256 {
		return none, fmt.Errorf("cleanup closure has no original unchanged-budget command")
	}
	vm, ok := unchangedCapacity(*op)
	if !ok || unchanged.CancelledVMSHA256 != rawDigest(vm.Observation.Data) || unchanged.CancellationSources.JournalSHA256 != vm.CommandSHA256 || s.Phase == Settled && model.validateUnchangedAbort(*op) != nil {
		return none, fmt.Errorf("cleanup closure lost original cancelled VM boundary")
	}
	return CleanupServiceBoundary{Admission: unchanged.Admission, CapacityGeneration: f.Shared.Observed.CapacityGeneration, BudgetCommitmentID: f.Request.BudgetCommitmentID}, nil
}

// 返回重新解码的值；调用者不能通过指针改持久原文。无网络副作用，也不
// 允许修改原Cleanup/原开放；所有资源前驱必须已明确，容量严格固定这一代。
func (model Model) CleanupServiceForClosure() (CleanupServiceBoundary, error) {
	if err := model.Validate(); err != nil {
		return CleanupServiceBoundary{}, err
	}
	return model.cleanupServiceBoundary()
}

func (model Model) validateCleanupServiceClosure() error {
	c := model.ServiceClosure
	b, err := model.cleanupServiceBoundary()
	digest, hashErr := operationClosureDigest(model.Active)
	if err != nil || hashErr != nil || c.Reason != "cleanup_source_unconfirmed" || c.OperationSHA256 != digest || c.Request.ExpectedCapacityGeneration != b.CapacityGeneration || c.Request.BudgetCommitmentID != b.BudgetCommitmentID || (b.Admission == nil) != (c.Original == nil) || b.Admission != nil && (*b.Admission != *c.Original || c.Request.ClosesOver(*b.Admission) != nil) {
		return fmt.Errorf("cleanup closure changed original operation, capacity or service budget")
	}
	if c.Original == nil {
		if c.ClosureReceipt != nil {
			return fmt.Errorf("read-only cleanup closure invented original setter")
		}
	} else if c.Receipt != nil || c.Phase == Settled && (c.ClosureReceipt == nil || c.ClosureReceipt.ValidateFor(control.AdmissionCloseRequest{ProtocolVersion: 1, Original: *c.Original, Close: c.Request}, *model.PGInstance, c.Query) != nil) {
		return fmt.Errorf("cleanup closure lost complete native original-close proof")
	}
	// 原Cleanup之前资源效果已明确，没有可继续提交的Resize。旧回执即使
	// 可在更高代次查询，也不能用来结清本边界的关闭或另一个生命周期。
	if c.Receipt != nil && c.Receipt.Observed.CapacityGeneration != b.CapacityGeneration || c.ClosureReceipt != nil && c.ClosureReceipt.ClosedBy.Observed.CapacityGeneration != b.CapacityGeneration || c.Continuation != nil && c.Continuation.Receipt != nil && c.Continuation.Receipt.ClosedBy.Observed.CapacityGeneration != b.CapacityGeneration {
		return fmt.Errorf("cleanup closure proof changed fixed original capacity")
	}
	return nil
}

// v18将完整原Active（包括Cleanup未知/完成和旧永久头）固定在独立关闭
// WAL中。关闭不生成Cleanup成功、Finish、Host结清或容量预算。普通Record/
// Abort/Finish改变原Active会因摘要失败；恢复必须走独立受控协议。
func (model Model) StartCleanupServiceClosure(actor Owner, request control.AdmissionRequest) (Model, error) {
	if err := model.Validate(); err != nil {
		return model, err
	}
	if model.ServiceClosure != nil {
		return model, fmt.Errorf("cleanup closure already retained")
	}
	b, err := model.cleanupServiceBoundary()
	if err != nil {
		return model, err
	}
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	digest, err := request.ContextSHA256()
	if err != nil {
		return model, err
	}
	opDigest, err := operationClosureDigest(model.Active)
	if err != nil {
		return model, err
	}
	next.ProtocolVersion = 18
	next.ServiceClosure = &ServiceClosure{Origin: actor, StartedSequence: next.Sequence, Reason: "cleanup_source_unconfirmed", Request: request, CommandSHA256: digest, Phase: Unconfirmed, RecordedSequence: 0, Query: false, Receipt: nil, Continuation: nil, Original: b.Admission, ClosureReceipt: nil, OperationSHA256: opDigest}
	return next, next.Validate()
}

// 只有原VM UID/RV写前CAS完整确认才返回票据；丢回复不能派发关闭，后继
// 查询或显式重放同一WAL。网络期间不持本地Mutex，Session仍由调用者认证。
func (store *Store) StartCleanupServiceClosure(ctx context.Context, s *Snapshot, actor Owner, request control.AdmissionRequest) (*Snapshot, *ServiceClosure, error) {
	if err := store.checked(s); err != nil {
		return nil, nil, err
	}
	if !s.Dispatchable() {
		return nil, nil, fmt.Errorf("cleanup closure requires original running VM")
	}
	next, err := s.model.StartCleanupServiceClosure(actor, request)
	if err != nil {
		return nil, nil, err
	}
	result, err := store.apply(ctx, s, next)
	if err != nil {
		return nil, nil, err
	}
	return result, result.State().ServiceClosure, nil
}

func (store *Store) RecordCleanupServiceClosure(ctx context.Context, s *Snapshot, actor Owner, fact control.AdmissionClosure, query bool) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	next, err := s.model.recordOriginalServiceClosure(actor, fact, query, "cleanup_source_unconfirmed")
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}
