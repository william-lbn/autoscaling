package admissionexec

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// ReplayCommittedAbortOriginal是独立的受信显式选择，只重放原Cleanup开放。
// 原计划/PG/VM参数永不重发。普通轮询、not-found和超时不调用它；接管只能
// 查询旧Cleanup，不能把旧setter换epoch或revision来绕过未知。
func (e *Executor) ReplayCommittedAbortOriginal(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	s, err := e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if key.Validate() != nil || s.State().Owner != e.actor || s.State().Active == nil || s.State().Active.Request.Key != key {
		return pgexec.AwaitingFact, fmt.Errorf("committed abort replay changed original key or owner")
	}
	handled, progress, err := e.advanceCommittedAbort(ctx, s, &key)
	if !handled {
		return pgexec.AwaitingFact, fmt.Errorf("no original complete-budget abort cleanup to replay")
	}
	return progress, err
}

func completeCapacity(op *journal.Operation) (*journal.Step, *journal.Step, bool) {
	var final, resume *journal.Step
	for i := range op.Steps {
		step := &op.Steps[i]
		if step.Action == journal.ResumeAdmission {
			resume = step
			continue
		}
		if step.Phase != journal.Settled || step.Observation == nil || step.Observation.Result != journal.Completed {
			return nil, nil, false
		}
		if step.Action == journal.FinalizeBudget {
			final = step
		}
	}
	return final, resume, final != nil && resume != nil
}

// checkCommittedBudget复用正常开放的所有真实来源检查。没有注入的“已完成”
// 回调，也不因取消放宽Root/Birth/Host/Outside/Runner或实际VM目标验证；不持
// 本地Mutex跨网络，最后仍以同VM UID/RV CAS线性化。该函数不产生容量动作。
func (e *Executor) checkCommittedBudget(ctx context.Context, s *journal.Snapshot, f budget.Finalized) (control.FenceResponse, control.SharedCostResponse, budget.RootAggregate, error) {
	var fence control.FenceResponse
	var shared control.SharedCostResponse
	var aggregate budget.RootAggregate
	fence, err := e.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return fence, shared, aggregate, err
	}
	if fence.Disposition != control.Fenced {
		return fence, shared, aggregate, fmt.Errorf("committed abort native owner fence changed")
	}
	shared, err = e.client.SharedCost(ctx)
	if err != nil {
		return fence, shared, aggregate, err
	}
	vm := s.VirtualMachine()
	if vm.Status.MemorySize == nil {
		return fence, shared, aggregate, fmt.Errorf("committed abort actual VM memory unavailable")
	}
	memory, exact := vm.Status.MemorySize.AsInt64()
	if !exact || memory <= 0 || shared.Observed.CapacityGeneration < fence.Observed.CapacityGeneration || f.ValidateFor(s.State().Active.Request, shared, e.actor.Epoch, vm.Status.PodName, vm.Status.RestartCount, uint64(memory)) != nil {
		return fence, shared, aggregate, fmt.Errorf("committed abort no longer has original full PG/VM budget")
	}
	aggregate, err = e.checkRoot(ctx, s, f)
	if err != nil {
		return fence, shared, aggregate, err
	}
	if _, _, err := e.checkClaims(ctx, s, f, aggregate); err != nil {
		return fence, shared, aggregate, err
	}
	if err := e.sameSnapshot(ctx, s); err != nil {
		return fence, shared, aggregate, err
	}
	return fence, shared, aggregate, nil
}

// advanceCommittedAbort处理真实容量已完成、当前完整预算已落账的取消恢复。
// 原Resume未知先交正常查询对账；其余部分效果不进入此路径。Cleanup保存独立
// 开放原命令；未知后只读，显式重放限原派发owner。落账与Finish分轮执行，
// 回复丢失均重读原WAL，不把业务取消伪装成容量回滚或NoEffect。
func (e *Executor) advanceCommittedAbort(ctx context.Context, s *journal.Snapshot, replay *control.Key) (bool, pgexec.Progress, error) {
	m := s.State()
	op := m.Active
	if op == nil || !op.AbortRequested {
		return false, pgexec.ExternalStep, nil
	}
	final, resume, eligible := completeCapacity(op)
	if !eligible {
		return false, pgexec.ExternalStep, nil
	}
	if op.Cleanup == nil && resume.Phase == journal.Unconfirmed {
		// 原开放可能已生效，必须先查询/完整落账，不能用新Cleanup掩盖它。
		return false, pgexec.ExternalStep, nil
	}
	if m.Owner != e.actor || !s.Dispatchable() || op.Request.Key.Instance != e.instance || e.resources == nil || e.claims == nil || e.runner == nil {
		return true, pgexec.AwaitingFact, fmt.Errorf("committed abort requires original live production budget domains")
	}
	if m.ProtocolVersion == 2 && m.BudgetHeld == nil && op.Cleanup == nil && replay == nil {
		finalizer, err := budgetexec.New(e.store, e.client, e.resources, e.actor, e.claims, e.runner)
		if err != nil {
			return true, pgexec.AwaitingFact, err
		}
		progress, err := finalizer.RetainOriginal(ctx)
		return true, progress, err
	}
	f, err := m.BudgetFor(op.Request)
	if err != nil {
		return true, pgexec.AwaitingFact, err
	}
	var command budget.CommittedAbortCommand
	step := op.Cleanup
	dispatchEpoch := e.actor.Epoch
	if step != nil {
		if step.DispatchedBy == nil || step.TargetID != e.instance.ComputeInstanceID || json.Unmarshal(step.Command, &command) != nil || command.FinalizationSHA256 != final.CommandSHA256 || command.ValidateFor(f, step.DispatchedBy.Epoch) != nil {
			return true, pgexec.AwaitingFact, fmt.Errorf("original committed abort cleanup command changed")
		}
		dispatchEpoch = step.DispatchedBy.Epoch
	}
	if replay != nil && (step == nil || step.Phase != journal.Unconfirmed || step.DispatchedBy == nil || *step.DispatchedBy != e.actor || !command.MaySend || *replay != op.Request.Key) {
		return true, pgexec.AwaitingFact, fmt.Errorf("committed abort replay cannot rebind original dispatcher or command")
	}
	query := step != nil && replay == nil
	var fact control.AdmissionResponse
	if query {
		// 已发送未知时先查原命令，缺Root/Host不能把它藏成没有发生。
		fact, err = e.client.AdmissionStatus(ctx, command.Admission)
		if err != nil || !fact.Confirms() {
			return true, pgexec.AwaitingFact, err
		}
	}
	pending, closing := e.client.PendingAdmission()
	if e.client.Pending() != nil || closing != nil || (pending != nil && (step == nil || *pending != command.Admission)) {
		return true, pgexec.AwaitingFact, resourceguard.ErrUnknown
	}
	_, shared, _, err := e.checkCommittedBudget(ctx, s, f)
	if err != nil {
		return true, pgexec.AwaitingFact, err
	}
	if step == nil {
		var originalAdmission control.AdmissionRequest
		command = budget.CommittedAbortCommand{ProtocolVersion: 1, Request: op.Request, FinalizationSHA256: final.CommandSHA256, Admission: originalAdmission, MaySend: resume.Phase == journal.Planned}
		if command.MaySend {
			if shared.Observed.AdmissionRevision == control.Uint64(math.MaxUint64) {
				return true, pgexec.AwaitingFact, fmt.Errorf("committed abort admission revision exhausted")
			}
			command.Admission = control.AdmissionRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch, Revision: shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: shared.Observed.CapacityGeneration, Open: true, BudgetCommitmentID: op.Request.BudgetCommitmentID}
		} else if resume.Phase != journal.Settled || resume.Observation == nil || resume.Observation.Result != journal.Completed || json.Unmarshal(resume.Command, &command.Admission) != nil {
			return true, pgexec.AwaitingFact, fmt.Errorf("committed abort original admission has not settled")
		}
		if command.ValidateFor(f, e.actor.Epoch) != nil {
			return true, pgexec.AwaitingFact, fmt.Errorf("committed abort admission differs from original complete budget")
		}
		raw, err := json.Marshal(command)
		if err != nil {
			return true, pgexec.AwaitingFact, err
		}
		var dispatch *journal.Dispatch
		s, dispatch, err = e.store.StartCleanup(ctx, s, e.actor, op.Request.Key, e.instance.ComputeInstanceID, raw)
		if err != nil {
			return true, pgexec.AwaitingFact, err
		}
		step = &dispatch.Step
		query = !command.MaySend
	}
	if !query {
		// 写前确认/显式选择之后仍重新核所有真实来源，不能在CAS窗口换源。
		if _, _, _, err := e.checkCommittedBudget(ctx, s, f); err != nil {
			return true, pgexec.AwaitingFact, err
		}
		fact, err = e.client.SetAdmission(ctx, command.Admission)
	} else if op.Cleanup == nil {
		fact, err = e.client.AdmissionStatus(ctx, command.Admission)
	}
	if err != nil || !fact.Confirms() {
		return true, pgexec.AwaitingFact, err
	}
	fence, shared, aggregate, err := e.checkCommittedBudget(ctx, s, f)
	if err != nil {
		return true, pgexec.AwaitingFact, err
	}
	// 保存写后读回，拒绝已被另一门禁代次覆盖的旧开放回执。单纯曾经
	// Applied不能证明此刻仍准入；失败保留原Cleanup，下一轮只查询。
	if !shared.Observed.AdmissionOpen || shared.Observed.AdmissionRevision != fact.Observed.AdmissionRevision {
		return true, pgexec.AwaitingFact, fmt.Errorf("committed abort admission was superseded after original receipt")
	}
	receipt := budget.CommittedAbortReceipt{ProtocolVersion: 1, Command: command, Query: query, Fence: fence, Shared: shared, Response: fact, PGAggregate: aggregate, ResourceEpoch: e.actor.Epoch}
	if err := receipt.ValidateFor(command, f, dispatchEpoch, e.actor.Epoch); err != nil {
		return true, pgexec.AwaitingFact, err
	}
	if step.Phase == journal.Settled {
		// 原审计不可用当前读取覆盖。完整原文校验后才关闭；CAS丢回复重读
		// 已关闭历史，不能通过重新创建Client或清掉SDK未知结束操作。
		if err := s.State().CheckCommittedAbort(); err != nil {
			return true, pgexec.AwaitingFact, err
		}
		_, err = e.store.Finish(ctx, s, e.actor, op.Request.Key)
	} else {
		raw, marshalErr := json.Marshal(receipt)
		if marshalErr != nil {
			return true, pgexec.AwaitingFact, marshalErr
		}
		_, err = e.store.Record(ctx, s, e.actor, op.Request.Key, journal.Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: journal.AbortReconcile, Result: journal.Completed, SourceID: e.instance.ComputeInstanceID, Data: raw})
	}
	if err != nil {
		return true, pgexec.AwaitingFact, err
	}
	return true, pgexec.Recorded, nil
}
