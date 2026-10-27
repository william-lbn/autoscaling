package admissionexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// ErrRecoverySelectionRequired表示原预算仍有效，但关闭状态的服务是否应重新
// 开放必须由可信策略决定。它是等待授权的状态，不是owner丢失或可自动重发的
// 网络错误；循环保留原Session与全部承诺，直到收到独立选择或被取消。
var ErrRecoverySelectionRequired = errors.New("unchanged abort reopening requires explicit trusted recovery selection")

func unchangedVM(op *journal.Operation) *journal.Step {
	for i := range op.Steps {
		if op.Steps[i].Action == journal.ResizeVM {
			return &op.Steps[i]
		}
	}
	return nil
}

// checkUnchangedBudget核旧完整预算而非被取消的新目标。原Guest/PG动作从未
// 派发仅是前提，仍要认证旧完成+新取消两端Runner回执、当前实际内存、原
// PG提交/成本、Root限额/出生和所有持续来源；新Host峰值不在这里提前释放。
func (e *Executor) checkUnchangedBudget(ctx context.Context, s *journal.Snapshot, f budget.Finalized, native vmguard.Command, cancelled vmexec.CancelledVM) (control.FenceResponse, control.SharedCostResponse, budget.RootAggregate, error) {
	var fence control.FenceResponse
	var shared control.SharedCostResponse
	var aggregate budget.RootAggregate
	fence, err := e.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return fence, shared, aggregate, err
	}
	if fence.Disposition != control.Fenced {
		return fence, shared, aggregate, fmt.Errorf("unchanged abort native owner fence differs")
	}
	shared, err = e.client.SharedCost(ctx)
	if err != nil {
		return fence, shared, aggregate, err
	}
	vm := s.VirtualMachine()
	if vm.Status.MemorySize == nil {
		return fence, shared, aggregate, fmt.Errorf("unchanged abort actual VM memory unavailable")
	}
	memory, exact := vm.Status.MemorySize.AsInt64()
	if !exact || memory <= 0 || shared.Observed.CapacityGeneration < fence.Observed.CapacityGeneration || f.ValidateFor(f.Request, shared, e.actor.Epoch, vm.Status.PodName, vm.Status.RestartCount, uint64(memory)) != nil {
		return fence, shared, aggregate, fmt.Errorf("unchanged abort original PG/VM full budget is no longer valid")
	}
	aggregate, err = e.checkRoot(ctx, s, f)
	if err != nil {
		return fence, shared, aggregate, err
	}
	claims, err := e.claims.ReadPrior(ctx, s)
	if err != nil {
		return fence, shared, aggregate, err
	}
	var final budget.FinalizationCommand
	if json.Unmarshal(s.State().BudgetHeld.Finalization.Command, &final) != nil || final.Validate() != nil || !reflect.DeepEqual(final.Finalized, f) || f.Sources == nil || f.Sources.Runner == nil {
		return fence, shared, aggregate, fmt.Errorf("unchanged abort lost original complete finalization")
	}
	joint, err := e.runner.CheckRetainedAfterCancelled(ctx, *f.Sources.Runner, f.Request, final.PreparedSHA256, final.GuestSHA256, f.Sources.HostClaim, native, cancelled.Sources.JournalSHA256, e.actor)
	if err != nil {
		return fence, shared, aggregate, err
	}
	if joint.Cancellation.Sources != cancelled.Sources || !reflect.DeepEqual(joint.Cancellation.Receipt, cancelled.Receipt) || joint.Cancellation.Actual != cancelled.Actual || joint.Prior.Birth.Identity.RunnerPodUID != claims.Host.RunnerPodUID || joint.Prior.Birth.Identity.NodeUID != claims.Host.NodeUID {
		return fence, shared, aggregate, fmt.Errorf("unchanged abort original VM cancellation or Host source changed")
	}
	if err := e.claims.RecheckPrior(ctx, s, claims); err != nil {
		return fence, shared, aggregate, err
	}
	if err := e.sameSnapshot(ctx, s); err != nil {
		return fence, shared, aggregate, err
	}
	return fence, shared, aggregate, nil
}

// advanceUnchangedAbort复用旧完整预算完成新Grow无容量效果的取消。它不重发
// PG/Guest/VM、不修改原目标，也不把原VM NoEffect扩大成整个操作清理。只有
// 独立Cleanup完整落账并再次复核后，Finish才恢复原预算关闭链供Host结清。
func (e *Executor) advanceUnchangedAbort(ctx context.Context, s *journal.Snapshot, replay *control.Key, allowNewOpening bool) (bool, pgexec.Progress, error) {
	m := s.State()
	f, err := m.UnchangedAbortBudget()
	if err != nil {
		return false, pgexec.ExternalStep, nil
	}
	op := m.Active
	if !s.Dispatchable() || m.Owner != e.actor || op.Request.Key.Instance != e.instance || e.resources == nil || e.claims == nil || e.runner == nil {
		return true, pgexec.AwaitingFact, fmt.Errorf("unchanged abort requires original live production budget domains")
	}
	vm := unchangedVM(op)
	var native vmguard.Command
	var cancelled vmexec.CancelledVM
	if vm == nil || vm.Observation == nil || json.Unmarshal(vm.Command, &native) != nil || native.Request != op.Request || vm.DispatchedBy == nil || native.Request.Key.IssuerEpoch != vm.DispatchedBy.Epoch || vm.TargetID != native.Identity.RunnerPodUID || json.Unmarshal(vm.Observation.Data, &cancelled) != nil || cancelled.ValidateFor(native, vm.CommandSHA256) != nil || native.BeforeBytes != f.Request.TargetVMMemoryBytes {
		return true, pgexec.AwaitingFact, fmt.Errorf("unchanged abort original VM NoEffect lacks complete native cancellation audit")
	}
	var command budget.UnchangedAbortCommand
	step := op.Cleanup
	dispatchEpoch := e.actor.Epoch
	if step != nil {
		if step.DispatchedBy == nil || step.TargetID != e.instance.ComputeInstanceID || json.Unmarshal(step.Command, &command) != nil || command.Request != op.Request || command.FinalizationSHA256 != m.BudgetHeld.Finalization.CommandSHA256 || command.CancelledVMSHA256 != vmguard.SourceDigest(vm.Observation.Data) || command.CancellationSources != cancelled.Sources || command.ValidateFor(f, step.DispatchedBy.Epoch) != nil {
			return true, pgexec.AwaitingFact, fmt.Errorf("unchanged abort cleanup changed original budget or cancelled VM")
		}
		dispatchEpoch = step.DispatchedBy.Epoch
	}
	if replay != nil && (step == nil || step.Phase != journal.Unconfirmed || step.DispatchedBy == nil || *step.DispatchedBy != e.actor || command.Admission == nil || *replay != op.Request.Key) {
		return true, pgexec.AwaitingFact, fmt.Errorf("unchanged abort replay cannot rebind original dispatcher or opening")
	}
	query := step != nil && replay == nil
	var response *control.AdmissionResponse
	if query && command.Admission != nil {
		fact, err := e.client.AdmissionStatus(ctx, *command.Admission)
		if err != nil || !fact.Confirms() {
			return true, pgexec.AwaitingFact, err
		}
		response = &fact
	}
	pending, closing := e.client.PendingAdmission()
	if e.client.Pending() != nil || closing != nil || pending != nil && (command.Admission == nil || *pending != *command.Admission) {
		return true, pgexec.AwaitingFact, resourceguard.ErrUnknown
	}
	_, shared, _, err := e.checkUnchangedBudget(ctx, s, f, native, cancelled)
	if err != nil {
		return true, pgexec.AwaitingFact, err
	}
	if step == nil {
		if !shared.Observed.AdmissionOpen && !allowNewOpening {
			// Grow自身没有Quiesce，当前关闭可能来自暂停/运维意图。
			// 普通取消轮询不能推测需要开放；必须有独立受信恢复选择。
			return true, pgexec.AwaitingFact, ErrRecoverySelectionRequired
		}
		command = budget.UnchangedAbortCommand{ProtocolVersion: 1, Request: op.Request, FinalizationSHA256: m.BudgetHeld.Finalization.CommandSHA256, CancelledVMSHA256: vmguard.SourceDigest(vm.Observation.Data), CancellationSources: cancelled.Sources, Admission: nil}
		if !shared.Observed.AdmissionOpen {
			if shared.Observed.AdmissionRevision == control.Uint64(math.MaxUint64) {
				return true, pgexec.AwaitingFact, fmt.Errorf("unchanged abort original admission revision exhausted")
			}
			command.Admission = &control.AdmissionRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch, Revision: shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: shared.Observed.CapacityGeneration, Open: true, BudgetCommitmentID: f.Request.BudgetCommitmentID}
		}
		if err := command.ValidateFor(f, e.actor.Epoch); err != nil {
			return true, pgexec.AwaitingFact, err
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
		query = command.Admission == nil
	}
	if !query {
		if _, _, _, err := e.checkUnchangedBudget(ctx, s, f, native, cancelled); err != nil {
			return true, pgexec.AwaitingFact, err
		}
		fact, err := e.client.SetAdmission(ctx, *command.Admission)
		if err != nil || !fact.Confirms() {
			return true, pgexec.AwaitingFact, err
		}
		response = &fact
	}
	fence, shared, aggregate, err := e.checkUnchangedBudget(ctx, s, f, native, cancelled)
	if err != nil {
		return true, pgexec.AwaitingFact, err
	}
	receipt := budget.UnchangedAbortReceipt{ProtocolVersion: 1, Command: command, Query: query, Fence: fence, Shared: shared, Response: response, PGAggregate: aggregate, ResourceEpoch: e.actor.Epoch}
	if err := receipt.ValidateFor(command, f, dispatchEpoch, e.actor.Epoch); err != nil {
		return true, pgexec.AwaitingFact, err
	}
	if step.Phase == journal.Settled {
		if err := s.State().CheckUnchangedAbort(); err != nil {
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

// RecoverUnchangedAdmission只供可信协调器在确认仍应提供服务后显式调用，
// 选择仅带原Key。它不覆盖已有Cleanup/未知命令；真正预算/来源/owner全核完
// 后才写前登记开放。普通Advance、not-found和超时永不产生这种授权。
func (e *Executor) RecoverUnchangedAdmission(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	s, err := e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if key.Validate() != nil || m.Owner != e.actor || m.Active == nil || m.Active.Request.Key != key || m.Active.Cleanup != nil {
		return pgexec.AwaitingFact, fmt.Errorf("explicit unchanged admission recovery changed original key or has existing cleanup")
	}
	handled, progress, err := e.advanceUnchangedAbort(ctx, s, nil, true)
	if !handled {
		return pgexec.AwaitingFact, fmt.Errorf("no unchanged original budget eligible for admission recovery")
	}
	return progress, err
}
