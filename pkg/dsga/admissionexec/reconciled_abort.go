package admissionexec

import (
	"context"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 原VM已派发必须从原完整命令和外层SHA读回；未派发只能借仍被永久预算
// 持有、且当前原State仍确认的旧来源。当前数值相同不替代原回执，不写QMP。
func (e *Executor) reconciledNative(ctx context.Context, m journal.Model) (budget.NativeCapacity, error) {
	var none budget.NativeCapacity
	vm := unchangedVM(m.Active)
	if vm == nil {
		return none, fmt.Errorf("actual recovery lost original VM step")
	}
	if vm.Phase != journal.Planned {
		var command vmguard.Command
		if vm.Phase != journal.Settled || vm.Observation == nil || vm.DispatchedBy == nil || json.Unmarshal(vm.Command, &command) != nil || command.Validate() != nil || command.Request != m.Active.Request || command.Request.Key.IssuerEpoch != vm.DispatchedBy.Epoch || vm.TargetID != command.Identity.RunnerPodUID {
			return none, fmt.Errorf("actual recovery original VM remains unconfirmed or changed")
		}
		source, _, err := e.runner.CapacityForOriginal(ctx, command, vm.CommandSHA256, e.actor)
		return source, err
	}
	if m.CapacityHeld != nil {
		var saved budget.ReconciledAbortCommand
		if m.CapacityHeld.Cleanup.DispatchedBy == nil || json.Unmarshal(m.CapacityHeld.Cleanup.Command, &saved) != nil || saved.ValidateFor(m.CapacityHeld.Cleanup.DispatchedBy.Epoch) != nil {
			return none, fmt.Errorf("actual recovery lost retained native capacity")
		}
		_, err := e.runner.CheckCapacity(ctx, saved.Capacity.VM, e.actor)
		return saved.Capacity.VM, err
	}
	if m.BudgetHeld != nil {
		var saved budget.FinalizationCommand
		if json.Unmarshal(m.BudgetHeld.Finalization.Command, &saved) != nil || saved.Validate() != nil || saved.Finalized.Sources == nil || saved.Finalized.Sources.Runner == nil {
			return none, fmt.Errorf("actual recovery lost retained complete VM sources")
		}
		f := saved.Finalized
		proof, err := e.runner.CheckRetained(ctx, *f.Sources.Runner, f.Request, saved.PreparedSHA256, saved.GuestSHA256, f.Sources.HostClaim, e.actor)
		if err != nil {
			return none, err
		}
		return vmguard.CapacitySource(proof)
	}
	return none, fmt.Errorf("undispatched VM has no retained authenticated capacity source")
}

// 每次均核原Root身份、floor、持久约束与Postmaster出生；不降低任何限额。
// SDK未知须由原资源执行器Observe解决，不能用当前View清除另一原命令。
func (e *Executor) reconciledRoot(ctx context.Context, s *journal.Snapshot) (budget.RootAggregate, control.Uint64, error) {
	var none budget.RootAggregate
	id, epoch := e.resources.Binding()
	if s.State().ResourceIdentity == nil || *s.State().ResourceIdentity != id || epoch != e.actor.Epoch {
		return none, 0, fmt.Errorf("actual recovery changed original resource binding")
	}
	if e.resources.Pending() != nil {
		return none, 0, resourceguard.ErrUnknown
	}
	view, err := e.resources.InspectState(ctx)
	if err != nil {
		return none, 0, err
	}
	if view.IssuerFloorEpoch != e.actor.Epoch || view.Actual.GuestMemTotalBytes == 0 {
		return none, 0, fmt.Errorf("actual recovery resource floor or usable memory changed")
	}
	aggregate, err := budget.RootAggregateFromView(view)
	if err != nil {
		return none, 0, err
	}
	process, err := e.resources.InspectProcess(ctx)
	if err != nil {
		return none, 0, err
	}
	if process != aggregate.Birth {
		return none, 0, fmt.Errorf("actual recovery original Postmaster birth changed")
	}
	return aggregate, view.Actual.GuestMemTotalBytes, nil
}

// 读取预算不会产生原PG/Guest/VM反向动作。新命令由认证来源构造；已有命令
// 逐字保持，即使当前成本下降也不改Root摘要/来源/代次或重算成另一份预算。
// 不持本地锁跨网络；所有读者围绕同VM RV重查，最终Start/Record/Finish CAS。
func (e *Executor) checkReconciled(ctx context.Context, s *journal.Snapshot, p budget.Prepared, existing *budget.ReconciledCapacity) (budget.ReconciledCapacity, control.FenceResponse, control.SharedCostResponse, budget.RootAggregate, error) {
	var capacity budget.ReconciledCapacity
	var fence control.FenceResponse
	var shared control.SharedCostResponse
	var root budget.RootAggregate
	fence, err := e.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return capacity, fence, shared, root, err
	}
	if fence.Disposition != control.Fenced {
		return capacity, fence, shared, root, fmt.Errorf("actual recovery native issuer floor changed")
	}
	shared, err = e.client.SharedCost(ctx)
	if err != nil {
		return capacity, fence, shared, root, err
	}
	root, usable, err := e.reconciledRoot(ctx, s)
	if err != nil {
		return capacity, fence, shared, root, err
	}
	if existing == nil {
		source, sourceErr := e.reconciledNative(ctx, s.State())
		if sourceErr != nil {
			return capacity, fence, shared, root, sourceErr
		}
		outside, selectErr := reconciledOutside(s.State(), p, source.MemoryBytes())
		if selectErr != nil {
			return capacity, fence, shared, root, selectErr
		}
		out, host, hostRef, readErr := e.claims.ReconcileReservations(ctx, s, p, outside)
		if readErr != nil {
			return capacity, fence, shared, root, readErr
		}
		capacity, err = budget.Reconcile(budget.ReconcileInputs{Prepared: p, Shared: shared, Root: root, Outside: out, OutsideClaim: outside, Host: host, HostClaim: hostRef, VM: source, GuestUsableMemoryBytes: usable, IssuerEpoch: e.actor.Epoch})
		if err != nil {
			return capacity, fence, shared, root, err
		}
	} else {
		capacity = *existing
	}
	hash, err := budget.RootDigest(root)
	vm := s.VirtualMachine()
	if err != nil || hash != capacity.RootSHA256 || root.Bound != capacity.PGAggregate || usable < capacity.GuestUsableMemoryBytes || vm.Status.MemorySize == nil || vm.Status.PodName != capacity.RunnerPodName || vm.Status.RestartCount != capacity.RunnerRestartCount {
		return capacity, fence, shared, root, fmt.Errorf("actual recovery original Root or Runner changed")
	}
	memory, exact := vm.Status.MemorySize.AsInt64()
	if !exact || memory <= 0 || shared.Observed.CapacityGeneration < fence.Observed.CapacityGeneration || capacity.ValidateCurrent(shared, e.actor.Epoch, control.Uint64(memory)) != nil {
		return capacity, fence, shared, root, fmt.Errorf("actual recovery current PG or VM capacity differs")
	}
	claims, err := e.claims.ReadReconciled(ctx, s, capacity)
	if err != nil {
		return capacity, fence, shared, root, err
	}
	native, err := e.runner.CheckCapacity(ctx, capacity.VM, e.actor)
	if err != nil {
		return capacity, fence, shared, root, err
	}
	if native.Birth.Identity.RunnerPodUID != claims.Host.RunnerPodUID || native.Birth.Identity.NodeUID != claims.Host.NodeUID {
		return capacity, fence, shared, root, fmt.Errorf("actual recovery native VM and Host domains differ")
	}
	if err := e.claims.RecheckReconciled(ctx, s, capacity, claims); err != nil {
		return capacity, fence, shared, root, err
	}
	if err := e.sameSnapshot(ctx, s); err != nil {
		return capacity, fence, shared, root, err
	}
	return capacity, fence, shared, root, nil
}

// advanceReconciledAbort在全部原容量效果已知后恢复实际预算。写前固定独立
// Cleanup，CAS丢回复不能发送；未知开放只查询，显式原重放仍限原派发者。
// 完整预算落账和Finish分轮完成；任何失败保留原意图/峰值，不自动反向伸缩。
func (e *Executor) advanceReconciledAbort(ctx context.Context, s *journal.Snapshot, replay *control.Key, selected bool) (bool, pgexec.Progress, error) {
	m := s.State()
	op := m.Active
	if !reconciledEligible(op) {
		return false, pgexec.ExternalStep, nil
	}
	if !s.Dispatchable() || m.Owner != e.actor || op.Request.Key.Instance != e.instance || m.ResourceIdentity == nil || e.resources == nil || e.claims == nil || e.runner == nil {
		return true, pgexec.AwaitingFact, fmt.Errorf("actual recovery requires original live production domains")
	}
	var prepared budget.Prepared
	var preparedSHA string
	for _, step := range op.Steps {
		if step.Action == journal.PrepareBudget && step.Observation != nil {
			if json.Unmarshal(step.Observation.Data, &prepared) != nil || prepared.ValidateFor(op.Request, *m.ResourceIdentity) != nil {
				return true, pgexec.AwaitingFact, fmt.Errorf("actual recovery lost complete original Prepare")
			}
			preparedSHA = vmguard.SourceDigest(step.Observation.Data)
		}
	}
	step := op.Cleanup
	var command budget.ReconciledAbortCommand
	dispatchEpoch := e.actor.Epoch
	if step != nil {
		if step.DispatchedBy == nil || step.TargetID != e.instance.ComputeInstanceID || json.Unmarshal(step.Command, &command) != nil || command.PreparedSHA256 != preparedSHA || command.ValidateFor(step.DispatchedBy.Epoch) != nil || m.CheckReconciledPlan(command) != nil {
			return true, pgexec.AwaitingFact, fmt.Errorf("actual recovery original Cleanup command changed")
		}
		dispatchEpoch = step.DispatchedBy.Epoch
	}
	if replay != nil && (step == nil || step.Phase != journal.Unconfirmed || step.DispatchedBy == nil || *step.DispatchedBy != e.actor || command.Admission == nil || *replay != op.Request.Key) {
		return true, pgexec.AwaitingFact, fmt.Errorf("actual recovery replay cannot rebind original dispatcher or opening")
	}
	query := step != nil && replay == nil
	var response *control.AdmissionResponse
	if query && command.Admission != nil {
		// 来源暂缺也不能隐藏原开放Unknown；先查原命令，再判断能否落账。
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
	if step == nil {
		capacity, _, shared, _, err := e.checkReconciled(ctx, s, prepared, nil)
		if err != nil {
			return true, pgexec.AwaitingFact, err
		}
		opening, err := reconciledOpening(op, prepared, capacity, shared, e.actor.Epoch, selected)
		if err != nil {
			return true, pgexec.AwaitingFact, err
		}
		command = budget.ReconciledAbortCommand{ProtocolVersion: 1, Capacity: capacity, PreparedSHA256: preparedSHA, Admission: opening}
		if command.ValidateFor(e.actor.Epoch) != nil || m.CheckReconciledPlan(command) != nil {
			return true, pgexec.AwaitingFact, fmt.Errorf("actual recovery budget contradicts original WAL")
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
		if _, _, _, _, err := e.checkReconciled(ctx, s, prepared, &command.Capacity); err != nil {
			return true, pgexec.AwaitingFact, err
		}
		fact, err := e.client.SetAdmission(ctx, *command.Admission)
		if err != nil || !fact.Confirms() {
			return true, pgexec.AwaitingFact, err
		}
		response = &fact
	}
	_, fence, shared, aggregate, err := e.checkReconciled(ctx, s, prepared, &command.Capacity)
	if err != nil {
		return true, pgexec.AwaitingFact, err
	}
	receipt := budget.ReconciledAbortReceipt{ProtocolVersion: 1, Command: command, Query: query, Fence: fence, Shared: shared, Response: response, PGAggregate: aggregate, ResourceEpoch: e.actor.Epoch}
	if err := receipt.ValidateFor(command, dispatchEpoch, e.actor.Epoch); err != nil {
		return true, pgexec.AwaitingFact, err
	}
	if step.Phase == journal.Settled {
		// 永久实际头必须由原Record原子保存；不能用当前读回替换旧原审计。
		if _, err := s.State().CapacityFor(op.Request); err != nil {
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

// RecoverCapacityAdmission是可信协调器对仍应提供服务的独立选择。只带原Key，
// 不允许覆盖任何已保存Cleanup/未知；预算和开放参数全由原实例认证事实导出。
func (e *Executor) RecoverCapacityAdmission(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	s, err := e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if key.Validate() != nil || m.Owner != e.actor || m.Active == nil || m.Active.Request.Key != key || m.Active.Cleanup != nil {
		return pgexec.AwaitingFact, fmt.Errorf("actual admission selection changed original key or has existing Cleanup")
	}
	// 原容量未变仍沿用既有预算/关闭链，不创建不必要的新实际头；只有
	// 旧路径不适用时才建立本次真实实际预算。
	handled, progress, err := e.advanceUnchangedAbort(ctx, s, nil, true)
	if handled {
		return progress, err
	}
	handled, progress, err = e.advanceReconciledAbort(ctx, s, nil, true)
	if !handled {
		return pgexec.AwaitingFact, fmt.Errorf("no original actual-capacity cancellation is eligible")
	}
	return progress, err
}
