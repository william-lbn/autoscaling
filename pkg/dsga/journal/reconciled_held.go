package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// HeldCapacity是部分效果取消后实际完整预算的独立永久头；不改写普通Finalized
// 或原目标。普通取消在同一Record CAS保存完整Cleanup和预算；独立恢复
// 则在新开放完成CAS保存原Cleanup及Recovery，Unknown保持Unknown。来源、
// owner和原操作不随历史淘汰清除；本头不代表业务Succeeded或Host已释放。
type HeldCapacity struct {
	ProtocolVersion  uint32                   `json:"protocol_version"`
	Identity         resourceguard.Identity   `json:"identity"`
	Origin           Owner                    `json:"origin"`
	RecordedSequence control.Uint64           `json:"recorded_sequence"`
	Cleanup          Step                     `json:"cleanup"`
	Recovery         *RecoveredCapacityOrigin `json:"recovery,omitempty"`
}

func (v *HeldCapacity) UnmarshalJSON(raw []byte) error {
	type plain HeldCapacity
	var p plain
	// 独立恢复头另含完整原操作和有界证明，和普通64KiB响应分开设限；
	// 总Model/注解预算及Proof各自上限仍须通过，不能借此放大网络请求。
	if err := control.UnmarshalObjectBounded(raw, &p, MaxRawBytes/2, 16384); err != nil {
		return err
	}
	*v = HeldCapacity(p)
	return nil
}

func rawDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// 检查原计划结构和来源摘要，网络真实性仍须真实PG/Root/Runner/持有读者核验。
// 容量有部分效果可以结算，但任何未确认动作都不能被Cleanup跨过。旧Finalize/
// Resume已经派发的情形由原完整预算恢复处理，不能在这里替换其未知命令。
func (model Model) reconciledPattern(op Operation, command budget.ReconciledAbortCommand) error {
	if err := model.checkInitialOutside(command.Capacity); err != nil {
		return err
	}
	return model.reconciledResourcePattern(op, command)
}

// 资源前驱结构可在永久恢复证明中独立核验。当前保存/执行入口仍须先经过
// reconciledPattern核原Initial持有来源；历史校验不重新收养当前外部来源。
func (model Model) reconciledResourcePattern(op Operation, command budget.ReconciledAbortCommand) error {
	c := command.Capacity
	if !op.AbortRequested || c.Request != op.Request || model.ResourceIdentity == nil || model.PGInstance == nil || *model.PGInstance != op.Request.Key.Instance || c.Validate() != nil {
		return fmt.Errorf("reconciled capacity changed original aborted operation")
	}
	var prepare, vm *Step
	for i := range op.Steps {
		s := &op.Steps[i]
		if s.Phase == Unconfirmed || ((s.Action == FinalizeBudget || s.Action == ResumeAdmission) && s.Phase != Planned) {
			return fmt.Errorf("reconciled cleanup cannot replace unconfirmed or original final admission")
		}
		if s.Action == PrepareBudget {
			prepare = s
		}
		if s.Action == ResizeVM {
			vm = s
		}
	}
	var p budget.Prepared
	if prepare == nil || prepare.Phase != Settled || prepare.Observation == nil || prepare.Observation.Result != Completed || prepare.TargetID != op.Request.Key.Instance.ComputeInstanceID || json.Unmarshal(prepare.Observation.Data, &p) != nil || p.ValidateFor(op.Request, *model.ResourceIdentity) != nil || rawDigest(prepare.Observation.Data) != command.PreparedSHA256 || p.VMBeforeMemoryBytes != c.VMBeforeMemoryBytes || p.RunnerPodName != c.RunnerPodName || p.RunnerRestartCount != c.RunnerRestartCount || p.Shared.Observed.MaximumBuffers != c.MaximumBuffers || p.Shared.StartupMainUpperBytes != c.StartupMainUpperBytes || p.Shared.StartupArraysUpperBytes != c.StartupArraysUpperBytes {
		return fmt.Errorf("reconciled cleanup lost original complete Prepare binding")
	}
	if c.CapacityGeneration == op.Request.ExpectedCapacityGeneration && c.Buffers != p.Shared.Observed.HighBuffers {
		return fmt.Errorf("reconciled unchanged PG generation differs from original prefix")
	}
	// 实际PG事实还须与原WAL结论一致：不能把已Completed的resize说成
	// 旧前缀，也不能在Planned/明确拒绝后借另一提交改容量。未知已在上面
	// 阻断，原生Committed必须经原步骤确认后才可关闭。
	for _, step := range op.Steps {
		if step.Action != ResizePG {
			continue
		}
		committed := c.PGOperation != nil && c.PGOperation.Key == op.Request.Key && c.PGOperation.Phase == control.Committed
		if step.Phase == Settled && step.Observation != nil && step.Observation.Result == Completed {
			if !committed {
				return fmt.Errorf("reconciled PG commit contradicts completed original resize")
			}
		} else if c.CapacityGeneration != op.Request.ExpectedCapacityGeneration || committed || (step.Phase == Planned && c.PGOperation != nil && c.PGOperation.Key == op.Request.Key) {
			return fmt.Errorf("reconciled PG effects cross an undispatched or rejected resize")
		}
	}
	if op.Request.Kind == control.Grow {
		for _, step := range op.Steps {
			if step.Action != ReserveHost {
				continue
			}
			var host budget.HostReservation
			if step.Phase != Settled || step.Observation == nil || step.Observation.Result != Completed || json.Unmarshal(step.Observation.Data, &host) != nil || host.ValidateFor(p, model.Incarnation) != nil || host.ProtocolVersion != 2 || step.TargetID != host.SourceID || host.SourceID != c.HostReservationID {
				return fmt.Errorf("reconciled Grow lost complete original Host reservation")
			}
		}
	}
	if vm == nil {
		return fmt.Errorf("reconciled original VM step missing")
	}
	if c.InitialVM != nil {
		// 原启动来源与原VM命令是互斥的事实。任何派发、未知或已结算
		// 的VM步骤都不能退回“从未派发”；完整Initial摘要已在上面核对。
		if vm.Phase != Planned || c.VMMemoryBytes != p.VMBeforeMemoryBytes {
			return fmt.Errorf("initial VM source cannot cross a dispatched original VM action")
		}
	} else if c.VM.Request.Key == op.Request.Key {
		if vm.Phase != Settled || vm.Observation == nil || c.VM.Sources.JournalSHA256 != vm.CommandSHA256 || c.VM.PreparedSHA256 != command.PreparedSHA256 || (c.VM.Cancelled && vm.Observation.Result != NoEffect) || (!c.VM.Cancelled && vm.Observation.Result != Completed) {
			return fmt.Errorf("reconciled current native VM differs from original terminal WAL")
		}
	} else if vm.Phase != Planned || c.VMMemoryBytes != p.VMBeforeMemoryBytes {
		return fmt.Errorf("reconciled older native source cannot cross a dispatched VM action")
	}
	return nil
}

func (model Model) heldCapacity(op Operation, cleanup Step) (*HeldCapacity, error) {
	var command budget.ReconciledAbortCommand
	var receipt budget.ReconciledAbortReceipt
	if cleanup.DispatchedBy == nil || cleanup.Phase != Settled || cleanup.Observation == nil || cleanup.Observation.Result != Completed || cleanup.TargetID != op.Request.Key.Instance.ComputeInstanceID || json.Unmarshal(cleanup.Command, &command) != nil || command.ValidateFor(cleanup.DispatchedBy.Epoch) != nil || model.reconciledPattern(op, command) != nil || json.Unmarshal(cleanup.Observation.Data, &receipt) != nil || receipt.PGAggregate.Identity != *model.ResourceIdentity || receipt.ValidateFor(command, cleanup.DispatchedBy.Epoch, model.Owner.Epoch) != nil {
		return nil, fmt.Errorf("reconciled capacity has no exact complete original Cleanup audit")
	}
	return &HeldCapacity{ProtocolVersion: 1, Identity: *model.ResourceIdentity, Origin: op.Origin, RecordedSequence: model.Sequence, Cleanup: cloneStep(cleanup)}, nil
}

func (model Model) validateHeldCapacity() error {
	h := model.CapacityHeld
	if h == nil {
		return nil
	}
	if h.ProtocolVersion == 2 || h.Recovery != nil {
		return model.validateRecoveredCapacity()
	}
	if model.ProtocolVersion < 8 || model.BudgetHeld != nil || h.ProtocolVersion != 1 || h.Origin.Validate() != nil || h.RecordedSequence == 0 || h.RecordedSequence > model.Sequence || model.ResourceIdentity == nil || h.Identity != *model.ResourceIdentity || model.PGInstance == nil || model.HighWater == nil || h.Cleanup.DispatchedBy == nil || h.Cleanup.Observation == nil {
		return fmt.Errorf("invalid permanent reconciled capacity header")
	}
	var c budget.ReconciledAbortCommand
	var receipt budget.ReconciledAbortReceipt
	if json.Unmarshal(h.Cleanup.Command, &c) != nil || json.Unmarshal(h.Cleanup.Observation.Data, &receipt) != nil || c.ValidateFor(h.Cleanup.DispatchedBy.Epoch) != nil || receipt.PGAggregate.Identity != h.Identity || receipt.ValidateFor(c, h.Cleanup.DispatchedBy.Epoch, model.Owner.Epoch) != nil || c.Capacity.Request.Key.Instance != *model.PGInstance || c.Capacity.Request.Key.Instance.VMUID != h.Identity.VMUID || c.Capacity.Request.Key.Instance.ComputeInstanceID != h.Identity.ComputeInstanceID || compareKeys(c.Capacity.Request.Key, *model.HighWater) > 0 || h.Origin.Epoch != c.Capacity.Request.Key.IssuerEpoch {
		return fmt.Errorf("permanent reconciled capacity changed original budget context")
	}
	r := c.Capacity.Request
	if err := model.checkInitialOutside(c.Capacity); err != nil {
		return err
	}
	hash, err := RequestDigest(r)
	if err != nil {
		return err
	}
	op := Operation{Request: r, RequestSHA256: hash, Origin: h.Origin, AbortRequested: true, Steps: nil, Cleanup: &h.Cleanup, PriorBudgetBoundary: nil}
	if model.validateOwners(op) != nil || h.Cleanup.Phase != Settled || h.Cleanup.Observation.Result != Completed || h.Cleanup.TargetID != r.Key.Instance.ComputeInstanceID || h.Cleanup.validate(op, len(plan(r.Kind))+1, AbortReconcile) != nil {
		return fmt.Errorf("permanent reconciled capacity changed original Cleanup WAL")
	}
	check := func(candidate Operation, closed control.Uint64) error {
		order := compareKeys(candidate.Request.Key, r.Key)
		if order == 0 && (candidate.Request != r || candidate.Origin != h.Origin || candidate.Cleanup == nil || !reflect.DeepEqual(*candidate.Cleanup, h.Cleanup) || model.reconciledPattern(candidate, c) != nil || (closed != 0 && closed <= h.RecordedSequence)) {
			return fmt.Errorf("permanent reconciled budget differs from original operation")
		}
		if closed != 0 && ((order < 0 && closed >= h.RecordedSequence) || (order > 0 && closed <= h.RecordedSequence)) {
			return fmt.Errorf("reconciled budget sequence contradicts terminal history")
		}
		for _, s := range candidate.Steps {
			if order > 0 && s.Action == FinalizeBudget && s.Phase == Settled && s.Observation.Result == Completed {
				return fmt.Errorf("new normal finalization did not atomically replace reconciled budget")
			}
		}
		return nil
	}
	for _, record := range model.History {
		if err := check(record.Operation, record.ClosedSequence); err != nil {
			return err
		}
	}
	if model.Active != nil {
		return check(*model.Active, 0)
	}
	return nil
}

// CapacityFor只返回准确原请求的实际预算，绝不投影成目标已成功的Finalized。
// 返回值重新严格解码，不共享PGOperation指针；网络来源仍须每次独立核验。
func (model Model) CapacityFor(request control.Request) (budget.ReconciledCapacity, error) {
	var c budget.ReconciledAbortCommand
	if model.Validate() != nil || model.CapacityHeld == nil || json.Unmarshal(model.CapacityHeld.Cleanup.Command, &c) != nil || c.Capacity.Request != request {
		return budget.ReconciledCapacity{}, fmt.Errorf("no exact permanently retained reconciled capacity")
	}
	return c.Capacity, nil
}

// CheckReconciledPlan给SDK恢复/生产执行器验证原Cleanup的容量计划，不返回任何
// 执行许可。来源仍须重新认证；Unknown本身不能因客户端重建而被遗忘。
func (model Model) CheckReconciledPlan(command budget.ReconciledAbortCommand) error {
	if model.Validate() != nil || model.Active == nil {
		return fmt.Errorf("reconciled original plan is not active")
	}
	return model.reconciledPattern(*model.Active, command)
}

func (model Model) CheckClosedCapacity(request control.Request) error {
	if _, err := model.CapacityFor(request); err != nil {
		return err
	}
	if model.Active != nil || model.HighWater == nil || model.BudgetBoundary == nil || model.BudgetBoundary.BudgetKey != request.Key || model.BudgetBoundary.ThroughKey != *model.HighWater {
		return fmt.Errorf("actual capacity has no complete persistent closed-through boundary")
	}
	return nil
}

func (model Model) validateReconciledAbort(op Operation) error {
	if model.CapacityHeld == nil || op.Cleanup == nil || !reflect.DeepEqual(model.CapacityHeld.Cleanup, *op.Cleanup) {
		return fmt.Errorf("reconciled abort lacks atomically retained actual budget")
	}
	var command budget.ReconciledAbortCommand
	if json.Unmarshal(op.Cleanup.Command, &command) != nil || model.reconciledPattern(op, command) != nil {
		return fmt.Errorf("reconciled abort changed original capacity plan")
	}
	return nil
}
