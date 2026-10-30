package admissionexec

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// RecoverServiceActual只在显式管理中固定完整独立实际预算。原Active/Unknown、
// Abort和旧永久预算不改；没有任何PG/Root/QMP反向动作，也不在这里开放或
// 释放Host。UID/RV CAS回复未知即停止；已保存证明每轮只读认证，不重新计算
// 成另一组更便宜的来源，更不能用删除原未知换取普通Finalize成功。
func (e *Executor) RecoverServiceActual(ctx context.Context, s *journal.Snapshot, selector string) error {
	m := s.State()
	if m.ServiceClosure == nil {
		return fmt.Errorf("service actual recovery lacks original closure")
	}
	_, _, _, selected := m.ServiceClosure.Selection()
	if selector != selected {
		return fmt.Errorf("service actual recovery selected another closure")
	}
	if m.ServiceClosure.ActualRecovery != nil {
		return e.CheckServiceActual(ctx, s)
	}
	capacity, current, root, err := e.checkServiceActual(ctx, s, nil)
	if err != nil {
		return err
	}
	_, err = e.store.RecordServiceActualRecovery(ctx, s, e.actor, selector, capacity, current, root)
	return err
}

// CheckServiceActual供普通轮询和后继重开反复认证已保存预算。保存时的
// Settled不是当前来源活性许可；接管、实际容量/Root/原CM漂移均必须重新核。
func (e *Executor) CheckServiceActual(ctx context.Context, s *journal.Snapshot) error {
	if s.State().ServiceClosure == nil || s.State().ServiceClosure.ActualRecovery == nil {
		return fmt.Errorf("service actual check lacks independent retained budget")
	}
	saved := s.State().ServiceClosure.ActualRecovery
	_, _, _, err := e.checkServiceActual(ctx, s, &saved.Capacity)
	return err
}

// 单独认证已持久的准入边界；预算来源检查可在原关门、新开放及异常再关闭
// 前后复用，但当前状态只能对应同一WAL中明确选择的命令。未知外部revision
// 即使布尔值相同也拒绝，不能将本预算检查当成任意开放或关闭许可。
func serviceActualGate(c *journal.ServiceClosure, current control.Snapshot) bool {
	if c == nil {
		return false
	}
	_, _, closed, _ := c.Selection()
	matches := func(request control.AdmissionRequest) bool {
		return request.Instance == current.Instance && request.Revision == current.AdmissionRevision && request.Open == current.AdmissionOpen
	}
	if !current.AdmissionOpen && matches(closed) {
		return true
	}
	r := c.Reopen
	if r == nil {
		return false
	}
	if matches(r.Request) && current.CapacityGeneration == r.Request.ExpectedCapacityGeneration {
		return true
	}
	if r.PriorClose != nil && matches(*r.PriorClose) && current.CapacityGeneration == r.PriorClose.ExpectedCapacityGeneration {
		return true
	}
	if r.Closing != nil && r.Closing.Close != nil {
		_, _, latest, _ := r.Closing.Close.Selection()
		return matches(latest) && current.CapacityGeneration == latest.ExpectedCapacityGeneration
	}
	return false
}

// 当前Root必须逐字对应原Guest已确认来源，或仍被旧永久预算覆盖。这样外部
// 增加一条相同字节的新Held也不能被收养；原Guest未收到并取消时保留旧Root，
// 并不据此把实际VM错误切回旧规格。normal和Outside分别认证、分别计费。
func serviceActualRoot(m journal.Model, root budget.RootAggregate) error {
	for _, step := range m.Active.Steps {
		if step.Action != journal.GuestReady {
			continue
		}
		if step.Phase == journal.Unconfirmed {
			p := m.ServiceClosure.ResourceRecovery
			if p == nil || p.Guest == nil || p.Guest.Phase != journal.Settled || p.Guest.State == nil {
				return fmt.Errorf("service actual Guest remains independently unknown")
			}
			expected, err := budget.RootAggregateFromView(*p.Guest.State)
			if err != nil || !reflect.DeepEqual(root, expected) {
				return fmt.Errorf("service actual Root changed independent original Guest source")
			}
			return nil
		}
		if step.Phase == journal.Settled && step.Observation != nil && step.Observation.Result == journal.Completed {
			var audit resourceexec.Audit
			if json.Unmarshal(step.Observation.Data, &audit) != nil || !reflect.DeepEqual(root, audit.PGAggregate) {
				return fmt.Errorf("service actual Root changed original completed Guest source")
			}
			return nil
		}
		if step.Phase != journal.Planned && (step.Observation == nil || step.Observation.Result != journal.NoEffect) {
			return fmt.Errorf("service actual original Guest effect is not authenticated")
		}
		break
	}
	if m.InitialHeld != nil {
		initial, err := m.RetainedInitialOutside()
		if err == nil && reflect.DeepEqual(root, initial.PGAggregate) {
			return nil
		}
	}
	if m.BudgetHeld != nil {
		var saved budget.FinalizationCommand
		if json.Unmarshal(m.BudgetHeld.Finalization.Command, &saved) == nil && saved.Validate() == nil && saved.Finalized.Aggregate != nil && root.Bound == *saved.Finalized.Aggregate {
			return nil
		}
	}
	if m.CapacityHeld != nil {
		// 独立实际头没有普通Cleanup；统一按整个有效Model读取原预算，
		// 保留同一Root摘要，不靠补写旧步骤或相同字节的新Held替换它。
		saved, err := m.CurrentCapacity()
		if err == nil {
			digest, err := budget.RootDigest(root)
			if err == nil && root.Bound == saved.PGAggregate && digest == saved.RootSHA256 {
				return nil
			}
		}
	}
	return fmt.Errorf("service actual Root is not held by original permanent budget")
}

func (e *Executor) serviceActualVM(ctx context.Context, m journal.Model) (budget.NativeCapacity, *budget.InitialVMSources, vmguard.Identity, error) {
	var none budget.NativeCapacity
	var id vmguard.Identity
	for _, step := range m.Active.Steps {
		if step.Action != journal.ResizeVM || step.Phase == journal.Planned {
			continue
		}
		if step.Phase == journal.Settled && step.Observation != nil && step.Observation.Result == journal.NoEffect {
			// 原普通取消已有不可重发屏障，复用原完整回执读者；不能
			// 再制造一条VMRecovery替换其原NoEffect审计。
			break
		}
		p := m.ServiceClosure.VMRecovery
		if p == nil || p.Phase != journal.Settled || p.Capacity == nil {
			return none, nil, id, fmt.Errorf("service actual VM remains independently unknown")
		}
		proof, err := e.runner.CheckCapacity(ctx, *p.Capacity, e.actor)
		return *p.Capacity, nil, proof.Birth.Identity, err
	}
	native, initial, err := e.reconciledNative(ctx, m)
	if err != nil {
		return none, nil, id, err
	}
	if initial != nil {
		original, err := m.RetainedInitialOutside()
		if err != nil {
			return none, nil, id, err
		}
		proof, err := e.runner.CheckInitialCapacity(ctx, *initial, original, e.actor)
		return native, initial, proof.Birth.Identity, err
	}
	proof, err := e.runner.CheckCapacity(ctx, native, e.actor)
	return native, nil, proof.Birth.Identity, err
}

func (e *Executor) checkServiceActual(ctx context.Context, s *journal.Snapshot, existing *budget.ReconciledCapacity) (budget.ReconciledCapacity, control.SharedCostResponse, budget.RootAggregate, error) {
	var capacity budget.ReconciledCapacity
	var shared control.SharedCostResponse
	var root budget.RootAggregate
	m := s.State()
	c := m.ServiceClosure
	version := m.ClosureFormatVersion()
	if c != nil && version == 20 && c.Reopen != nil {
		version = c.Reopen.ClosureVersion
	}
	if c != nil && version == 19 && c.Rearm != nil {
		version = c.Rearm.ClosureVersion
	}
	if !s.Dispatchable() || m.Owner != e.actor || m.Active == nil || m.ResourceIdentity == nil || m.PGInstance == nil || *m.PGInstance != e.instance || c == nil || c.Pending() || version != 16 && version != 17 || e.resources == nil || e.runner == nil || e.claims == nil {
		return capacity, shared, root, fmt.Errorf("service actual recovery requires complete original closed production domains")
	}
	p, _, err := m.OriginalServicePrepared()
	if err != nil {
		return capacity, shared, root, err
	}
	// Fence必须已取得原生串行权；不能用PENDING和一次当前容量数值替代
	// 原执行退出。原Key对账证明由PG执行器先持久，SDK未知在本层不清。
	if pending := e.client.Pending(); pending != nil && (*pending != m.Active.Request || c.PGRecovery == nil && c.PGRejection == nil) {
		return capacity, shared, root, fmt.Errorf("service actual lacks retained proof for original PG pending")
	}
	// 已持久的原终态可能随后从原生缓存淘汰。此处仅收集全部实际来源，
	// 保留同一SDK Pending；完整预算CAS确认后，再由专用SDK入口核当前
	// 串行Fence/Shared/原Key与执行槽并清栅栏。不能退回旧单域查询把已
	// 保存终态重标隔离，也不能因Pending存在跳过本次原生Fence。
	fence, err := e.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return capacity, shared, root, err
	}
	if fence.Disposition != control.Fenced {
		return capacity, shared, root, fmt.Errorf("service actual PG serialized fence remains pending")
	}
	shared, err = e.client.SharedCost(ctx)
	if err != nil {
		return capacity, shared, root, err
	}
	if !serviceActualGate(c, shared.Observed) || !control.SameResizeRecoveryBoundary(fence.Observed, shared.Observed) {
		return capacity, shared, root, fmt.Errorf("service actual PG changed stable original closed boundary")
	}
	root, actualUsable, err := e.reconciledRoot(ctx, s)
	if err != nil {
		return capacity, shared, root, err
	}
	if err := serviceActualRoot(m, root); err != nil {
		return capacity, shared, root, err
	}
	native, initialVM, nativeID, err := e.serviceActualVM(ctx, m)
	if err != nil {
		return capacity, shared, root, err
	}
	memory := native.MemoryBytes()
	if initialVM != nil {
		memory = initialVM.MemoryBytes
	}
	if existing == nil {
		var initial *budget.Initial
		var outside *budget.ObjectRef
		ref, selectErr := reconciledOutside(m, p, memory)
		if selectErr == nil && initialVM == nil {
			outside = &ref
		} else {
			saved, err := m.RetainedInitialOutside()
			if err != nil || saved.VMMemoryBytes != memory {
				return capacity, shared, root, fmt.Errorf("service actual VM lacks exact retained outside qualification")
			}
			initial = &saved
		}
		out, host, hostRef, usable, err := e.claims.ServiceActualReservations(ctx, s, p, memory, outside, initial)
		if err != nil {
			return capacity, shared, root, err
		}
		var noRef budget.ObjectRef
		if outside != nil {
			noRef = *outside
		}
		inputs := budget.ReconcileInputs{Prepared: p, Shared: shared, Root: root, Outside: out, OutsideClaim: noRef, Host: host, HostClaim: hostRef, VM: native, InitialVM: initialVM, GuestUsableMemoryBytes: usable, IssuerEpoch: e.actor.Epoch}
		if c.PGRecovery != nil || c.PGRejection != nil {
			var proof budget.RecoveryPG
			if c.PGRecovery != nil {
				proof = budget.RecoveryPG{Fence: c.PGRecovery.Fence, Status: c.PGRecovery.Status}
			} else {
				// 明确Busy/Rejected的原审计不改；更高floor隔离只为
				// 当前实际预算提供来源，不补成原生Denied或成功。
				proof = budget.RecoveryPG{Fence: c.PGRejection.Fence, Status: c.PGRejection.Status}
			}
			if initial == nil {
				capacity, err = budget.ReconcileRecovery(inputs, proof)
			} else {
				capacity, err = budget.ReconcileInitialRecovery(inputs, *initial, proof)
			}
		} else {
			// 未派发只取完整原Prepared.Shared；真实Model须仍为Planned。
			// 已派发而缺证明不能降级为unchanged，即便当前数值相同。
			for _, step := range m.Active.Steps {
				if step.Action == journal.ResizePG && step.Phase != journal.Planned {
					return capacity, shared, root, fmt.Errorf("service actual dispatched PG lacks independent recovery")
				}
			}
			if initial == nil {
				capacity, err = budget.ReconcileServiceUnchanged(inputs, p.Shared)
			} else {
				capacity, err = budget.ReconcileInitialServiceUnchanged(inputs, *initial, p.Shared)
			}
		}
		if err != nil {
			return capacity, shared, root, err
		}
	} else {
		capacity = *existing
	}
	digest, err := budget.RootDigest(root)
	vm := s.VirtualMachine()
	if err != nil || digest != capacity.RootSHA256 || root.Bound != capacity.PGAggregate || actualUsable < capacity.GuestUsableMemoryBytes || vm.Status.MemorySize == nil || vm.Status.PodName != capacity.RunnerPodName || vm.Status.RestartCount != capacity.RunnerRestartCount || !reflect.DeepEqual(native, capacity.VM) || !reflect.DeepEqual(initialVM, capacity.InitialVM) {
		return capacity, shared, root, fmt.Errorf("service actual current Root or original native VM source changed")
	}
	vmMemory, exact := vm.Status.MemorySize.AsInt64()
	if !exact || vmMemory <= 0 || control.Uint64(vmMemory) != memory || capacity.ValidateCurrent(shared, e.actor.Epoch, memory) != nil {
		return capacity, shared, root, fmt.Errorf("service actual current PG or VM differs from retained budget")
	}
	claims, err := e.claims.ReadServiceActual(ctx, s, capacity)
	if err != nil {
		return capacity, shared, root, err
	}
	if nativeID.RunnerPodUID != claims.Host.RunnerPodUID || nativeID.NodeUID != claims.Host.NodeUID {
		return capacity, shared, root, fmt.Errorf("service actual original Runner and Host domains differ")
	}
	// 二次PG/Root/Runner认证包围来源读取，不跨RPC持mutex。成本可真实
	// 下降但不能超过已保存上界；容量、Root摘要或原生来源变化一律等待。
	latest, err := e.client.SharedCost(ctx)
	if err != nil || capacity.ValidateCurrent(latest, e.actor.Epoch, memory) != nil || latest.Observed.AdmissionOpen != shared.Observed.AdmissionOpen || latest.Observed.AdmissionRevision != shared.Observed.AdmissionRevision {
		return capacity, shared, root, fmt.Errorf("service actual PG changed during source verification")
	}
	againRoot, againUsable, err := e.reconciledRoot(ctx, s)
	if err != nil || !reflect.DeepEqual(againRoot, root) || againUsable < capacity.GuestUsableMemoryBytes {
		return capacity, shared, root, fmt.Errorf("service actual Root changed during source verification")
	}
	againVM, againInitial, _, err := e.serviceActualVM(ctx, m)
	if err != nil || againVM != native || !reflect.DeepEqual(againInitial, initialVM) {
		return capacity, shared, root, fmt.Errorf("service actual native capacity changed during source verification")
	}
	if err := e.claims.RecheckServiceActual(ctx, s, capacity, claims); err != nil {
		return capacity, shared, root, err
	}
	if err := e.sameSnapshot(ctx, s); err != nil {
		return capacity, shared, root, err
	}
	return capacity, shared, root, nil
}
