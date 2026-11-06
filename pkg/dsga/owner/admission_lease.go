package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/klog/v2"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 仅原生期限事实设置此标志；一般来源错误仍保留自己的关闭/未知路径。
// 它属于安全等待，不触发换owner，也不自动恢复期限或开放服务。
var errNativeAdmissionExpired = fmt.Errorf("%w: native admission lease expired", ErrServiceSafety)

// 原生过期后保留管理入口，但只准查询、原Root有界修复和关闭恢复。
// 显式的旧开放重放/Begin/取消同样不能越过期限；未来受控重开须有自己
// 完整持久WAL及来源审计，不能把此处普通恢复动作当作rearm许可。
func expiredAdmissionControlAllowed(action control.CoordinationAction) bool {
	switch action {
	case control.CoordinationConfirmNormalQuotaDurability:
		// 仅独立Root确认原count4耐久性；不继续PG/VM/CAS，不清expired latch。
		return true
	case control.CoordinationRecoverServiceLifecycle, control.CoordinationState, control.CoordinationReplayServiceClosure, control.CoordinationContinueServiceClosure,
		control.CoordinationRearmService, control.CoordinationContinueServiceRearm, control.CoordinationObserveServiceRearm, control.CoordinationReplayServiceRearm,
		control.CoordinationReopenService, control.CoordinationObserveServiceReopen, control.CoordinationReplayServiceReopen,
		control.CoordinationCloseServiceReopen, control.CoordinationReplayServiceReopenClosure, control.CoordinationContinueServiceReopenClosure, control.CoordinationCompleteServiceReopen, control.CoordinationRetryServiceReopen,
		control.CoordinationRearmServiceReopen, control.CoordinationObserveServiceReopenRearm, control.CoordinationReplayServiceReopenRearm:
		// 新开放入口自身必须检查有效期限/完整来源；进入管理通道不
		// 表示已获开门许可，更不能清 Run 本地过期 latch。
		return true
	default:
		return action.ResourceOnly()
	}
}

// 精确本地取消不曾授予服务或派发任何领域动作。Finish丢回复后，原取消
// 可能已经从Active移入历史；只要完整保留的每条历史都证明零派发，读回它
// 仍不应获取PG凭据。历史有淘汰/未知、任一持有预算、普通Active或任何已
// 派发步骤都失去此特例，新的Begin必须先协商期限，不能据空闲状态推定。
func nativeLeaseLocalAbort(m journal.Model) bool {
	if m.InitialHeld != nil || m.BudgetHeld != nil || m.CapacityHeld != nil || m.ServiceClosure != nil || m.ExpiredResults != 0 || m.Active == nil && len(m.History) == 0 {
		return false
	}
	unstarted := func(op journal.Operation) bool {
		if !op.AbortRequested || op.Cleanup != nil {
			return false
		}
		for _, step := range op.Steps {
			if step.Phase != journal.Planned {
				return false
			}
		}
		return true
	}
	for _, record := range m.History {
		if record.Outcome != journal.AbortedBeforeDispatch || !unstarted(record.Operation) {
			return false
		}
	}
	return m.Active == nil || unstarted(*m.Active)
}

// observeAdmissionLease不发心跳；来源损坏/失租/旧SDK都不能借只读观察延长
// PG期限。协议缺失或过期明确返回，正式Run没有无限期开放的旧路径降级。
func (w *PGWorker) observeAdmissionLease(ctx context.Context) (control.AdmissionLeaseResponse, error) {
	instance, epoch := w.Binding()
	return w.client.AdmissionLease(ctx, control.AdmissionLeaseRequest{
		ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch,
		ExpectedCapacityGeneration: 1, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe",
	})
}

// 只从Run原串行goroutine调用。先复核完整原服务边界，再读原生deadline，
// 最后再次检查Lease和同一VM UID/RV。任一错误都停止续租，PG独立关闭；
// 不创建修复WAL，不改旧预算/未知，不把来源恢复解释为rearm或开门授权。
// 网络之间不能形成跨域原子事务，原生期限只界定最后一次已核验后仍可
// 服务的有限窗口。长时间卡住的动作也会关门，不能用盲心跳掩盖来源失联。
func (w *PGWorker) renewAdmissionLease(ctx context.Context) error {
	if err := w.session.Check(ctx); err != nil {
		return err
	}
	before, err := w.session.store.Load(ctx)
	if err != nil {
		return err
	}
	// 同一在途PG命令仅走完整专用审计及exact Native CAS；不得在其后
	// 落回通用exact=nil再续一次，也不能让无live任务的MAY降级成partial。
	if w.pgInflightAuditSelected(before) {
		return w.inspectPGInflightService(ctx, before)
	}
	if handled, _, safetyErr := w.advanceServiceSafety(ctx, ""); handled {
		if safetyErr != nil {
			return safetyErr
		}
		return ErrServiceSafety
	}
	fact, err := w.observeAdmissionLease(ctx)
	if err != nil {
		return err
	}
	if fact.Expired {
		return errNativeAdmissionExpired
	}
	if fact.Observed.AdmissionOpen {
		if _, err := before.State().ServiceBudgetID(); err != nil {
			return err
		}
		if before.State().Active != nil && w.qualification == nil {
			return fmt.Errorf("open admission renewal lacks original active qualification")
		}
	}
	if !fact.Armed {
		return nil
	}
	return w.renewAuditedAdmissionLease(ctx, before, fact, nil)
}

// 完整来源审计后的最后线性化检查；任何UID/RV、代次或原生期限变化都不续。
// 只续原来的30秒窗口，不rearm、不改变准入状态，不写业务/预算WAL。
func (w *PGWorker) renewAuditedAdmissionLease(ctx context.Context, before *journal.Snapshot, fact control.AdmissionLeaseResponse, exact *control.Snapshot) error {
	if exact != nil && (!fact.Armed || fact.Expired || fact.LeaseEpoch != w.session.Actor().Epoch || fact.Observed.IssuerFloorEpoch != w.session.Actor().Epoch) {
		if fact.Expired {
			return errNativeAdmissionExpired
		}
		return ErrServiceSafety
	}
	if err := w.session.Check(ctx); err != nil {
		return err
	}
	after, err := w.session.store.Load(ctx)
	if err != nil {
		return err
	}
	if before.State().Owner != w.session.Actor() || after.State().Owner != w.session.Actor() || !after.Dispatchable() {
		return fmt.Errorf("admission lease lost original audited VM authority")
	}
	if before.ResourceVersion() != after.ResourceVersion() {
		// 同UID、同Owner且仍可派发的状态更新使本轮来源快照失效。保留
		// 零续租拒绝，由原Run下一轮完整重审；不能因此退出Owner、换代次，
		// 也不能重放旧动作或跳过任何原始身份/期限核验。
		return fmt.Errorf("%w: admission lease lost original audited VM revision", ErrServiceSafety)
	}
	request := fact.Request
	request.Action = "renew"
	request.ExpectedCapacityGeneration = fact.Observed.CapacityGeneration
	request.ExpectedAdmissionRevision = fact.Observed.AdmissionRevision
	request.ExpectedDeadlineNS = fact.DeadlineNS
	renewed, err := w.client.AdmissionLease(ctx, request)
	if err != nil {
		return err
	}
	if !renewed.Renewed || exact != nil && (renewed.Expired || !renewed.Armed || renewed.LeaseEpoch != fact.LeaseEpoch || renewed.DeadlineNS <= fact.DeadlineNS || !reflect.DeepEqual(renewed.Observed, *exact)) {
		if renewed.Expired {
			return errNativeAdmissionExpired
		}
		return ErrServiceSafety
	}
	// 只在真实Native renew及全部原校验成功后记固定数值，不输出Observed或证明。
	klog.InfoS("DSGA audited native admission lease renewed", "lease_epoch", renewed.LeaseEpoch, "capacity_generation", renewed.Observed.CapacityGeneration, "admission_revision", renewed.Observed.AdmissionRevision, "deadline_ns", renewed.DeadlineNS, "exact_snapshot", exact != nil)
	return nil
}

// 仅Prepare原步骤尚无PG/Guest/VM派发时可接续有限服务。Unconfirmed只保留
// 原Outside创建未知；不能将其当完成、重发Create或覆盖任何旧命令。
func prepareLeaseEligible(m journal.Model, actor journal.Owner) bool {
	if m.Owner != actor || m.ServiceClosure != nil || m.Active == nil || m.Active.Origin != actor || m.Active.AbortRequested || m.Active.Cleanup != nil || len(m.Active.Steps) == 0 {
		return false
	}
	for n, step := range m.Active.Steps {
		if n == 0 {
			if step.Action != journal.PrepareBudget || step.Phase != journal.Planned && step.Phase != journal.Unconfirmed || step.Phase == journal.Unconfirmed && (step.DispatchedBy == nil || *step.DispatchedBy != actor) {
				return false
			}
		} else if step.Phase != journal.Planned {
			return false
		}
	}
	return true
}

func (w *PGWorker) renewPreparedAdmissionLease(ctx context.Context, s *journal.Snapshot, c budget.PrepareCommand, root budget.RootAggregate, guestUsable control.Uint64) error {
	if !prepareLeaseEligible(s.State(), w.session.Actor()) {
		return nil
	}
	if c.Validate() != nil || c.Prepared.Request != s.State().Active.Request {
		return ErrServiceSafety
	}
	return w.renewPrepareLease(ctx, s, c.Prepared.Shared, root, guestUsable, false)
}

// 新资格只证明新目标，不能替代仍在服务的原永久承诺。每次回调认证原
// Initial/Finalized/Capacity完整来源和Runner终态；旧成本匹配沿原生产函数。
// current Shared来自真实本轮读取，最终observe仅作同一原生gate的精确CAS，
// 不以observe拼造Shared或从新的target填当前Guest容量。
func (w *PGWorker) renewPrepareLease(ctx context.Context, s *journal.Snapshot, shared control.SharedCostResponse, root budget.RootAggregate, guestUsable control.Uint64, sourcesAudited bool) error {
	m, vm := s.State(), s.VirtualMachine()
	if !prepareLeaseEligible(m, w.session.Actor()) {
		return nil
	}
	instance, epoch := w.Binding()
	if w.safety == nil || w.resourceOwner == nil || !s.Dispatchable() || m.PGInstance == nil || *m.PGInstance != instance || m.ResourceIdentity == nil || *m.ResourceIdentity != root.Identity || w.client.Pending() != nil || w.resourceOwner.client.Pending() != nil || shared.ValidateFor(instance) != nil || shared.Observed.IssuerFloorEpoch != epoch || vm.Status.MemorySize == nil {
		return ErrServiceSafety
	}
	pending, closing := w.client.PendingAdmission()
	if pending != nil || closing != nil {
		return ErrServiceSafety
	}
	pg := shared.Observed
	if pg.Operation != nil && pg.Operation.Phase != control.Committed && pg.Operation.Phase != control.Denied {
		return ErrServiceSafety
	}
	for _, a := range pg.Arrays {
		if a.AllocationUnknown {
			return ErrServiceSafety
		}
	}
	memory, exact := vm.Status.MemorySize.AsInt64()
	expected, minimum, err := idleServiceBound(m)
	if !exact || memory <= 0 || err != nil || root.Validate() != nil || root.Bound != expected || guestUsable < minimum {
		return ErrServiceSafety
	}
	if m.InitialHeld != nil {
		i, err := m.InitialBudgetFor()
		if err != nil {
			return err
		}
		if !sourcesAudited {
			if _, err := w.safety.claims.ReadInitialBudget(ctx, s, i); err != nil {
				return err
			}
		}
		if err := i.MatchCurrent(shared, root, epoch, control.Uint64(memory), guestUsable); err != nil {
			return err
		}
		_, _, published, err := w.safety.runner.Inspect(ctx, &i.RunnerBirth, m.Owner)
		if err != nil {
			return err
		}
		if published.State == nil || published.Actual == nil || published.State.Pending != nil || published.Actual.RequestedBytes != control.Uint64(memory) || published.Actual.ActualBytes != control.Uint64(memory) {
			return ErrServiceSafety
		}
		if last := published.State.Last; last != nil {
			proof, err := w.safety.runner.ObserveAbortOriginal(ctx, last.Intent.Command, last.Intent.JournalSHA256, m.Owner)
			if err != nil {
				return err
			}
			if !proof.Receipt.Complete() && !proof.Receipt.CancelledBeforeSend() {
				return ErrServiceSafety
			}
		} else if _, err := w.safety.runner.InitialCapacityFor(ctx, i, m.Owner); err != nil {
			return err
		}
	} else {
		if !sourcesAudited {
			if _, err := w.safety.claims.ReadActiveHeld(ctx, s); err != nil {
				return err
			}
		}
		if _, err := w.safety.runner.CheckActiveHeldBudget(ctx, m); err != nil {
			return err
		}
		if m.CapacityHeld != nil {
			c, err := m.CurrentCapacity()
			if err != nil {
				return err
			}
			digest, err := budget.RootDigest(root)
			if err != nil || digest != c.RootSHA256 || root.Bound != c.PGAggregate {
				return ErrServiceSafety
			}
			if err := c.ValidateCurrent(shared, epoch, control.Uint64(memory)); err != nil {
				return err
			}
		} else if m.BudgetHeld != nil && m.BudgetHeld.Finalization.Observation != nil {
			var f budget.Finalized
			if json.Unmarshal(m.BudgetHeld.Finalization.Observation.Data, &f) != nil || f.Aggregate == nil || *f.Aggregate != root.Bound {
				return ErrServiceSafety
			}
			if err := f.ValidateFor(f.Request, shared, epoch, vm.Status.PodName, vm.Status.RestartCount, uint64(memory)); err != nil {
				return err
			}
		} else {
			return ErrServiceSafety
		}
	}
	fact, err := w.observeAdmissionLease(ctx)
	if err != nil {
		return err
	}
	if fact.Expired {
		return errNativeAdmissionExpired
	}
	if !reflect.DeepEqual(fact.Observed, shared.Observed) {
		return ErrServiceSafety
	}
	return w.renewAuditedAdmissionLease(ctx, s, fact, &shared.Observed)
}

// 正常完成阶段只在原完整audit成功的同一调用内接续；恢复有自己的显式
// scope/WAL，过期latch、Unknown或旧owner动作不得借本出口恢复期限。
func (w *PGWorker) checkBeforeFinalizationLeaseAudit(ctx context.Context, s *journal.Snapshot, expired bool) error {
	if expired || w.qualification == nil || ctx.Value(recoveryLeaseScopeKey{}) != nil {
		return w.finalize.CheckBeforeFinalization(ctx, s)
	}
	return w.finalize.CheckBeforeFinalizationWithAudit(ctx, s, w.renewCompletedServiceAudit)
}

func (w *PGWorker) checkRetainedServiceLeaseAudit(ctx context.Context, s *journal.Snapshot, expired bool) error {
	if expired || w.qualification == nil || ctx.Value(recoveryLeaseScopeKey{}) != nil {
		return w.admission.CheckRetainedService(ctx, s)
	}
	return w.admission.CheckRetainedServiceWithAudit(ctx, s, w.renewCompletedServiceAudit)
}

// Shared本身不是完整来源证明：此函数只能由刚完成全部Root/PG/Host/
// Outside/Runner及sameSnapshot核验的两个原auditor回调。随后仍fresh Observe、
// 原Session和同VM UID/RV精确CAS；不沿用物理动作之前的audit，不开放门禁。
func (w *PGWorker) renewCompletedServiceAudit(ctx context.Context, s *journal.Snapshot, shared control.SharedCostResponse) error {
	if s == nil {
		return ErrServiceSafety
	}
	m := s.State()
	if ctx.Value(recoveryLeaseScopeKey{}) != nil || m.ServiceClosure != nil || m.Active == nil || m.Active.Origin != w.session.Actor() || m.Active.AbortRequested || m.Active.Cleanup != nil {
		return nil
	}
	for _, step := range m.Active.Steps {
		if step.Phase == journal.Unconfirmed {
			return nil
		}
	}
	if w.client.Pending() != nil || w.resourceOwner == nil || w.resourceOwner.client.Pending() != nil {
		return nil
	}
	pending, closing := w.client.PendingAdmission()
	if pending != nil || closing != nil {
		return nil
	}
	return w.renewCurrentServiceAudit(ctx, s, shared)
}

// 只消费执行器刚完成的同快照完整审计，不能由Shared单独构造许可。
func (w *PGWorker) renewCurrentServiceAudit(ctx context.Context, s *journal.Snapshot, shared control.SharedCostResponse) error {
	m := s.State()
	instance, epoch := w.Binding()
	if m.Owner != w.session.Actor() || !s.Dispatchable() || shared.ValidateFor(instance) != nil || shared.Observed.IssuerFloorEpoch != epoch {
		return ErrServiceSafety
	}
	pg := shared.Observed
	if pg.Operation != nil && pg.Operation.Phase == control.Preparing {
		return ErrServiceSafety
	}
	for _, array := range pg.Arrays {
		if array.AllocationUnknown {
			return ErrServiceSafety
		}
	}
	fact, err := w.observeAdmissionLease(ctx)
	if err != nil {
		return err
	}
	if fact.Expired {
		return errNativeAdmissionExpired
	}
	if !reflect.DeepEqual(fact.Observed, shared.Observed) {
		return ErrServiceSafety
	}
	return w.renewAuditedAdmissionLease(ctx, s, fact, &shared.Observed)
}

// VM执行器只在本轮原完整来源审计后同步调用。Grow/Shrink及ObserveVM
// 共用精确当前游标；本函数不把Shared升级成许可，也不重算资源分配。
// 当前VM可仍MAY，其他未知拒绝；旧Held只核原来源，不要求Shrink post仍old实际值。
func (w *PGWorker) renewVMServiceAudit(ctx context.Context, s *journal.Snapshot, shared control.SharedCostResponse) error {
	if s == nil || ctx == nil || w.session == nil {
		return ErrServiceSafety
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m := s.State()
	actor := w.session.Actor()
	auditedMode := w.executor != nil && w.executor.InFlightEnabled()
	if !auditedMode {
		// 零值false/direct旧合同：仅原Grow/ResizeVM出口，不要求新增Held来源。
		if w.qualification == nil || ctx.Value(recoveryLeaseScopeKey{}) != nil || m.ServiceClosure != nil || m.Active == nil || m.Active.Origin != actor || m.Active.AbortRequested || m.Active.Cleanup != nil || m.Active.Request.Kind != control.Grow {
			return nil
		}
		found := false
		for _, step := range m.Active.Steps {
			if step.Action == journal.ResizeVM {
				if found || step.Phase != journal.Planned && step.Phase != journal.Unconfirmed || step.Phase == journal.Unconfirmed && (step.DispatchedBy == nil || *step.DispatchedBy != actor) {
					return nil
				}
				found = true
			} else if !found {
				if step.Phase != journal.Settled || step.Observation == nil || step.Observation.Result != journal.Completed {
					return nil
				}
			} else if step.Phase != journal.Planned {
				return nil
			}
		}
		if !found || w.client.Pending() != nil || w.resourceOwner == nil || w.resourceOwner.client.Pending() != nil {
			return nil
		}
		pending, closing := w.client.PendingAdmission()
		if pending != nil || closing != nil {
			return nil
		}
		return w.renewCurrentServiceAudit(ctx, s, shared)
	}
	// 显式同Session Run opt-in后只用完整新谓词；缺来源不能回退legacy。
	if w.qualification == nil || ctx.Value(recoveryLeaseScopeKey{}) != nil || m.ServiceClosure != nil || m.Active == nil || m.Active.Origin != actor || m.Active.AbortRequested || m.Active.Cleanup != nil {
		return nil
	}

	instance, epoch := w.Binding()
	if m.Validate() != nil || !s.Dispatchable() || m.Owner != actor || actor.Epoch != epoch || m.PGInstance == nil || *m.PGInstance != instance || m.Active.Request.Key.Instance != instance || m.Active.Request.Key.IssuerEpoch != epoch || (m.Active.Request.Kind != control.Grow && m.Active.Request.Kind != control.Shrink) {
		return nil
	}
	found := false
	for _, step := range m.Active.Steps {
		if !found {
			if step.Phase == journal.Settled && step.Observation != nil && step.Observation.Result == journal.Completed {
				continue
			}
			if (step.Action != journal.ResizeVM && step.Action != journal.ObserveVM) || (step.Phase != journal.Planned && step.Phase != journal.Unconfirmed) || step.Phase == journal.Unconfirmed && (step.DispatchedBy == nil || *step.DispatchedBy != actor) {
				return nil
			}
			found = true
		} else if step.Phase != journal.Planned {
			return nil
		}
	}
	if !found || w.client.Pending() != nil || w.resourceOwner == nil || w.resourceOwner.client.Pending() != nil || w.resourceOwner.client.ArrayAuthorityPending() != nil {
		return nil
	}
	pending, closing := w.client.PendingAdmission()
	if pending != nil || closing != nil {
		return nil
	}
	// 原VM auditor已核新Prepared/Outside/Host/Runner/Root/PG全部当前来源。
	// 此处补同一原Reader的旧永久头首尾持有；没有新预算/TTL或跨VM数值推断。
	recheckHeld, err := w.captureActiveHeldSources(ctx, s)
	if err != nil {
		return err
	}
	if err = recheckHeld(ctx); err != nil {
		return err
	}
	return w.renewCurrentServiceAudit(ctx, s, shared)
}
