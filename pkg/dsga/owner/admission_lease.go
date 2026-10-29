package owner

import (
	"context"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
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
	case control.CoordinationState, control.CoordinationReplayServiceClosure, control.CoordinationContinueServiceClosure,
		control.CoordinationRearmService, control.CoordinationObserveServiceRearm, control.CoordinationReplayServiceRearm,
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
	if err := w.session.Check(ctx); err != nil {
		return err
	}
	after, err := w.session.store.Load(ctx)
	if err != nil {
		return err
	}
	if before.ResourceVersion() != after.ResourceVersion() || before.State().Owner != w.session.Actor() || !after.Dispatchable() {
		return fmt.Errorf("admission lease lost original audited VM revision")
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
	if !renewed.Renewed {
		if renewed.Expired {
			return errNativeAdmissionExpired
		}
		return ErrServiceSafety
	}
	return nil
}
