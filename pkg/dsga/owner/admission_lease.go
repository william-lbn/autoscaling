package owner

import (
	"context"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

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
		return ErrServiceSafety
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
		return ErrServiceSafety
	}
	return nil
}
