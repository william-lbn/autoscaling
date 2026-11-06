package owner

import (
	"context"
	"errors"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// observeServiceRecoveryLease只供三个显式期限恢复入口在完整来源审计后调用。
// Fence只提高原生floor，不续租/重开，也不替代后继原WAL选择、同UID/RV和预算门。
// 首次观察已经同floor时保留原路径；低floor只有真实armed+expired+closed才可推进。
func (w *PGWorker) observeServiceRecoveryLease(ctx context.Context) (control.AdmissionLeaseResponse, error) {
	if err := w.session.Check(ctx); err != nil {
		return control.AdmissionLeaseResponse{}, err
	}
	before, err := w.observeAdmissionLease(ctx)
	if err != nil {
		return before, err
	}
	instance, epoch := w.Binding()
	if before.Observed.IssuerFloorEpoch == epoch {
		return before, nil
	}
	if before.Observed.IssuerFloorEpoch > epoch || !before.Armed || !before.Expired || before.Observed.AdmissionOpen {
		return before, ErrServiceSafety
	}
	// 原Session来自真实K8s Lease。观察与Fence之间再次查租，失租不派发。
	if err := w.session.Check(ctx); err != nil {
		return before, err
	}
	fence, err := w.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch})
	if err != nil || fence.Disposition != control.Fenced {
		return before, errors.Join(ErrServiceSafety, err)
	}
	if err := w.session.Check(ctx); err != nil {
		return control.AdmissionLeaseResponse{}, err
	}
	current, err := w.observeAdmissionLease(ctx)
	if err != nil {
		return current, err
	}
	if current.Observed.IssuerFloorEpoch != epoch || !current.Armed || !current.Expired || current.Observed.AdmissionOpen {
		return current, ErrServiceSafety
	}
	if err := w.session.Check(ctx); err != nil {
		return current, err
	}
	// 不从Fence回执或旧快照拼造新事实。deadline/revision/capacity全部来自这次鲜读，
	// 再交原Start...CAS核验；Fence未知时本函数已返回，绝不发送rearm或创建替代意图。
	return current, nil
}

// 硬预算归档只取已有的 authenticated Capabilities 和 Memory 两个只读入口。
// postmaster 启动策略已固定在 Client 上，旧 boot 的有限租期仍取完整原生回执。
func (w *PGWorker) observeServiceAdmission(ctx context.Context) (control.ServiceAdmission, error) {
	if w.client.AdmissionPolicy() != control.AdmissionPolicyEnforcedBudget {
		fact, err := w.observeAdmissionLease(ctx)
		if err != nil {
			return control.ServiceAdmission{}, err
		}
		return fact.ForServiceAdmission(), nil
	}
	caps, err := w.client.Capabilities(ctx)
	if err != nil {
		return control.ServiceAdmission{}, err
	}
	memory, err := w.client.Memory(ctx)
	if err != nil {
		return control.ServiceAdmission{}, err
	}
	fact := control.ServiceAdmission{Budget: &control.BudgetAdmissionRead{Capabilities: caps, Memory: memory}}
	instance, _ := w.Binding()
	if !fact.ValidRead(instance, memory.Observed.IssuerFloorEpoch, false) {
		return control.ServiceAdmission{}, ErrServiceSafety
	}
	return fact, nil
}

// 明确选择已结清的原关闭后，才允许推进同 boot 的 PG 单调写者下界。
// Fence 不改准入和容量，不重放未知动作。等待旧操作或丢失回复均停在原记录。
func (w *PGWorker) fenceClosedBudget(ctx context.Context, s *journal.Snapshot, closed control.AdmissionRequest) error {
	instance, epoch := w.Binding()
	if closed.Instance != instance || closed.Open || !s.Dispatchable() || s.State().Owner != w.session.Actor() {
		return ErrServiceSafety
	}
	read, err := w.observeServiceAdmission(ctx)
	if err != nil {
		return err
	}
	pg := read.Snapshot()
	if !read.BudgetBound() || pg.AdmissionOpen || pg.AdmissionRevision != closed.Revision || pg.IssuerFloorEpoch > epoch {
		return ErrServiceSafety
	}
	if pg.IssuerFloorEpoch == epoch {
		return w.checkReopenSnapshot(ctx, s)
	}
	if err := w.checkReopenSnapshot(ctx, s); err != nil {
		return err
	}
	fence, err := w.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch})
	if err != nil || fence.Disposition != control.Fenced {
		return errors.Join(ErrServiceSafety, err)
	}
	after, err := w.observeServiceAdmission(ctx)
	if err != nil {
		return err
	}
	now := after.Snapshot()
	if !after.BudgetBound() || now.IssuerFloorEpoch != epoch || now.AdmissionOpen || now.AdmissionRevision != closed.Revision || now.CapacityGeneration != pg.CapacityGeneration {
		return ErrServiceSafety
	}
	return w.checkReopenSnapshot(ctx, s)
}
