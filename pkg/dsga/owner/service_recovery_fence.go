package owner

import (
	"context"
	"errors"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
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
