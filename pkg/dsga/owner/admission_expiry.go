package owner

import (
	"context"
	"errors"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// advanceNativeAdmissionExpiry只在Run的原过期等待中执行。不能把旧本地布尔、
// SDK重建或Root修复当作新关闭的依据；每次先读取原生同boot/floor的权威事实。
// 观察失败/未过期保持原等待且不写WAL。已过期也只复用原持久关闭选择；原
// Active、Key、Unknown及预算全部保留，后继仍须显式完整rearm/reopen协议。
func (w *PGWorker) advanceNativeAdmissionExpiry(ctx context.Context) (pgexec.Progress, error) {
	if w.safety == nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	if err := w.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	fact, err := w.observeAdmissionLease(ctx)
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	instance, epoch := w.Binding()
	if !fact.Armed || !fact.Expired || fact.Observed.IssuerFloorEpoch > epoch {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	// 接管的PG floor可能尚未推进；Fence返回Pending也可能已写floor，故
	// 每轮都必须取得原Fenced串行屏障，不能用下一轮floor相等冒充join。
	// 实际PG17对已armed租约的Fence保留deadline/LeaseEpoch/expired及历史。
	before := fact
	fenced, err := w.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch})
	if err != nil || fenced.Disposition != control.Fenced {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	fact, err = w.observeAdmissionLease(ctx)
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	if !fact.Armed || !fact.Expired || fact.Observed.IssuerFloorEpoch != epoch || fact.DeadlineNS != before.DeadlineNS || fact.LeaseEpoch != before.LeaseEpoch || fact.TimeoutMS != before.TimeoutMS {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	if err := w.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	_, progress, err := w.advanceServiceSafetySelected(ctx, "", true)
	// 完整关闭的确认也不清Run过期latch，更不能让下一轮普通Advance派发VM。
	return progress, errors.Join(ErrServiceSafety, err)
}
