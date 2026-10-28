package budgetclaims

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// ReconcileReservations仅读取构造实际预算所需的原声明，不返回派发或结清
// 许可。Host名称固定到本次原请求，Outside引用来自原Prepare或永久旧预算；
// 不扫描任选同数值CM，不创建替代来源。构造后必须ReadReconciled认证真实
// Node/Pod持有，再Recheck包围其他域请求，最后同VM UID/RV CAS。
func (r *Reader) ReconcileReservations(ctx context.Context, s *journal.Snapshot, p budget.Prepared, outside budget.ObjectRef) (budget.OutsideReservation, budget.HostReservation, budget.ObjectRef, error) {
	var out budget.OutsideReservation
	var host budget.HostReservation
	var ref budget.ObjectRef
	if s == nil || s.State().Active == nil || !s.State().Active.AbortRequested || s.State().Active.Request != p.Request || s.State().ResourceIdentity == nil || p.ValidateFor(p.Request, *s.State().ResourceIdentity) != nil || s.VirtualMachine().UID != r.binding.VMUID || s.State().Incarnation != r.binding.Incarnation {
		return out, host, ref, fmt.Errorf("actual budget declarations require original active cancellation")
	}
	outCM, err := r.claim(ctx, outside, OutsideKind)
	if err != nil {
		return out, host, ref, err
	}
	if json.Unmarshal([]byte(outCM.Data[ClaimKey]), &out) != nil || out.ProtocolVersion != 1 || out.VMShrinkPermit || !out.HoldUntilExplicitSettlement || out.Identity != p.Identity || out.Incarnation != r.binding.Incarnation || out.Request.Key.Instance != p.Request.Key.Instance {
		return out, host, ref, fmt.Errorf("actual budget outside declaration changed original domain")
	}
	ref, err = r.HostReference(ctx, p.Request)
	if err != nil {
		return out, host, ref, err
	}
	hostCM, err := r.claim(ctx, ref, HostKind)
	if err != nil {
		return out, host, ref, err
	}
	if json.Unmarshal([]byte(hostCM.Data[ClaimKey]), &host) != nil || host.ProtocolVersion != 2 || host.ValidateFor(p, r.binding.Incarnation) != nil {
		return out, host, ref, fmt.Errorf("actual budget Host declaration lost original peak")
	}
	return out, host, ref, nil
}
