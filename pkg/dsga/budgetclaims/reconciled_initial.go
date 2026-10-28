package budgetclaims

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	core "k8s.io/api/core/v1"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 原Initial只在仍由VM永久头持有且首次开放已落账时，可为新Cleanup提供
// Outside来源。它不是一条Resize声明，故返回真实本次Host峰值而无Outside
// CM；后续ReadReconciled再核Node发布的原Host UID，不在这里授释放权限。
func (r *Reader) ReconcileInitialReservations(ctx context.Context, s *journal.Snapshot, p budget.Prepared, initial budget.Initial) (budget.HostReservation, budget.ObjectRef, error) {
	var none budget.HostReservation
	var noRef budget.ObjectRef
	if s == nil || s.State().Active == nil || !s.State().Active.AbortRequested || s.State().Active.Request != p.Request || s.State().ResourceIdentity == nil || p.ValidateFor(p.Request, *s.State().ResourceIdentity) != nil {
		return none, noRef, fmt.Errorf("initial recovery requires original retained boot budget and active cancellation")
	}
	saved, err := s.State().RetainedInitialOutside()
	if err != nil || !reflect.DeepEqual(saved, initial) {
		return none, noRef, fmt.Errorf("initial recovery changed original retained complete budget")
	}
	if _, err := r.ReadInitialBudget(ctx, s, initial); err != nil {
		return none, noRef, err
	}
	ref, err := r.HostReference(ctx, p.Request)
	if err != nil {
		return none, noRef, err
	}
	cm, err := r.claim(ctx, ref, HostKind)
	if err != nil {
		return none, noRef, err
	}
	var host budget.HostReservation
	if json.Unmarshal([]byte(cm.Data[ClaimKey]), &host) != nil || host.ProtocolVersion != 2 || host.ValidateFor(p, r.binding.Incarnation) != nil || host.RunnerPodUID != initial.RunnerPodUID || host.NodeUID != initial.NodeUID || host.HostOverheadBytes != initial.HostOverheadBytes {
		return none, noRef, fmt.Errorf("initial recovery lost original live Host peak")
	}
	return host, ref, nil
}

// 已落账后InitialHeld可被同CAS替换，但完整原Initial已随CapacityHeld保存。
// 所以当前读者按预算内的原引用认证，而不要求旧头仍在；不存在新发现/替代
// UID。v1仍逐项读原Outside CM，v2仅按原Initial资格，二者不能互相降级。
func (r *Reader) readCapacityOutside(ctx context.Context, s *journal.Snapshot, c budget.ReconciledCapacity) (budget.OutsideReservation, *core.ConfigMap, *InitialSources, error) {
	var out budget.OutsideReservation
	if i := c.InitialOutside; i != nil {
		if i.Incarnation != r.binding.Incarnation || i.PGAggregate.Identity != *s.State().ResourceIdentity {
			return out, nil, nil, fmt.Errorf("reconciled initial source changed original Guest or incarnation")
		}
		sources, err := r.ReadInitialBudget(ctx, s, *i)
		return out, nil, sources, err
	}
	cm, err := r.claim(ctx, c.OutsideClaim, OutsideKind)
	if err != nil {
		return out, nil, nil, err
	}
	if json.Unmarshal([]byte(cm.Data[ClaimKey]), &out) != nil || out.ValidateSource() != nil || out.VMShrinkPermit || !out.HoldUntilExplicitSettlement || out.Request != c.OutsideRequest || out.Request.TargetVMMemoryBytes != c.VMMemoryBytes || out.Identity != *s.State().ResourceIdentity || out.Incarnation != r.binding.Incarnation || out.ResidualProfileID != c.ResidualProfileID || out.SystemOutsidePG != c.SystemOutsidePG || out.Transition != c.Transition || out.Safety != c.Safety {
		return out, nil, nil, fmt.Errorf("reconciled outside source does not cover original actual VM")
	}
	if err := r.checkOutsideQualification(ctx, out); err != nil {
		return out, nil, nil, err
	}
	return out, cm, nil, nil
}

func sameClaimSnapshot(a, b *core.ConfigMap) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.UID == b.UID && a.ResourceVersion == b.ResourceVersion && reflect.DeepEqual(a.Data, b.Data)
}

func sameInitialSources(a, b *InitialSources) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return reflect.DeepEqual(a.Qualification, b.Qualification) && a.HostProfileID == b.HostProfileID && a.HostOverheadBytes == b.HostOverheadBytes && a.pod.UID == b.pod.UID && a.node.UID == b.node.UID
}
