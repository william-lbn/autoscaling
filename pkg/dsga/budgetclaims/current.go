package budgetclaims

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// Current仅供维持原VM当前预算及Host结清读取。它不使已结清的旧峰值重新
// 获得执行资格；Runner/VM动作仍必须用Read，遇ErrSettled即禁止派发。
type Current struct {
	Closed  hostguard.Settlement
	Host    budget.HostReservation
	outside *core.ConfigMap
	host    *core.ConfigMap
	pod     *core.Pod
	node    *core.Node
}

// ReadCurrent直接使用永久BudgetHeld，不依赖16条可淘汰操作历史。完整
// Finalization原命令已由VM模型严格核验；这里独立从可信API重新GET原CM/
// Pod/Node，逐项匹配持有，不拿旧Finalized成功当当前来源仍存活的证明。
func (r *Reader) ReadCurrent(ctx context.Context, s *journal.Snapshot) (*Current, error) {
	if s == nil || s.VirtualMachine().UID != r.binding.VMUID || s.State().Incarnation != r.binding.Incarnation {
		return nil, fmt.Errorf("current budget reader requires original VM snapshot")
	}
	m, vm := s.State(), s.VirtualMachine()
	closed, err := hostguard.SettlementFor(m)
	if err != nil {
		return nil, err
	}
	f, err := m.BudgetFor(closed.Request)
	if err != nil || f.Sources == nil || m.ResourceIdentity == nil {
		return nil, fmt.Errorf("current complete budget unavailable")
	}
	out, err := r.claim(ctx, f.Sources.OutsideClaim, OutsideKind)
	if err != nil {
		return nil, err
	}
	var outside budget.OutsideReservation
	if json.Unmarshal([]byte(out.Data[ClaimKey]), &outside) != nil || outside.ProtocolVersion != 1 || outside.VMShrinkPermit || !outside.HoldUntilExplicitSettlement || outside.Request != closed.Request || outside.Identity != *m.ResourceIdentity || outside.Incarnation != r.binding.Incarnation || control.ValidateID(outside.ResidualProfileID, "qualified residual profile") != nil || outside.SystemOutsidePG != f.SystemOutsidePG || outside.Transition != f.Transition || outside.Safety != f.Safety {
		return nil, fmt.Errorf("current outside source differs from original retained budget")
	}
	cm, err := r.claim(ctx, closed.HostClaim, HostKind)
	if err != nil {
		return nil, err
	}
	var host budget.HostReservation
	if json.Unmarshal([]byte(cm.Data[ClaimKey]), &host) != nil || host.ProtocolVersion != 2 || host.VMShrinkPermit || !host.HoldUntilExplicitSettlement || host.Request != closed.Request || host.Identity != *m.ResourceIdentity || host.Incarnation != r.binding.Incarnation || host.SourceID != f.HostReservationID || host.RunnerPodName != f.RunnerPodName || host.HostOverheadBytes == 0 {
		return nil, fmt.Errorf("current Host source differs from original retained budget")
	}
	pod, err := r.core.Pods(r.binding.Namespace).Get(ctx, host.RunnerPodName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if pod == nil {
		return nil, fmt.Errorf("current original Runner Pod unavailable")
	}
	owner := metav1.GetControllerOf(pod)
	if pod.Namespace != r.binding.Namespace || pod.Name != host.RunnerPodName || string(pod.UID) != host.RunnerPodUID || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Status.Phase != core.PodRunning || pod.Spec.NodeName != host.NodeName || pod.Status.PodIP != vm.Status.PodIP || vm.Status.PodName != pod.Name || vm.Status.Node != host.NodeName || vm.Status.RestartCount != f.RunnerRestartCount || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != r.binding.Name || owner.UID != r.binding.VMUID {
		return nil, fmt.Errorf("current original Runner Pod changed")
	}
	node, err := r.core.Nodes().Get(ctx, host.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if node == nil || string(node.UID) != host.NodeUID || node.Name != host.NodeName || node.ResourceVersion == "" || node.DeletionTimestamp != nil {
		return nil, fmt.Errorf("current original Host Node changed")
	}
	var model hostguard.Model
	if json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &model) != nil {
		return nil, fmt.Errorf("current complete Host ledger unavailable")
	}
	var plan *hostguard.Plan
	for _, h := range model.Holds {
		if h.Plan.Request == closed.Request && h.Plan.Namespace == r.binding.Namespace && h.Plan.VMName == r.binding.Name && h.Plan.Incarnation == r.binding.Incarnation && h.Plan.RunnerPodUID == host.RunnerPodUID && h.Plan.HostOverheadBytes == host.HostOverheadBytes {
			copyPlan := h.Plan
			plan = &copyPlan
		}
	}
	if plan == nil {
		return nil, fmt.Errorf("current original Host plan is not retained")
	}
	bytes, err := plan.Bytes()
	if err != nil || bytes != host.ReservedMemoryBytes {
		return nil, fmt.Errorf("original Host peak declaration changed")
	}
	if err := hostguard.InspectRetainedClaim(node, *plan, host.SourceID, closed.HostClaim, closed); err != nil {
		return nil, err
	}
	return &Current{Closed: closed, Host: host, outside: out, host: cm, pod: pod, node: node}, nil
}

func (r *Reader) RecheckCurrent(ctx context.Context, s *journal.Snapshot, original *Current) error {
	if original == nil {
		return fmt.Errorf("original current budget observation missing")
	}
	next, err := r.ReadCurrent(ctx, s)
	if err != nil {
		return err
	}
	if next.Closed != original.Closed || next.Host != original.Host || next.outside.UID != original.outside.UID || next.outside.ResourceVersion != original.outside.ResourceVersion || !reflect.DeepEqual(next.outside.Data, original.outside.Data) || next.host.UID != original.host.UID || next.host.ResourceVersion != original.host.ResourceVersion || !reflect.DeepEqual(next.host.Data, original.host.Data) || next.pod.UID != original.pod.UID || next.node.UID != original.node.UID {
		return fmt.Errorf("original current budget source changed across verification")
	}
	return nil
}
