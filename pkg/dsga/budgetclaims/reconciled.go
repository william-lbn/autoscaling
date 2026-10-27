package budgetclaims

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// ReconciledSources只是本次读取的来源副本，没有派发/释放权限。旧VM仍大时
// Outside须覆盖原大VM，新请求Host峰值也继续完整持有。Root/PG/Runner和最终
// VM UID/RV CAS须由协调器另核，不能把来源自报数字当作持续持有证明。
type ReconciledSources struct {
	Outside budget.OutsideReservation
	Host    budget.HostReservation
	outside *core.ConfigMap
	host    *core.ConfigMap
	pod     *core.Pod
	node    *core.Node
}

// ReadReconciled只读同Active取消的实际容量来源，不释放/重建原CM。它不是
// ReadCurrent的降级入口；关闭后的永久预算需独立账本接线。CM必须原UID、
// immutable、protected且同VM，Node还须发布同一Host CM UID对应的峰值。
func (r *Reader) ReadReconciled(ctx context.Context, s *journal.Snapshot, c budget.ReconciledCapacity) (*ReconciledSources, error) {
	if s == nil || c.Validate() != nil || !s.Dispatchable() || s.VirtualMachine().UID != r.binding.VMUID || s.State().Incarnation != r.binding.Incarnation || s.State().Active == nil || !s.State().Active.AbortRequested || s.State().Active.Request != c.Request || s.State().PGInstance == nil || *s.State().PGInstance != c.Request.Key.Instance || s.State().ResourceIdentity == nil {
		return nil, fmt.Errorf("reconciled sources require original live aborted VM and actual budget")
	}
	outside, err := r.claim(ctx, c.OutsideClaim, OutsideKind)
	if err != nil {
		return nil, err
	}
	var out budget.OutsideReservation
	id := *s.State().ResourceIdentity
	if json.Unmarshal([]byte(outside.Data[ClaimKey]), &out) != nil || out.ProtocolVersion != 1 || out.VMShrinkPermit || !out.HoldUntilExplicitSettlement || out.Request != c.OutsideRequest || out.Request.TargetVMMemoryBytes != c.VMMemoryBytes || out.Identity != id || out.Incarnation != r.binding.Incarnation || out.ResidualProfileID != c.ResidualProfileID || out.SystemOutsidePG != c.SystemOutsidePG || out.Transition != c.Transition || out.Safety != c.Safety {
		return nil, fmt.Errorf("reconciled outside source does not cover original actual VM")
	}
	host, err := r.claim(ctx, c.HostClaim, HostKind)
	if err != nil {
		return nil, err
	}
	var h budget.HostReservation
	if json.Unmarshal([]byte(host.Data[ClaimKey]), &h) != nil || h.ProtocolVersion != 2 || h.VMShrinkPermit || !h.HoldUntilExplicitSettlement || h.Request != c.Request || h.Identity != id || h.Incarnation != r.binding.Incarnation || h.SourceID != c.HostReservationID || h.RunnerPodName != c.RunnerPodName || h.HostOverheadBytes == 0 {
		return nil, fmt.Errorf("reconciled Host source changed original peak hold")
	}
	pod, err := r.core.Pods(r.binding.Namespace).Get(ctx, h.RunnerPodName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if pod == nil {
		return nil, fmt.Errorf("reconciled original Runner Pod unavailable")
	}
	vm := s.VirtualMachine()
	owner := metav1.GetControllerOf(pod)
	if pod.ResourceVersion == "" || string(pod.UID) != h.RunnerPodUID || pod.DeletionTimestamp != nil || pod.Status.Phase != core.PodRunning || pod.Spec.NodeName != h.NodeName || pod.Status.PodIP != vm.Status.PodIP || vm.Status.PodName != pod.Name || vm.Status.Node != h.NodeName || vm.Status.RestartCount != c.RunnerRestartCount || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != vm.Name || owner.UID != r.binding.VMUID {
		return nil, fmt.Errorf("reconciled original Runner Pod changed")
	}
	node, err := r.core.Nodes().Get(ctx, h.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if node == nil || node.Name != h.NodeName || string(node.UID) != h.NodeUID || node.ResourceVersion == "" || node.DeletionTimestamp != nil {
		return nil, fmt.Errorf("reconciled original Host Node changed")
	}
	plan := hostguard.Plan{Namespace: r.binding.Namespace, VMName: r.binding.Name, Incarnation: r.binding.Incarnation, RunnerPodUID: h.RunnerPodUID, Request: c.Request, BeforeBytes: c.VMBeforeMemoryBytes, HostOverheadBytes: h.HostOverheadBytes}
	peak, err := plan.Bytes()
	if err != nil || peak != h.ReservedMemoryBytes {
		return nil, fmt.Errorf("reconciled original Host peak no longer covers actual VM")
	}
	if err := hostguard.InspectClaim(node, plan, h.SourceID, c.HostClaim); err != nil {
		return nil, err
	}
	return &ReconciledSources{Outside: out, Host: h, outside: outside, host: host, pod: pod, node: node}, nil
}

// 重读包围其他域的网络请求。Node其他计划可变化，但原持有必须仍在且原CM
// UID/RV/body、Pod/Node身份一致；失败只保留原峰值和未知，不自动清账。
func (r *Reader) RecheckReconciled(ctx context.Context, s *journal.Snapshot, c budget.ReconciledCapacity, original *ReconciledSources) error {
	if original == nil {
		return fmt.Errorf("original reconciled sources missing")
	}
	next, err := r.ReadReconciled(ctx, s, c)
	if err != nil {
		return err
	}
	if next.Outside != original.Outside || next.Host != original.Host || next.outside.UID != original.outside.UID || next.outside.ResourceVersion != original.outside.ResourceVersion || !reflect.DeepEqual(next.outside.Data, original.outside.Data) || next.host.UID != original.host.UID || next.host.ResourceVersion != original.host.ResourceVersion || !reflect.DeepEqual(next.host.Data, original.host.Data) || next.pod.UID != original.pod.UID || next.node.UID != original.node.UID {
		return fmt.Errorf("reconciled original source changed during verification")
	}
	return nil
}
