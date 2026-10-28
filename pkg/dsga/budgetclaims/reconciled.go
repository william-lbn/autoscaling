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
	initial *InitialSources
}

// ReadReconciled只读同Active取消的实际容量来源，不释放/重建原CM。它不是
// ReadCurrent的降级入口；关闭后的永久预算走独立认证分支。CM必须原UID、
// immutable、protected且同VM，Node还须发布同一Host CM UID对应的峰值。
func (r *Reader) ReadReconciled(ctx context.Context, s *journal.Snapshot, c budget.ReconciledCapacity) (*ReconciledSources, error) {
	return r.readReconciled(ctx, s, c, nil)
}

func (r *Reader) readReconciled(ctx context.Context, s *journal.Snapshot, c budget.ReconciledCapacity, closed *hostguard.Settlement) (*ReconciledSources, error) {
	if s == nil || c.Validate() != nil || !s.Dispatchable() || s.VirtualMachine().UID != r.binding.VMUID || s.State().Incarnation != r.binding.Incarnation || s.State().PGInstance == nil || *s.State().PGInstance != c.Request.Key.Instance || s.State().ResourceIdentity == nil {
		return nil, fmt.Errorf("reconciled sources require original live aborted VM and actual budget")
	}
	if closed == nil {
		if s.State().Active == nil || !s.State().Active.AbortRequested || s.State().Active.Request != c.Request {
			return nil, fmt.Errorf("reconciled active source reader requires original cancellation")
		}
	} else if s.State().CheckClosedCapacity(c.Request) != nil {
		return nil, fmt.Errorf("reconciled current source reader requires original closed permanent budget")
	}
	out, outside, initial, err := r.readCapacityOutside(ctx, s, c)
	if err != nil {
		return nil, err
	}
	id := *s.State().ResourceIdentity
	host, err := r.claim(ctx, c.HostClaim, HostKind)
	if err != nil {
		return nil, err
	}
	var h budget.HostReservation
	if json.Unmarshal([]byte(host.Data[ClaimKey]), &h) != nil || h.ProtocolVersion != 2 || h.VMShrinkPermit || !h.HoldUntilExplicitSettlement || h.Request != c.Request || h.Identity != id || h.Incarnation != r.binding.Incarnation || h.SourceID != c.HostReservationID || h.RunnerPodName != c.RunnerPodName || h.HostOverheadBytes == 0 {
		return nil, fmt.Errorf("reconciled Host source changed original peak hold")
	}
	if i := c.InitialOutside; i != nil && (h.RunnerPodUID != i.RunnerPodUID || h.NodeName != i.NodeName || h.NodeUID != i.NodeUID || h.HostOverheadBytes != i.HostOverheadBytes) {
		return nil, fmt.Errorf("reconciled initial and current Host domains differ")
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
	var checked error
	if closed == nil {
		checked = hostguard.InspectClaim(node, plan, h.SourceID, c.HostClaim)
	} else {
		checked = hostguard.InspectRetainedClaim(node, plan, h.SourceID, c.HostClaim, *closed)
	}
	if err := checked; err != nil {
		return nil, err
	}
	return &ReconciledSources{Outside: out, Host: h, outside: outside, host: host, pod: pod, node: node, initial: initial}, nil
}

// 当前实际预算只从永久头取得，不能由调用者传一份与原VM不同的数值来释放
// 容量。结清后的来源只供只读维持预算，不恢复已结清的执行权限。
func (r *Reader) readCurrentCapacity(ctx context.Context, s *journal.Snapshot) (*Current, error) {
	var command budget.ReconciledAbortCommand
	m := s.State()
	if m.CapacityHeld == nil || json.Unmarshal(m.CapacityHeld.Cleanup.Command, &command) != nil {
		return nil, fmt.Errorf("current actual budget lacks original permanent command")
	}
	capacity, err := m.CapacityFor(command.Capacity.Request)
	if err != nil {
		return nil, err
	}
	closed, err := hostguard.SettlementFor(m)
	if err != nil {
		return nil, err
	}
	observed, err := r.readReconciled(ctx, s, capacity, &closed)
	if err != nil {
		return nil, err
	}
	return &Current{Closed: closed, Host: observed.Host, outside: observed.outside, host: observed.host, pod: observed.pod, node: observed.node, initial: observed.initial}, nil
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
	if !reflect.DeepEqual(next.Outside, original.Outside) || next.Host != original.Host || !sameClaimSnapshot(next.outside, original.outside) || !sameInitialSources(next.initial, original.initial) || next.host.UID != original.host.UID || next.host.ResourceVersion != original.host.ResourceVersion || !reflect.DeepEqual(next.host.Data, original.host.Data) || next.pod.UID != original.pod.UID || next.node.UID != original.node.UID {
		return fmt.Errorf("reconciled original source changed during verification")
	}
	return nil
}
