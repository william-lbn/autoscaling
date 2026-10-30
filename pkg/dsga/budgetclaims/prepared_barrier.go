package budgetclaims

import (
	"context"
	"encoding/json"
	"fmt"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// NewWithCoord固定与原Core同一可信集群的Coordination客户端。仅新Completed
// Prepare屏障读取需要Host Lease；旧New缺此能力时只能拒绝该新分支。
func NewWithCoord(core coreclient.CoreV1Interface, coord coordclient.CoordinationV1Interface, binding journal.Binding) (*Reader, error) {
	if coord == nil {
		return nil, fmt.Errorf("prepared barrier reader requires original coordination client")
	}
	r, err := New(core, binding)
	if err != nil {
		return nil, err
	}
	r.coord = coord
	return r, nil
}

// ReadPreparedBarrier是Agent的纯读取入口。Node与Lease均由固定可信客户端
// GET，完整Prepare/原关闭/VM域水位和原Pod/Node绑定交叉认证；不相信404、
// 外层ReserveHost Planned或CM自报SourceID，也绝不在此创建/封存来源。
func (r *Reader) ReadPreparedBarrier(ctx context.Context, s *journal.Snapshot) (budget.HostReservationBarrier, error) {
	var empty budget.HostReservationBarrier
	if r.coord == nil || s == nil || !s.Dispatchable() || s.VirtualMachine().UID != r.binding.VMUID {
		return empty, fmt.Errorf("prepared barrier requires original running VM and fixed coordination reader")
	}
	m, vm := s.State(), s.VirtualMachine()
	closure, err := m.PreparedHostClosure()
	if err != nil {
		return empty, err
	}
	prepared := closure.Prepared
	pod, err := r.core.Pods(r.binding.Namespace).Get(ctx, prepared.RunnerPodName, metav1.GetOptions{})
	if err != nil {
		return empty, err
	}
	if pod == nil {
		return empty, fmt.Errorf("prepared barrier original Runner Pod unavailable")
	}
	owner := metav1.GetControllerOf(pod)
	if pod.UID == "" || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Status.Phase != core.PodRunning || pod.Spec.NodeName != vm.Status.Node || vm.Status.PodName != pod.Name || vm.Status.RestartCount != prepared.RunnerRestartCount || pod.Status.PodIP != vm.Status.PodIP || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != r.binding.Name || owner.UID != r.binding.VMUID {
		return empty, fmt.Errorf("prepared barrier changed original live Runner/VM binding")
	}
	node, err := r.core.Nodes().Get(ctx, vm.Status.Node, metav1.GetOptions{})
	if err != nil {
		return empty, err
	}
	if node == nil || node.DeletionTimestamp != nil || node.ResourceVersion == "" {
		return empty, fmt.Errorf("prepared barrier original Node unavailable")
	}
	var ledger hostguard.Model
	if json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger) != nil || ledger.Binding.NodeName != node.Name || ledger.Binding.NodeUID != node.UID || ledger.Profile == nil {
		return empty, fmt.Errorf("prepared barrier original Node ledger binding unavailable")
	}
	plan := hostguard.Plan{Namespace: r.binding.Namespace, VMName: r.binding.Name, Incarnation: r.binding.Incarnation, RunnerPodUID: string(pod.UID), Request: prepared.Request, BeforeBytes: prepared.VMBeforeMemoryBytes, HostOverheadBytes: ledger.Profile.VMOverheadBytes}
	store, err := hostguard.NewStore(r.core, r.coord, ledger.Binding, "read-only-prepared-barrier")
	if err != nil {
		return empty, err
	}
	b, err := store.ReadPreparedBarrier(ctx, plan)
	if err != nil {
		return empty, err
	}
	if b.NodeUID != string(node.UID) || m.CheckPreparedHostBarrier(b) != nil {
		return empty, fmt.Errorf("prepared barrier changed original Node or VM closure sources")
	}
	return b, nil
}
