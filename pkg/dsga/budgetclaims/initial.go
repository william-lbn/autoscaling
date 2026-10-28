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

// InitialSources来自原平台资格及真实Node初始placement，不假造某次Resize的
// HostReservation。仅是一次跨对象观察，无开放/派发/结清权限；PG/root/Runner
// 仍须各自认证，最终VM UID/RV CAS包围重读。Node其他VM的正常活动不锁全局。
type InitialSources struct {
	Qualification     budget.PlatformQualification
	HostProfileID     string
	HostMemoryBytes   control.Uint64
	HostOverheadBytes control.Uint64
	pod               *core.Pod
	node              *core.Node
}

// ReadInitialPlacement不相信Pod传播注解或调用者自报的Host额度。vmBytes须由
// 调用者认证同Runner的原生实际容量；本函数核原Node持有覆盖它，不发QMP，也
// 不把这一个维度的成功提升为完整首次服务许可。
func (r *Reader) ReadInitialPlacement(ctx context.Context, s *journal.Snapshot, source budget.QualificationSource, vmBytes control.Uint64) (*InitialSources, error) {
	if s == nil || !s.Dispatchable() || s.VirtualMachine().UID != r.binding.VMUID || s.State().ResourceIdentity == nil || s.State().PGInstance == nil || vmBytes == 0 {
		return nil, fmt.Errorf("initial sources require original live bound Guest/PG/VM")
	}
	q, err := r.Qualification(ctx, source)
	if err != nil {
		return nil, err
	}
	m, vm := s.State(), s.VirtualMachine()
	if q.Identity != *m.ResourceIdentity || q.Identity.ComputeInstanceID != m.PGInstance.ComputeInstanceID || q.RunnerPodName != vm.Status.PodName || q.RunnerRestartCount != vm.Status.RestartCount || q.NodeName != vm.Status.Node {
		return nil, fmt.Errorf("initial qualification belongs to another original Guest/Runner")
	}
	if _, err := q.Target(vmBytes); err != nil {
		return nil, err
	}
	pod, err := r.core.Pods(r.binding.Namespace).Get(ctx, q.RunnerPodName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if pod == nil {
		return nil, fmt.Errorf("initial original Runner Pod unavailable")
	}
	owner := metav1.GetControllerOf(pod)
	if pod.Namespace != r.binding.Namespace || pod.Name != q.RunnerPodName || string(pod.UID) != q.RunnerPodUID || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Status.Phase != core.PodRunning || pod.Status.PodIP != vm.Status.PodIP || pod.Spec.NodeName != q.NodeName || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != r.binding.Name || owner.UID != r.binding.VMUID {
		return nil, fmt.Errorf("initial original live Runner Pod changed")
	}
	node, err := r.core.Nodes().Get(ctx, q.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if node == nil || node.Name != q.NodeName || string(node.UID) != q.NodeUID || node.ResourceVersion == "" || node.DeletionTimestamp != nil {
		return nil, fmt.Errorf("initial original Host Node changed")
	}
	if err := hostguard.InspectInitialVM(node, r.binding, q.RunnerPodName, q.RunnerPodUID, vmBytes); err != nil {
		return nil, err
	}
	var ledger hostguard.Model
	if json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger) != nil || ledger.Profile == nil {
		return nil, fmt.Errorf("initial complete Host profile unavailable")
	}
	placement, err := ledger.Placement(q.RunnerPodUID)
	if err != nil {
		return nil, err
	}
	if placement.HostOverheadBytes != ledger.Profile.VMOverheadBytes {
		return nil, fmt.Errorf("initial placement changed qualified Host overhead")
	}
	return &InitialSources{Qualification: q, HostProfileID: ledger.Profile.ID, HostMemoryBytes: placement.MemoryBytes, HostOverheadBytes: placement.HostOverheadBytes, pod: pod, node: node}, nil
}

// ReadInitialBudget核原启动预算中固定的资格/Pod/Node及当前完整Host持有。
// 允许后续保守峰值大于原值，但不能小于真实VM或改变原开销资格；不据此
// 退还任何容量，也不将旧初始placement冒充后续Host操作来源。
func (r *Reader) ReadInitialBudget(ctx context.Context, s *journal.Snapshot, i budget.Initial) (*InitialSources, error) {
	if i.Validate() != nil || i.Instance.VMUID != string(r.binding.VMUID) || i.Incarnation != r.binding.Incarnation || s == nil || s.State().PGInstance == nil || *s.State().PGInstance != i.Instance {
		return nil, fmt.Errorf("initial budget differs from original durable VM/PG")
	}
	observed, err := r.ReadInitialPlacement(ctx, s, i.Qualification, i.VMMemoryBytes)
	if err != nil {
		return nil, err
	}
	if i.MatchQualification(observed.Qualification) != nil || observed.HostProfileID != i.HostProfileID || observed.HostOverheadBytes != i.HostOverheadBytes {
		return nil, fmt.Errorf("original initial qualification or Host profile changed")
	}
	return observed, nil
}

// 重读包围其他域请求；原资格内容由固定摘要重新核验，Pod/Node UID不能替换。
// Node RV/其他placement可前进，但本VM仍须完整持有；不因心跳误停，也不因
// 来源丢失创建替代声明。本结构没有可由调用者修改的持有票据。
func (r *Reader) RecheckInitialBudget(ctx context.Context, s *journal.Snapshot, i budget.Initial, original *InitialSources) error {
	if original == nil {
		return fmt.Errorf("initial original source observation missing")
	}
	next, err := r.ReadInitialBudget(ctx, s, i)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(next.Qualification, original.Qualification) || next.HostProfileID != original.HostProfileID || next.HostOverheadBytes != original.HostOverheadBytes || next.pod.UID != original.pod.UID || next.node.UID != original.node.UID {
		return fmt.Errorf("initial original source changed across domain verification")
	}
	return nil
}
