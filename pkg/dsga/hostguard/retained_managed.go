package hostguard

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ObserveRetainedManaged只读确认关闭中原受管Pod已经持久存在的完整保守占账。
// Unknown、缺VM/Birth或尚未完成的退休不等于退出；本函数不查询/构造执行结果，
// 不补行、不改任何水位/峰值/保护，也不发调度、发布或退款许可。调用方只能把
// 返回的原Placement用于事件恢复本地计费视图；真正退出仍由原退役事务证明。
func (s *Store) ObserveRetainedManaged(ctx context.Context, snapshot *Snapshot, expected *core.Pod) (Placement, error) {
	var none Placement
	if snapshot == nil || snapshot.model.Binding != s.binding || snapshot.model.ProtocolVersion < 10 || snapshot.model.Profile == nil || expected == nil || expected.UID == "" || expected.ResourceVersion == "" || expected.Spec.NodeName != s.binding.NodeName || expected.Spec.SchedulerName != snapshot.model.Profile.SchedulerName || (expected.DeletionTimestamp == nil && expected.Status.Phase != core.PodSucceeded && expected.Status.Phase != core.PodFailed) || !slices.Contains(expected.Finalizers, PodProtectionFinalizer) {
		return none, fmt.Errorf("retained managed observation requires original protected closed Pod and complete Host floor")
	}
	p, err := snapshot.model.Placement(string(expected.UID))
	if err != nil {
		return none, fmt.Errorf("retained managed original allocation is absent; observation cannot create it")
	}
	owner := metav1.GetControllerOf(expected)
	if !p.Managed || p.PodMemoryLimitBytes == 0 || p.Namespace != expected.Namespace || p.PodName != expected.Name || p.HostOverheadBytes != snapshot.model.Profile.VMOverheadBytes || len(expected.OwnerReferences) != 1 || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != p.VMName || string(owner.UID) != p.VMUID {
		return none, fmt.Errorf("retained managed original controller/allocation differs")
	}
	limit, err := PodMemoryLimit(expected)
	if err != nil || limit != p.PodMemoryLimitBytes {
		return none, fmt.Errorf("retained managed original Pod hardlimit floor differs")
	}
	if _, err := p.Bytes(); err != nil {
		return none, err
	}
	// 与已存在ordinary观察相同，先核新鲜原Lease→Node UID/RV→Lease；
	// 这仅确认本次原账本读回，绝不把未知VM状态转换成物理Completed。
	_, capacity, err := s.placementNode(ctx, snapshot)
	if err != nil {
		return none, err
	}
	if used, err := snapshot.model.UsedBytes(); err != nil || used > capacity {
		return none, fmt.Errorf("retained managed Host commitments exceed current capacity")
	}
	pod, err := s.core.Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return none, err
	}
	if pod.UID != expected.UID || pod.ResourceVersion != expected.ResourceVersion || pod.Namespace != expected.Namespace || pod.Name != expected.Name || !reflect.DeepEqual(pod.Spec, expected.Spec) || !reflect.DeepEqual(pod.OwnerReferences, expected.OwnerReferences) || !reflect.DeepEqual(pod.Finalizers, expected.Finalizers) || !reflect.DeepEqual(pod.Annotations, expected.Annotations) || !reflect.DeepEqual(pod.DeletionTimestamp, expected.DeletionTimestamp) || pod.Status.Phase != expected.Status.Phase {
		return none, fmt.Errorf("retained managed Pod changed across original Host observation")
	}
	// 关闭事务可能在Pod读取期间提交Node CAS。不能用已删除/更改的行完成
	// 初始观察；若最后读取后发生变化，最多本地暂时多计，永不释放原账本。
	node, err := s.node(ctx)
	if err != nil {
		return none, err
	}
	if node.ResourceVersion != snapshot.ResourceVersion() || node.DeletionTimestamp != nil || ctx.Err() != nil {
		return none, fmt.Errorf("retained managed Node changed during original observation")
	}
	return p, nil
}
