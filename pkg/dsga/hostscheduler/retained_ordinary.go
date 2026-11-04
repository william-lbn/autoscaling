package hostscheduler

import (
	"context"
	"fmt"
	"reflect"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

func closedAssignedPod(pod *core.Pod) bool {
	return pod != nil && (pod.DeletionTimestamp != nil || pod.Status.Phase == core.PodSucceeded || pod.Status.Phase == core.PodFailed)
}

// observeRetainedOrdinary恢复或补全初始化/事件所需的完整保守计费视图。
// ContainerStatusUnknown不解释为已停止；原retirePod独占完整退出后的释放。
func (s *Service) observeRetainedOrdinary(ctx context.Context, d domain, expected *core.Pod) (*core.Pod, hostguard.Placement, error) {
	var none hostguard.Placement
	if expected == nil || expected.UID == "" || expected.ResourceVersion == "" || !closedAssignedPod(expected) {
		return nil, none, fmt.Errorf("original retained ordinary Pod identity is required")
	}
	pod, err := s.core.Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return nil, none, err
	}
	if pod.UID != expected.UID || pod.ResourceVersion == "" || pod.Spec.NodeName != d.config.Binding.NodeName || !reflect.DeepEqual(pod.Spec, expected.Spec) || !reflect.DeepEqual(pod.OwnerReferences, expected.OwnerReferences) || !closedAssignedPod(pod) {
		return nil, none, fmt.Errorf("retained ordinary Pod identity/Node/scheduler/resources changed")
	}
	snapshot, err := d.store.Load(ctx)
	if err != nil {
		return nil, none, err
	}
	if err := checkProfile(snapshot, d.config); err != nil {
		return nil, none, err
	}
	held, err := d.store.ObserveRetainedOrdinary(ctx, snapshot, pod)
	if err != nil {
		return nil, none, err
	}
	return pod.DeepCopy(), held, nil
}

// ObserveRetiredOrdinary供插件识别已由原退休事务闭合的普通终态事件。它不
// 返回伪零Placement或授予容量；普通/受管活体仍走原Reserve/Filter完整路径。
func (s *Service) ObserveRetiredOrdinary(ctx context.Context, node string, expected *core.Pod) (*core.Pod, error) {
	d, ok := s.domains[node]
	if !ok || expected == nil || expected.UID == "" || expected.ResourceVersion == "" {
		return nil, fmt.Errorf("confirmed retired event requires original configured Host and Pod")
	}
	pod, err := s.core.Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if pod.UID != expected.UID || pod.Namespace != expected.Namespace || pod.Name != expected.Name || pod.ResourceVersion == "" || pod.Spec.NodeName != d.config.Binding.NodeName || !reflect.DeepEqual(pod.Spec, expected.Spec) || !reflect.DeepEqual(pod.OwnerReferences, expected.OwnerReferences) {
		return nil, fmt.Errorf("confirmed retired ordinary identity/Node/scheduler/resources changed")
	}
	snapshot, err := d.store.Load(ctx)
	if err != nil {
		return nil, err
	}
	if err := checkProfile(snapshot, d.config); err != nil {
		return nil, err
	}
	if err := d.store.ObserveRetiredOrdinary(ctx, snapshot, pod); err != nil {
		return nil, err
	}
	return pod.DeepCopy(), nil
}
