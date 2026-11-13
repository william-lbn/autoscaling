package hostscheduler

import (
	"context"
	"fmt"
	"reflect"

	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ObserveRetainedManaged只供已绑定关闭Pod的事件恢复，不接入Framework Reserve。
// 原VM/Birth未知不妨碍继续计算原完整成本，但缺原行/保护/硬限仍必须失败关闭。
// 返回原Pod及原Placement；不改传播注解、不按ordinary小requests重新分类。
func (s *Service) ObserveRetainedManaged(ctx context.Context, node string, expected *core.Pod) (*core.Pod, hostguard.Placement, error) {
	var none hostguard.Placement
	d, ok := s.domains[node]
	if !ok || expected == nil || expected.UID == "" || expected.ResourceVersion == "" || !closedAssignedPod(expected) || expected.Spec.NodeName != node || expected.Spec.SchedulerName != d.config.Profile.SchedulerName {
		return nil, none, fmt.Errorf("retained managed event needs original assigned closed Pod")
	}
	pod, err := s.core.Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return nil, none, err
	}
	if pod.UID != expected.UID || pod.ResourceVersion == "" || pod.Namespace != expected.Namespace || pod.Name != expected.Name || !reflect.DeepEqual(pod.Spec, expected.Spec) || !reflect.DeepEqual(pod.OwnerReferences, expected.OwnerReferences) || !closedAssignedPod(pod) {
		return nil, none, fmt.Errorf("retained managed original Pod changed before observation")
	}
	snapshot, err := d.store.Load(ctx)
	if err != nil {
		return nil, none, err
	}
	if err := checkProfile(snapshot, d.config); err != nil {
		return nil, none, err
	}
	held, err := d.store.ObserveRetainedManaged(ctx, snapshot, pod)
	if err != nil {
		return nil, none, err
	}
	return pod.DeepCopy(), held, nil
}
