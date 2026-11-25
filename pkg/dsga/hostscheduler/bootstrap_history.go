package hostscheduler

import (
	"context"
	"fmt"
	"reflect"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

// bootstrapHistory 只在 Store 已确认本域从未初始化后使用。不把其他系统
// 保留的已结束 Job 当活动资源，也不将“终态/404”当已有资源释放证明。
// List 与 GET 必须是同一原 UID/RV、Node、Spec 和完整退出证据；变化或读
// 取未知均拒绝本轮首次清点，下一轮重新取完整快照，绝不清空原账本。
func (s *Service) bootstrapHistory(ctx context.Context, node string, expected *core.Pod) error {
	if !hostguard.UnprotectedTerminalOrdinary(expected) || expected.Spec.NodeName != node {
		return fmt.Errorf("historical ordinary Pod is not a complete original terminal observation")
	}
	live, err := s.core.Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	before, _ := hostguard.TerminalPodDigest(expected)
	after, terminalErr := hostguard.TerminalPodDigest(live)
	if !hostguard.UnprotectedTerminalOrdinary(live) || terminalErr != nil || before != after ||
		live.UID != expected.UID || live.ResourceVersion != expected.ResourceVersion ||
		live.Namespace != expected.Namespace || live.Name != expected.Name || live.Spec.NodeName != node ||
		!reflect.DeepEqual(live.Spec, expected.Spec) || !reflect.DeepEqual(live.OwnerReferences, expected.OwnerReferences) ||
		!reflect.DeepEqual(live.Finalizers, expected.Finalizers) || !reflect.DeepEqual(live.Annotations, expected.Annotations) {
		return fmt.Errorf("historical ordinary Pod identity/resourceVersion/termination changed")
	}
	return nil
}

// ObserveUntrackedTerminalOrdinary 为插件初始事件与 Owner 发现提供单独的
// 只读入口。已有账本必须可认证且无该 Pod 的 Placement/Hold，不能用一个
// 零值 Placement 冒充成功授予，也不借此绕过受管 VM 的退休事务。
func (s *Service) ObserveUntrackedTerminalOrdinary(ctx context.Context, node string, pod *core.Pod) error {
	d, ok := s.domains[node]
	if !ok || pod == nil || pod.Spec.NodeName != node {
		return fmt.Errorf("historical observation requires the original configured Host")
	}
	snapshot, err := d.store.Load(ctx)
	if err != nil {
		return err
	}
	if err = checkProfile(snapshot, d.config); err != nil {
		return err
	}
	return d.store.ObserveUntrackedTerminalOrdinary(ctx, snapshot, pod)
}
