package hostguard

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
)

// ObserveRetainedOrdinary只确认或补全已经绑定的原普通Pod完整保守占账。
// 未知终态不证明退出，不发调度许可；原保护/退休状态不变且旧额度绝不减少。
func (s *Store) ObserveRetainedOrdinary(ctx context.Context, snapshot *Snapshot, expected *core.Pod) (Placement, error) {
	var none Placement
	if snapshot == nil || snapshot.model.Binding != s.binding || snapshot.model.Profile == nil || expected == nil || expected.UID == "" || expected.ResourceVersion == "" || expected.Spec.NodeName != s.binding.NodeName || (expected.DeletionTimestamp == nil && expected.Status.Phase != core.PodSucceeded && expected.Status.Phase != core.PodFailed) {
		return none, fmt.Errorf("retained ordinary observation requires original closed assigned Pod and Host snapshot")
	}
	p, rowErr := snapshot.model.Placement(string(expected.UID))
	if _, hasVM := vmv1.VirtualMachineOwnerForPod(expected); hasVM {
		return none, fmt.Errorf("VM Pod cannot use retained ordinary observation")
	}
	if _, _, migration := vmv1.MigrationOwnerForPod(expected); migration {
		return none, fmt.Errorf("migration cannot use retained ordinary observation")
	}
	actual, err := PodMemoryLimit(expected)
	if err != nil {
		return none, err
	}
	if rowErr == nil {
		if p.Managed || p.VMUID != "" || p.VMName != "" || p.Incarnation != "" || p.PodMemoryLimitBytes != 0 || p.Settled != nil || p.Barrier != nil || p.Namespace != expected.Namespace || p.PodName != expected.Name || p.HostOverheadBytes != snapshot.model.Profile.PodOverheadBytes || actual > p.MemoryBytes {
			return none, fmt.Errorf("retained ordinary observation requires unchanged existing ordinary allocation")
		}
	} else {
		// 只补已经绑定且仍有原保护的普通Pod，不复活任何已有退休意图。
		// 容器Unknown保持Unknown，实际硬limit来自完整Pod而非调用方数值。
		if !slices.Contains(expected.Finalizers, PodProtectionFinalizer) || expected.Annotations[RetirementAnnotation] != "" {
			return none, fmt.Errorf("missing ordinary allocation requires original protection without retirement intent")
		}
		p = Placement{Namespace: expected.Namespace, PodName: expected.Name, PodUID: string(expected.UID), MemoryBytes: actual, HostOverheadBytes: snapshot.model.Profile.PodOverheadBytes}
	}
	if _, err := p.Bytes(); err != nil {
		return none, err
	}
	// 与原多写者容量CAS共享Lease→Node→Lease，绝不抬floor或减少任一行。
	n, total, err := s.placementNode(ctx, snapshot)
	if err != nil {
		return none, err
	}
	if used, err := snapshot.model.UsedBytes(); err != nil || used > total {
		return none, fmt.Errorf("retained Host allocations exceed current Node capacity")
	}
	pod, err := s.core.Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return none, err
	}
	if pod.UID != expected.UID || pod.ResourceVersion != expected.ResourceVersion || pod.Namespace != expected.Namespace || pod.Name != expected.Name || !reflect.DeepEqual(pod.Spec, expected.Spec) || !reflect.DeepEqual(pod.OwnerReferences, expected.OwnerReferences) || !reflect.DeepEqual(pod.Finalizers, expected.Finalizers) || pod.Annotations[RetirementAnnotation] != expected.Annotations[RetirementAnnotation] || !reflect.DeepEqual(pod.DeletionTimestamp, expected.DeletionTimestamp) || pod.Status.Phase != expected.Status.Phase {
		return none, fmt.Errorf("retained original Pod changed across Host observation")
	}
	if rowErr == nil {
		return p, nil
	}
	// 只单调新增完整保守行；原256条/容量/overflow/Node UID-RV门均不旁路。
	next, err := snapshot.model.reservePlacementFor(p, nil, total)
	if err != nil {
		return none, err
	}
	confirmed, err := s.write(ctx, n, next, false)
	if err != nil {
		return none, err
	}
	return confirmed.model.Placement(p.PodUID)
}

// ObserveRetiredOrdinary只确认正式退休已闭合的同一普通Pod。缺行/404/终态
// 都不单独代表退休；原完整摘要、绑定、Node origin/序号及无行/峰值须同时成立。
func (s *Store) ObserveRetiredOrdinary(ctx context.Context, snapshot *Snapshot, expected *core.Pod) error {
	if snapshot == nil || snapshot.model.Binding != s.binding || snapshot.model.Profile == nil || expected == nil || expected.UID == "" || expected.ResourceVersion == "" || expected.Spec.NodeName != s.binding.NodeName || (expected.Status.Phase != core.PodSucceeded && expected.Status.Phase != core.PodFailed) || slices.Contains(expected.Finalizers, PodProtectionFinalizer) {
		return fmt.Errorf("confirmed retired ordinary event requires original complete unprotected terminal Pod")
	}
	if _, hasVM := vmv1.VirtualMachineOwnerForPod(expected); hasVM {
		return fmt.Errorf("VM cannot use retired ordinary event")
	}
	if _, _, migration := vmv1.MigrationOwnerForPod(expected); migration {
		return fmt.Errorf("migration cannot use retired ordinary event")
	}
	retired, err := InspectRetirement(expected)
	if err != nil {
		return err
	}
	p := retired.Placement
	if retired.Binding != s.binding || retired.Origin != snapshot.model.Origin || p.Managed || p.VMUID != "" || p.VMName != "" || p.Incarnation != "" || p.PodMemoryLimitBytes != 0 || p.HostOverheadBytes != snapshot.model.Profile.PodOverheadBytes {
		return fmt.Errorf("retired ordinary proof changed configured Host domain or classification")
	}
	actual, err := PodMemoryLimit(expected)
	if err != nil || actual > p.MemoryBytes {
		return fmt.Errorf("retired ordinary Pod hardlimit changed original complete allocation")
	}
	if err := InspectRetiredPod(snapshot.node, expected); err != nil {
		return err
	}
	if _, _, err := s.placementNode(ctx, snapshot); err != nil {
		return err
	}
	pod, err := s.core.Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if pod.UID != expected.UID || pod.ResourceVersion != expected.ResourceVersion || pod.Namespace != expected.Namespace || pod.Name != expected.Name || !reflect.DeepEqual(pod.Spec, expected.Spec) || !reflect.DeepEqual(pod.OwnerReferences, expected.OwnerReferences) || !reflect.DeepEqual(pod.Finalizers, expected.Finalizers) || pod.Annotations[RetirementAnnotation] != expected.Annotations[RetirementAnnotation] {
		return fmt.Errorf("retired ordinary event changed across original Host observation")
	}
	return InspectRetiredPod(snapshot.node, pod)
}
