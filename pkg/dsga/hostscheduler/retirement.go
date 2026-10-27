package hostscheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/neondatabase/autoscaling/pkg/util/patch"
)

// protectPod在调度成功前保存对象保护，保证删除请求先停容器，再由节点owner
// 结清容量，最后才允许原Pod消失。对原UID/RV做条件PATCH并真实GET恢复未知；
// 不把内存副本中的VM资源传播注解写回或用Patch响应当作容量授予。
func (s *Service) protectPod(ctx context.Context, pod *core.Pod) (*core.Pod, error) {
	if slices.Contains(pod.Finalizers, hostguard.PodProtectionFinalizer) {
		return pod, nil
	}
	finalizers := append(slices.Clone(pod.Finalizers), hostguard.PodProtectionFinalizer)
	ops := patch.JSONPatch{{Op: patch.OpTest, Path: "/metadata/uid", Value: pod.UID}, {Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: pod.ResourceVersion}, {Op: patch.OpAdd, Path: "/metadata/finalizers", Value: finalizers}}
	raw, err := json.Marshal(ops)
	if err != nil {
		return nil, err
	}
	_, writeErr := s.core.Pods(pod.Namespace).Patch(ctx, pod.Name, types.JSONPatchType, raw, metav1.PatchOptions{})
	actual, err := s.core.Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Join(writeErr, err)
	}
	if actual.UID != pod.UID || actual.ResourceVersion == "" || actual.DeletionTimestamp != nil || !slices.Contains(actual.Finalizers, hostguard.PodProtectionFinalizer) {
		return nil, fmt.Errorf("original Pod allocation protection is unconfirmed")
	}
	canonical := pod.DeepCopy()
	canonical.ResourceVersion, canonical.Finalizers = actual.ResourceVersion, slices.Clone(actual.Finalizers)
	return canonical, nil
}

func (s *Service) terminalRunner(ctx context.Context, d domain, pod *core.Pod, p hostguard.Placement, expected *hostguard.Retirement) (budget.ObjectRef, budget.ObjectRef, string, error) {
	var none budget.ObjectRef
	binding := journal.Binding{Namespace: p.Namespace, Name: p.VMName, VMUID: types.UID(p.VMUID), Incarnation: p.Incarnation}
	vm, err := s.vm.NeonvmV1().VirtualMachines(p.Namespace).Get(ctx, p.VMName, metav1.GetOptions{})
	if err != nil {
		return none, none, "", err
	}
	if vm.UID != binding.VMUID || vm.Spec.DSGABootstrap == nil || vm.Spec.DSGABootstrap.Incarnation != p.Incarnation || vm.ResourceVersion == "" {
		return none, none, "", fmt.Errorf("retiring Runner original VM changed")
	}
	birthCM, err := s.core.ConfigMaps(p.Namespace).Get(ctx, vmguard.BindingName(binding), metav1.GetOptions{})
	if err != nil {
		return none, none, "", err
	}
	var original *budget.ObjectRef
	if expected != nil {
		original = expected.RunnerBirth
	}
	birth, ref, err := vmguard.InspectBirthForExit(birthCM, binding, original)
	if err != nil {
		return none, none, "", err
	}
	if birth.Identity.RunnerPodUID != p.PodUID || birth.Identity.NodeUID != string(d.config.Binding.NodeUID) || pod.Spec.RestartPolicy != core.RestartPolicyNever {
		return none, none, "", fmt.Errorf("original stopped Runner Pod/Node/restart policy differs")
	}
	exitCM, err := s.core.ConfigMaps(p.Namespace).Get(ctx, vmguard.ExitClaimName(binding, birth), metav1.GetOptions{})
	if err != nil {
		return none, none, "", err
	}
	exited, err := vmguard.InspectProcessExit(exitCM, binding, birth, ref)
	if err != nil {
		return none, none, "", err
	}
	matched := false
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "neonvm-runner" && status.RestartCount == 0 && status.ContainerID == exited.ContainerID {
			matched = true
		}
	}
	if !matched {
		return none, none, "", fmt.Errorf("original stopped Runner container differs from child wait proof")
	}
	exitRef := budget.ObjectRef{Name: exitCM.Name, UID: string(exitCM.UID)}
	digest := vmguard.SourceDigest([]byte(exitCM.Data[vmguard.ClaimKey]))
	if expected != nil && (expected.RunnerExit == nil || *expected.RunnerExit != exitRef || expected.RunnerExitSHA != digest) {
		return none, none, "", fmt.Errorf("original immutable Runner exit source changed")
	}
	return ref, exitRef, digest, nil
}

func (s *Service) persistRetirement(ctx context.Context, pod *core.Pod, r hostguard.Retirement) (*core.Pod, error) {
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	annotations := map[string]string{}
	for k, v := range pod.Annotations {
		annotations[k] = v
	}
	annotations[hostguard.RetirementAnnotation] = string(raw)
	ops := patch.JSONPatch{{Op: patch.OpTest, Path: "/metadata/uid", Value: pod.UID}, {Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: pod.ResourceVersion}, {Op: patch.OpAdd, Path: "/metadata/annotations", Value: annotations}}
	body, err := json.Marshal(ops)
	if err != nil {
		return nil, err
	}
	_, writeErr := s.core.Pods(pod.Namespace).Patch(ctx, pod.Name, types.JSONPatchType, body, metav1.PatchOptions{})
	actual, err := s.core.Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Join(writeErr, err)
	}
	if actual.UID != pod.UID || actual.ResourceVersion == "" || actual.Annotations[hostguard.RetirementAnnotation] != string(raw) {
		return nil, fmt.Errorf("original Pod retirement intent is unconfirmed")
	}
	return actual, nil
}

// VM锚点必须先于Pod保护移除；否则GC可在Controller读取前删除原Pod，造成
// 已结清事实丢失。只在原UID/同incarnation写同一原文，不覆盖其他boot锚点。
// 这属于关闭元数据，不改VM操作WAL/Outcome；Agent仍不得把停止当成功resize。
func (s *Service) recordVMRetirement(ctx context.Context, r hostguard.Retirement) error {
	p := r.Placement
	vm, err := s.vm.NeonvmV1().VirtualMachines(p.Namespace).Get(ctx, p.VMName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(vm.UID) != p.VMUID || vm.ResourceVersion == "" || vm.Spec.DSGABootstrap == nil || vm.Spec.DSGABootstrap.Incarnation != p.Incarnation {
		return fmt.Errorf("retiring original VM identity changed")
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if prior := vm.Annotations[hostguard.VMRetirementAnnotation]; prior != "" {
		if prior != string(raw) {
			return fmt.Errorf("original VM retains another retirement; controlled recovery required")
		}
		return nil
	}
	annotations := map[string]string{}
	for k, v := range vm.Annotations {
		annotations[k] = v
	}
	annotations[hostguard.VMRetirementAnnotation] = string(raw)
	ops := patch.JSONPatch{{Op: patch.OpTest, Path: "/metadata/uid", Value: vm.UID}, {Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: vm.ResourceVersion}, {Op: patch.OpAdd, Path: "/metadata/annotations", Value: annotations}}
	body, err := json.Marshal(ops)
	if err != nil {
		return err
	}
	_, writeErr := s.vm.NeonvmV1().VirtualMachines(p.Namespace).Patch(ctx, p.VMName, types.JSONPatchType, body, metav1.PatchOptions{})
	actual, err := s.vm.NeonvmV1().VirtualMachines(p.Namespace).Get(ctx, p.VMName, metav1.GetOptions{})
	if err != nil {
		return errors.Join(writeErr, err)
	}
	if actual.UID != vm.UID || actual.ResourceVersion == "" || actual.Annotations[hostguard.VMRetirementAnnotation] != string(raw) {
		return fmt.Errorf("original VM retirement anchor is unconfirmed")
	}
	return nil
}

// retirePod是实际节点owner循环的明确停止结清。顺序固定为原Node快照/fence
// →原终态Pod/VM/Runner Wait来源→Pod关闭意图持久→来源重读→同Node CAS→
// 再确认原Node/来源→移除Pod保护。任一未知不删来源/不按超时释放；可能已发
// 的旧Resize仍保留原VM/Runner WAL，Guest停止不冒充该动作Completed。
func (s *Service) retirePod(ctx context.Context, d domain, expected *core.Pod) error {
	n, err := d.store.Load(ctx)
	if err != nil {
		return err
	}
	n, err = d.store.Fence(ctx, n)
	if err != nil {
		return err
	}
	pod, err := s.core.Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if pod.UID != expected.UID || pod.Spec.NodeName != d.config.Binding.NodeName || pod.Spec.SchedulerName != d.config.Profile.SchedulerName {
		return fmt.Errorf("original terminal Pod placement changed")
	}
	hash, err := hostguard.TerminalPodDigest(pod)
	if err != nil {
		return err
	}
	r, err := hostguard.InspectRetirement(pod)
	if pod.Annotations[hostguard.RetirementAnnotation] == "" {
		p, loadErr := n.State().Placement(string(pod.UID))
		if loadErr != nil {
			return loadErr
		}
		r = hostguard.Retirement{ProtocolVersion: 1, Binding: d.config.Binding, Origin: n.State().Origin, BeforeSequence: n.State().Sequence, Placement: p, TerminalSHA256: hash, RunnerBirth: nil, RunnerExit: nil, RunnerExitSHA: ""}
		if p.Managed {
			birth, exit, digest, sourceErr := s.terminalRunner(ctx, d, pod, p, nil)
			if sourceErr != nil {
				return sourceErr
			}
			r.RunnerBirth, r.RunnerExit, r.RunnerExitSHA = &birth, &exit, digest
		}
		if err := r.Validate(); err != nil {
			return err
		}
		pod, err = s.persistRetirement(ctx, pod, r)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if r.Binding != d.config.Binding || r.Origin != n.State().Origin {
		return fmt.Errorf("retirement belongs to another original Node domain")
	}
	if !slices.Contains(pod.Finalizers, hostguard.PodProtectionFinalizer) {
		return hostguard.InspectRetiredPod(n.Node(), pod)
	}
	if r.Placement.Managed {
		if _, _, _, err := s.terminalRunner(ctx, d, pod, r.Placement, &r); err != nil {
			return err
		}
	}
	if _, err := d.store.Retire(ctx, n, r); err != nil {
		return err
	}
	// 最终GET不能用CAS返回对象替代。回复丢失后下一轮按同一意图恢复；
	// 首次CAS升级v5使旧调度程序拒绝，新程序也须重读终态原Pod再授予。
	confirmed, err := d.store.Load(ctx)
	if err != nil {
		return err
	}
	confirmed, err = d.store.Fence(ctx, confirmed)
	if err != nil {
		return err
	}
	pod, err = s.core.Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := hostguard.InspectRetiredPod(confirmed.Node(), pod); err != nil {
		return err
	}
	if r.Placement.Managed {
		if _, _, _, err := s.terminalRunner(ctx, d, pod, r.Placement, &r); err != nil {
			return err
		}
		if err := s.recordVMRetirement(ctx, r); err != nil {
			return err
		}
	}
	finalizers := slices.DeleteFunc(slices.Clone(pod.Finalizers), func(f string) bool { return f == hostguard.PodProtectionFinalizer })
	ops := patch.JSONPatch{{Op: patch.OpTest, Path: "/metadata/uid", Value: pod.UID}, {Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: pod.ResourceVersion}, {Op: patch.OpReplace, Path: "/metadata/finalizers", Value: finalizers}}
	body, err := json.Marshal(ops)
	if err != nil {
		return err
	}
	_, err = s.core.Pods(pod.Namespace).Patch(ctx, pod.Name, types.JSONPatchType, body, metav1.PatchOptions{})
	return err
}
