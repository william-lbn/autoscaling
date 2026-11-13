package hostscheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/neondatabase/autoscaling/pkg/util/patch"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
)

// 未绑定发现独立于已分配循环：Reserve可先保护Pod再因容量拒绝，因此不能
// 仅扫描已绑定Node。单轮最多两项/两秒，保留分页与待办进度，让坏对象既不
// 饿死尾页，也不耗尽同轮已运行VM的原Host结算预算。
func (s *Service) reconcileUnbound(ctx context.Context, d domain) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	w := d.work
	if len(w.unboundPending) == 0 && w.unboundEnd {
		w.unboundEnd = false
	}
	var failures []error
	pages, attempted := 0, 0
	for attempted < 2 && ctx.Err() == nil {
		if len(w.unboundPending) == 0 {
			if w.unboundEnd || pages == maxWorkPages {
				break
			}
			list, err := s.core.Pods("").List(ctx, metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("spec.nodeName", "").String(), Limit: workPageSize, Continue: w.unboundContinuation})
			if err != nil {
				if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
					w.unboundContinuation = ""
				}
				return errors.Join(append(failures, err)...)
			}
			if list == nil || len(list.Items) > workPageSize || w.unboundContinuation != "" && list.Continue == w.unboundContinuation {
				return fmt.Errorf("unbound Pod discovery page is invalid")
			}
			pages++
			w.unboundContinuation, w.unboundEnd = list.Continue, list.Continue == ""
			for i := range list.Items {
				p := &list.Items[i]
				if p.Spec.NodeName != "" {
					return fmt.Errorf("unbound discovery selector changed")
				}
				if p.Spec.SchedulerName == d.config.Profile.SchedulerName && p.DeletionTimestamp != nil && p.Status.Phase == core.PodFailed && slices.Contains(p.Finalizers, hostguard.PodProtectionFinalizer) {
					w.unboundPending = append(w.unboundPending, *p)
				}
			}
			if len(w.unboundPending) == 0 {
				continue
			}
		}
		pod := w.unboundPending[0]
		w.unboundPending[0] = core.Pod{}
		w.unboundPending = w.unboundPending[1:]
		attempted++
		if err := s.retireUnbound(ctx, d, &pod); err != nil {
			failures = append(failures, fmt.Errorf("unbound Pod %s/%s: %w", pod.Namespace, pod.Name, err))
		}
	}
	return errors.Join(failures...)
}

func (s *Service) unboundDomains() []hostguard.Binding {
	out := make([]hostguard.Binding, 0, len(s.domains))
	for _, d := range s.domains {
		out = append(out, d.config.Binding)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeName < out[j].NodeName })
	return out
}

func (s *Service) retireUnbound(ctx context.Context, d domain, expected *core.Pod) error {
	if expected == nil || expected.UID == "" {
		return fmt.Errorf("unbound discovery lacks original Pod")
	}
	pod, err := s.core.Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if pod.UID != expected.UID || pod.Spec.SchedulerName != d.config.Profile.SchedulerName {
		return fmt.Errorf("unbound Pod identity or scheduler changed")
	}
	digest, err := hostguard.UnboundPodDigest(pod)
	if err != nil {
		return err
	}
	owner := metav1.GetControllerOf(pod)
	vm, err := s.vm.NeonvmV1().VirtualMachines(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if vm.Spec.DSGABootstrap == nil {
		return fmt.Errorf("unbound closure requires managed VM")
	}
	domains := s.unboundDomains()
	r := hostguard.UnboundRetirement{ProtocolVersion: 1, Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), PodSHA256: digest, VMName: owner.Name, VMUID: string(owner.UID), Incarnation: vm.Spec.DSGABootstrap.Incarnation, Domains: domains, Confirmed: []hostguard.UnboundNode{}}
	if raw := pod.Annotations[hostguard.UnboundRetirementAnnotation]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			return err
		}
		if !slices.Equal(r.Domains, domains) {
			return fmt.Errorf("original unbound configured domains changed")
		}
	}
	if err := r.InspectPod(pod); err != nil {
		return err
	}
	if err := r.InspectVM(vm); err != nil {
		return err
	}
	if pod.Annotations[hostguard.RetirementAnnotation] != "" || vm.Annotations[hostguard.VMRetirementAnnotation] != "" {
		return fmt.Errorf("unbound closure conflicts with original Runner retirement")
	}
	binding := journal.Binding{Namespace: r.Namespace, Name: r.VMName, VMUID: types.UID(r.VMUID), Incarnation: r.Incarnation}
	if err := vmguard.ConfirmBirthSourcesAbsent(ctx, s.core, binding); err != nil {
		return err
	}
	found := false
	for _, proof := range r.Confirmed {
		if proof.Binding == d.config.Binding {
			found = true
			node, err := s.core.Nodes().Get(ctx, proof.Binding.NodeName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if err := hostguard.InspectUnboundNode(node, r, proof); err != nil {
				return err
			}
		}
	}
	if !found {
		n, err := d.store.Load(ctx)
		if err != nil {
			return err
		}
		if err := checkProfile(n, d.config); err != nil {
			return err
		}
		proof, err := d.store.SealUnboundAbsence(ctx, n, r)
		if err != nil {
			return err
		}
		r.Confirmed = append(r.Confirmed, proof)
		sort.Slice(r.Confirmed, func(i, j int) bool { return r.Confirmed[i].Binding.NodeName < r.Confirmed[j].Binding.NodeName })
		pod, err = s.patchUnboundPod(ctx, pod, r, false)
		if err != nil {
			return err
		}
	}
	if len(r.Confirmed) != len(r.Domains) {
		return nil
	}
	if err := r.Validate(true); err != nil {
		return err
	}
	for _, proof := range r.Confirmed {
		node, err := s.core.Nodes().Get(ctx, proof.Binding.NodeName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if err := hostguard.InspectUnboundNode(node, r, proof); err != nil {
			return err
		}
	}
	// 所有Node屏障确认后重读原VM，日志仍须是首次初始化状态。先写独立
	// VM关闭锚点并读回，再让Pod消失；Controller据此在GC后恢复同一关闭。
	vm, err = s.vm.NeonvmV1().VirtualMachines(r.Namespace).Get(ctx, r.VMName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := r.InspectVM(vm); err != nil {
		return err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if prior := vm.Annotations[hostguard.UnboundRetirementAnnotation]; prior != "" && prior != string(raw) {
		return fmt.Errorf("original VM has another never-bound closure")
	}
	if vm.Annotations[hostguard.UnboundRetirementAnnotation] == "" {
		annotations := map[string]string{}
		for k, v := range vm.Annotations {
			annotations[k] = v
		}
		annotations[hostguard.UnboundRetirementAnnotation] = string(raw)
		ops := patch.JSONPatch{{Op: patch.OpTest, Path: "/metadata/uid", Value: vm.UID}, {Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: vm.ResourceVersion}, {Op: patch.OpAdd, Path: "/metadata/annotations", Value: annotations}}
		payload, _ := json.Marshal(ops)
		_, writeErr := s.vm.NeonvmV1().VirtualMachines(r.Namespace).Patch(ctx, r.VMName, types.JSONPatchType, payload, metav1.PatchOptions{})
		actual, readErr := s.vm.NeonvmV1().VirtualMachines(r.Namespace).Get(ctx, r.VMName, metav1.GetOptions{})
		if readErr != nil {
			return errors.Join(writeErr, readErr)
		}
		if r.InspectVM(actual) != nil || actual.Annotations[hostguard.UnboundRetirementAnnotation] != string(raw) {
			return errors.Join(writeErr, fmt.Errorf("never-bound VM anchor is unconfirmed"))
		}
	}
	pod, err = s.core.Pods(r.Namespace).Get(ctx, r.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := r.InspectPod(pod); err != nil {
		return err
	}
	_, err = s.patchUnboundPod(ctx, pod, r, true)
	return err
}

func (s *Service) patchUnboundPod(ctx context.Context, pod *core.Pod, r hostguard.UnboundRetirement, release bool) (*core.Pod, error) {
	if err := r.InspectPod(pod); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	annotations := map[string]string{}
	for k, v := range pod.Annotations {
		annotations[k] = v
	}
	annotations[hostguard.UnboundRetirementAnnotation] = string(raw)
	ops := patch.JSONPatch{{Op: patch.OpTest, Path: "/metadata/uid", Value: pod.UID}, {Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: pod.ResourceVersion}, {Op: patch.OpAdd, Path: "/metadata/annotations", Value: annotations}}
	if release {
		if r.Validate(true) != nil || pod.Annotations[hostguard.UnboundRetirementAnnotation] != string(raw) {
			return nil, fmt.Errorf("unbound Pod completion is unconfirmed")
		}
		ops = append(ops, patch.Operation{Op: patch.OpReplace, Path: "/metadata/finalizers", Value: slices.DeleteFunc(slices.Clone(pod.Finalizers), func(f string) bool { return f == hostguard.PodProtectionFinalizer })})
	}
	payload, _ := json.Marshal(ops)
	_, writeErr := s.core.Pods(pod.Namespace).Patch(ctx, pod.Name, types.JSONPatchType, payload, metav1.PatchOptions{})
	actual, readErr := s.core.Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if release && apierrors.IsNotFound(readErr) {
		return nil, nil
	}
	if readErr != nil {
		return nil, errors.Join(writeErr, readErr)
	}
	if r.InspectPod(actual) != nil || actual.Annotations[hostguard.UnboundRetirementAnnotation] != string(raw) || release && slices.Contains(actual.Finalizers, hostguard.PodProtectionFinalizer) {
		return nil, errors.Join(writeErr, fmt.Errorf("never-bound Pod write is unconfirmed"))
	}
	return actual, nil
}
