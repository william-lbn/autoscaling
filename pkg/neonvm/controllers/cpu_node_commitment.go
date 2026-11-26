package controllers

import (
	"context"
	"fmt"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	core "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

func (r *VMReconciler) verifyCPUNodeCommitment(ctx context.Context, vm *vmv1.VirtualMachine, pod *core.Pod,
	identity runnercpu.Identity, observed runnercpu.NodeCommitment) error {
	bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if r.APIReader == nil || observed.Validate() != nil {
		return fmt.Errorf("original CPU Node commitment source unavailable")
	}
	maximum, committed, err := vm.Spec.DSGARuntimeCPUBudget()
	if err != nil {
		return err
	}
	node := new(core.Node)
	if err = r.APIReader.Get(bounded, client.ObjectKey{Name: pod.Spec.NodeName}, node); err != nil {
		return err
	}
	if string(node.UID) != identity.NodeUID {
		return fmt.Errorf("original CPU Node UID changed")
	}
	current, err := hostguard.InspectCPUCommitment(node, journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID,
		Incarnation: vm.Spec.DSGABootstrap.Incarnation}, pod.Name, string(pod.UID), maximum, committed)
	if err != nil {
		return err
	}
	currentPod := new(core.Pod)
	if err = r.APIReader.Get(bounded, client.ObjectKeyFromObject(pod), currentPod); err != nil {
		return err
	}
	if currentPod.UID != pod.UID || currentPod.ResourceVersion == "" || currentPod.DeletionTimestamp != nil ||
		currentPod.Spec.NodeName != node.Name {
		return fmt.Errorf("original CPU Pod identity changed during commitment verification")
	}
	request, err := hostguard.PodCPURequest(currentPod)
	if err != nil || uint64(request) > current.CommittedMillicores {
		return fmt.Errorf("current whole-Pod CPU requests exceed original Node hold")
	}
	found := false
	for _, container := range currentPod.Spec.Containers {
		if container.Name != "neonvm-runner" {
			continue
		}
		found = true
		actualRequest, requestErr := vmv1.ExactRuntimeMillicores(*container.Resources.Requests.Cpu())
		actualLimit, limitErr := vmv1.ExactRuntimeMillicores(*container.Resources.Limits.Cpu())
		if requestErr != nil || limitErr != nil || actualRequest != committed || actualLimit != committed {
			return fmt.Errorf("current original Runner CPU resources differ from fixed commitment")
		}
	}
	if !found {
		return fmt.Errorf("current original Runner CPU container unavailable")
	}
	if current.LedgerOrigin != observed.LedgerOrigin || observed.LedgerSequence > current.LedgerSequence ||
		observed.CommittedMillicores != current.CommittedMillicores {
		return fmt.Errorf("CPU receipt differs from current original same-Pod Node commitment")
	}
	return nil
}
