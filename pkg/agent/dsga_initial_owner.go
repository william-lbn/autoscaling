package agent

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/neondatabase/autoscaling/pkg/util"
	"github.com/neondatabase/autoscaling/pkg/util/watch"
)

// 首次Owner先于QEMU。只把真实已调度Pod交给现有per-VM Runner；不改VM status、
// 不创建第二套Lease或Session。首次Pod选择固定到原UID，终止/替换不能重选。
const dsgaInitialStartupTimeout = 5 * time.Minute

type dsgaInitialSelection struct {
	pod       util.NamespacedName
	uid       string
	handedOff bool
}

func dsgaInitialCandidate(vm *vmv1.VirtualMachine, config *Config) bool {
	if vm == nil || config == nil || config.DSGA == nil || vm.UID == "" || vm.DeletionTimestamp != nil || vm.Spec.DSGABootstrap == nil || !vm.Spec.DSGABootstrap.Ready || vm.Status.PodName == "" || !api.HasAutoscalingEnabled(vm) || vm.Spec.SchedulerName != config.Scheduler.SchedulerName {
		return false
	}
	held, err := vm.DSGABootstrapHeld()
	return err == nil && !held && control.ValidateManagedTransition(vm.Annotations, vm.Annotations, false) == nil && (vm.Status.Phase == vmv1.VmPending || vm.Status.Phase == vmv1.VmRunning || vm.Status.Phase == vmv1.VmScaling)
}

func dsgaOriginalPod(vm *vmv1.VirtualMachine, pod *core.Pod, node, uid, ip string) error {
	if vm == nil || pod == nil || vm.DeletionTimestamp != nil || pod.DeletionTimestamp != nil || pod.UID == "" || pod.ResourceVersion == "" || pod.Namespace != vm.Namespace || pod.Name != vm.Status.PodName || pod.Spec.NodeName != node || pod.Status.PodIP == "" || (pod.Status.Phase != core.PodPending && pod.Status.Phase != core.PodRunning) {
		return fmt.Errorf("original DSGA scheduled Pod is unconfirmed")
	}
	parent := metav1.GetControllerOf(pod)
	if len(pod.OwnerReferences) != 1 || parent == nil || parent.Kind != "VirtualMachine" || parent.APIVersion != vmv1.SchemeGroupVersion.String() || parent.Name != vm.Name || parent.UID != vm.UID || uid != "" && string(pod.UID) != uid || ip != "" && pod.Status.PodIP != ip {
		return fmt.Errorf("original DSGA Pod identity changed")
	}
	if vm.Status.Node != "" && vm.Status.Node != node || vm.Status.PodIP != "" && vm.Status.PodIP != pod.Status.PodIP || pod.Spec.ServiceAccountName != vm.Spec.ServiceAccountName || pod.Spec.ServiceAccountName == "" || pod.Spec.ServiceAccountName == "default" {
		return fmt.Errorf("original DSGA Pod placement changed")
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "neonvm-runner" && (status.RestartCount != 0 || status.State.Terminated != nil) {
			return fmt.Errorf("original DSGA Runner stopped or restarted")
		}
	}
	return nil
}

func (s *agentState) stopInitialDSGA(ctx context.Context, selection dsgaInitialSelection, vmUID types.UID) {
	if err := s.lock.TryLock(ctx); err != nil {
		return
	}
	defer s.lock.Unlock()
	if state, ok := s.pods[selection.pod]; ok && state.runner.vmUID == vmUID && (state.runner.dsgaPodUID == "" || state.runner.dsgaPodUID == selection.uid) {
		state.stop()
	}
}

func (s *agentState) ensureInitialDSGA(ctx context.Context, logger *zap.Logger, vm *vmv1.VirtualMachine, pod *core.Pod) error {
	event, err := makeVMEvent(logger, vm, vmEventAdded)
	if err != nil {
		return err
	}
	event.podName, event.podIP, event.podUID = pod.Name, pod.Status.PodIP, string(pod.UID)
	if err := s.lock.TryLock(ctx); err != nil {
		return err
	}
	defer s.lock.Unlock()
	name := util.NamespacedName{Namespace: vm.Namespace, Name: pod.Name}
	if existing, ok := s.pods[name]; ok {
		if existing.runner.vmUID != vm.UID || existing.runner.podIP != pod.Status.PodIP || existing.runner.dsgaPodUID != "" && existing.runner.dsgaPodUID != string(pod.UID) {
			return fmt.Errorf("DSGA per-VM Runner selection changed")
		}
		return nil // Ready事件与首次轮询共用原Runner，不再选第二个Owner。
	}
	s.handleVMEventAdded(ctx, event, name)
	return nil
}

func (s *agentState) syncInitialDSGA(ctx context.Context, logger *zap.Logger, vms []*vmv1.VirtualMachine, selected map[types.UID]dsgaInitialSelection) {
	present := map[types.UID]bool{}
	for _, vm := range vms {
		if vm == nil {
			continue
		}
		present[vm.UID] = true
		prior, fixed := selected[vm.UID]
		if !dsgaInitialCandidate(vm, s.config) {
			if fixed {
				s.stopInitialDSGA(ctx, prior, vm.UID)
			}
			continue
		}
		if vmIsOurResponsibility(vm, s.config, s.envArgs.K8sNodeName) {
			if !fixed {
				continue
			} // 已由正常VM watcher负责，不再重复每秒GET Pod。
			if vm.Status.PodName == prior.pod.Name && vm.Status.Node == s.envArgs.K8sNodeName && vm.Status.PodIP != "" && (vm.Status.Phase == vmv1.VmRunning || vm.Status.Phase == vmv1.VmScaling) {
				prior.handedOff = true
				selected[vm.UID] = prior
				continue // 同一个原Runner/Session继续，终态与替换交原watch处理。
			}
		}

		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		pod, err := s.kubeClient.CoreV1().Pods(vm.Namespace).Get(call, vm.Status.PodName, metav1.GetOptions{})
		cancel()
		if err != nil {
			if fixed && apierrors.IsNotFound(err) {
				s.stopInitialDSGA(ctx, prior, vm.UID)
			}
			continue
		}
		// 其它节点由该节点的Agent负责，不能因VM.status.node尚未填就跨节点抢占。
		if pod.Spec.NodeName != s.envArgs.K8sNodeName {
			if fixed {
				s.stopInitialDSGA(ctx, prior, vm.UID)
			}
			continue
		}
		uid := ""
		if fixed {
			uid = prior.uid
			if prior.pod.Name != pod.Name {
				s.stopInitialDSGA(ctx, prior, vm.UID)
				continue
			}
		}
		if err := dsgaOriginalPod(vm, pod, s.envArgs.K8sNodeName, uid, ""); err != nil {
			if fixed {
				s.stopInitialDSGA(ctx, prior, vm.UID)
			}
			continue
		}
		if !fixed {
			selected[vm.UID] = dsgaInitialSelection{pod: util.NamespacedName{Namespace: vm.Namespace, Name: pod.Name}, uid: string(pod.UID)}
		}
		if err := s.ensureInitialDSGA(ctx, logger, vm, pod); err != nil {
			logger.Warn("DSGA initial Runner selection unconfirmed")
		}
	}
	for uid, selection := range selected {
		if !present[uid] {
			s.stopInitialDSGA(ctx, selection, uid)
			delete(selected, uid)
		}
	}
}

func (s *agentState) runInitialDSGA(ctx context.Context, logger *zap.Logger, store *watch.Store[vmv1.VirtualMachine]) error {
	selected := map[types.UID]dsgaInitialSelection{}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		s.syncInitialDSGA(ctx, logger, store.Items(), selected)
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

func (r *Runner) initialDSGAPod(ctx context.Context, vm *vmv1.VirtualMachine, uid string) (*core.Pod, error) {
	request := time.Duration(r.global.config.DSGA.RequestTimeoutSeconds) * time.Second
	if request <= 0 || request > 30*time.Second {
		return nil, fmt.Errorf("finite DSGA initial API timeout required")
	}
	call, cancel := context.WithTimeout(ctx, request)
	defer cancel()
	pod, err := r.global.kubeClient.CoreV1().Pods(vm.Namespace).Get(call, r.podName.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if vm.UID != r.vmUID || vm.Namespace != r.vmName.Namespace || vm.Name != r.vmName.Name || vm.Status.PodName != r.podName.Name || !dsgaInitialCandidate(vm, r.global.config) {
		return nil, fmt.Errorf("original DSGA VM initial binding changed")
	}
	if err := dsgaOriginalPod(vm, pod, r.global.envArgs.K8sNodeName, uid, r.podIP); err != nil {
		return nil, err
	}
	return pod, nil
}

// 续租在原Elector独立运行；这里只读等Controller真实发布Running/Node/IP，
// 在此之前不请求会因PodVerifier未绑定而拒绝/poison的Gateway凭据。
func (r *Runner) awaitInitialDSGABinding(ctx context.Context, session *owner.Session, uid string, timeout, interval time.Duration) error {
	if timeout <= 0 || interval <= 0 {
		return fmt.Errorf("finite DSGA initial wait required")
	}
	wait, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		if err := wait.Err(); err != nil {
			return err
		}
		if err := session.Check(wait); err != nil {
			return err
		}
		call, done := context.WithTimeout(wait, time.Duration(r.global.config.DSGA.RequestTimeoutSeconds)*time.Second)
		vm, err := r.global.vmClient.NeonvmV1().VirtualMachines(r.vmName.Namespace).Get(call, r.vmName.Name, metav1.GetOptions{})
		done()
		if err != nil {
			return err
		}
		if _, err := r.initialDSGAPod(wait, vm, uid); err != nil {
			return err
		}
		if vm.Status.Node == r.global.envArgs.K8sNodeName && vm.Status.PodIP == r.podIP && (vm.Status.Phase == vmv1.VmRunning || vm.Status.Phase == vmv1.VmScaling) {
			return nil
		}
		r.dsgaState(wait, DSGARunnerState{Phase: "awaiting-initial-runner", IssuerEpoch: session.Actor().Epoch, Instance: control.Instance{VMUID: string(r.vmUID)}})
		select {
		case <-wait.Done():
			return wait.Err()
		case <-tick.C:
		}
	}
}
