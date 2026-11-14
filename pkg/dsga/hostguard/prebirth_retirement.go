package hostguard

import (
	"fmt"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PreBirthStop把未发布出生来源的独立终态关闭绑定到原VM日志，不伪造Runner
// 出生、进程启动、退出或伸缩成功回执，也不宣称QEMU从未启动。仅受信Scheduler
// 在独立重读删除中的原VM、未使用且未改变的日志、真实缺失的birth/state来源
// 及完整原Pod容器终态后可选择此关闭。容量仍由原Node owner按UID/RV CAS结清。
type PreBirthStop struct {
	VMJournalSHA256 string `json:"vm_journal_sha256"`
}

// PreBirthTerminalPod比一般终态检查更严格：必须已请求删除、Never重启、完整
// app/init容器均真正退出，且不能有任何先前容器尝试，避免隐藏另一Runner代次。
// Pod Failed、删除标记或容器waiting单独出现时均不构成关闭来源。
func PreBirthTerminalPod(pod *core.Pod) error {
	if _, err := TerminalPodDigest(pod); err != nil {
		return err
	}
	if pod.DeletionTimestamp == nil || pod.Spec.RestartPolicy != core.RestartPolicyNever {
		return fmt.Errorf("pre-birth cleanup requires a deleting never-restarted Pod")
	}
	found := false
	for _, group := range [][]core.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, status := range group {
			if status.RestartCount != 0 || status.LastTerminationState.Running != nil || status.LastTerminationState.Waiting != nil || status.LastTerminationState.Terminated != nil {
				return fmt.Errorf("pre-birth cleanup cannot adopt a previous container attempt")
			}
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "neonvm-runner" {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("pre-birth cleanup lacks original Runner container")
	}
	return nil
}

// ManagedPreBirthEvictedPod verifies the immutable Pod identity for the
// separate trusted Node stop source. The Node source proves physical absence.
func ManagedPreBirthEvictedPod(pod *core.Pod, p Placement) bool {
	if pod == nil || pod.ResourceVersion == "" || pod.DeletionTimestamp == nil ||
		pod.Spec.RestartPolicy != core.RestartPolicyNever || pod.Status.Phase != core.PodFailed ||
		pod.Status.Reason != "Evicted" || pod.Spec.NodeName == "" ||
		len(pod.Spec.EphemeralContainers) != 0 || len(pod.Status.EphemeralContainerStatuses) != 0 ||
		len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Name != "neonvm-runner" ||
		pod.Namespace != p.Namespace || pod.Name != p.PodName || string(pod.UID) != p.PodUID ||
		len(pod.OwnerReferences) != 1 {
		return false
	}
	owner := metav1.GetControllerOf(pod)
	return owner != nil && owner.APIVersion == "vm.neon.tech/v1" && owner.Kind == "VirtualMachine" &&
		owner.Name == p.VMName && string(owner.UID) == p.VMUID
}

func managedPreBirthPlacement(p Placement) bool {
	_, err := p.Bytes()
	return err == nil && p.Managed && p.PodMemoryLimitBytes != 0 && p.Settled == nil && p.Barrier == nil
}
