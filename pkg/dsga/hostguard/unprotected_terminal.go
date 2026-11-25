package hostguard

import (
	"context"
	"fmt"
	"strings"

	core "k8s.io/api/core/v1"
)

// UnprotectedTerminalOrdinary 只识别可进一步核验的历史普通 Pod，不代表它
// 已无记账或可释放容量。首次清点和运行期观察均须继续重读原对象；已有
// Placement/Hold、VM/迁移、原生保护或退出来源不能因 Pod 终态被跳过。
func UnprotectedTerminalOrdinary(pod *core.Pod) bool {
	if !ordinaryPod(pod) || pod.DeletionTimestamp != nil {
		return false
	}
	for key := range pod.Annotations {
		if strings.HasPrefix(key, "internal.autoscaling.neon.tech/dsga-") {
			return false
		}
	}
	for _, key := range pod.Finalizers {
		if strings.HasPrefix(key, "autoscaling.neon.tech/dsga-") {
			return false
		}
	}
	_, err := TerminalPodDigest(pod)
	return err == nil
}

// ObserveUntrackedTerminalOrdinary 只读确认未受本域保护的历史普通 Pod
// 确实未占原账本。共用原节点双重核验和原 UID/RV/完整退出证据检查；
// 不写退休意图、不移除 Finalizer、不降低或重建任何已有容量持有。
func (s *Store) ObserveUntrackedTerminalOrdinary(ctx context.Context, snapshot *Snapshot, pod *core.Pod) error {
	if !UnprotectedTerminalOrdinary(pod) {
		return fmt.Errorf("historical ordinary Pod requires complete unprotected termination")
	}
	return s.observeUnchargedTerminalOrdinary(ctx, snapshot, pod)
}
