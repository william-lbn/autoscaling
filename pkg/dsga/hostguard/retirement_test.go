package hostguard

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTerminalPodProofRequiresCompleteContainerSet(t *testing.T) {
	for _, what := range []string{"complete", "running", "missing-init", "waiting", "different-id", "reversed-time", "duplicate", "ephemeral"} {
		t.Run(what, func(t *testing.T) {
			now := metav1.Now()
			status := core.ContainerStatus{Name: "app", ContainerID: "containerd://app", State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: "containerd://app", StartedAt: now, FinishedAt: now}}}
			init := *status.DeepCopy()
			init.Name = "init"
			pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "testing", UID: "pod-uid", ResourceVersion: "1"}, Spec: core.PodSpec{NodeName: "node", Containers: []core.Container{{Name: "app"}}, InitContainers: []core.Container{{Name: "init"}}}, Status: core.PodStatus{Phase: core.PodSucceeded, ContainerStatuses: []core.ContainerStatus{status}, InitContainerStatuses: []core.ContainerStatus{init}}}
			switch what {
			case "running":
				pod.Status.Phase = core.PodRunning
			case "missing-init":
				pod.Status.InitContainerStatuses = nil
			case "waiting":
				pod.Status.ContainerStatuses[0].State = core.ContainerState{Waiting: &core.ContainerStateWaiting{}}
			case "different-id":
				pod.Status.ContainerStatuses[0].ContainerID = "containerd://another"
			case "reversed-time":
				pod.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.NewTime(now.Add(-time.Second))
			case "duplicate":
				pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, status)
			case "ephemeral":
				pod.Spec.EphemeralContainers = []core.EphemeralContainer{{}}
			}
			_, err := TerminalPodDigest(pod)
			if what == "complete" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestSuccessfulSettlementPreservesRetirementVersionBarrier(t *testing.T) {
	_, _, snapshot, _, current, closed := settlementFixture(t)
	m := snapshot.State()
	m.ProtocolVersion = 5
	next, _, err := m.settle(m.LeaseFloor, current.RunnerPodUID, closed)
	require.NoError(t, err)
	require.EqualValues(t, 5, next.ProtocolVersion, "其他运行中VM结清不能把节点降回旧读者可读取的v4")
	require.NoError(t, next.Validate())
}
