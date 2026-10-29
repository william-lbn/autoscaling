package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kt "k8s.io/client-go/testing"
)

// Kubelet终态是明确API夹具；真实父进程失去Wait由独立子进程测试覆盖。
// 此处不把Terminal状态写入fake当作真实CRI/容器销毁或物理回收认证。
func stoppedPodFixture(t *testing.T) (*runnerFixture, *core.Pod) {
	t.Helper()
	f := newRunnerFixture(t)
	readyProcessContainer(t, f)
	pod, err := f.core.CoreV1().Pods(f.store.Binding().Namespace).Get(context.Background(), f.config.PodName, metav1.GetOptions{})
	require.NoError(t, err)
	pod.Spec.RestartPolicy = core.RestartPolicyNever
	pod.Spec.Containers = []core.Container{{Name: "neonvm-runner"}}
	started := pod.Status.ContainerStatuses[0].State.Running.StartedAt
	pod.Status.Phase = core.PodFailed
	pod.Status.ContainerStatuses[0].State = core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: pod.Status.ContainerStatuses[0].ContainerID, StartedAt: started, FinishedAt: metav1.Now(), ExitCode: 137}}
	stamp := metav1.Now()
	pod.DeletionTimestamp = &stamp
	require.NoError(t, f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
	return f, pod
}

func TestWholeStoppedPodSourceIsIndependentOfWaitAndPreservesRunnerWAL(t *testing.T) {
	f, pod := stoppedPodFixture(t)
	ctx := context.Background()
	before, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
	require.NoError(t, err)
	cm, err := PublishStoppedPod(ctx, f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding, pod)
	require.NoError(t, err)
	fact, err := InspectStoppedPod(cm, f.store.Binding(), f.runner.birth, f.runner.engine.binding, pod)
	require.NoError(t, err)
	require.Equal(t, f.runner.birth.Identity, fact.Identity)
	require.Equal(t, pod.Status.ContainerStatuses[0].ContainerID, fact.ContainerID)
	_, err = InspectProcessExit(cm, f.store.Binding(), f.runner.birth, f.runner.engine.binding)
	require.Error(t, err, "旧Wait读者必须明确拒绝整Pod终态来源")
	again, err := PublishStoppedPod(ctx, f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding, pod)
	require.NoError(t, err)
	require.Equal(t, cm.UID, again.UID)
	require.Equal(t, cm.Data, again.Data)
	_, _, _, err = ReadPublished(ctx, f.core.CoreV1(), f.store.Binding(), &f.runner.engine.binding)
	require.ErrorIs(t, err, ErrStopped, "原发布读者不能跨整Pod关闭继续授权")
	after, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
	require.NoError(t, err)
	require.Equal(t, before, after, "不改原未知内存命令、代次或结果")
}

func TestWholeStoppedPodSourceRejectsChangedIdentityAndIncompleteTermination(t *testing.T) {
	for _, mode := range []string{"pod-uid", "owner", "restart-policy", "restart-count", "running", "sidecar", "init", "container-id", "node-during-get", "missing-pod"} {
		t.Run(mode, func(t *testing.T) {
			f, pod := stoppedPodFixture(t)
			actual := pod.DeepCopy()
			switch mode {
			case "pod-uid":
				actual.UID = "replacement-pod"
			case "owner":
				actual.OwnerReferences[0].UID = "replacement-vm"
			case "restart-policy":
				actual.Spec.RestartPolicy = core.RestartPolicyAlways
			case "restart-count":
				actual.Status.ContainerStatuses[0].RestartCount++
			case "running":
				actual.Status.Phase = core.PodRunning
			case "sidecar":
				actual.Spec.Containers = append(actual.Spec.Containers, core.Container{Name: "live-sidecar"})
			case "init":
				actual.Spec.InitContainers = []core.Container{{Name: "missing-init"}}
			case "container-id":
				actual.Status.ContainerStatuses[0].ContainerID = "containerd://replacement"
			case "node-during-get":
				actual.Spec.NodeName = "replacement-node"
			case "missing-pod":
				require.NoError(t, f.core.Tracker().Delete(core.SchemeGroupVersion.WithResource("pods"), pod.Namespace, pod.Name))
			}
			if mode != "missing-pod" {
				require.NoError(t, f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), actual, actual.Namespace))
			}
			_, err := PublishStoppedPod(context.Background(), f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding, pod)
			require.Error(t, err)
			_, err = f.core.CoreV1().ConfigMaps(pod.Namespace).Get(context.Background(), StoppedPodClaimName(f.store.Binding(), f.runner.birth), metav1.GetOptions{})
			require.Error(t, err, "不完整来源在CREATE前拒绝")
		})
	}
}

func TestWholeStoppedPodSourceStrictDecodingAndRetainedProof(t *testing.T) {
	f, pod := stoppedPodFixture(t)
	cm, err := PublishStoppedPod(context.Background(), f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding, pod)
	require.NoError(t, err)
	raw := cm.Data[ClaimKey]
	for _, body := range []string{strings.Replace(raw, "{", "{\"protocol_version\":1,", 1), strings.Replace(raw, "{", "{\"process_id\":42,", 1)} {
		var fact StoppedPod
		require.Error(t, json.Unmarshal([]byte(body), &fact))
	}
	for _, mode := range []string{"source-kind", "mutable", "unprotected", "birth", "source-owner", "termination", "node"} {
		t.Run(mode, func(t *testing.T) {
			changed, actual := cm.DeepCopy(), pod.DeepCopy()
			switch mode {
			case "source-kind":
				changed.Labels[ClaimLabel] = exitKind
			case "mutable":
				*changed.Immutable = false
			case "unprotected":
				changed.Finalizers = nil
			case "birth":
				changed.Data[ClaimKey] = strings.Replace(raw, f.runner.engine.binding.UID, "replacement-birth", 1)
			case "source-owner":
				changed.OwnerReferences[0].UID = "replacement-vm"
			case "termination":
				actual.Status.ContainerStatuses[0].State.Terminated.ExitCode++
			case "node":
				actual.Spec.NodeName = "replacement-node"
			}
			_, err := InspectStoppedPod(changed, f.store.Binding(), f.runner.birth, f.runner.engine.binding, actual)
			require.Error(t, err)
		})
	}
}

func TestWholeStoppedPodSourceRecoversLostCreateAndConfirmation(t *testing.T) {
	f, pod := stoppedPodFixture(t)
	ctx := context.Background()
	name := StoppedPodClaimName(f.store.Binding(), f.runner.birth)
	created, loseRead := 0, true
	f.core.PrependReactor("create", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		cm := action.(kt.CreateAction).GetObject().(*core.ConfigMap).DeepCopy()
		if cm.Name != name {
			return false, nil, nil
		}
		created++
		cm.UID, cm.ResourceVersion = types.UID("original-terminal-uid"), "1"
		if err := f.core.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace); err != nil {
			return true, nil, err
		}
		return true, nil, errors.New("fixture terminal CREATE reply lost after commit")
	})
	f.core.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		if action.(kt.GetAction).GetName() == name && loseRead {
			loseRead = false
			return true, nil, errors.New("fixture original terminal confirmation unavailable")
		}
		return false, nil, nil
	})
	_, err := PublishStoppedPod(ctx, f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding, pod)
	require.Error(t, err, "CREATE落库但无真实GET确认不能宣称成功")
	again, err := PublishStoppedPod(ctx, f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding, pod)
	require.NoError(t, err)
	require.EqualValues(t, "original-terminal-uid", again.UID, "相同确定名称恢复原UID，不能换名创建关闭许可")
	require.Equal(t, 2, created)
	_, err = InspectStoppedPod(again, f.store.Binding(), f.runner.birth, f.runner.engine.binding, pod)
	require.NoError(t, err)
}
