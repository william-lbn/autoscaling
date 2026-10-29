package vmguard

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

type noWaitExport struct {
	Binding journal.Binding
	Pod     core.Pod
	Claims  []core.ConfigMap
}

// 此helper只由隔离Launcher exec启动；PID必须是该进程的真实父PID，不能
// 使用主测试进程或任意环境PID。先写标记再SIGKILL，保证Runner越过Start，
// 且不会执行Wait落账或任何defer；本helper随后立即退出，不留后台负载。
func TestNoWaitQEMUChildHelper(t *testing.T) {
	root := os.Getenv("DSGA_NO_WAIT_ROOT")
	if root == "" {
		return
	}
	parent, err := strconv.Atoi(os.Getenv("DSGA_NO_WAIT_LAUNCHER_PID"))
	require.NoError(t, err)
	require.Greater(t, parent, 1)
	require.Equal(t, os.Getppid(), parent)
	require.NoError(t, os.WriteFile(filepath.Join(root, "child-started"), []byte(strconv.Itoa(os.Getpid())), 0o600))
	require.NoError(t, syscall.Kill(parent, syscall.SIGKILL))
	os.Exit(0)
}

func TestNoWaitLauncherHelper(t *testing.T) {
	root := os.Getenv("DSGA_NO_WAIT_ROOT")
	if root == "" {
		return
	}
	ctx := context.Background()
	f := runnerBirthFixture(t, false)
	f.config.StateDir = filepath.Join(root, "wal")
	readyProcessContainer(t, f)
	runner, err := Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
	require.NoError(t, err)
	pod, err := f.core.CoreV1().Pods(f.store.Binding().Namespace).Get(ctx, f.config.PodName, metav1.GetOptions{})
	require.NoError(t, err)
	cms, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	raw, err := json.Marshal(noWaitExport{Binding: f.store.Binding(), Pod: *pod, Claims: cms.Items})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "original-sources.json"), raw, 0o600))
	self, err := os.Executable()
	require.NoError(t, err)
	program := filepath.Join(root, "qemu-system-aarch64")
	require.NoError(t, os.Symlink(self, program))
	args := append([]string{"-test.run=^TestNoWaitQEMUChildHelper$", "--"}, runner.QMPArguments()...)
	cmd := exec.Command(program, args...)
	cmd.Env = append(os.Environ(), "DSGA_NO_WAIT_LAUNCHER_PID="+strconv.Itoa(os.Getpid()))
	_ = runner.RunQEMU(ctx, cmd)
	t.Fatal("子进程必须在Runner得到Wait事实前杀死隔离Launcher")
}

// 真正Start后的父进程SIGKILL、原fsync launch/WAL及缺失Wait是本机执行；
// 全容器终态/CM为明确Kube夹具，绝不把父进程被杀当作Guest已经停止。
func TestWholeStoppedPodRecoversActualLauncherDeathWithoutWait(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "wal"), 0o700))
	scratch := filepath.Join(root, "scratch")
	require.NoError(t, os.Mkdir(scratch, 0o700))
	self, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, self, "-test.run=^TestNoWaitLauncherHelper$", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "DSGA_NO_WAIT_ROOT="+root, "TMPDIR="+scratch)
	output, err := cmd.CombinedOutput()
	var stopped *exec.ExitError
	require.ErrorAs(t, err, &stopped, "%s", output)
	wait, ok := stopped.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.True(t, wait.Signaled(), "%s", output)
	require.Equal(t, syscall.SIGKILL, wait.Signal(), "%s", output)
	_, err = os.Stat(filepath.Join(root, "child-started"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(root, "wal", "process-launch.json"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(root, "wal", "process-exit.json"))
	require.True(t, os.IsNotExist(err), "不能预先制造Wait退出事实")
	wal, err := os.ReadFile(filepath.Join(root, "wal", "state.json"))
	require.NoError(t, err)
	raw, err := os.ReadFile(filepath.Join(root, "original-sources.json"))
	require.NoError(t, err)
	var exported noWaitExport
	require.NoError(t, json.Unmarshal(raw, &exported))
	objects := []runtime.Object{&exported.Pod}
	for i := range exported.Claims {
		objects = append(objects, &exported.Claims[i])
	}
	client := kubefake.NewSimpleClientset(objects...)
	client.PrependReactor("create", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
		cm := a.(kt.CreateAction).GetObject().(*core.ConfigMap).DeepCopy()
		cm.UID, cm.ResourceVersion = types.UID("whole-pod-source-uid"), "1"
		return true, cm, client.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace)
	})
	binding := exported.Binding
	birthCM, err := client.CoreV1().ConfigMaps(binding.Namespace).Get(context.Background(), BindingName(binding), metav1.GetOptions{})
	require.NoError(t, err)
	birth, ref, err := InspectBirthForExit(birthCM, binding, nil)
	require.NoError(t, err)
	_, err = ReadProcessExit(context.Background(), client.CoreV1(), binding, birth, ref)
	require.Error(t, err)
	pod := exported.Pod.DeepCopy()
	_, err = PublishStoppedPod(context.Background(), client.CoreV1(), binding, birth, ref, pod)
	require.Error(t, err, "Runner父进程已死也不能代替整个Pod终态")
	pod.Spec.RestartPolicy = core.RestartPolicyNever
	pod.Spec.Containers = []core.Container{{Name: "neonvm-runner"}}
	pod.Status.Phase = core.PodFailed
	started := pod.Status.ContainerStatuses[0].State.Running.StartedAt
	pod.Status.ContainerStatuses[0].State = core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: pod.Status.ContainerStatuses[0].ContainerID, StartedAt: started, FinishedAt: metav1.Now(), ExitCode: 137}}
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
	cm, err := PublishStoppedPod(context.Background(), client.CoreV1(), binding, birth, ref, pod)
	require.NoError(t, err)
	_, err = InspectStoppedPod(cm, binding, birth, ref, pod)
	require.NoError(t, err)
	after, err := os.ReadFile(filepath.Join(root, "wal", "state.json"))
	require.NoError(t, err)
	require.Equal(t, wal, after)
}
