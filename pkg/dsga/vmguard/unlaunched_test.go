package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kt "k8s.io/client-go/testing"
)

func TestRunnerUnlaunchedClosurePreservesWALAndFencesLateStart(t *testing.T) {
	f := newRunnerFixture(t)
	readyProcessContainer(t, f)
	ctx := context.Background()
	before, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
	require.NoError(t, err)
	closed, err := f.runner.CloseUnlaunched(ctx)
	require.NoError(t, err)
	require.True(t, closed)
	exited, err := ReadProcessExit(ctx, f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding)
	require.NoError(t, err)
	require.EqualValues(t, 3, exited.ProtocolVersion)
	require.Empty(t, exited.LaunchNonce)
	require.Empty(t, exited.CommandSHA256)
	require.Zero(t, exited.ProcessID)
	require.Equal(t, SourceDigest(before), exited.RecordSHA256)
	after, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
	require.NoError(t, err)
	require.Equal(t, before, after, "init关闭不能清原未知或改变内存WAL")
	_, err = os.Stat(filepath.Join(f.config.StateDir, "process-launch.json"))
	require.True(t, os.IsNotExist(err), "不补造根本没有发生的launch")
	cmd := processCommand(t, f, "success")
	require.ErrorIs(t, f.runner.RunQEMU(ctx, cmd), ErrStopped)
	require.Nil(t, cmd.Process)
	f.runner.engine.native = &exitForbiddenNative{t: t}
	require.ErrorIs(t, f.runner.Tick(ctx), ErrStopped)
	_, _, _, err = ReadPublished(ctx, f.core.CoreV1(), f.store.Binding(), &f.runner.engine.binding)
	require.ErrorIs(t, err, ErrStopped)
	for _, change := range []func(*ProcessExit){
		func(e *ProcessExit) { e.ProtocolVersion = 1 },
		func(e *ProcessExit) { e.ProtocolVersion = 2 },
		func(e *ProcessExit) { e.ProtocolVersion = 4 },
		func(e *ProcessExit) { e.LaunchNonce = e.Identity.RunnerBootID },
		func(e *ProcessExit) { e.CommandSHA256 = SourceDigest([]byte("invented argv")) },
		func(e *ProcessExit) { e.ProcessID = 123 },
		func(e *ProcessExit) { e.ExitCode = 1 },
		func(e *ProcessExit) { e.Signal = 9 },
	} {
		bad := exited
		change(&bad)
		raw, marshalErr := json.Marshal(bad)
		require.NoError(t, marshalErr)
		var decoded ProcessExit
		require.Error(t, json.Unmarshal(raw, &decoded))
	}
}

func TestRunnerUnlaunchedClosureRetriesOnlyOriginalAfterPublishFailure(t *testing.T) {
	f := newRunnerFixture(t)
	readyProcessContainer(t, f)
	ctx := context.Background()
	fail := true
	name := ExitClaimName(f.store.Binding(), f.runner.birth)
	f.core.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		if action.(kt.GetAction).GetName() == name && fail {
			_, err := os.ReadFile(filepath.Join(f.config.StateDir, "process-exit.json"))
			require.NoError(t, err, "发布必须后于本地关闭持久化")
			return true, nil, errors.New("unlaunched CREATE confirmation lost")
		}
		return false, nil, nil
	})
	closed, err := f.runner.CloseUnlaunched(ctx)
	require.True(t, closed, "API失败仍保留本地关闭，不允许再Start")
	require.Error(t, err)
	before, err := os.ReadFile(filepath.Join(f.config.StateDir, "process-exit.json"))
	require.NoError(t, err)
	fail = false
	// 已有关闭不重新发现容器；原Pod已不可读时也只补发同一原文。
	f.core.PrependReactor("get", "pods", func(kt.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("original Pod is no longer readable")
	})
	closed, err = f.runner.CloseUnlaunched(ctx)
	require.NoError(t, err)
	require.True(t, closed)
	after, err := os.ReadFile(filepath.Join(f.config.StateDir, "process-exit.json"))
	require.NoError(t, err)
	require.Equal(t, before, after)
	cm, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).Get(ctx, name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, string(before), cm.Data[ClaimKey])
}

func TestRunnerUnlaunchedClosureAllowsOriginalDeletingPod(t *testing.T) {
	f := newRunnerFixture(t)
	readyProcessContainer(t, f)
	ctx := context.Background()
	pod, err := f.core.CoreV1().Pods(f.store.Binding().Namespace).Get(ctx, f.config.PodName, metav1.GetOptions{})
	require.NoError(t, err)
	stamp := metav1.Now()
	pod.DeletionTimestamp = &stamp
	require.NoError(t, f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
	cmd := processCommand(t, f, "success")
	require.Error(t, f.runner.RunQEMU(ctx, cmd), "原Pod删除后仍不允许Start")
	require.Nil(t, cmd.Process)
	closed, err := f.runner.CloseUnlaunched(ctx)
	require.NoError(t, err)
	require.True(t, closed, "原Running容器可保存关闭屏障，不新增执行权")
}

func TestRunnerUnlaunchedClosureDoesNotReplaceLaunchOrMissingSource(t *testing.T) {
	for _, mode := range []string{"launch-exists", "missing-state", "malformed-state", "exit-symlink", "launch-symlink", "replaced-pod"} {
		t.Run(mode, func(t *testing.T) {
			f := newRunnerFixture(t)
			readyProcessContainer(t, f)
			ctx := context.Background()
			switch mode {
			case "launch-exists":
				// 有字节即不能证明尚未启动，即使不是可解码的完整launch；
				// 此路径不重写它，也不推导NoEffect或发退出来源。
				require.NoError(t, os.WriteFile(filepath.Join(f.config.StateDir, "process-launch.json"), []byte("unknown original launch"), 0o600))
			case "missing-state":
				require.NoError(t, os.Remove(filepath.Join(f.config.StateDir, "state.json")))
			case "malformed-state":
				require.NoError(t, os.WriteFile(filepath.Join(f.config.StateDir, "state.json"), []byte("{}"), 0o600))
			case "exit-symlink":
				require.NoError(t, os.Symlink("missing", filepath.Join(f.config.StateDir, "process-exit.json")))
			case "launch-symlink":
				require.NoError(t, os.Symlink("missing", filepath.Join(f.config.StateDir, "process-launch.json")))
			case "replaced-pod":
				pod, err := f.core.CoreV1().Pods(f.store.Binding().Namespace).Get(ctx, f.config.PodName, metav1.GetOptions{})
				require.NoError(t, err)
				pod.UID = "different-original-pod"
				require.NoError(t, f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
			}
			closed, err := f.runner.CloseUnlaunched(ctx)
			require.False(t, closed)
			if mode == "launch-exists" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			_, err = ReadProcessExit(ctx, f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding)
			require.Error(t, err, "不能用init关闭重建缺失来源或替换未知launch")
		})
	}
}

// 两个真实调用同时竞争原flock；RunQEMU先登记launch才Start，Close先同步
// 屏障则Start必须失败。不能只在测试里比较布尔值而不实际运行Start/Wait。
func TestRunnerUnlaunchedClosureSerializesAgainstActualStart(t *testing.T) {
	f := newRunnerFixture(t)
	readyProcessContainer(t, f)
	ctx := context.Background()
	cmd := processCommand(t, f, "success")
	start := make(chan struct{})
	var wg sync.WaitGroup
	var runErr, closeErr error
	var closed bool
	wg.Add(2)
	go func() { defer wg.Done(); <-start; runErr = f.runner.RunQEMU(ctx, cmd) }()
	go func() { defer wg.Done(); <-start; closed, closeErr = f.runner.CloseUnlaunched(ctx) }()
	close(start)
	wg.Wait()
	require.NoError(t, closeErr)
	exited, err := ReadProcessExit(ctx, f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding)
	require.NoError(t, err)
	if closed {
		require.ErrorIs(t, runErr, ErrStopped)
		require.Nil(t, cmd.Process)
		require.EqualValues(t, 3, exited.ProtocolVersion)
	} else {
		require.NoError(t, runErr)
		require.NotNil(t, cmd.ProcessState)
		require.EqualValues(t, 1, exited.ProtocolVersion)
	}
}
