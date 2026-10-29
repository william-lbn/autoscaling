package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kt "k8s.io/client-go/testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 子进程真实进入Initialize后以os.Exit越过全部defer，保留实际flock/fsync
// 文件。Kube仍为明确fake：退出前导出其已提交CM，父进程按原UID重装，
// 不把fake API或测试进程当作KVM/真实apiserver/物理回收证明。
func TestCrashedBirthProcessHelper(t *testing.T) {
	root := os.Getenv("DSGA_CRASH_BIRTH_DIR")
	if root == "" {
		return
	}
	mode := os.Getenv("DSGA_CRASH_BIRTH_MODE")
	f := runnerBirthFixture(t, false)
	f.config.StateDir = filepath.Join(root, "wal")
	readyProcessContainer(t, f)
	ctx := context.Background()
	crash := func() {
		obj, err := f.core.Tracker().List(core.SchemeGroupVersion.WithResource("configmaps"), core.SchemeGroupVersion.WithKind("ConfigMap"), f.store.Binding().Namespace)
		require.NoError(t, err)
		raw, err := json.Marshal(obj.(*core.ConfigMapList).Items)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(root, "committed-cms.json"), raw, 0o600))
		os.Exit(23) // 故意不执行Initialize的失败封存defer。
	}
	f.core.PrependReactor("create", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
		cm := a.(kt.CreateAction).GetObject().(*core.ConfigMap).DeepCopy()
		if cm.Name == stateName(f.store.Binding()) && mode == "before-state-create" {
			crash()
		}
		if cm.Name == BindingName(f.store.Binding()) && mode == "after-state-checkpoint" {
			crash()
		}
		if cm.Name == stateName(f.store.Binding()) && mode == "state-create-unknown" || cm.Name == BindingName(f.store.Binding()) && mode == "binding-create-unknown" {
			f.nextUID++
			cm.UID, cm.ResourceVersion = types.UID("crash-committed-uid"), "1"
			require.NoError(t, f.core.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
			crash()
		}
		return false, nil, nil
	})
	runner, err := Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
	require.NoError(t, err)
	require.NotNil(t, runner)
	require.Equal(t, "before-returned-runner-start", mode)
	crash()
}

// 直接崩溃后的关闭必须逐字尊重持久选择和已确认UID；缺/坏历史、旧格式
// 孤立nonce或未知launch不能转成新出生。这里修改文件/远端对象是明确故障注入，
// 不把这些改写描述成真实崩溃产物；原崩溃窗口由上面的独立子进程复现。
func TestCrashedBirthRejectsChangedSelectionAndExecutionEvidence(t *testing.T) {
	for _, mode := range []string{"legacy-intent", "missing-owner", "wrong-owner", "wrong-pod", "wrong-node", "intent-symlink", "launch-present", "bad-state", "advanced-state", "state-replaced", "binding-replaced", "state-checkpoint-missing", "state-checkpoint-symlink", "sources-checkpoint-bad"} {
		t.Run(mode, func(t *testing.T) {
			f := crashedBirthFixture(t, "before-returned-runner-start")
			ctx := context.Background()
			intentPath := filepath.Join(f.config.StateDir, "birth-intent.json")
			raw, err := os.ReadFile(intentPath)
			require.NoError(t, err)
			var selected failedBirth
			require.NoError(t, json.Unmarshal(raw, &selected))
			writeJSON := func(name string, value any) {
				t.Helper()
				body, err := json.Marshal(value)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(f.config.StateDir, name), body, 0o600))
			}
			switch mode {
			case "legacy-intent":
				writeJSON("birth-intent.json", selected.Identity)
			case "missing-owner":
				var fields map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(raw, &fields))
				delete(fields, "owner")
				writeJSON("birth-intent.json", fields)
			case "wrong-owner":
				selected.Owner.Epoch++
				writeJSON("birth-intent.json", selected)
			case "wrong-pod":
				f.config.PodUID = "another-pod-uid"
			case "wrong-node":
				f.config.NodeName = "another-node"
			case "intent-symlink":
				require.NoError(t, os.Rename(intentPath, intentPath+".original"))
				require.NoError(t, os.Symlink(intentPath+".original", intentPath))
			case "launch-present":
				require.NoError(t, os.WriteFile(filepath.Join(f.config.StateDir, "process-launch.json"), []byte("unknown launch"), 0o600))
			case "bad-state":
				writeJSON("state.json", map[string]any{})
			case "advanced-state":
				s, err := lockState(ctx, f.config.StateDir)
				require.NoError(t, err)
				prior, err := s.load()
				require.NoError(t, err)
				prior.Sequence++
				err = s.save(prior)
				s.close()
				require.NoError(t, err)
			case "state-replaced", "binding-replaced":
				name := stateName(f.store.Binding())
				if mode == "binding-replaced" {
					name = BindingName(f.store.Binding())
				}
				cm, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).Get(ctx, name, metav1.GetOptions{})
				require.NoError(t, err)
				cm.UID = "same-name-replacement"
				require.NoError(t, f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
			case "state-checkpoint-missing":
				require.NoError(t, os.Remove(filepath.Join(f.config.StateDir, "birth-close-state.json")))
			case "state-checkpoint-symlink":
				name := filepath.Join(f.config.StateDir, "birth-close-state.json")
				require.NoError(t, os.Rename(name, name+".original"))
				require.NoError(t, os.Symlink(name+".original", name))
			case "sources-checkpoint-bad":
				writeJSON("birth-close-sources.json", failedBirthSources{State: budget.ObjectRef{Name: "wrong-state", UID: "wrong-uid"}, Binding: budget.ObjectRef{Name: BindingName(f.store.Binding()), UID: "wrong-binding"}})
			}
			before, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
			require.NoError(t, err)
			claims, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			runner, err := Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
			require.Error(t, err)
			require.Nil(t, runner)
			after, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
			require.NoError(t, err)
			require.Equal(t, before, after)
			_, err = os.Stat(filepath.Join(f.config.StateDir, "process-exit.json"))
			require.True(t, os.IsNotExist(err), "来源或本地历史不完整不得生产NoStart")
			actual, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			require.ElementsMatch(t, claims.Items, actual.Items)
		})
	}
}

// Initialize的崩溃接续与已经返回的旧Runner竞争同一flock：Start先登记
// launch则恢复不得封存；恢复先封存则旧Start必须被拒绝。实际运行测试子进程
// 的Start/Wait，不能仅用布尔状态模拟这条跨进程启动不变量。
func TestCrashedBirthClosureSerializesAgainstReturnedRunnerStart(t *testing.T) {
	f := newRunnerFixture(t)
	readyProcessContainer(t, f)
	ctx := context.Background()
	cmd := processCommand(t, f, "success")
	var wg sync.WaitGroup
	var initErr, runErr error
	var recovered *Runner
	start := make(chan struct{})
	wg.Add(2)
	go func() { defer wg.Done(); <-start; runErr = f.runner.RunQEMU(ctx, cmd) }()
	go func() {
		defer wg.Done()
		<-start
		recovered, initErr = Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
	}()
	close(start)
	wg.Wait()
	require.Error(t, initErr)
	require.Nil(t, recovered)
	exited, err := ReadProcessExit(ctx, f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding)
	require.NoError(t, err)
	if exited.ProtocolVersion == 3 {
		require.ErrorIs(t, initErr, ErrStopped)
		require.ErrorIs(t, runErr, ErrStopped)
		require.Nil(t, cmd.Process)
	} else {
		require.EqualValues(t, 1, exited.ProtocolVersion)
		require.NoError(t, runErr)
		require.NotNil(t, cmd.ProcessState)
	}
}

// 历史版本在失败defer里已封存完整原owner/UID时，新读者可继续补发同一
// NoStart关闭；旧版只有Identity且没有abort的孤立文件仍在上面的负矩阵拒绝。
// 旧Identity解码器也必须拒绝新的完整选择，不能忽略owner后启动第二个实例。
func TestCrashedBirthLegacySealedFailureKeepsOriginalClosure(t *testing.T) {
	f := runnerBirthFixture(t, false)
	readyProcessContainer(t, f)
	ctx := context.Background()
	failed := false
	f.core.PrependReactor("get", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
		if a.(kt.GetAction).GetName() != BindingName(f.store.Binding()) || failed {
			return false, nil, nil
		}
		failed = true
		return true, nil, errors.New("explicit old sealed birth interruption")
	})
	runner, err := Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
	require.Error(t, err)
	require.Nil(t, runner)
	require.True(t, failed)
	path := filepath.Join(f.config.StateDir, "birth-intent.json")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var legacy Identity
	require.Error(t, json.Unmarshal(raw, &legacy), "旧严格读者必须拒绝新出生选择")
	var selected failedBirth
	require.NoError(t, json.Unmarshal(raw, &selected))
	old, err := json.Marshal(selected.Identity)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, old, 0o600))
	exitPath := filepath.Join(f.config.StateDir, "process-exit.json")
	before, err := os.ReadFile(exitPath)
	require.NoError(t, err)
	runner, err = Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
	require.ErrorIs(t, err, ErrStopped)
	require.Nil(t, runner)
	after, err := os.ReadFile(exitPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func crashedBirthFixture(t *testing.T, mode string) *runnerFixture {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "wal"), 0o700))
	scratch := filepath.Join(root, "child-tmp")
	require.NoError(t, os.Mkdir(scratch, 0o700))
	self, err := os.Executable()
	require.NoError(t, err)
	childCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(childCtx, self, "-test.run=^TestCrashedBirthProcessHelper$", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "DSGA_CRASH_BIRTH_DIR="+root, "DSGA_CRASH_BIRTH_MODE="+mode, "TMPDIR="+scratch)
	output, err := cmd.CombinedOutput()
	var stopped *exec.ExitError
	require.ErrorAs(t, err, &stopped, "%s", output)
	require.Equal(t, 23, stopped.ExitCode(), "%s", output)
	f := runnerBirthFixture(t, false)
	readyProcessContainer(t, f)
	f.config.StateDir = filepath.Join(root, "wal")
	raw, err := os.ReadFile(filepath.Join(root, "committed-cms.json"))
	require.NoError(t, err)
	var cms []core.ConfigMap
	require.NoError(t, json.Unmarshal(raw, &cms))
	for i := range cms {
		require.NoError(t, f.core.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), &cms[i], cms[i].Namespace))
	}
	_, err = os.Stat(filepath.Join(f.config.StateDir, "birth-abort.json"))
	require.True(t, os.IsNotExist(err), "子进程不得先跑失败defer再冒充无屏障崩溃")
	return f
}

func TestCrashedBirthRecoversOnlyOriginalNoStartAndBlocksLateStart(t *testing.T) {
	for _, mode := range []string{"before-state-create", "state-create-unknown", "after-state-checkpoint", "binding-create-unknown", "before-returned-runner-start"} {
		t.Run(mode, func(t *testing.T) {
			f := crashedBirthFixture(t, mode)
			ctx := context.Background()
			before, err := f.store.Load(ctx)
			require.NoError(t, err)
			intent, err := os.ReadFile(filepath.Join(f.config.StateDir, "birth-intent.json"))
			require.NoError(t, err)
			var selected failedBirth
			require.NoError(t, json.Unmarshal(intent, &selected))
			id := selected.Identity
			runner, err := Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
			require.ErrorIs(t, err, ErrStopped)
			require.Nil(t, runner)
			body, err := os.ReadFile(filepath.Join(f.config.StateDir, "birth-abort.json"))
			require.NoError(t, err)
			var aborted failedBirth
			require.NoError(t, json.Unmarshal(body, &aborted))
			require.Equal(t, id, aborted.Identity)
			birthCM, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).Get(ctx, BindingName(f.store.Binding()), metav1.GetOptions{})
			require.NoError(t, err)
			birth, ref, err := InspectBirthForExit(birthCM, f.store.Binding(), nil)
			require.NoError(t, err)
			exit, err := ReadProcessExit(ctx, f.core.CoreV1(), f.store.Binding(), birth, ref)
			require.NoError(t, err)
			require.EqualValues(t, 3, exit.ProtocolVersion)
			require.Equal(t, id, exit.Identity)
			require.Zero(t, exit.ProcessID)
			require.Empty(t, exit.LaunchNonce)
			f.runner, err = recoverFailedBirth(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config, aborted)
			require.NoError(t, err)
			f.runner.engine.native = &exitForbiddenNative{t: t}
			require.ErrorIs(t, f.runner.Tick(ctx), ErrStopped)
			cmd := processCommand(t, f, "success")
			require.ErrorIs(t, f.runner.RunQEMU(ctx, cmd), ErrStopped)
			require.Nil(t, cmd.Process)
			again, err := Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
			require.ErrorIs(t, err, ErrStopped)
			require.Nil(t, again)
			original, err := os.ReadFile(filepath.Join(f.config.StateDir, "birth-intent.json"))
			require.NoError(t, err)
			require.Equal(t, intent, original)
			after, err := f.store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, before.State(), after.State(), "原外层预算/意图不变，不把出生关闭作释放许可")
		})
	}
}
