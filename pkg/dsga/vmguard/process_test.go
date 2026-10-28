package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kt "k8s.io/client-go/testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 子进程运行本测试二进制，真实执行Start/Wait/信号/文件同步；它并非QEMU，
// 不据此宣称KVM或物理回收通过。仅借固定程序名验证实际启动封装与身份参数。
func TestRunnerWaitChildHelper(t *testing.T) {
	mode := os.Getenv("DSGA_WAIT_TEST_CHILD")
	if mode == "" {
		return
	}
	if mode == "signal" {
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		for {
			time.Sleep(time.Second)
		}
	}
	if mode == "error" {
		os.Exit(17)
	}
	os.Exit(0)
}

func processCommand(t *testing.T, f *runnerFixture, mode string) *exec.Cmd {
	t.Helper()
	self, err := os.Executable()
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "qemu-system-aarch64")
	require.NoError(t, os.Symlink(self, path))
	args := append([]string{"-test.run=^TestRunnerWaitChildHelper$", "--"}, f.runner.QMPArguments()...)
	cmd := exec.Command(path, args...)
	cmd.Env = append(os.Environ(), "DSGA_WAIT_TEST_CHILD="+mode)
	return cmd
}

func readyProcessContainer(t *testing.T, f *runnerFixture) {
	t.Helper()
	pod, err := f.core.CoreV1().Pods(f.store.Binding().Namespace).Get(context.Background(), f.config.PodName, metav1.GetOptions{})
	require.NoError(t, err)
	pod.Status.ContainerStatuses = []core.ContainerStatus{{Name: "neonvm-runner", ContainerID: "containerd://original-runner", State: core.ContainerState{Running: &core.ContainerStateRunning{StartedAt: metav1.Now()}}}}
	require.NoError(t, f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
}

func TestRunnerProcessWaitClosesOriginalWAL(t *testing.T) {
	for _, mode := range []string{"success", "error", "signal"} {
		t.Run(mode, func(t *testing.T) {
			f := newRunnerFixture(t)
			readyProcessContainer(t, f)
			before, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
			require.NoError(t, err)
			cmd := processCommand(t, f, mode)
			err = f.runner.RunQEMU(context.Background(), cmd)
			if mode == "success" {
				require.NoError(t, err)
			} else {
				var child *exec.ExitError
				require.ErrorAs(t, err, &child)
			}
			exited, err := ReadProcessExit(context.Background(), f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding)
			require.NoError(t, err)
			require.EqualValues(t, cmd.Process.Pid, exited.ProcessID)
			require.Equal(t, "containerd://original-runner", exited.ContainerID)
			if mode == "signal" {
				require.EqualValues(t, syscall.SIGTERM, exited.Signal)
				require.EqualValues(t, -1, exited.ExitCode)
			} else {
				require.Zero(t, exited.Signal)
				require.EqualValues(t, cmd.ProcessState.ExitCode(), exited.ExitCode)
			}
			require.Equal(t, SourceDigest(before), exited.RecordSHA256)
			after, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
			require.NoError(t, err)
			require.Equal(t, before, after, "进程退出不改写内存动作WAL/未决结果")
			// 若关闭检查顺序错误，nil native会panic；必须先于任何QMP读写拒绝。
			f.runner.engine.native = &exitForbiddenNative{t: t}
			require.ErrorIs(t, f.runner.Tick(context.Background()), ErrStopped)
			err = f.runner.RunQEMU(context.Background(), processCommand(t, f, "success"))
			require.ErrorIs(t, err, ErrStopped)
			_, _, _, err = ReadPublished(context.Background(), f.core.CoreV1(), f.store.Binding(), &f.runner.engine.binding)
			require.ErrorIs(t, err, ErrStopped, "旧published实际值不能跨退出继续授权")
		})
	}
}

type exitForbiddenNative struct{ t *testing.T }

func (n *exitForbiddenNative) Read(context.Context) (Observation, error) {
	n.t.Error("QMP read after durable process exit")
	return Observation{RequestedBytes: 0, ActualBytes: 0}, errors.New("forbidden read")
}

func (n *exitForbiddenNative) Set(context.Context, control.Uint64) error {
	n.t.Error("QMP write after durable process exit")
	return errors.New("forbidden write")
}

func TestRunnerProcessRejectsAlternateCommandBeforeStart(t *testing.T) {
	for _, what := range []string{"uuid", "monitor", "duplicate", "powerdown-duplicate", "powerdown-options", "daemon", "migration", "program", "wrapper"} {
		t.Run(what, func(t *testing.T) {
			f := newRunnerFixture(t)
			cmd := processCommand(t, f, "success")
			switch what {
			case "uuid":
				cmd.Args[len(cmd.Args)-3] = "another-uuid"
			case "monitor":
				cmd.Args[len(cmd.Args)-1] = "tcp:127.0.0.1:4444"
			case "duplicate":
				cmd.Args = append(cmd.Args, f.runner.QMPArguments()...)
			case "powerdown-duplicate":
				for range 2 {
					cmd.Args = append(cmd.Args, "-qmp", "unix:"+FixedPowerdownSocket+",server,wait=off")
				}
			case "powerdown-options":
				cmd.Args = append(cmd.Args, "-qmp", "unix:"+FixedPowerdownSocket+",server,wait=off,websocket=on")
			case "daemon":
				cmd.Args = append(cmd.Args, "-daemonize")
			case "migration":
				cmd.Args = append(cmd.Args, "-incoming", "defer")
			case "program":
				cmd.Path = "/bin/sh"
			case "wrapper":
				cmd.Path = "/usr/bin/cgexec"
			}
			before := len(f.core.Actions())
			require.Error(t, f.runner.RunQEMU(context.Background(), cmd))
			require.Nil(t, cmd.Process)
			require.Len(t, f.core.Actions(), before, "参数拒绝必须早于API/启动")
			_, err := os.Stat(filepath.Join(f.config.StateDir, "process-launch.json"))
			require.True(t, os.IsNotExist(err))
		})
	}
}

// 生产生成器保留原SIGTERM私有socket并追加内存socket；此前最终校验只
// 接受一个-qmp，导致受管命令即使配置正确也在Start之前被自己拒绝。这里
// 用完整双socket形状实际Start/Wait，既不放宽任意路径，也不只测字符串。
func TestRunnerProcessAcceptsFixedPowerdownAlongsideOriginalMemoryMonitor(t *testing.T) {
	f := newRunnerFixture(t)
	readyProcessContainer(t, f)
	cmd := processCommand(t, f, "success")
	// QEMU参数留在测试子程序的--之后，不交给Go test自身的flag解析器。
	cmd.Args = append(cmd.Args, "-qmp", "unix:"+FixedPowerdownSocket+",server,wait=off")
	require.NoError(t, f.runner.RunQEMU(context.Background(), cmd))
	exited, err := ReadProcessExit(context.Background(), f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding)
	require.NoError(t, err)
	require.EqualValues(t, 1, exited.ProtocolVersion)
	require.EqualValues(t, cmd.ProcessState.Pid(), exited.ProcessID)
	closed, err := f.runner.CloseUnlaunched(context.Background())
	require.NoError(t, err)
	require.False(t, closed, "原launch存在时不能改写真实Wait事实为v3")
}

func TestRunnerProcessExitLocalFirstAndLostReplyRecovery(t *testing.T) {
	f := newRunnerFixture(t)
	readyProcessContainer(t, f)
	podGets, exitGets := 0, 0
	f.core.PrependReactor("get", "pods", func(kt.Action) (bool, runtime.Object, error) {
		podGets++
		if podGets > 1 {
			return true, nil, errors.New("API Pod read unavailable after launch")
		}
		return false, nil, nil
	})
	f.core.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		if action.(kt.GetAction).GetName() == ExitClaimName(f.store.Binding(), f.runner.birth) {
			exitGets++
			body, err := os.ReadFile(filepath.Join(f.config.StateDir, "process-exit.json"))
			require.NoError(t, err, "外部发布必须后于本地关闭屏障")
			var exited ProcessExit
			require.NoError(t, json.Unmarshal(body, &exited))
			if exitGets == 1 {
				return true, nil, errors.New("exit GET reply lost after commit")
			}
		}
		return false, nil, nil
	})
	require.NoError(t, f.runner.RunQEMU(context.Background(), processCommand(t, f, "success")))
	require.Equal(t, 1, podGets, "不能在退出后临时寻找/收养容器身份")
	require.GreaterOrEqual(t, exitGets, 2)
	require.ErrorIs(t, f.runner.Tick(context.Background()), ErrStopped)
}

func TestRunnerProcessCloseRejectsDamagedFileAndNeverResets(t *testing.T) {
	f := newRunnerFixture(t)
	readyProcessContainer(t, f)
	require.NoError(t, f.runner.RunQEMU(context.Background(), processCommand(t, f, "success")))
	path := filepath.Join(f.config.StateDir, "process-exit.json")
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	var exited ProcessExit
	require.NoError(t, json.Unmarshal(body, &exited))
	exited.RecordSequence++
	body, err = json.Marshal(exited)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, body, 0o600))
	f.runner.engine.native = &exitForbiddenNative{t: t}
	require.ErrorContains(t, f.runner.Tick(context.Background()), "retained WAL")
	_, err = Initialize(context.Background(), f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
	require.ErrorContains(t, err, "controlled recovery")
}

func TestRunnerProcessExitPreservesMayHaveSentAction(t *testing.T) {
	f := newRunnerFixture(t)
	readyProcessContainer(t, f)
	command := fixtureCommand(f.runner.birth.Identity)
	command.BindingClaim, command.Request.Key.IssuerEpoch = f.runner.engine.binding, f.actor.Epoch
	digest, err := command.digest()
	require.NoError(t, err)
	s, err := lockState(context.Background(), f.config.StateDir)
	require.NoError(t, err)
	state, err := s.load()
	require.NoError(t, err)
	state.Sequence, state.LeaseUID, state.HighWater = 2, "lease-uid", &command.Request.Key
	state.Pending = &intent{Command: command, CommandSHA256: digest, JournalSHA256: strings.Repeat("d", 64), Owner: f.actor, Stage: mayHaveSent, ReplayNonce: "", Before: Observation{RequestedBytes: command.BeforeBytes, ActualBytes: command.BeforeBytes}}
	require.NoError(t, s.save(state))
	s.close()
	before, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
	require.NoError(t, err)
	require.NoError(t, f.runner.RunQEMU(context.Background(), processCommand(t, f, "success")))
	after, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
	require.NoError(t, err)
	require.Equal(t, before, after)
	var retained record
	require.NoError(t, json.Unmarshal(after, &retained))
	require.NotNil(t, retained.Pending)
	require.Nil(t, retained.Last, "Guest退出不能把可能已发动作补成完成/未发送")
	returned := make(chan struct{})
	go func() { f.runner.Run(context.Background(), nil); close(returned) }()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("closed Runner loop must terminate without sending QMP")
	}
}

func TestOriginalProcessFileCannotOverwriteOrFollowLinks(t *testing.T) {
	for _, what := range []string{"same", "different", "symlink", "public", "oversize", "path"} {
		t.Run(what, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0o700))
			s, err := lockState(context.Background(), dir)
			require.NoError(t, err)
			defer s.close()
			name, raw := "process-exit.json", []byte(`{"original":true}`)
			switch what {
			case "same", "different":
				require.NoError(t, s.writeOnce(name, raw))
				if what == "different" {
					raw = []byte(`{"original":false}`)
				}
			case "symlink":
				require.NoError(t, os.Symlink("elsewhere", filepath.Join(dir, name)))
			case "public":
				require.NoError(t, os.WriteFile(filepath.Join(dir, name), raw, 0o644))
			case "oversize":
				raw = []byte(strings.Repeat("a", processFileLimit+1))
			case "path":
				name = "../process-exit.json"
			}
			err = s.writeOnce(name, raw)
			if what == "same" {
				require.NoError(t, err)
			} else {
				require.Error(t, err, fmt.Sprintf("must preserve original fact: %s", what))
			}
		})
	}
}
