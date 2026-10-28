package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	kt "k8s.io/client-go/testing"
)

// 真实Mac exec.Start故障，不用自报NoStart夹具；只替代可执行程序，原Runner
// 文件锁、完整启动WAL、关闭屏障和CM发布均执行生产代码。不能外推Linux
// fork/exec/CRI或整Pod物理回收正确性；这些仍保留目标环境专项。
func TestRunnerFailedStartClosesOriginalBirthWithoutInventingWait(t *testing.T) {
	for _, mode := range []string{"missing-executable", "not-executable", "invalid-format", "cancelled-command", "lost-publish-reply"} {
		t.Run(mode, func(t *testing.T) {
			f := newRunnerFixture(t)
			readyProcessContainer(t, f)
			before, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
			require.NoError(t, err)
			cmd := processCommand(t, f, "success")
			switch mode {
			case "cancelled-command":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				cmd = exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
			default:
				require.NoError(t, os.Remove(cmd.Path))
				if mode == "not-executable" || mode == "invalid-format" {
					permission := os.FileMode(0o600)
					if mode == "invalid-format" {
						permission = 0o700
					}
					require.NoError(t, os.WriteFile(cmd.Path, []byte("not a QEMU executable\n"), permission))
				}
			}
			exitGets := 0
			if mode == "lost-publish-reply" {
				f.core.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
					if action.(kt.GetAction).GetName() != ExitClaimName(f.store.Binding(), f.runner.birth) {
						return false, nil, nil
					}
					exitGets++
					_, readErr := os.ReadFile(filepath.Join(f.config.StateDir, "process-exit.json"))
					require.NoError(t, readErr, "外部发布之前原NoStart必须已持久")
					if exitGets == 1 {
						return true, nil, errors.New("original NoStart GET reply lost")
					}
					return false, nil, nil
				})
			}
			err = f.runner.RunQEMU(context.Background(), cmd)
			require.Error(t, err, "成功归档关闭事实不能吞掉原启动错误")
			if mode == "cancelled-command" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				var pathErr *os.PathError
				require.ErrorAs(t, err, &pathErr)
			}
			require.Nil(t, cmd.Process)
			require.Nil(t, cmd.ProcessState)
			exited, err := ReadProcessExit(context.Background(), f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding)
			require.NoError(t, err)
			require.EqualValues(t, 2, exited.ProtocolVersion)
			require.Zero(t, exited.ProcessID)
			require.Zero(t, exited.ExitCode)
			require.Zero(t, exited.Signal)
			require.Equal(t, SourceDigest(before), exited.RecordSHA256)
			after, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
			require.NoError(t, err)
			require.Equal(t, before, after, "NoStart不改写原内存WAL")
			f.runner.engine.native = &exitForbiddenNative{t: t}
			require.ErrorIs(t, f.runner.Tick(context.Background()), ErrStopped)
			require.ErrorIs(t, f.runner.RunQEMU(context.Background(), processCommand(t, f, "success")), ErrStopped)
			_, _, _, err = ReadPublished(context.Background(), f.core.CoreV1(), f.store.Binding(), &f.runner.engine.binding)
			require.ErrorIs(t, err, ErrStopped)
			if mode == "lost-publish-reply" {
				require.GreaterOrEqual(t, exitGets, 2)
			}
			// v2不能夹带伪造的Wait信息，也不能被降成v1接受零PID。
			for _, change := range []func(*ProcessExit){
				func(e *ProcessExit) { e.ProtocolVersion = 1 },
				func(e *ProcessExit) { e.ProtocolVersion = 3 },
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
		})
	}
}

func TestRunnerLaunchPersistenceFailureNeverStartsOrPublishesNoStart(t *testing.T) {
	f := newRunnerFixture(t)
	readyProcessContainer(t, f)
	// 不跟随原启动文件的符号链接，不能在缺完整持久启动来源时假造NoStart。
	require.NoError(t, os.Symlink("missing-original", filepath.Join(f.config.StateDir, "process-launch.json")))
	cmd := processCommand(t, f, "success")
	require.Error(t, f.runner.RunQEMU(context.Background(), cmd))
	require.Nil(t, cmd.Process)
	_, err := os.Stat(filepath.Join(f.config.StateDir, "process-exit.json"))
	require.True(t, os.IsNotExist(err))
	_, err = ReadProcessExit(context.Background(), f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding)
	require.Error(t, err)
}
