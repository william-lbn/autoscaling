//go:build linux

package vmguard

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kt "k8s.io/client-go/testing"

	"github.com/neondatabase/autoscaling/pkg/api/runnerdiagnostic"
)

type diagnosticFreezeLifecycle struct {
	initialLifecycle
	cause  error
	cancel context.CancelFunc
}

func (l *diagnosticFreezeLifecycle) freeze(context.Context, *exec.Cmd) error {
	if l.cancel != nil {
		l.cancel()
	}
	return l.cause
}

// 用真实RunQEMU和原关闭WAL核四处分支接线；仅冻结/只读API故障点为注入。
// 不启动子进程、不声称通过实际Guest初启或物理资格；NoStart仍由原生产路径生成。
func TestRunQEMUNoStartDiagnosticDistinguishesActualBoundaries(t *testing.T) {
	for _, stage := range []runnerdiagnostic.Stage{runnerdiagnostic.StageInitialOwnerWait, runnerdiagnostic.StageInitialFreeze, runnerdiagnostic.StageCommandStart, runnerdiagnostic.StageContextCanceled} {
		t.Run(string(stage), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			f, _, cmd := initialBootFixture(t, false)
			readyProcessContainer(t, f)
			cause := apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, "private-name", errors.New("private-token"))
			switch stage {
			case runnerdiagnostic.StageInitialOwnerWait:
				f.core.PrependReactor("get", "nodes", func(kt.Action) (bool, runtime.Object, error) { return true, nil, cause })
			case runnerdiagnostic.StageInitialFreeze:
				f.runner.initial = &diagnosticFreezeLifecycle{initialLifecycle: f.runner.initial, cause: cause}
			case runnerdiagnostic.StageContextCanceled:
				f.runner.initial = &diagnosticFreezeLifecycle{initialLifecycle: f.runner.initial, cancel: cancel}
			case runnerdiagnostic.StageCommandStart:
				// 保留原命令身份，禁止freeze尝试读取已移除的ELF；该分支专核Start。
				f.runner.initial = &diagnosticFreezeLifecycle{initialLifecycle: f.runner.initial}
				require.NoError(t, os.Remove(cmd.Path))
			}
			err := f.runner.RunQEMU(ctx, cmd)
			require.Error(t, err)
			report := runnerdiagnostic.FromError(err)
			require.Equal(t, stage, report.Stage)
			if stage == runnerdiagnostic.StageInitialOwnerWait || stage == runnerdiagnostic.StageInitialFreeze {
				require.ErrorIs(t, err, cause)
				require.Equal(t, runnerdiagnostic.CodeAPIForbidden, report.Code)
			}
			if stage == runnerdiagnostic.StageContextCanceled {
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, runnerdiagnostic.CodeContextCanceled, report.Code)
			}
			require.Nil(t, cmd.Process)
			require.Nil(t, cmd.ProcessState)
			exit, err := ReadProcessExit(context.Background(), f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding)
			require.NoError(t, err)
			require.EqualValues(t, 2, exit.ProtocolVersion)
			require.Zero(t, exit.ProcessID)
		})
	}
}
