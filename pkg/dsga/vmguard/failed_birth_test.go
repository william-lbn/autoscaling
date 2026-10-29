package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kt "k8s.io/client-go/testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 实际Initialize/目录flock/fsync/CREATE读回及v3关闭；Kube来源仍为明确
// fake，并非apiserver/RBAC/QEMU验收。每次从未出生的原环境进入，不删旧日志。
func TestFailedBirthSealsOriginalNonceAndPublishesOnlyNoStart(t *testing.T) {
	for _, mode := range []string{"state-create", "state-readback", "binding-create", "binding-readback"} {
		t.Run(mode, func(t *testing.T) {
			f := runnerBirthFixture(t, false)
			readyProcessContainer(t, f)
			ctx := context.Background()
			before, err := f.store.Load(ctx)
			require.NoError(t, err)
			failed := false
			verb, name := "create", stateName(f.store.Binding())
			if mode == "state-readback" || mode == "binding-readback" {
				verb = "get"
			}
			if mode == "binding-create" || mode == "binding-readback" {
				name = BindingName(f.store.Binding())
			}
			f.core.PrependReactor(verb, "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
				actual := ""
				if verb == "get" {
					actual = a.(kt.GetAction).GetName()
				} else {
					actual = a.(kt.CreateAction).GetObject().(*core.ConfigMap).Name
				}
				if actual != name || failed {
					return false, nil, nil
				}
				failed = true
				return true, nil, errors.New("explicit original birth interruption")
			})
			runner, err := Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
			require.Error(t, err)
			require.Nil(t, runner, "关闭补齐成功也不能把失败出生返回成可执行Runner")
			require.True(t, failed)
			raw, err := os.ReadFile(filepath.Join(f.config.StateDir, "birth-abort.json"))
			require.NoError(t, err)
			var aborted failedBirth
			require.NoError(t, json.Unmarshal(raw, &aborted))
			require.NoError(t, aborted.validate(f.store.Binding(), f.config))
			intent, err := os.ReadFile(filepath.Join(f.config.StateDir, "birth-intent.json"))
			require.NoError(t, err)
			var id Identity
			require.NoError(t, json.Unmarshal(intent, &id))
			require.Equal(t, id, aborted.Identity)
			birthCM, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).Get(ctx, BindingName(f.store.Binding()), metav1.GetOptions{})
			require.NoError(t, err)
			birth, ref, err := InspectBirthForExit(birthCM, f.store.Binding(), nil)
			require.NoError(t, err)
			require.Equal(t, id, birth.Identity)
			exited, err := ReadProcessExit(ctx, f.core.CoreV1(), f.store.Binding(), birth, ref)
			require.NoError(t, err)
			require.EqualValues(t, 3, exited.ProtocolVersion)
			require.Zero(t, exited.ProcessID)
			require.Empty(t, exited.LaunchNonce)
			_, err = os.Stat(filepath.Join(f.config.StateDir, "process-launch.json"))
			require.True(t, os.IsNotExist(err))
			after, err := f.store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, before.State(), after.State(), "出生关闭不改外层意图或预算")
			knownSources, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			wal, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
			require.NoError(t, err)
			runner, err = Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
			require.ErrorIs(t, err, ErrStopped)
			require.Nil(t, runner)
			actualSources, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			require.ElementsMatch(t, knownSources.Items, actualSources.Items, "补发原退出CREATE可以遇到AlreadyExists，但原CM UID/原文必须全部不变")
			f.runner, err = recoverFailedBirth(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config, aborted)
			require.NoError(t, err)
			f.runner.engine.native = &exitForbiddenNative{t: t}
			require.ErrorIs(t, f.runner.Tick(ctx), ErrStopped)
			cmd := processCommand(t, f, "success")
			require.ErrorIs(t, f.runner.RunQEMU(ctx, cmd), ErrStopped)
			require.Nil(t, cmd.Process)
			actual, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
			require.NoError(t, err)
			require.Equal(t, wal, actual)
		})
	}
}

// 首次State读回未知，关闭接续已确认State而Binding仍失败。再次进入时必须
// 使用独立fsync的State UID；来源被删除/同名重建也不能借原nonce重新收养。
func TestFailedBirthRetainsStateCheckpointAcrossRepeatedBindingFailure(t *testing.T) {
	for _, mode := range []string{"original", "replaced", "deleted"} {
		t.Run(mode, func(t *testing.T) {
			f := runnerBirthFixture(t, false)
			readyProcessContainer(t, f)
			ctx := context.Background()
			interrupted, available := false, false
			f.core.PrependReactor("get", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
				if a.(kt.GetAction).GetName() != stateName(f.store.Binding()) || interrupted {
					return false, nil, nil
				}
				interrupted = true
				return true, nil, errors.New("first State readback lost")
			})
			f.core.PrependReactor("create", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
				if a.(kt.CreateAction).GetObject().(*core.ConfigMap).Name == BindingName(f.store.Binding()) && !available {
					return true, nil, errors.New("Binding temporarily unavailable")
				}
				return false, nil, nil
			})
			runner, err := Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
			require.Error(t, err)
			require.Nil(t, runner)
			require.True(t, interrupted)
			raw, err := os.ReadFile(filepath.Join(f.config.StateDir, "birth-close-state.json"))
			require.NoError(t, err)
			var checkpoint budget.ObjectRef
			require.NoError(t, json.Unmarshal(raw, &checkpoint))
			confirmed, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).Get(ctx, checkpoint.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, checkpoint.UID, string(confirmed.UID))
			switch mode {
			case "replaced":
				replacement := confirmed.DeepCopy()
				replacement.UID = "same-name-new-state"
				require.NoError(t, f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), replacement, replacement.Namespace))
			case "deleted":
				require.NoError(t, f.core.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), confirmed.Namespace, confirmed.Name))
			}
			available = true
			runner, err = Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
			require.ErrorIs(t, err, ErrStopped)
			require.Nil(t, runner)
			actual, err := os.ReadFile(filepath.Join(f.config.StateDir, "birth-close-state.json"))
			require.NoError(t, err)
			require.Equal(t, raw, actual)
			_, statErr := os.Stat(filepath.Join(f.config.StateDir, "process-exit.json"))
			if mode != "original" {
				require.True(t, os.IsNotExist(statErr), "已确认State丢失不能生产关闭证据")
				_, err = f.core.CoreV1().ConfigMaps(confirmed.Namespace).Get(ctx, BindingName(f.store.Binding()), metav1.GetOptions{})
				require.True(t, apierrors.IsNotFound(err))
				return
			}
			require.NoError(t, statErr)
			birthCM, err := f.core.CoreV1().ConfigMaps(confirmed.Namespace).Get(ctx, BindingName(f.store.Binding()), metav1.GetOptions{})
			require.NoError(t, err)
			birth, ref, err := InspectBirthForExit(birthCM, f.store.Binding(), nil)
			require.NoError(t, err)
			require.Equal(t, checkpoint, birth.StateClaim)
			exited, err := ReadProcessExit(ctx, f.core.CoreV1(), f.store.Binding(), birth, ref)
			require.NoError(t, err)
			require.EqualValues(t, 3, exited.ProtocolVersion)
		})
	}
}

func TestFailedBirthRejectsReplacedConfirmedSourceAndUnknownLocalExecution(t *testing.T) {
	for _, mode := range []string{"confirmed-state-replaced", "launch-exists", "malformed-state"} {
		t.Run(mode, func(t *testing.T) {
			f := runnerBirthFixture(t, false)
			readyProcessContainer(t, f)
			ctx := context.Background()
			f.core.PrependReactor("create", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
				cm := a.(kt.CreateAction).GetObject().(*core.ConfigMap)
				if cm.Name != BindingName(f.store.Binding()) {
					return false, nil, nil
				}
				switch mode {
				case "confirmed-state-replaced":
					obj, err := f.core.Tracker().Get(core.SchemeGroupVersion.WithResource("configmaps"), f.store.Binding().Namespace, stateName(f.store.Binding()))
					require.NoError(t, err)
					replacement := obj.(*core.ConfigMap).DeepCopy()
					replacement.UID = "recreated-state-uid"
					require.NoError(t, f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), replacement, replacement.Namespace))
				case "launch-exists":
					require.NoError(t, os.WriteFile(filepath.Join(f.config.StateDir, "process-launch.json"), []byte("unknown original launch"), 0o600))
				case "malformed-state":
					require.NoError(t, os.WriteFile(filepath.Join(f.config.StateDir, "state.json"), []byte("{}"), 0o600))
				}
				return true, nil, errors.New("birth failed before binding source")
			})
			runner, err := Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
			require.Error(t, err)
			require.Nil(t, runner)
			_, err = os.Stat(filepath.Join(f.config.StateDir, "process-exit.json"))
			require.True(t, os.IsNotExist(err), "未知launch/坏state/已知UID替换不能补造NoStart")
			claims, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			for _, cm := range claims.Items {
				require.NotEqual(t, exitKind, cm.Labels[ClaimLabel])
			}
		})
	}
}
