//go:build linux

package vmguard

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootstrap"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 在原出生前构造通用Compute的完整immutable输入，复用正式producer生成v3。
// API与可执行文件是明确模型；本组只验证首次RunQEMU冻结边界，不声称KVM资格。
func initialBootGenericRuntime(t *testing.T, f *runnerFixture) ([]byte, *corev1.Secret) {
	t.Helper()
	_, secret := initialBootFullRuntime(t, f)
	var source resourceguard.SourceConfig
	require.NoError(t, json.Unmarshal(secret.Data[resourceguard.ResourceSecretKey], &source))
	source.Config.ExecutionModel = resourceguard.TrustedControllerExecutionModel
	secret.Data[resourceguard.ResourceSecretKey] = initialBootCanonical(source)
	var native map[string]any
	require.NoError(t, json.Unmarshal(secret.Data["config.json"], &native))
	native["compute_ctl_config"].(map[string]any)["jwks"].(map[string]any)["keys"].([]any)[0].(map[string]any)["kid"] = "original-control-key"
	secret.Data["config.json"] = initialBootCanonical(native)
	secret.Data[resourceguard.ComputeStartupSecretKey] = []byte(`{"protocol_version":1,"identity_source":"runtime-v1"}`)
	snap, err := f.store.Load(context.Background())
	require.NoError(t, err)
	doc, err := bootstrap.Runtime(snap.VirtualMachine(), secret)
	require.NoError(t, err)
	require.EqualValues(t, 3, doc.ProtocolVersion)
	require.NotNil(t, doc.ComputeStartup)
	return initialBootCanonical(doc), secret
}

// 必须穿过真实RunQEMU→initial.freeze→原Secret GET→完整资产派生→WAL冻结。
// 夹具文件刻意不是可执行格式；只有冻结成功后真实exec.Start才返回ENOEXEC，
// 从而严格区分版本兼容成功与任意较早的NoStart失败，不创建Guest或假造QMP观察。
func TestInitialRuntimeFreezeRunQEMUOriginalV2V3(t *testing.T) {
	for _, generic := range []bool{false, true} {
		name, runtimeSource := "v2", initialBootFullRuntime
		if generic {
			name, runtimeSource = "v3", initialBootGenericRuntime
		}
		t.Run(name, func(t *testing.T) {
			f, raw, cmd := initialBootFixtureWithRuntime(t, false, runtimeSource)
			readyProcessContainer(t, f)
			before, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
			require.NoError(t, err)
			err = f.runner.RunQEMU(context.Background(), cmd)
			require.ErrorIs(t, err, syscall.ENOEXEC, "必须到达真实Start，不能把初启冻结拒绝误判为兼容")
			require.Nil(t, cmd.Process)
			require.Nil(t, cmd.ProcessState)
			lifecycle := f.runner.initial.(*linuxInitialLifecycle)
			require.NotNil(t, lifecycle.source, "生产initial.freeze必须完成原来源读回")
			state, err := lockState(context.Background(), f.config.StateDir)
			require.NoError(t, err)
			entries, err := initialBootRead(state)
			state.close()
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.Equal(t, "source_frozen", entries[0].Stage)
			require.Equal(t, *lifecycle.source, entries[0].Source)
			require.Equal(t, SourceDigest(raw), entries[0].Source.RuntimeSHA256)
			require.Equal(t, f.runner.birth.InitialMode.RuntimeSHA256, entries[0].Source.RuntimeSHA256)
			assets, err := resourceguard.RenderRuntimePolicyAssets(raw)
			require.NoError(t, err)
			_, hasPublic := assets[resourceguard.RuntimeComputePublicAssetName]
			require.Equal(t, generic, hasPublic)
			exited, err := ReadProcessExit(context.Background(), f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding)
			require.NoError(t, err)
			require.EqualValues(t, 2, exited.ProtocolVersion)
			require.Zero(t, exited.ProcessID)
			after, err := os.ReadFile(filepath.Join(f.config.StateDir, "state.json"))
			require.NoError(t, err)
			require.Equal(t, before, after, "冻结及NoStart不能假造业务内存进展")
		})
	}
}

// v3必须绑定原Secret的全部新增输入；结构合法但来自别的配置或名称同样拒绝。
// 每项均走RunQEMU正式入口，验证无Start、无初启阶段，但保留原不可变NoStart。
func TestInitialRuntimeFreezeRunQEMURejectsV3SourceDrift(t *testing.T) {
	for _, scenario := range []string{"native_bytes", "native_kid", "selection_missing", "selection_unknown_field", "resource_missing", "templates_missing", "runtime_workload", "runtime_native_digest", "runtime_kid", "runtime_compute_missing", "runtime_noncanonical", "runtime_unknown_field", "runtime_duplicate_field"} {
		t.Run(scenario, func(t *testing.T) {
			f, raw, cmd := initialBootFixtureWithRuntime(t, false, initialBootGenericRuntime)
			readyProcessContainer(t, f)
			ctx := context.Background()
			secret, err := f.core.CoreV1().Secrets(f.store.Binding().Namespace).Get(ctx, "vm-config", metav1.GetOptions{})
			require.NoError(t, err)
			var doc resourceguard.RuntimeDocument
			require.NoError(t, json.Unmarshal(raw, &doc))
			switch scenario {
			case "native_bytes":
				secret.Data["config.json"] = append([]byte(" "), secret.Data["config.json"]...)
			case "native_kid":
				secret.Data["config.json"] = []byte(strings.Replace(string(secret.Data["config.json"]), "original-control-key", "other-control-key", 1))
			case "selection_missing":
				delete(secret.Data, resourceguard.ComputeStartupSecretKey)
			case "selection_unknown_field":
				secret.Data[resourceguard.ComputeStartupSecretKey] = []byte(`{"protocol_version":1,"identity_source":"runtime-v1","untrusted":true}`)
			case "resource_missing":
				delete(secret.Data, resourceguard.ResourceSecretKey)
			case "templates_missing":
				delete(secret.Data, resourceguard.PolicyTemplatesSecretKey)
			case "runtime_workload":
				doc.ComputeStartup.WorkloadName = "different-workload"
				raw = initialBootCanonical(doc)
			case "runtime_native_digest":
				doc.ComputeStartup.NativeConfigSHA256 = strings.Repeat("ab", 32)
				raw = initialBootCanonical(doc)
			case "runtime_kid":
				doc.ComputeStartup.ControlKeyID = "different-control-key"
				raw = initialBootCanonical(doc)
			case "runtime_compute_missing":
				doc.ComputeStartup = nil
				raw = initialBootCanonical(doc)
			case "runtime_noncanonical":
				raw = append([]byte(" "), raw...)
			case "runtime_unknown_field":
				raw = append([]byte(`{"untrusted":true,`), raw[1:]...)
			case "runtime_duplicate_field":
				raw = append([]byte(`{"protocol_version":3,`), raw[1:]...)
			}
			// 仅fake tracker注入不可能通过immutable API更新的漂移，用来验证读者
			// 不只信UID或同名对象；生产路径仍为原typed GET和严格字节核对。
			require.NoError(t, f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("secrets"), secret, secret.Namespace))
			f.runner.initial.(*linuxInitialLifecycle).config.RuntimeJSON = raw
			err = f.runner.RunQEMU(ctx, cmd)
			require.Error(t, err)
			require.NotErrorIs(t, err, syscall.ENOEXEC, "错误来源必须在Start之前")
			require.Nil(t, cmd.Process)
			require.Nil(t, cmd.ProcessState)
			require.Nil(t, f.runner.initial.(*linuxInitialLifecycle).source)
			_, err = os.Stat(filepath.Join(f.config.StateDir, initialBootFile(0)))
			require.True(t, os.IsNotExist(err), "拒绝不能冻结初启来源")
			exited, err := ReadProcessExit(ctx, f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding)
			require.NoError(t, err)
			require.EqualValues(t, 2, exited.ProtocolVersion)
			require.Zero(t, exited.ProcessID)
		})
	}
}
