//go:build linux

package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	vmfake "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/fake"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/util/gzip64"
)

// 使用原VM/immutable Secret/journal夹具及实际Runner解码器；不冒充K8s启动验收。
func TestRuntimeGenericComputeUsesOriginalIdentityAndPublicConfig(t *testing.T) {
	vm, secret := runtimeTemplatesProducerFixture(t, "original-startup-secret")
	var source resourceguard.SourceConfig
	if err := json.Unmarshal(secret.Data[resourceguard.ResourceSecretKey], &source); err != nil {
		t.Fatal(err)
	}
	source.Config.ExecutionModel = resourceguard.TrustedControllerExecutionModel
	secret.Data[resourceguard.ResourceSecretKey], _ = json.Marshal(source)
	var native map[string]any
	json.Unmarshal(secret.Data["config.json"], &native)
	ctl := native["compute_ctl_config"].(map[string]any)
	ctl["jwks"].(map[string]any)["keys"].([]any)[0].(map[string]any)["kid"] = "original-control-key"
	// 假角色verifier也不能随公钥资产泄露进Runtime/Runner参数。
	native["spec"].(map[string]any)["test_private_value"] = "private-test-value"
	secret.Data["config.json"], _ = json.Marshal(native)
	selection, _ := json.Marshal(resourceguard.ComputeStartupSelection{ProtocolVersion: 1, IdentitySource: resourceguard.RuntimeComputeIdentitySource})
	secret.Data[resourceguard.ComputeStartupSecretKey] = selection
	doc, err := Runtime(vm, secret)
	if err != nil {
		t.Fatal(err)
	}
	if doc.ProtocolVersion != 3 || doc.ComputeStartup == nil || doc.ComputeStartup.WorkloadName != vm.Name || vm.Name == source.Config.ComputeInstanceID || doc.ComputeStartup.NativeConfigSHA256 != runtimeTemplateSHA(secret.Data["config.json"]) {
		t.Fatal("logical Compute and physical execution identity were conflated")
	}
	raw, _ := json.Marshal(doc)
	assets, err := DecodeRuntimeAssets(&vm.Spec, gzip64.Encode(raw))
	if err != nil {
		t.Fatal(err)
	}
	public := assets[resourceguard.RuntimeComputePublicAssetName]
	if bytes.Contains(raw, []byte("private-test-value")) || bytes.Contains(public, []byte("private-test-value")) {
		t.Fatal("native configuration leaked into public startup")
	}
	var initial map[string]any
	if json.Unmarshal(public, &initial) != nil || initial["spec"] != nil {
		t.Fatal("generic Guest must await authenticated configuration")
	}
	if initial["compute_ctl_config"].(map[string]any)["jwks"].(map[string]any)["keys"].([]any)[0].(map[string]any)["x"] != source.ControlPublicKey {
		t.Fatal("startup authentication key changed")
	}
	// 改原生配置只改变原字节摘要，不能静默借旧Runtime绑定。
	changed := secret.DeepCopy()
	changed.Data["config.json"] = append([]byte(" "), changed.Data["config.json"]...)
	next, err := Runtime(vm, changed)
	if err != nil || next.ComputeStartup.NativeConfigSHA256 == doc.ComputeStartup.NativeConfigSHA256 {
		t.Fatal("native original bytes were normalized away", err)
	}
	for _, selection := range []string{`{}`, `{"protocol_version":1,"identity_source":"unknown"}`, `{"protocol_version":1,"identity_source":"runtime-v1","workload_name":"foreign"}`, `{"protocol_version":1,"identity_source":"runtime-v1","Identity_Source":"runtime-v1"}`} {
		changed := secret.DeepCopy()
		changed.Data[resourceguard.ComputeStartupSecretKey] = []byte(selection)
		if _, err := Runtime(vm, changed); err == nil {
			t.Fatal("untrusted startup selection accepted")
		}
	}
	for _, name := range []string{"../escape", "foreign name", "-invalid"} {
		changed := vm.DeepCopy()
		changed.Name = name
		if _, err := Runtime(changed, secret); err == nil {
			t.Fatal("invalid original workload name accepted")
		}
	}
	// 旧发行输入仍为v2且不添加新资产，不能隐式升级现有运行实例。
	delete(secret.Data, resourceguard.ComputeStartupSecretKey)
	old, err := Runtime(vm, secret)
	if err != nil || old.ProtocolVersion != 2 || old.ComputeStartup != nil {
		t.Fatal("old startup silently upgraded", err)
	}
}

// 共用投影可重读已选主的原实例，但首次启动门仍拒绝已运行日志。
func TestRuntimeObservationSharesGenericProjectionWithoutRelaxingStartup(t *testing.T) {
	for _, generic := range []bool{false, true} {
		vm, secret := runtimeTemplatesProducerFixture(t, "original-observed-secret")
		var source resourceguard.SourceConfig
		if err := json.Unmarshal(secret.Data[resourceguard.ResourceSecretKey], &source); err != nil {
			t.Fatal(err)
		}
		source.Config.ExecutionModel = resourceguard.TrustedControllerExecutionModel
		secret.Data[resourceguard.ResourceSecretKey], _ = json.Marshal(source)
		if generic {
			var native map[string]any
			json.Unmarshal(secret.Data["config.json"], &native)
			native["compute_ctl_config"].(map[string]any)["jwks"].(map[string]any)["keys"].([]any)[0].(map[string]any)["kid"] = "original-control-key"
			secret.Data["config.json"], _ = json.Marshal(native)
			secret.Data[resourceguard.ComputeStartupSecretKey] = []byte(`{"protocol_version":1,"identity_source":"runtime-v1"}`)
		}
		initial, err := Runtime(vm, secret)
		if err != nil {
			t.Fatal(err)
		}
		clients := vmfake.NewSimpleClientset(vm)
		store, err := journal.NewStore(clients.NeonvmV1().VirtualMachines(vm.Namespace), journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		snap, err := store.Load(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		snap, err = store.Takeover(context.Background(), snap, snap.State().Owner, "actual-running-owner")
		if err != nil {
			t.Fatal(err)
		}
		running := snap.VirtualMachine()
		if _, err = Runtime(running, secret); err == nil {
			t.Fatal("observation relaxed original first-start gate")
		}
		observed, err := RuntimeForObservation(running, secret)
		if err != nil {
			t.Fatal(err)
		}
		before, _ := json.Marshal(initial)
		after, _ := json.Marshal(observed)
		if !bytes.Equal(before, after) {
			t.Fatal("running observation changed original startup bytes")
		}
		changed := secret.DeepCopy()
		changed.UID = "foreign-secret"
		if _, err = RuntimeForObservation(running, changed); err == nil {
			t.Fatal("observation accepted replacement Secret")
		}
	}
}
