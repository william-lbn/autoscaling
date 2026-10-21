package v1

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	extensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	extensionsvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	schemacel "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	kubejson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
)

// 直接载入发布CRD并使用锁定Kubernetes原生CEL引擎执行规则，不手抄表达式。
// 这是Mac本机schema语义验证，不是apiserver/etcd/Webhook部署认证；schema
// 缺规则或编译失败也必须让正/反用例失败，不能只检查yaml里有某段文字。
func TestDSGAPublishedCRDActuallyFencesHoldAndIdentityChanges(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "crd", "bases", "vm.neon.tech_virtualmachines.yaml"))
	require.NoError(t, err)
	var crd extensionsv1.CustomResourceDefinition
	require.NoError(t, yaml.Unmarshal(raw, &crd))
	require.NotEmpty(t, crd.Spec.Versions)
	var definition extensions.CustomResourceDefinition
	require.NoError(t, extensionsv1.Convert_v1_CustomResourceDefinition_To_apiextensions_CustomResourceDefinition(&crd, &definition, nil))
	definition.Status.StoredVersions = []string{crd.Spec.Versions[0].Name}
	require.Empty(t, extensionsvalidation.ValidateCustomResourceDefinition(context.Background(), &definition), "published definition must pass native schema/CEL admission compilation and cost checks")
	var internal extensions.JSONSchemaProps
	require.NoError(t, extensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(crd.Spec.Versions[0].Schema.OpenAPIV3Schema, &internal, nil))
	structural, err := schema.NewStructural(&internal)
	require.NoError(t, err)
	validator := schemacel.NewValidator(structural, true, celconfig.PerCallLimit)
	require.NotNil(t, validator, "removing every CEL rule must not pass")
	object := func(vm *VirtualMachine) map[string]any {
		// 走实际apiserver JSON解码语义（保留int64），不用直接Go对象转换器
		// 把uint32变成CEL不接受的uint64，也不经float64损失控制协议整数。
		raw, err := json.Marshal(vm)
		require.NoError(t, err)
		var result map[string]any
		err = kubejson.Unmarshal(raw, &result)
		require.NoError(t, err)
		return result
	}
	validate := func(next, old *VirtualMachine) bool {
		var previous any
		if old != nil {
			previous = object(old)
		}
		errors, _ := validator.Validate(context.Background(), field.NewPath("root"), structural, object(next), previous, celconfig.RuntimeCELCostBudget)
		if len(errors) != 0 {
			t.Logf("actual CRD CEL result: %v", errors)
		}
		return len(errors) == 0
	}
	held := heldBootstrapVM()
	require.True(t, validate(held, nil), "valid held CREATE must compile/evaluate all root and nested rules")
	for _, scenario := range []string{"remove", "add_online", "change_ciid", "pod_name", "node", "network", "ssh", "restart", "running_phase", "partial_release", "zero_incarnation"} {
		t.Run(scenario, func(t *testing.T) {
			old, next := held.DeepCopy(), held.DeepCopy()
			switch scenario {
			case "remove":
				next.Spec.DSGABootstrap = nil
			case "add_online":
				old.Spec.DSGABootstrap = nil
			case "change_ciid":
				next.Spec.DSGABootstrap.ComputeInstanceID = "another"
			case "pod_name":
				next.Status.PodName = "premature-runner"
			case "node":
				next.Status.Node = "premature-node"
			case "network":
				next.Status.ExtraNetIP = "10.0.0.1"
			case "ssh":
				next.Status.SSHSecretName = "premature-ssh"
			case "restart":
				next.Status.RestartCount = 1
			case "running_phase":
				next.Status.Phase = VmRunning
			case "partial_release":
				next.Spec.DSGABootstrap.VMUID = string(next.UID)
			case "zero_incarnation":
				next.Spec.DSGABootstrap.Incarnation = "00000000000000000000000000000000"
				old = nil
			}
			require.False(t, validate(next, old), "published CRD must reject unsafe transition itself")
		})
	}
	ready := held.DeepCopy()
	ready.Spec.DSGABootstrap.Ready = true
	ready.Spec.DSGABootstrap.VMUID = string(ready.UID)
	ready.Spec.DSGABootstrap.ConfigSecretUID = "config-uid"
	require.True(t, validate(ready, held), "complete release can persist atomically")
	running := ready.DeepCopy()
	running.Status.Phase = VmRunning
	running.Status.PodName = "runner"
	require.True(t, validate(running, ready), "released VM may enter ordinary runtime lifecycle")
	regressed := ready.DeepCopy()
	regressed.Spec.DSGABootstrap.Ready = false
	regressed.Spec.DSGABootstrap.VMUID = ""
	regressed.Spec.DSGABootstrap.ConfigSecretUID = ""
	require.False(t, validate(regressed, ready), "ready must never become held again")
}
