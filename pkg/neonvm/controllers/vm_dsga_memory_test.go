package controllers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootstrap"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/neondatabase/autoscaling/pkg/util/gzip64"
)

// 只验证Controller的实际直接读取/唯一来源解码及状态路径。Kube API/Runner
// 发布数据是显式夹具，不启动QEMU，也不以此测试声称最终预算/内存热拔完成。
func TestManagedControllerMemoryConsumesOriginalRunnerPublication(t *testing.T) {
	for _, what := range []string{"complete", "missing-reader", "pod-uid", "node-uid", "floor-holder", "state-uid", "state-deleting", "birth-unprotected"} {
		t.Run(what, func(t *testing.T) {
			p := newTestParams(t)
			require.NoError(t, core.AddToScheme(p.r.Scheme))
			_, vm, _ := controllerBootstrapInputs(t)
			vm.Status.PodName, vm.Status.Node, vm.Status.PodIP = "runner", "node", "10.0.0.8"
			binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation}
			snapshot, err := journal.Inspect(vm, binding)
			require.NoError(t, err)
			id := vmguard.Identity{VMUID: string(vm.UID), Incarnation: binding.Incarnation, RunnerPodUID: "runner-uid", RunnerBootID: "0123456789abcdef0123456789abcdef", NodeUID: "node-uid", MinimumBytes: 1 << 30, MaximumBytes: 4 << 30, BlockBytes: 8 << 20}
			birthRef, stateRef := budget.ObjectRef{Name: vmguard.BindingName(binding), UID: "birth-uid"}, budget.ObjectRef{Name: "runner-state", UID: "state-uid"}
			birth := vmguard.Birth{ProtocolVersion: 1, Identity: id, StateClaim: stateRef}
			view := vmguard.View{ProtocolVersion: 1, Identity: id, BindingClaim: birthRef, Sequence: 1, Floor: snapshot.State().Owner, LeaseUID: "lease-uid", HighWater: nil, Pending: nil, Last: nil, LastPublished: nil}
			publication := vmguard.Publication{ProtocolVersion: 1, Identity: id, BindingClaim: &birthRef, State: &view, Actual: &vmguard.Observation{RequestedBytes: 2 << 30, ActualBytes: 2 << 30}}
			controller, block, immutable, mutable := true, true, true, false
			owner := []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: vm.Name, UID: vm.UID, Controller: &controller, BlockOwnerDeletion: &block}}
			birthCM := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: birthRef.Name, Namespace: vm.Namespace, UID: "birth-uid", ResourceVersion: "1", Labels: map[string]string{vmguard.ClaimLabel: "binding-v1"}, Finalizers: []string{vmguard.ProtectionFinalizer}, OwnerReferences: owner}, Immutable: &immutable}
			stateCM := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: stateRef.Name, Namespace: vm.Namespace, UID: "state-uid", ResourceVersion: "1", Labels: map[string]string{vmguard.ClaimLabel: "state-v1"}, Finalizers: []string{vmguard.ProtectionFinalizer}, OwnerReferences: owner}, Immutable: &mutable}
			pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: vm.Namespace, UID: "runner-uid", ResourceVersion: "1", OwnerReferences: owner}, Spec: core.PodSpec{NodeName: "node"}, Status: core.PodStatus{Phase: core.PodRunning, PodIP: vm.Status.PodIP}}
			node := &core.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid", ResourceVersion: "1"}}
			switch what {
			case "pod-uid":
				pod.UID = "new-pod"
			case "node-uid":
				node.UID = "new-node"
			case "floor-holder":
				publication.State.Floor.HolderID = "another-holder"
			case "state-uid":
				stateCM.UID = "new-state"
			case "state-deleting":
				stamp := metav1.Now()
				stateCM.DeletionTimestamp = &stamp
			case "birth-unprotected":
				birthCM.Finalizers = nil
			}
			b, err := json.Marshal(birth)
			require.NoError(t, err)
			birthCM.Data = map[string]string{vmguard.ClaimKey: string(b)}
			b, err = json.Marshal(publication)
			require.NoError(t, err)
			stateCM.Data = map[string]string{vmguard.ClaimKey: string(b)}
			p.r.APIReader = fake.NewClientBuilder().WithScheme(p.r.Scheme).WithObjects(vm, birthCM, stateCM, pod, node).Build()
			if what == "missing-reader" {
				p.r.APIReader = nil
			}
			memory, pending, err := p.r.dsgaMemory(context.Background(), vm)
			if what != "complete" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, int64(2<<30), memory.Value())
			require.False(t, pending)
		})
	}
}

func TestManagedRunnerPodBuildsOriginalPrivateMemoryEnvironment(t *testing.T) {
	for _, what := range []string{"complete", "empty-service-account", "default-service-account", "qmp-cpu", "nil-cpu"} {
		t.Run(what, func(t *testing.T) {
			p := newTestParams(t)
			_, vm, secret := controllerBootstrapInputs(t)
			mode := vmv1.CpuScalingModeSysfs
			vm.Spec.CpuScalingMode, vm.Spec.ServiceAccountName = &mode, "original-dsga-runner"
			switch what {
			case "empty-service-account":
				vm.Spec.ServiceAccountName = ""
			case "default-service-account":
				vm.Spec.ServiceAccountName = "default"
			case "qmp-cpu":
				mode = vmv1.CpuScalingModeQMP
			case "nil-cpu":
				vm.Spec.CpuScalingMode = nil
			}
			key := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
			source := resourceguard.SourceConfig{ProtocolVersion: 1, Config: resourceguard.Config{VMUID: string(vm.UID), ComputeInstanceID: vm.Spec.DSGABootstrap.ComputeInstanceID, StateDir: resourceguard.FixedStateDir, SystemCgroup: false}, IssuerEpoch: 1, BudgetCommitmentID: "original-bootstrap-budget", Limits: resourceguard.Limits{MemoryMaxBytes: 64 << 20, OutsideUpperBytes: 32 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 8}}, PidsMaxTasks: 0, SystemMemoryMaxBytes: 0}, ControlPublicKey: key}
			raw, err := json.Marshal(source)
			require.NoError(t, err)
			secret.Data[resourceguard.ResourceSecretKey] = raw
			native := map[string]any{"spec": map[string]any{"dynamic_shared_buffers": map[string]any{"protocol_version": 2, "vm_uid": string(vm.UID), "compute_instance_id": source.Config.ComputeInstanceID, "minimum_guest_memory_bytes": 128 << 20, "bootstrap_buffer_bytes": 2 << 20}}, "compute_ctl_config": map[string]any{"jwks": map[string]any{"keys": []any{map[string]any{"use": "sig", "key_ops": []string{"verify"}, "alg": "EdDSA", "kty": "OKP", "crv": "Ed25519", "x": key}}}}}
			raw, err = json.Marshal(native)
			require.NoError(t, err)
			secret.Data["config.json"] = raw
			p.r.APIReader = fake.NewClientBuilder().WithScheme(p.r.Scheme).WithObjects(vm, secret).Build()
			pod, err := p.r.podForVirtualMachine(context.Background(), vm, nil)
			if what != "complete" {
				require.ErrorContains(t, err, "explicit service account and sysfs CPU mode")
				require.Nil(t, pod)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, pod.Spec.AutomountServiceAccountToken)
			require.True(t, *pod.Spec.AutomountServiceAccountToken)
			require.Equal(t, vm.Spec.ServiceAccountName, pod.Spec.ServiceAccountName)
			found := false
			for _, c := range pod.Spec.Containers {
				if c.Name != "neonvm-runner" {
					continue
				}
				found = true
				for _, port := range c.Ports {
					require.NotEqual(t, "qmp", port.Name)
					require.NotEqual(t, "qmp-manual", port.Name)
				}
				env := map[string]core.EnvVar{}
				for _, v := range c.Env {
					env[v.Name] = v
				}
				require.Equal(t, "metadata.uid", env["K8S_POD_UID"].ValueFrom.FieldRef.FieldPath)
				require.Equal(t, "metadata.namespace", env["K8S_NAMESPACE"].ValueFrom.FieldRef.FieldPath)
				require.Equal(t, "spec.nodeName", env["K8S_NODE_NAME"].ValueFrom.FieldRef.FieldPath)
				require.Equal(t, vm.Name, env["DSGA_VM_NAME"].Value)
				mounted := false
				for _, m := range c.VolumeMounts {
					if m.Name == "dsga-runner-state" && m.MountPath == "/vm/dsga-runner" {
						mounted = true
					}
				}
				require.True(t, mounted)
				flag := false
				for i, arg := range c.Command {
					if arg == "-dsga-resource" {
						require.Less(t, i+1, len(c.Command))
						decoded, err := gzip64.Decode(c.Command[i+1])
						require.NoError(t, err)
						original, err := bootstrap.DecodeRuntime(&vm.Spec, c.Command[i+1])
						require.NoError(t, err)
						require.Equal(t, decoded, original)
						flag = true
					}
				}
				require.True(t, flag)
			}
			require.True(t, found)
		})
	}
}
