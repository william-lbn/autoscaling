//go:build linux

package bootstrap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmfake "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/fake"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/util/gzip64"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func runtimeTemplateSHA(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }

// 使用实际VM/Secret结构与原journal的release操作；仅Fake API，绝非真实Kube授权。
func runtimeTemplatesProducerFixture(t *testing.T, secretUID string) (*vmv1.VirtualMachine, *corev1.Secret) {
	t.Helper()
	vm := new(vmv1.VirtualMachine)
	vm.Name, vm.Namespace, vm.UID, vm.ResourceVersion = "vm", "testing", "vm-uid", "1"
	vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ComputeInstanceID: "compute-1"}
	clients := vmfake.NewSimpleClientset(vm)
	store, err := journal.NewStore(clients.NeonvmV1().VirtualMachines(vm.Namespace), journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Initialize(context.Background(), "1", "bootstrap-owner")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.ReleaseBootstrap(context.Background(), snapshot.ResourceVersion(), types.UID(secretUID))
	if err != nil {
		t.Fatal(err)
	}
	vm = snapshot.VirtualMachine()
	controller, immutable := true, true
	secret := new(corev1.Secret)
	secret.Name, secret.Namespace, secret.ResourceVersion = "vm-config", "testing", "1"
	secret.UID = types.UID(secretUID)
	secret.Immutable = &immutable
	secret.OwnerReferences = []metav1.OwnerReference{{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine", Name: vm.Name, UID: vm.UID, Controller: &controller}}
	key := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	source := resourceguard.SourceConfig{ProtocolVersion: 1, Config: resourceguard.Config{VMUID: string(vm.UID), ComputeInstanceID: "compute-1", StateDir: resourceguard.FixedStateDir, SystemCgroup: true}, IssuerEpoch: 1, BudgetCommitmentID: "template-bootstrap-budget",
		Limits: resourceguard.Limits{MemoryMaxBytes: 512 << 20, OutsideUpperBytes: 128 << 20, SystemMemoryMaxBytes: 64 << 20, PidsMaxTasks: 128, AddressSpaceMaxBytes: 4 << 30, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 0}, {PageSizeKB: 1048576, Pages: 0}}}, ControlPublicKey: key}
	resource, _ := json.Marshal(source)
	config := map[string]any{"spec": map[string]any{"dynamic_shared_buffers": map[string]any{"protocol_version": 2, "vm_uid": string(vm.UID), "compute_instance_id": "compute-1", "minimum_guest_memory_bytes": 1 << 30, "bootstrap_buffer_bytes": 2 << 20}}, "compute_ctl_config": map[string]any{"jwks": map[string]any{"keys": []any{map[string]any{"use": "sig", "key_ops": []string{"verify"}, "alg": "EdDSA", "kty": "OKP", "crv": "Ed25519", "x": key}}}}}
	configRaw, _ := json.Marshal(config)
	ref, err := resourceguard.LookupPhysicalKernelBuild(resourceguard.NormalSplitV1PhysicalKernelProfileID)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"dsga-physical-kernel.json", "dsga-kernel-bootstrap.json", "dsga-prebootstrap-system.json", "dsga-normal-split.json", "dsga-native-birth.json"}
	policies := []map[string]any{
		{"protocol_version": 1, "profile_id": ref.ProfileID, "kernel_release": ref.KernelRelease, "kernel_notes_sha256": ref.KernelNotesSHA256, "kernel_artifact_sha256": ref.KernelArtifactSHA256, "kernel_config_sha256": ref.KernelConfigSHA256},
		{"protocol_version": 1, "profile_id": ref.ProfileID, "controls": resourceguard.NormalV1KernelAllocatorControls{PIDMax: 512, MinFreeKBytes: 2048, WatermarkScaleFactor: 10, PerCPUPagelistHighFraction: 16}, "modules_disabled": 1, "numa_balancing": 0, "demotion_enabled": false},
		{"protocol_version": 2, "scope": "pre-bootstrap-system-placement-only", "physical_profile_id": ref.ProfileID, "filesystem_profile_sha256": strings.Repeat("ab", 32), "boot_memory_upper_bytes": "268435456"},
		{"protocol_version": 1, "scope": "normal-split-topology-preparation-only", "physical_profile_id": ref.ProfileID, "private_memory_max_bytes": "67108864", "initial_arrays_memory_max_bytes": "67108864", "private_pids_max_tasks": "64"},
		{"protocol_version": 1, "scope": "neon-native-birth-policy-v1", "build_manifest_sha256": strings.Repeat("cd", 32), "pg_major": 17, "max_buffers": 128, "startup_buffers": 32, "page_bytes": 4096, "element_bytes": [4]uint64{64, 8192, 16, 20}},
	}
	var templates []map[string]any
	for i, p := range policies {
		templates = append(templates, map[string]any{"asset": names[i], "policy": p})
	}
	blob, _ := json.Marshal(map[string]any{"protocol_version": 1, "scope": "neon-dsga-policy-templates-v1", "templates": templates})
	secret.Data = map[string][]byte{"config.json": configRaw, resourceguard.ResourceSecretKey: append([]byte(" \n"), resource...), resourceguard.PolicyTemplatesSecretKey: append([]byte(" \n"), blob...)}
	return vm, secret
}

func TestRuntimeTemplatesProducerOriginalUIDNoCycleAndAssets(t *testing.T) {
	var priorDoc []byte
	var priorAssets map[string][]byte
	for _, uid := range []string{"first-assigned-config-uid", "second-assigned-config-uid"} {
		vm, secret := runtimeTemplatesProducerFixture(t, uid)
		d, err := Runtime(vm, secret)
		if err != nil {
			t.Fatal(err)
		}
		if d.ProtocolVersion != 2 || d.ConfigSecretUID != uid || !bytes.Equal(d.PolicyTemplatesJSON, secret.Data[resourceguard.PolicyTemplatesSecretKey]) || !bytes.Equal(d.SourceJSON, secret.Data[resourceguard.ResourceSecretKey]) {
			t.Fatal("producer lost original UID/raw bytes")
		}
		raw, _ := json.Marshal(d)
		decoded, err := DecodeRuntime(&vm.Spec, gzip64.Encode(raw))
		if err != nil || !bytes.Equal(decoded, raw) {
			t.Fatal("old decoder changed exact raw contract", err)
		}
		assets, err := DecodeRuntimeAssets(&vm.Spec, gzip64.Encode(raw))
		if err != nil || len(assets) != 6 || !bytes.Equal(assets[resourceguard.RuntimeResourceAssetName], raw) {
			t.Fatal("assets decoder failed", err, len(assets))
		}
		for name, policy := range assets {
			if name == resourceguard.RuntimeResourceAssetName {
				continue
			}
			var fields map[string]any
			_ = json.Unmarshal(policy, &fields)
			if fields["runtime_document_sha256"] != runtimeTemplateSHA(raw) {
				t.Fatal("effective policy did not bind assigned UID document", name)
			}
			if priorDoc != nil && bytes.Equal(priorAssets[name], policy) {
				t.Fatal("different assigned UID reused effective policy", name)
			}
		}
		if priorDoc != nil && runtimeTemplateSHA(priorDoc) == runtimeTemplateSHA(raw) {
			t.Fatal("UID did not affect document")
		}
		if bytes.Contains(secret.Data[resourceguard.PolicyTemplatesSecretKey], []byte("runtime_document_sha256")) {
			t.Fatal("effective SHA fed back into immutable Secret")
		}
		priorDoc, priorAssets = raw, assets
	}
}
func TestRuntimeTemplatesProducerRejectsUntrustedOrPartialInputs(t *testing.T) {
	for _, name := range []string{"mutable", "owner", "secret_uid", "empty", "oversize", "partial", "scope", "dynamic", "missing_parent", "pg18"} {
		t.Run(name, func(t *testing.T) {
			vm, s := runtimeTemplatesProducerFixture(t, "original-secret")
			blob := s.Data[resourceguard.PolicyTemplatesSecretKey]
			switch name {
			case "mutable":
				v := false
				s.Immutable = &v
			case "owner":
				s.OwnerReferences[0].UID = "foreign-vm"
			case "secret_uid":
				s.UID = "recreated-secret"
			case "empty":
				blob = nil
			case "oversize":
				blob = bytes.Repeat([]byte(" "), 16<<10+1)
			case "partial":
				blob = blob[:len(blob)-1]
			case "scope":
				blob = bytes.Replace(blob, []byte("neon-dsga-policy-templates-v1"), []byte("other-scope"), 1)
			case "dynamic":
				blob = bytes.Replace(blob, []byte(`"pg_major":17`), []byte(`"runtime_document_sha256":"","pg_major":17`), 1)
			case "pg18":
				blob = bytes.Replace(blob, []byte(`"pg_major":17`), []byte(`"pg_major":18`), 1)
			case "missing_parent":
				var envelope map[string]any
				_ = json.Unmarshal(blob, &envelope)
				entries := envelope["templates"].([]any)
				envelope["templates"] = entries[1:]
				blob, _ = json.Marshal(envelope)
			}
			s.Data[resourceguard.PolicyTemplatesSecretKey] = blob
			if _, err := Runtime(vm, s); err == nil {
				t.Fatal("invalid original Secret/templates accepted")
			}
		})
	}
}
func TestRuntimeTemplatesDecodeBindsReadyIdentityAndBounds(t *testing.T) {
	vm, s := runtimeTemplatesProducerFixture(t, "original-secret")
	d, err := Runtime(vm, s)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(d)
	encoded := gzip64.Encode(raw)
	for _, name := range []string{"vm_uid", "secret_uid", "incarnation", "compute", "not_ready", "unmanaged"} {
		t.Run(name, func(t *testing.T) {
			spec := vm.Spec.DeepCopy()
			switch name {
			case "vm_uid":
				spec.DSGABootstrap.VMUID = "foreign-vm"
			case "secret_uid":
				spec.DSGABootstrap.ConfigSecretUID = "foreign-secret"
			case "incarnation":
				spec.DSGABootstrap.Incarnation = "2234567890abcdef1234567890abcdef"
			case "compute":
				spec.DSGABootstrap.ComputeInstanceID = "foreign-compute"
			case "not_ready":
				spec.DSGABootstrap.Ready = false
			case "unmanaged":
				spec.DSGABootstrap = nil
			}
			if _, err := DecodeRuntimeAssets(spec, encoded); err == nil {
				t.Fatal("foreign/unready runtime accepted")
			}
		})
	}
	for _, bad := range []string{strings.Repeat("A", 64<<10+1), gzip64.Encode(bytes.Repeat([]byte("x"), 64<<10+1)), gzip64.Encode(raw[:len(raw)-1])} {
		if _, err := DecodeRuntimeAssets(&vm.Spec, bad); err == nil {
			t.Fatal("oversize/partial runtime accepted")
		}
	}
	noncanonical := append([]byte(" \n"), raw...)
	if _, err := DecodeRuntime(&vm.Spec, gzip64.Encode(noncanonical)); err != nil {
		t.Fatal("legacy decoder behavior was unnecessarily changed", err)
	}
	if _, err := DecodeRuntimeAssets(&vm.Spec, gzip64.Encode(noncanonical)); err == nil {
		t.Fatal("assets renderer accepted noncanonical v2")
	}
	d.PolicyTemplatesJSON = append([]byte(" "), d.PolicyTemplatesJSON...)
	drift, _ := json.Marshal(d)
	if _, err := DecodeRuntimeAssets(&vm.Spec, gzip64.Encode(drift)); err == nil {
		t.Fatal("template raw SHA drift accepted")
	}
}
func TestRuntimeTemplatesV1ProducerRemainsByteCompatible(t *testing.T) {
	vm, s := runtimeTemplatesProducerFixture(t, "original-secret")
	delete(s.Data, resourceguard.PolicyTemplatesSecretKey)
	d, err := Runtime(vm, s)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(d)
	if d.ProtocolVersion != 1 || bytes.Contains(raw, []byte("policy_templates")) {
		t.Fatal("v1 byte contract expanded")
	}
	assets, err := DecodeRuntimeAssets(&vm.Spec, gzip64.Encode(raw))
	if err != nil || len(assets) != 1 || !bytes.Equal(assets[resourceguard.RuntimeResourceAssetName], raw) {
		t.Fatal("v1 resource asset changed", err)
	}
	spec := vm.Spec.DeepCopy()
	spec.DSGABootstrap = nil
	if assets, err := DecodeRuntimeAssets(spec, ""); err != nil || assets != nil {
		t.Fatal("unmanaged legacy behavior changed", assets, err)
	}
}
