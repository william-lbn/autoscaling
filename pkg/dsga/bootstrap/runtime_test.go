package bootstrap

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/util/gzip64"
)

// 仅验证typed输入/字节与模式合约；不执行K8s API网络/RBAC/Pod或Guest内核。
func TestRuntimeSourceRequiresOriginalImmutableNativeAndResourceInputs(t *testing.T) {
	vm, secret := readyInputs(t)
	key := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	s := resourceguard.SourceConfig{ProtocolVersion: 1, Config: resourceguard.Config{VMUID: string(vm.UID), ComputeInstanceID: vm.Spec.DSGABootstrap.ComputeInstanceID, StateDir: resourceguard.FixedStateDir}, IssuerEpoch: 1, BudgetCommitmentID: "bootstrap-budget", Limits: resourceguard.Limits{MemoryMaxBytes: 64 << 20, OutsideUpperBytes: 32 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 8}, {PageSizeKB: 1048576, Pages: 0}}}, ControlPublicKey: key}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	secret.Data[resourceguard.ResourceSecretKey] = append([]byte(" \n"), raw...)
	config := map[string]any{"spec": map[string]any{"dynamic_shared_buffers": map[string]any{"protocol_version": 2, "vm_uid": string(vm.UID), "compute_instance_id": s.Config.ComputeInstanceID, "minimum_guest_memory_bytes": 128 << 20, "bootstrap_buffer_bytes": 2 << 20}}, "compute_ctl_config": map[string]any{"jwks": map[string]any{"keys": []any{map[string]any{"use": "sig", "key_ops": []string{"verify"}, "alg": "EdDSA", "kty": "OKP", "crv": "Ed25519", "x": key}}}}}
	secret.Data["config.json"], err = json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	d, err := Runtime(vm, secret)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(d.SourceJSON, secret.Data[resourceguard.ResourceSecretKey]) {
		t.Fatal("original source bytes were normalized away")
	}
	doc, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRuntime(&vm.Spec, gzip64.Encode(doc))
	if err != nil || !bytes.Equal(decoded, doc) {
		t.Fatal("actual Runner decoder lost original document", err)
	}
	for _, name := range []string{"missing", "mutable", "uid", "foreign_source", "native_key"} {
		t.Run(name, func(t *testing.T) {
			x := secret.DeepCopy()
			switch name {
			case "missing":
				delete(x.Data, resourceguard.ResourceSecretKey)
			case "mutable":
				f := false
				x.Immutable = &f
			case "uid":
				x.UID = "another-secret"
			case "foreign_source":
				next := s
				next.Config.VMUID = "another-vm"
				x.Data[resourceguard.ResourceSecretKey], _ = json.Marshal(next)
			case "native_key":
				next := s
				next.ControlPublicKey = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32))
				x.Data[resourceguard.ResourceSecretKey], _ = json.Marshal(next)
			}
			if _, err := Runtime(vm, x); err == nil {
				t.Fatal("invalid original input accepted")
			}
		})
	}
	if _, err := DecodeRuntime(&vm.Spec, ""); err == nil {
		t.Fatal("managed source omitted")
	}
	other := vm.Spec.DeepCopy()
	other.DSGABootstrap.ComputeInstanceID = "another-compute"
	if _, err := DecodeRuntime(other, gzip64.Encode(doc)); err == nil {
		t.Fatal("Runner accepted foreign Compute")
	}
	other.DSGABootstrap = nil
	if _, err := DecodeRuntime(other, gzip64.Encode(doc)); err == nil {
		t.Fatal("source accepted on legacy VM")
	}
	if _, err := DecodeRuntime(&vm.Spec, gzip64.Encode(bytes.Repeat([]byte("a"), 64*1024+1))); err == nil {
		t.Fatal("expanded source exceeds limit")
	}
}
