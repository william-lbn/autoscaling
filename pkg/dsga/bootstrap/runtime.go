package bootstrap

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	corev1 "k8s.io/api/core/v1"
)

// Runtime从调用方直接读取并已绑定的原VM/Secret构建启动来源，不含密码或私钥。
// 只处理真正pristine的首次创建；已有owner/history不能装成新Guest。运行后重启/
// 迁移须另走保留旧资源/VM不确定性的受控恢复，不以此包重新初始化epoch1。
func Runtime(vm *vmv1.VirtualMachine, secret *corev1.Secret) (resourceguard.RuntimeDocument, error) {
	var d resourceguard.RuntimeDocument
	if err := CheckReady(vm, secret, true); err != nil {
		return d, err
	}
	raw := secret.Data[resourceguard.ResourceSecretKey]
	if len(raw) == 0 || len(raw) > 16*1024 {
		return d, fmt.Errorf("original resource config missing or oversized")
	}
	d = resourceguard.RuntimeDocument{ProtocolVersion: 1, Incarnation: vm.Spec.DSGABootstrap.Incarnation, ConfigSecretUID: string(secret.UID), SourceJSON: append([]byte(nil), raw...)}
	sum := sha256.Sum256(raw)
	d.SourceSHA256 = hex.EncodeToString(sum[:])
	source, err := d.Source()
	if err != nil {
		return d, err
	}
	if source.Config.VMUID != string(vm.UID) || source.Config.ComputeInstanceID != vm.Spec.DSGABootstrap.ComputeInstanceID {
		return d, fmt.Errorf("resource source differs from original VM")
	}
	var native struct {
		Spec struct {
			Dynamic struct {
				MinimumGuestMemoryBytes uint64 `json:"minimum_guest_memory_bytes"`
				BootstrapBufferBytes    uint64 `json:"bootstrap_buffer_bytes"`
			} `json:"dynamic_shared_buffers"`
		} `json:"spec"`
		Control struct {
			JWKS struct {
				Keys []struct {
					Use       string   `json:"use"`
					Ops       []string `json:"key_ops"`
					Algorithm string   `json:"alg"`
					Type      string   `json:"kty"`
					Curve     string   `json:"crv"`
					X         string   `json:"x"`
				} `json:"keys"`
			} `json:"jwks"`
		} `json:"compute_ctl_config"`
	}
	config := secret.Data["config.json"]
	if err := control.ValidateJSON(config, 1<<20, 65536); err != nil {
		return d, err
	}
	if err := json.Unmarshal(config, &native); err != nil {
		return d, err
	}
	upper, err := source.Limits.TotalUpperBytes()
	if err != nil || upper > native.Spec.Dynamic.MinimumGuestMemoryBytes || native.Spec.Dynamic.BootstrapBufferBytes == 0 || uint64(source.Limits.MemoryMaxBytes) < native.Spec.Dynamic.BootstrapBufferBytes {
		return d, fmt.Errorf("resource source does not fit original native bootstrap")
	}
	keys := native.Control.JWKS.Keys
	if len(keys) != 1 || keys[0].Use != "sig" || len(keys[0].Ops) != 1 || keys[0].Ops[0] != "verify" || keys[0].Algorithm != "EdDSA" || keys[0].Type != "OKP" || keys[0].Curve != "Ed25519" || keys[0].X != source.ControlPublicKey {
		return d, fmt.Errorf("resource source control key differs from original Compute key")
	}
	return d, nil
}

// DecodeRuntime在实际Runner入口用固定上限解压并核对完整VM启动身份。这里没有
// Kube授权能力，不能把手工构造的flag当独立原UID证明；生产flag只由上层可信
// Controller生成。最终Runner/QMP持久fence仍须单独执行，来源不是VM缩容许可。
func DecodeRuntime(spec *vmv1.VirtualMachineSpec, encoded string) ([]byte, error) {
	if spec == nil {
		return nil, fmt.Errorf("missing VM spec")
	}
	if spec.DSGABootstrap == nil {
		if encoded != "" {
			return nil, fmt.Errorf("resource source on unmanaged VM")
		}
		return nil, nil
	}
	b := spec.DSGABootstrap
	if encoded == "" || len(encoded) > 64*1024 || b.ProtocolVersion != 1 || !b.Ready {
		return nil, fmt.Errorf("managed VM lacks original ready resource source")
	}
	compressed, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	gz, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	raw, err := io.ReadAll(io.LimitReader(gz, 64*1024+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 64*1024 {
		return nil, fmt.Errorf("expanded resource source exceeds bound")
	}
	var d resourceguard.RuntimeDocument
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	s, err := d.Source()
	if err != nil {
		return nil, err
	}
	if d.Incarnation != b.Incarnation || d.ConfigSecretUID != b.ConfigSecretUID || s.Config.VMUID != b.VMUID || s.Config.ComputeInstanceID != b.ComputeInstanceID {
		return nil, fmt.Errorf("runtime resource source differs from ready VM")
	}
	return raw, nil
}
