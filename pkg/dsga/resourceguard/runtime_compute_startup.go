package resourceguard

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 通用镜像只接受一个实例名称槽位。原资源预算仍由原Runtime/Secret绑定；
// 此元数据不含口令/私钥，不签发资源许可，也不替代原configure认证。
const ComputeStartupSecretKey = "dsga-compute-startup.json"
const RuntimeComputeIdentitySource = "runtime-v1"
const RuntimeComputeIdentityPlaceholder = "@neon-workload@"
const RuntimeComputePublicAssetName = "dsga-compute-public.json"
const RuntimeComputePublicPath = "/neonvm/runtime/" + RuntimeComputePublicAssetName

type ComputeStartupSelection struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	IdentitySource  string `json:"identity_source"`
}

func (s *ComputeStartupSelection) UnmarshalJSON(raw []byte) error {
	type plain ComputeStartupSelection
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	*s = ComputeStartupSelection(next)
	return nil
}
func (s ComputeStartupSelection) Validate() error {
	if s.ProtocolVersion != 1 || s.IdentitySource != RuntimeComputeIdentitySource {
		return fmt.Errorf("unsupported original Compute startup selection")
	}
	return nil
}

type RuntimeComputeStartup struct {
	ProtocolVersion    uint32 `json:"protocol_version"`
	WorkloadName       string `json:"workload_name"`
	NativeConfigSHA256 string `json:"native_config_sha256"`
	ControlKeyID       string `json:"control_key_id"`
}

func (s *RuntimeComputeStartup) UnmarshalJSON(raw []byte) error {
	type plain RuntimeComputeStartup
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	*s = RuntimeComputeStartup(next)
	return nil
}

var runtimeWorkloadName = regexp.MustCompile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
var runtimeKeyID = regexp.MustCompile("^[A-Za-z0-9_-]{1,128}$")

func (s RuntimeComputeStartup) validate() error {
	digest, err := hex.DecodeString(s.NativeConfigSHA256)
	if s.ProtocolVersion != 1 || len(s.WorkloadName) > 63 || !runtimeWorkloadName.MatchString(s.WorkloadName) ||
		err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != s.NativeConfigSHA256 || !runtimeKeyID.MatchString(s.ControlKeyID) {
		return fmt.Errorf("invalid original Compute startup binding")
	}
	return nil
}
func validateRuntimeComputeStartup(d RuntimeDocument, source SourceConfig) error {
	if d.ProtocolVersion != 3 {
		if d.ComputeStartup != nil {
			return fmt.Errorf("Compute startup requires runtime v3")
		}
		return nil
	}
	if d.ComputeStartup == nil || source.Config.ExecutionModel != TrustedControllerExecutionModel {
		return fmt.Errorf("runtime v3 requires trusted-controller Compute startup")
	}
	return d.ComputeStartup.validate()
}

// 公钥来自同一原resource.json；kid与配置摘要来自同一immutable config.json。
// spec为空，PG仍等待正式configure，不以生成资产冒充数据库已启动。
func runtimeComputePublicConfig(d RuntimeDocument, source SourceConfig) ([]byte, error) {
	if err := validateRuntimeComputeStartup(d, source); err != nil {
		return nil, err
	}
	if d.ComputeStartup == nil {
		return nil, nil
	}
	return json.Marshal(map[string]any{
		"spec": nil,
		"compute_ctl_config": map[string]any{"tls": nil, "jwks": map[string]any{"keys": []any{
			map[string]any{"use": "sig", "key_ops": []string{"verify"}, "alg": "EdDSA", "kid": d.ComputeStartup.ControlKeyID, "kty": "OKP", "crv": "Ed25519", "x": source.ControlPublicKey},
		}}},
	})
}

// 只允许--compute-id的一个明确槽位；不能将runtime扩成任意argv/env模板。
// 旧固定镜像保持原合同，新模式必须等待原认证configure并使用派生公钥文件。
func ValidateComputeIdentityTemplate(identitySource, model string, argv, env []string) error {
	if identitySource == "" {
		for _, value := range append(append([]string(nil), argv...), env...) {
			if strings.Contains(value, RuntimeComputeIdentityPlaceholder) {
				return fmt.Errorf("runtime identity placeholder without explicit selection")
			}
		}
		return nil
	}
	if identitySource != RuntimeComputeIdentitySource || model != TrustedControllerExecutionModel {
		return fmt.Errorf("unsupported Compute identity source")
	}
	ids, configs := 0, 0
	for i, value := range argv {
		if value == "--compute-id" {
			ids++
			if i+1 >= len(argv) || argv[i+1] != RuntimeComputeIdentityPlaceholder {
				return fmt.Errorf("Compute identity must use the original runtime slot")
			}
		} else if value == "-i" || strings.HasPrefix(value, "--compute-id=") {
			return fmt.Errorf("alternate Compute identity override")
		}
		if value == "--config" {
			configs++
			if i+1 >= len(argv) || argv[i+1] != RuntimeComputePublicPath {
				return fmt.Errorf("Compute public config must use the original runtime asset")
			}
		} else if value == "-c" || strings.HasPrefix(value, "--config=") || value == "-p" || value == "--control-plane-uri" || strings.HasPrefix(value, "--control-plane-uri=") {
			return fmt.Errorf("alternate Compute configuration override")
		}
		if strings.Contains(value, RuntimeComputeIdentityPlaceholder) && (value != RuntimeComputeIdentityPlaceholder || i == 0 || argv[i-1] != "--compute-id") {
			return fmt.Errorf("Compute identity placeholder outside fixed slot")
		}
	}
	for _, value := range env {
		if strings.Contains(value, RuntimeComputeIdentityPlaceholder) {
			return fmt.Errorf("Compute identity cannot be an environment template")
		}
	}
	if ids != 1 || configs != 1 {
		return fmt.Errorf("runtime Compute requires one identity and public config slot")
	}
	return nil
}
