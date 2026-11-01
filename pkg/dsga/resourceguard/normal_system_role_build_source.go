package resourceguard

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const normalSystemRoleBrokerLifetime = "initial-root-to-pid1-after-durable-release"

const NormalSystemRoleBrokerDeclarationPath = "/neonvm/config/dsga-normal-role-broker.json"
const normalSystemRoleBrokerDeclarationPath = NormalSystemRoleBrokerDeclarationPath
const normalSystemRoleServiceSocketPath = "/run/neon-dsga/system-role.sock"
const normalSystemRoleBrokerDeclarationScope = "fixed-system-role-broker-declaration-v1"
const normalSystemRoleBrokerDeclarationV2Scope = "fixed-system-role-broker-declaration-v2"
const normalSystemRoleBrokerOwnedAuthorityMode = "broker-owned-same-guard-v1"
const normalSystemRoleBrokerExecutable = "/neonvm/bin/dsga-resource-guard"

type normalSystemRoleBrokerDeclaration struct {
	// 仅新声明2显式选择；声明1省略此字段，原JSON字节与R3行为不变。
	ResourceAuthorityMode    string `json:"resource_authority_mode,omitempty"`
	ProtocolVersion          uint32 `json:"protocol_version"`
	Scope                    string `json:"scope"`
	ExecutablePath           string `json:"executable_path"`
	ExecutableSHA256         string `json:"executable_sha256"`
	ArgvSHA256               string `json:"argv_sha256"`
	EnvironmentSHA256        string `json:"environment_sha256"`
	NamespaceProtocolVersion uint32 `json:"namespace_protocol_version"`
	GateProtocolVersion      uint32 `json:"gate_protocol_version"`
	Lifetime                 string `json:"lifetime"`
	ComputeBuildPlanSHA256   string `json:"compute_build_plan_sha256"`
}

func normalSystemRoleBrokerArgv() []string {
	return []string{normalSystemRoleBrokerExecutable, "system-role-broker"}
}
func normalSystemRoleBrokerEnvironment() []string { return []string{"PATH=/neonvm/bin"} }

// 只生成构建期固定声明；两个摘要不能授予Guest来源或normal降限资格。
// 与运行期typed reader共用同一个DTO/固定向量，避免不同构建端重复协议。
func NormalSystemRoleBrokerDeclarationJSON(executableSHA256, computeBuildPlanSHA256 string) ([]byte, error) {
	for _, h := range []string{executableSHA256, computeBuildPlanSHA256} {
		b, e := hex.DecodeString(h)
		if e != nil || len(b) != 32 || hex.EncodeToString(b) != h || h == strings.Repeat("0", 64) {
			return nil, fmt.Errorf("fixed broker declaration requires canonical nonzero SHA256")
		}
	}
	argv, e := normalSystemVectorSHA(normalSystemRoleBrokerArgv())
	if e != nil {
		return nil, e
	}
	env, e := normalSystemVectorSHA(normalSystemRoleBrokerEnvironment())
	if e != nil {
		return nil, e
	}
	return json.Marshal(normalSystemRoleBrokerDeclaration{ProtocolVersion: 1, Scope: normalSystemRoleBrokerDeclarationScope, ExecutablePath: normalSystemRoleBrokerExecutable, ExecutableSHA256: executableSHA256, ArgvSHA256: argv, EnvironmentSHA256: env, NamespaceProtocolVersion: 1, GateProtocolVersion: 1, Lifetime: normalSystemRoleBrokerLifetime, ComputeBuildPlanSHA256: computeBuildPlanSHA256})
}

// 新构建期固定选择：只能R2同一个Guard，不接受动态mode/预算/路径。
// 生成声明仍不证明运行来源；原System库存/RuntimeClosure必须绑定实际字节。
func NormalSystemRoleBrokerDeclarationV2JSON(executableSHA256, computeBuildPlanSHA256 string) ([]byte, error) {
	raw, e := NormalSystemRoleBrokerDeclarationJSON(executableSHA256, computeBuildPlanSHA256)
	if e != nil {
		return nil, e
	}
	var d normalSystemRoleBrokerDeclaration
	if e = json.Unmarshal(raw, &d); e != nil {
		return nil, e
	}
	d.ProtocolVersion = 2
	d.Scope = normalSystemRoleBrokerDeclarationV2Scope
	d.ResourceAuthorityMode = normalSystemRoleBrokerOwnedAuthorityMode
	return json.Marshal(d)
}
func (d normalSystemRoleBrokerDeclaration) brokerOwnsAuthority() bool {
	return d.ProtocolVersion == 2 && d.Scope == normalSystemRoleBrokerDeclarationV2Scope && d.ResourceAuthorityMode == normalSystemRoleBrokerOwnedAuthorityMode
}
