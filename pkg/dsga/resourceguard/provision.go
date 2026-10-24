package resourceguard

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ManagedGuest只用于收紧旧入口，绝不是预算/身份授权。错误和残留原配置均保持
// 受管，不能因runtime盘暂时不可读或原文件变符号链接就退回未认证控制通道。
func ManagedGuest() bool {
	if managedGuestPaths([]string{FixedRuntimePath, "/etc/neon-dsga", FixedStateDir}) {
		return true
	}
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile("/proc/cmdline")
		if err != nil {
			return true
		}
		return managedKernelArgs(string(b))
	}
	return false
}
func managedKernelArgs(raw string) bool {
	for _, arg := range strings.Fields(raw) {
		if arg == ManagedKernelArg {
			return true
		}
	}
	return false
}
func managedGuestPaths(paths []string) bool {
	for _, p := range paths {
		if _, err := os.Lstat(p); err == nil || !os.IsNotExist(err) {
			return true
		}
	}
	return false
}

const (
	FixedStateDir          = "/var/lib/neon-dsga/state"
	FixedRuntimePath       = "/neonvm/runtime/dsga-resource.json"
	FixedSealedRuntimePath = "/etc/neon-dsga/runtime.json"
	ResourceSecretKey      = "resource.json"
	ManagedKernelArg       = "neonvm.dsga=1"
	FixedManagementTLSPath = "/run/neon/management"
)

// SourceConfig在原创建计划/immutable Secret中冻结。它不是租户提交的限额，也
// 不从启动RSS/百分比补默认值；完整外部上界仍须由平台生产者真实保留。只携带
// 管理公钥，永远不携带签名seed、TLS私钥或数据库口令。初始资源epoch固定1，
// 之后由独立于PG boot的资源floor接管，不能在恢复时重造这份初始意图。
type SourceConfig struct {
	ProtocolVersion    uint32         `json:"protocol_version"`
	Config             Config         `json:"config"`
	IssuerEpoch        control.Uint64 `json:"issuer_epoch"`
	BudgetCommitmentID string         `json:"budget_commitment_id"`
	Limits             Limits         `json:"limits"`
	ControlPublicKey   string         `json:"control_public_key"`
}

// RuntimeDocument由可信Controller/Runner从原UID的immutable配置生成，在只读
// runtime ISO上交Guest。SourceSHA256覆盖原Secret的准确resource.json字节；不能
// 重序列化/只比较几个字段后丢弃来源。该包验证结构与字节绑定，不认证自报Secret
// UID；实际生产者必须先核对原VM/journal/ownerref/Secret UID和所有启动输入。
type RuntimeDocument struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	Incarnation     string `json:"incarnation"`
	ConfigSecretUID string `json:"config_secret_uid"`
	SourceSHA256    string `json:"source_sha256"`
	SourceJSON      []byte `json:"source_json"`
}

func (s SourceConfig) Validate() error {
	if s.ProtocolVersion != 1 || s.IssuerEpoch != 1 || s.Config.StateDir != FixedStateDir || control.ValidateID(s.Config.VMUID, "VM UID") != nil || control.ValidateID(s.Config.ComputeInstanceID, "Compute instance") != nil || control.ValidateID(s.BudgetCommitmentID, "bootstrap commitment") != nil {
		return fmt.Errorf("invalid original resource source")
	}
	if err := s.Limits.Validate(); err != nil {
		return err
	}
	// 发行配置按64KiB对齐，同时可表示Linux4KiB/64KiB Guest，内核仍读真实页大小。
	if uint64(s.Limits.MemoryMaxBytes)%(64<<10) != 0 {
		return fmt.Errorf("resource source memory.max is not base-page aligned")
	}
	key, err := base64.RawURLEncoding.DecodeString(s.ControlPublicKey)
	if err != nil || len(key) != ed25519.PublicKeySize || base64.RawURLEncoding.EncodeToString(key) != s.ControlPublicKey {
		return fmt.Errorf("invalid original resource control public key")
	}
	nonzero := false
	for _, b := range key {
		nonzero = nonzero || b != 0
	}
	if !nonzero {
		return fmt.Errorf("zero resource public key")
	}
	return nil
}

func (d RuntimeDocument) Source() (SourceConfig, error) {
	var s SourceConfig
	if d.ProtocolVersion != 1 || control.ValidateIncarnation(d.Incarnation) != nil || control.ValidateID(d.ConfigSecretUID, "config Secret UID") != nil || len(d.SourceJSON) == 0 || len(d.SourceJSON) > 16*1024 {
		return s, fmt.Errorf("invalid resource runtime source binding")
	}
	sum := sha256.Sum256(d.SourceJSON)
	if d.SourceSHA256 != hex.EncodeToString(sum[:]) {
		return s, fmt.Errorf("resource runtime bytes differ from original Secret")
	}
	if err := json.Unmarshal(d.SourceJSON, &s); err != nil {
		return s, err
	}
	return s, s.Validate()
}

// TotalUpperBytes供生产者在任何资源写/Secret创建之前校验整份显式额度；只做
// 规范/溢出与总量，不把调用此函数当作约束已经落实或外部上界已保留的证明。
func (l Limits) TotalUpperBytes() (uint64, error) {
	if err := l.Validate(); err != nil {
		return 0, err
	}
	return l.totalUpper()
}

func (v *SourceConfig) UnmarshalJSON(data []byte) error {
	type plain SourceConfig
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	*v = SourceConfig(next)
	return nil
}
func (v *RuntimeDocument) UnmarshalJSON(data []byte) error {
	type plain RuntimeDocument
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	*v = RuntimeDocument(next)
	return nil
}
