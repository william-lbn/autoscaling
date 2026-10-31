// Package bootprotocol定义Host/Guest共用的早期启动线合同。
// 本包只校验语法和原challenge绑定；JSON、摘要或Match成功均不是资源/normal资格。
// PG版本不进入上层启动合同。实际Host保留、原QEMU与Guest来源由两侧生产者独立核验。
package bootprotocol

import (
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const InitialPrebootScope = "neon-initial-preboot-challenge-v1"
const MaxFrameBytes = 8192

// RunnerIdentity保留原vmguard.Identity的完整线字段；不迁移旧v1账本或其base语义。
type RunnerIdentity struct {
	VMUID        string         `json:"vm_uid"`
	Incarnation  string         `json:"incarnation"`
	RunnerPodUID string         `json:"runner_pod_uid"`
	RunnerBootID string         `json:"runner_boot_id"`
	NodeUID      string         `json:"node_uid"`
	MinimumBytes control.Uint64 `json:"minimum_bytes"`
	MaximumBytes control.Uint64 `json:"maximum_bytes"`
	BlockBytes   control.Uint64 `json:"block_bytes"`
}

func (id RunnerIdentity) Validate() error {
	if control.ValidateID(id.VMUID, "VM") != nil || control.ValidateIncarnation(id.Incarnation) != nil || control.ValidateID(id.RunnerPodUID, "Runner Pod UID") != nil || control.ValidateIncarnation(id.RunnerBootID) != nil || control.ValidateID(id.NodeUID, "Node UID") != nil || id.MinimumBytes == 0 || id.MaximumBytes < id.MinimumBytes || id.MaximumBytes > math.MaxInt64 || id.BlockBytes == 0 || id.BlockBytes&(id.BlockBytes-1) != 0 || (id.MaximumBytes-id.MinimumBytes)%id.BlockBytes != 0 {
		return fmt.Errorf("invalid original Runner identity or virtio-mem geometry")
	}
	return nil
}

// source与host_stage摘要由原Host生产者生成；本结构无法自行认证它们。
type InitialPrebootChallenge struct {
	HostStageSHA256   string         `json:"host_stage_sha256"`
	ProtocolVersion   uint32         `json:"protocol_version"`
	Scope             string         `json:"scope"`
	SourceSHA256      string         `json:"source_sha256"`
	Nonce             string         `json:"nonce"`
	Identity          RunnerIdentity `json:"identity"`
	RuntimeSHA256     string         `json:"runtime_document_sha256"`
	ResourceSHA256    string         `json:"resource_source_sha256"`
	ConfigSecretUID   string         `json:"config_secret_uid"`
	KernelBuildSHA256 string         `json:"kernel_build_sha256"`
	OriginalBase      control.Uint64 `json:"original_base_bytes"`
	BusinessMinimum   control.Uint64 `json:"business_minimum_bytes"`
	Maximum           control.Uint64 `json:"maximum_bytes"`
}

func (c InitialPrebootChallenge) Validate() error {
	if c.Identity.Validate() != nil || control.ValidateID(c.ConfigSecretUID, "original config Secret") != nil || c.ProtocolVersion != 1 || c.Scope != InitialPrebootScope || !digest(c.Nonce) || c.OriginalBase == 0 || c.OriginalBase > c.Identity.MinimumBytes || c.OriginalBase%c.Identity.BlockBytes != 0 || (c.Identity.MinimumBytes-c.OriginalBase)%c.Identity.BlockBytes != 0 || c.BusinessMinimum != c.Identity.MinimumBytes || c.Maximum != c.Identity.MaximumBytes {
		return fmt.Errorf("original preboot challenge identity/geometry/version differs")
	}
	for _, h := range []string{c.SourceSHA256, c.HostStageSHA256, c.RuntimeSHA256, c.ResourceSHA256, c.KernelBuildSHA256} {
		if !digest(h) {
			return fmt.Errorf("original preboot challenge digest missing")
		}
	}
	return nil
}

type InitialPrebootReceipt struct {
	Challenge               InitialPrebootChallenge `json:"challenge"`
	GuestBootID             string                  `json:"guest_boot_id"`
	RootInstanceNonce       string                  `json:"root_instance_nonce"`
	KernelPreparationSHA256 string                  `json:"kernel_preparation_sha256"`
	PhysicalOriginSHA256    string                  `json:"physical_origin_sha256"`
	PreSystemSHA256         string                  `json:"pre_system_sha256"`
	CurrentPhysicalSHA256   string                  `json:"current_physical_sha256"`
}

func (r InitialPrebootReceipt) Validate() error {
	if r.Challenge.Validate() != nil || !uuid(r.GuestBootID) || !digest(r.RootInstanceNonce) {
		return fmt.Errorf("original preboot receipt identity differs")
	}
	for _, h := range []string{r.KernelPreparationSHA256, r.PhysicalOriginSHA256, r.PreSystemSHA256, r.CurrentPhysicalSHA256} {
		if !digest(h) {
			return fmt.Errorf("original preboot receipt digest missing")
		}
	}
	return nil
}

// MatchChallenge仅完整匹配原期待值，不能把调用者提供的receipt提升为可信Guest事实。
func (r InitialPrebootReceipt) MatchChallenge(expected InitialPrebootChallenge) error {
	if expected.Validate() != nil || r.Validate() != nil || r.Challenge != expected {
		return fmt.Errorf("original preboot receipt binding differs")
	}
	return nil
}

func digest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func uuid(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	raw := strings.ReplaceAll(s, "-", "")
	b, e := hex.DecodeString(raw)
	return e == nil && len(b) == 16 && hex.EncodeToString(b) == raw
}

// 与原协议相同：任意层重复键、缺字段、未知键、大小写借用与非规范Uint64均拒绝。
func (id *RunnerIdentity) UnmarshalJSON(raw []byte) error {
	type plain RunnerIdentity
	var p plain
	if e := control.UnmarshalObjectBounded(raw, &p, MaxFrameBytes, 512); e != nil {
		return e
	}
	*id = RunnerIdentity(p)
	return nil
}
func (c *InitialPrebootChallenge) UnmarshalJSON(raw []byte) error {
	type plain InitialPrebootChallenge
	var p plain
	if e := control.UnmarshalObjectBounded(raw, &p, MaxFrameBytes, 512); e != nil {
		return e
	}
	*c = InitialPrebootChallenge(p)
	return nil
}
func (r *InitialPrebootReceipt) UnmarshalJSON(raw []byte) error {
	type plain InitialPrebootReceipt
	var p plain
	if e := control.UnmarshalObjectBounded(raw, &p, MaxFrameBytes, 512); e != nil {
		return e
	}
	*r = InitialPrebootReceipt(p)
	return nil
}
