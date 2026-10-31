package vmguard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// InitialBootstrapConfig只选择新启动模式；值不是Host预算或Guest已完成证明。
// RuntimeJSON必须是Controller原字节，构造期复制；base是明确选择，再由真实QMP互证。
type InitialBootstrapConfig struct {
	RuntimeJSON       []byte
	OriginalBaseBytes uint64
}
type InitialBootstrapMode struct {
	ProtocolVersion   uint32         `json:"protocol_version"`
	Scope             string         `json:"scope"`
	OriginalBaseBytes control.Uint64 `json:"original_base_bytes"`
	RuntimeSHA256     string         `json:"runtime_document_sha256"`
	ResourceSHA256    string         `json:"resource_source_sha256"`
	ConfigSecretUID   string         `json:"config_secret_uid"`
	KernelBuildSHA256 string         `json:"kernel_build_sha256"`
}

const initialModeScope = "neon-initial-base-mode-v2"

func (m InitialBootstrapMode) validate(id Identity) error {
	if m.ProtocolVersion != 2 || m.Scope != initialModeScope || id.Validate() != nil || id.BlockBytes != VirtioBlockBytes || m.OriginalBaseBytes == 0 || m.OriginalBaseBytes > id.MinimumBytes || m.OriginalBaseBytes%id.BlockBytes != 0 || (id.MinimumBytes-m.OriginalBaseBytes)%id.BlockBytes != 0 || id.MaximumBytes == m.OriginalBaseBytes || control.ValidateID(m.ConfigSecretUID, "initial config Secret") != nil {
		return fmt.Errorf("original initial mode differs")
	}
	for _, h := range []string{m.RuntimeSHA256, m.ResourceSHA256, m.KernelBuildSHA256} {
		if !digestValid(h) {
			return fmt.Errorf("initial static mode digest missing")
		}
	}
	return nil
}
func cloneInitialMode(m *InitialBootstrapMode) *InitialBootstrapMode {
	if m == nil {
		return nil
	}
	v := *m
	return &v
}
func sameInitialMode(a, b *InitialBootstrapMode) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
func initialBirthVersion(mode *InitialBootstrapMode) uint32 {
	if mode != nil {
		return 2
	}
	return 1
}
func initialVersionMatches(version uint32, mode *InitialBootstrapMode, id Identity) bool {
	return version == 1 && mode == nil || version == 2 && mode != nil && mode.validate(id) == nil
}
func initialModeForConfig(c Config) (*InitialBootstrapMode, error) {
	if c.InitialBootstrap == nil {
		return nil, nil
	}
	if len(c.InitialBootstrap.RuntimeJSON) == 0 || len(c.InitialBootstrap.RuntimeJSON) > 64*1024 || strings.ContainsAny(c.StateDir, ",\r\n\x00") {
		return nil, fmt.Errorf("initial mode needs bounded raw runtime and fixed socket-safe directory")
	}
	raw := c.InitialBootstrap.RuntimeJSON
	assets, e := resourceguard.RenderRuntimePolicyAssets(raw)
	if e != nil {
		return nil, e
	}
	assets[resourceguard.RuntimeResourceAssetName] = raw
	req, e := resourceguard.DeriveRuntimeBootRequirements(raw, assets)
	if e != nil {
		return nil, e
	}
	if !req.Enabled() || c.InitialBootstrap.OriginalBaseBytes > req.BootMemoryUpperBoundBytes() {
		return nil, fmt.Errorf("explicit initial mode is unsupported or exceeds boot upper")
	}
	return &InitialBootstrapMode{ProtocolVersion: 2, Scope: initialModeScope, OriginalBaseBytes: control.Uint64(c.InitialBootstrap.OriginalBaseBytes), RuntimeSHA256: req.RuntimeDocumentSHA256(), ResourceSHA256: req.ResourceSourceSHA256(), ConfigSecretUID: req.ConfigSecretUID(), KernelBuildSHA256: req.KernelBuildReference().KernelArtifactSHA256}, nil
}
func (m *InitialBootstrapMode) UnmarshalJSON(raw []byte) error {
	type plain InitialBootstrapMode
	var next plain
	if e := control.UnmarshalObject(raw, &next); e != nil {
		return e
	}
	*m = InitialBootstrapMode(next)
	return nil
}

// 只投影已经真实执行、落盘并由原Runner发布的完成事实；它本身不签normal。
// 完成entry内含固定Source与typed Guest原回执，completion在entry持久之后生成，
// 因而没有Birth UID/Source SHA或完成entry自身的哈希环。
type InitialBootstrapPublication struct {
	ProtocolVersion    uint32                         `json:"protocol_version"`
	Scope              string                         `json:"scope"`
	CompletedEntryJSON []byte                         `json:"completed_entry_json"`
	Completion         bootprotocol.InitialCompletion `json:"completion"`
}

func (p *InitialBootstrapPublication) UnmarshalJSON(raw []byte) error {
	type plain InitialBootstrapPublication
	var next plain
	if e := control.UnmarshalObjectBounded(raw, &next, 32*1024, 4096); e != nil {
		return e
	}
	*p = InitialBootstrapPublication(next)
	return nil
}

type initialLifecycle interface {
	nativeMemory
	arguments() []string
	freeze(context.Context, *exec.Cmd) error
	started(context.Context, *exec.Cmd) (initialLifecycleRun, error)
	gate(*lockedState) error
	publication(*lockedState) (*InitialBootstrapPublication, error)
}
type initialLifecycleRun interface{ stopAndWait() error }

// v1明确禁止出现v2字段，包括null；不能把“未知字段被忽略”当兼容。
// 完整v2 proof由唯一Linux validator核源/回执/完成entry，空发布只供首次等待。
func initialPublicationMatches(p Publication, b Birth, ref *budget.ObjectRef, allowEmpty bool) error {
	if b.Validate() != nil || p.ProtocolVersion != b.ProtocolVersion || p.Identity != b.Identity {
		return fmt.Errorf("Runner publication version/identity changed")
	}
	if p.ProtocolVersion == 1 {
		if p.InitialBootstrap != nil {
			return fmt.Errorf("legacy publication cannot carry initial mode proof")
		}
		return nil
	}
	if allowEmpty && p.BindingClaim == nil && p.State == nil && p.Actual == nil && p.InitialBootstrap == nil {
		return nil
	}
	if ref == nil || p.BindingClaim == nil || *p.BindingClaim != *ref || p.InitialBootstrap == nil {
		return fmt.Errorf("initial mode has no original complete publication")
	}
	return validateInitialLifecyclePublication(b, *ref, *p.InitialBootstrap)
}
func rejectLegacyInitialField(raw []byte, version uint32, key string) error {
	if version != 1 {
		return nil
	}
	var fields map[string]json.RawMessage
	if e := json.Unmarshal(raw, &fields); e != nil {
		return e
	}
	if _, ok := fields[key]; ok {
		return fmt.Errorf("legacy protocol cannot contain %s", key)
	}
	return nil
}
func initialProofEqual(a, b *InitialBootstrapPublication) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}
