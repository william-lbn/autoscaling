//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"reflect"
)

const normalKernelPrepV2Scope = "pre-origin-kernel-preparation-userns-v2-only"
const normalKernelPrepV2IntentName = "kernel-bootstrap-v2-intent.json"
const normalKernelPrepV2CompleteName = "kernel-bootstrap-v2-complete.json"
const NormalV2KernelBootstrapArg = "neonvm.dsga.kernel_bootstrap=normal-split-metadata-v2"

// v2独立选择18c；旧v1类型、JSON及WAL不变。显式pointer拒绝省略值假装0。
type NormalV2KernelBootstrapPolicy struct {
	ProtocolVersion            uint32                          `json:"protocol_version"`
	ProfileID                  string                          `json:"profile_id"`
	RuntimeDocumentSHA256      string                          `json:"runtime_document_sha256"`
	PhysicalOriginPolicySHA256 string                          `json:"physical_origin_policy_sha256"`
	Controls                   NormalV1KernelAllocatorControls `json:"controls"`
	ModulesDisabled            uint32                          `json:"modules_disabled"`
	NUMABalancing              uint32                          `json:"numa_balancing"`
	DemotionEnabled            bool                            `json:"demotion_enabled"`
	MaxUserNamespaces          *control.Uint64                 `json:"max_user_namespaces"`
}

func NewNormalV2KernelBootstrapPolicy(runtimeSHA, physicalSHA string, controls NormalV1KernelAllocatorControls) (NormalV2KernelBootstrapPolicy, error) {
	zero := control.Uint64(0)
	p := NormalV2KernelBootstrapPolicy{ProtocolVersion: 2, ProfileID: NormalSplitMetadataV1PhysicalKernelProfileID, RuntimeDocumentSHA256: runtimeSHA, PhysicalOriginPolicySHA256: physicalSHA, Controls: controls, ModulesDisabled: 1, MaxUserNamespaces: &zero}
	return p, p.Validate()
}
func (p NormalV2KernelBootstrapPolicy) Validate() error {
	ref, e := LookupPhysicalKernelBuild(p.ProfileID)
	if e != nil || p.ProtocolVersion != 2 || p.ProfileID != NormalSplitMetadataV1PhysicalKernelProfileID || !ref.ControlMetadataGuard() || !ref.StrictPageCounterIRQBoundary() || p.MaxUserNamespaces == nil || *p.MaxUserNamespaces != 0 || p.ModulesDisabled != 1 || p.NUMABalancing != 0 || p.DemotionEnabled || !physicalDigestValid(p.RuntimeDocumentSHA256) || !physicalDigestValid(p.PhysicalOriginPolicySHA256) {
		return fmt.Errorf("kernel preparation v2 requires explicit exact18c userns-zero policy")
	}
	return p.Controls.Validate()
}
func (p *NormalV2KernelBootstrapPolicy) UnmarshalJSON(raw []byte) error {
	type plain NormalV2KernelBootstrapPolicy
	var v plain
	if e := control.UnmarshalObject(raw, &v); e != nil {
		return e
	}
	n := NormalV2KernelBootstrapPolicy(v)
	if e := n.Validate(); e != nil {
		return e
	}
	*p = n
	return nil
}

type normalKernelPrepV2Decoded struct {
	document RuntimeDocument
	source   SourceConfig
	physical PhysicalKernelOriginPolicy
	policy   NormalV2KernelBootstrapPolicy
}

func decodeNormalKernelPrepV2(in kernelPrepRawInputs, cfg Config) (normalKernelPrepV2Decoded, error) {
	var d normalKernelPrepV2Decoded
	if e := json.Unmarshal(in.RuntimeRaw, &d.document); e != nil {
		return d, e
	}
	s, e := d.document.Source()
	if e != nil {
		return d, e
	}
	d.source = s
	if s.Config != cfg {
		return d, fmt.Errorf("v2 original resource config differs")
	}
	if e = json.Unmarshal(in.PhysicalPolicyRaw, &d.physical); e != nil {
		return d, e
	}
	if e = json.Unmarshal(in.PolicyRaw, &d.policy); e != nil {
		return d, e
	}
	if d.physical.ProfileID != NormalSplitMetadataV1PhysicalKernelProfileID || d.policy.ProfileID != d.physical.ProfileID || d.physical.RuntimeDocumentSHA256 != physicalDigest(in.RuntimeRaw) || d.policy.RuntimeDocumentSHA256 != physicalDigest(in.RuntimeRaw) || d.policy.PhysicalOriginPolicySHA256 != physicalDigest(in.PhysicalPolicyRaw) {
		return d, fmt.Errorf("v2 full original ISO source differs")
	}
	return d, nil
}

type normalKernelPrepV2Object struct {
	Device  uint64 `json:"device"`
	Inode   uint64 `json:"inode"`
	MountID uint64 `json:"mount_id"`
	Mode    uint32 `json:"mode"`
	UID     uint32 `json:"uid"`
	GID     uint32 `json:"gid"`
}
type normalKernelPrepV2Snapshot struct {
	Kernel            kernelPrepSnapshot                  `json:"kernel"`
	OriginalRoot      nativeBirthConsumer                 `json:"original_root"`
	ProducerELFSHA256 string                              `json:"producer_elf_sha256"`
	Namespaces        []normalSystemRoleNamespaceIdentity `json:"namespaces"`
	Control           normalKernelPrepV2Object            `json:"userns_control"`
	Lock              normalKernelPrepV2Object            `json:"original_resource_lock"`
	MaxUserNamespaces control.Uint64                      `json:"max_user_namespaces"`
}

func (s normalKernelPrepV2Snapshot) validate(d normalKernelPrepV2Decoded) error {
	k := s.Kernel
	if k.Identity.Validate() != nil || k.Identity.VMUID != d.source.Config.VMUID || k.Identity.ComputeInstanceID != d.source.Config.ComputeInstanceID || k.KernelRelease != d.physical.KernelRelease || k.KernelNotesSHA256 != d.physical.KernelNotesSHA256 || k.PossibleCPUs == 0 || k.PossibleCPUs > 1024 || k.PIDMax != k.HardPIDMax || k.HardPIDMax != d.policy.Controls.PIDMax || k.Capabilities.Validate() != nil || k.Capabilities.HardPIDMaxLimit == nil || *k.Capabilities.HardPIDMaxLimit != k.HardPIDMax || k.ModulesDisabled > 1 || k.NUMABalancing > 3 || len(k.LoadedModules) != 0 || k.StateDevice == 0 || k.StateInode == 0 {
		return fmt.Errorf("v2 exact kernel source differs")
	}
	if s.OriginalRoot.PID <= 1 || s.OriginalRoot.StartTicks <= 0 || !normalSystemRoleDeclarationDigest(s.OriginalRoot.InstanceNonce) || !physicalDigestValid(s.ProducerELFSHA256) || len(s.Namespaces) != 8 || s.Lock.Device == 0 || s.Lock.Inode == 0 || s.Lock.MountID == 0 || s.Lock.UID != 0 || s.Lock.GID != 0 || s.Lock.Mode&0777 != 0600 || s.Control.Device == 0 || s.Control.Inode == 0 || s.Control.MountID == 0 || s.Control.UID != 0 || s.Control.GID != 0 || s.Control.Mode&0777 != 0644 || uint64(s.MaxUserNamespaces) > 0x7fffffff {
		return fmt.Errorf("v2 original producer/procfs source differs")
	}
	seen := map[string]bool{}
	for _, n := range s.Namespaces {
		if n.Device == 0 || n.Inode == 0 || seen[n.Name] {
			return fmt.Errorf("v2 namespace source differs")
		}
		seen[n.Name] = true
		if n.Name == "user" && n.Inode != 0xEFFFFFFD {
			return fmt.Errorf("v2 requires initial user namespace")
		}
	}
	for _, n := range []string{"mnt", "pid", "user", "cgroup", "ipc", "uts", "net", "time"} {
		if !seen[n] {
			return fmt.Errorf("v2 missing original namespace")
		}
	}
	size, e := physicalNonMovable(k.Zones)
	if e != nil {
		return e
	}
	if uint64(d.policy.Controls.MinFreeKBytes) > size/1024 {
		return fmt.Errorf("v2 minfree exceeds original geometry")
	}
	return physicalPIDCappedBootPolicy(k.CommandLine, k.HardPIDMax)
}
func (s normalKernelPrepV2Snapshot) zeroMovable() bool { return s.Kernel.zeroMovable() }
func (s normalKernelPrepV2Snapshot) controls() NormalV1KernelAllocatorControls {
	return s.Kernel.controls()
}
func (s normalKernelPrepV2Snapshot) fixedMatch(current normalKernelPrepV2Snapshot) error {
	if e := s.Kernel.fixedMatch(current.Kernel); e != nil {
		return e
	}
	s.Kernel, current.Kernel = kernelPrepSnapshot{}, kernelPrepSnapshot{}
	s.MaxUserNamespaces, current.MaxUserNamespaces = 0, 0
	if !reflect.DeepEqual(s, current) {
		return fmt.Errorf("v2 original Root/namespace/procfs object changed")
	}
	return nil
}
func (s normalKernelPrepV2Snapshot) target(p NormalV2KernelBootstrapPolicy) bool {
	return s.MaxUserNamespaces == 0 && s.controls() == p.Controls && s.Kernel.ModulesDisabled == 1 && s.Kernel.NUMABalancing == 0 && !s.Kernel.DemotionEnabled && len(s.Kernel.LoadedModules) == 0
}

type normalKernelPrepV2Intent struct {
	ProtocolVersion uint32                     `json:"protocol_version"`
	Nonce           string                     `json:"nonce"`
	Inputs          kernelPrepRawInputs        `json:"inputs"`
	Before          normalKernelPrepV2Snapshot `json:"before"`
}

// 新WAL仅组件事实；没有接旧initial8图，不授normal或持续Root writer资格。
type NormalV2KernelBootstrapEvidence struct {
	durabilityConfirmed        bool
	source                     *normalKernelPrepV2Source
	ProtocolVersion            uint32                     `json:"protocol_version"`
	Scope                      string                     `json:"scope"`
	VMShrinkPermit             bool                       `json:"vm_shrink_permit"`
	AllSourcesClosed           bool                       `json:"all_sources_closed"`
	Nonce                      string                     `json:"nonce"`
	IntentSHA256               string                     `json:"intent_sha256"`
	PolicySHA256               string                     `json:"policy_sha256"`
	RuntimeDocumentSHA256      string                     `json:"runtime_document_sha256"`
	PhysicalOriginPolicySHA256 string                     `json:"physical_origin_policy_sha256"`
	After                      normalKernelPrepV2Snapshot `json:"after"`
}

func (e NormalV2KernelBootstrapEvidence) ReferenceSHA256() (string, error) {
	if e.ProtocolVersion != 2 || e.Scope != normalKernelPrepV2Scope || e.VMShrinkPermit || e.AllSourcesClosed || control.ValidateIncarnation(e.Nonce) != nil || e.Nonce == "00000000000000000000000000000000" || !physicalDigestValid(e.IntentSHA256) || !physicalDigestValid(e.PolicySHA256) || !physicalDigestValid(e.RuntimeDocumentSHA256) || !physicalDigestValid(e.PhysicalOriginPolicySHA256) {
		return "", fmt.Errorf("v2 component reference differs")
	}
	b, x := json.Marshal(e)
	return physicalDigest(b), x
}
func (e NormalV2KernelBootstrapEvidence) DurabilityConfirmed() bool { return e.durabilityConfirmed }
func (e NormalV2KernelBootstrapEvidence) originalUserNamespaceBarrierSource() (*normalKernelPrepV2Source, error) {
	if !e.durabilityConfirmed || e.source == nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("raw v2 evidence cannot construct original source"))
	}
	return e.source, nil
}
func (s *normalKernelPrepV2Source) requireContinuousQualification(context.Context) error {
	return errors.Join(ErrUnknown, fmt.Errorf("userns preparation does not close future Root writers or full Source"))
}
