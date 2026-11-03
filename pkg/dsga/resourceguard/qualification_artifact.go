package resourceguard

import (
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"reflect"
)

// GuestQualificationArtifact只是原Root的只读物理来源投影，不是资格对象、降限许可或CM。
// 发布者还须独立核真实Runner/Host/原Secret以及完整预算；缺字段绝不能以0补齐。
const GuestQualificationArtifactScope = "neon-dsga-original-guest-qualification-artifact-v1"
const MaxGuestQualificationArtifactBytes = 24 * 1024

// 明确区分旧Guest没有该能力与新Guest的原来源暂不可确认，不升级为任何许可。
type GuestQualificationUnavailable struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	Reason          string `json:"reason"`
}

func (u GuestQualificationUnavailable) Validate() error {
	if u.ProtocolVersion != 1 {
		return fmt.Errorf("unsupported Guest qualification availability version")
	}
	switch u.Reason {
	case "original_source_unconfirmed", "source_changed_during_collection", "response_size_limit", "backend_unsupported":
		return nil
	}
	return fmt.Errorf("unknown Guest qualification unavailability reason")
}
func (u *GuestQualificationUnavailable) UnmarshalJSON(raw []byte) error {
	type plain GuestQualificationUnavailable
	var p plain
	if e := control.UnmarshalObject(raw, &p); e != nil {
		return e
	}
	n := GuestQualificationUnavailable(p)
	if e := n.Validate(); e != nil {
		return e
	}
	*u = n
	return nil
}

type GuestQualificationOutside struct {
	SystemBytes                control.Uint64 `json:"system_bytes"`
	KernelAndInitialRootBytes  control.Uint64 `json:"kernel_and_initial_root_bytes"`
	ConditionalOccupiedBytes   control.Uint64 `json:"conditional_occupied_bytes"`
	CompactionFreeReserveBytes control.Uint64 `json:"compaction_free_reserve_bytes"`
	CPUStockPaidBytes          control.Uint64 `json:"cpu_stock_paid_bytes"`
	PCPFreeReserveBytes        control.Uint64 `json:"pcp_free_reserve_bytes"`
	WatermarkFreeReserveBytes  control.Uint64 `json:"watermark_free_reserve_bytes"`
	TotalBytes                 control.Uint64 `json:"total_bytes"`
}

type GuestQualificationArtifact struct {
	ProtocolVersion            uint32                    `json:"protocol_version"`
	Scope                      string                    `json:"scope"`
	VMShrinkPermit             bool                      `json:"vm_shrink_permit"`
	Identity                   Identity                  `json:"identity"`
	Incarnation                string                    `json:"incarnation"`
	ConfigSecretUID            string                    `json:"config_secret_uid"`
	ResourceSourceSHA256       string                    `json:"resource_source_sha256"`
	RuntimeDocumentSHA256      string                    `json:"runtime_document_sha256"`
	OriginalSourceSHA256       string                    `json:"original_source_sha256"`
	RootStateSHA256            string                    `json:"root_state_sha256"`
	RootBootstrapSHA256        string                    `json:"root_bootstrap_sha256"`
	MinimumVMBytes             control.Uint64            `json:"minimum_vm_bytes"`
	MaximumVMBytes             control.Uint64            `json:"maximum_vm_bytes"`
	BlockBytes                 control.Uint64            `json:"block_bytes"`
	ObservedGuestMemTotalBytes control.Uint64            `json:"observed_guest_mem_total_bytes"`
	MaximumVCPUs               uint32                    `json:"maximum_vcpus"`
	PrivateMemoryMaxBytes      control.Uint64            `json:"private_memory_max_bytes"`
	ArraysMemoryMaxBytes       control.Uint64            `json:"arrays_memory_max_bytes"`
	CurrentLimits              Limits                    `json:"current_limits"`
	OriginalPhysical           PhysicalKernelObservation `json:"original_physical"`
	// 私有source DTO原样保留，包括组件自身的非赋权边界，不把MissingSources改成闭合。
	CurrentFacts       json.RawMessage           `json:"current_facts"`
	InflightTerms      json.RawMessage           `json:"inflight_terms"`
	MinimumNormalBytes control.Uint64            `json:"minimum_normal_bytes"`
	Outside            GuestQualificationOutside `json:"outside"`
}

func qualificationStateSHA(v View) (string, error) {
	// 排除RSS/current等瞬时量以及HTTP端外加的能力字段，只绑定原持久事实和硬限。
	raw, err := json.Marshal(struct {
		Identity         Identity
		Floor            control.Uint64
		Bootstrap        BootstrapCommand
		BootstrapApplied bool
		SystemPlaced     bool
		Original         *Command
		Held             *Command
		Applied          bool
		Cancelled        *Command
		Birth            *ProcessIdentity
		GuestMemTotal    control.Uint64
	}{v.Identity, v.IssuerFloorEpoch, v.Bootstrap, v.BootstrapApplied, v.SystemPlacementObserved, v.Original, v.Held, v.Applied, v.Cancelled, v.Birth, v.Actual.GuestMemTotalBytes})
	if err != nil {
		return "", err
	}
	return physicalDigest(raw), nil
}

func qualificationOutside(system uint64, p normalInflightPlanningResult, t normalInflightSourceTerms) (GuestQualificationOutside, error) {
	total, err := normalInflightSum(system, p.OutsideKernelAndInitialRootBytes, t.KnownConditionalOccupiedBytes, t.CompactionIsolatedFreeReserveBytes, t.CPUStockPaidBytes, p.PCPTotalFreeReserveBytes, p.MovableWatermarkReserveBytes)
	if err != nil {
		return GuestQualificationOutside{}, err
	}
	return GuestQualificationOutside{control.Uint64(system), control.Uint64(p.OutsideKernelAndInitialRootBytes), control.Uint64(t.KnownConditionalOccupiedBytes), control.Uint64(t.CompactionIsolatedFreeReserveBytes), control.Uint64(t.CPUStockPaidBytes), control.Uint64(p.PCPTotalFreeReserveBytes), control.Uint64(p.MovableWatermarkReserveBytes), control.Uint64(total)}, nil
}

func (a GuestQualificationArtifact) Validate() error {
	rawSize, marshalErr := json.Marshal(a)
	if marshalErr != nil || len(rawSize) > MaxGuestQualificationArtifactBytes {
		return fmt.Errorf("Guest qualification artifact exceeds existing response envelope")
	}

	if a.ProtocolVersion != 1 || a.Scope != GuestQualificationArtifactScope || a.VMShrinkPermit || a.Identity.Validate() != nil || a.Identity.SystemCgroupInode == 0 || control.ValidateIncarnation(a.Incarnation) != nil || control.ValidateID(a.ConfigSecretUID, "artifact Config Secret") != nil || a.CurrentLimits.Validate() != nil || a.MaximumVCPUs == 0 || a.MaximumVCPUs > 64 || a.MinimumVMBytes == 0 || a.MaximumVMBytes < a.MinimumVMBytes || a.BlockBytes != 8<<20 || a.MinimumVMBytes%a.BlockBytes != 0 || a.MaximumVMBytes%a.BlockBytes != 0 || a.ObservedGuestMemTotalBytes == 0 || a.ObservedGuestMemTotalBytes > a.MaximumVMBytes || a.PrivateMemoryMaxBytes == 0 || a.ArraysMemoryMaxBytes == 0 {
		return fmt.Errorf("incomplete original Guest qualification artifact")
	}
	for _, h := range []string{a.ResourceSourceSHA256, a.RuntimeDocumentSHA256, a.OriginalSourceSHA256, a.RootStateSHA256, a.RootBootstrapSHA256} {
		if !physicalDigestValid(h) {
			return fmt.Errorf("artifact original source digest missing")
		}
	}
	var f normalInflightSourceFacts
	var t normalInflightSourceTerms
	if control.UnmarshalObject(a.CurrentFacts, &f) != nil || control.UnmarshalObject(a.InflightTerms, &t) != nil {
		return fmt.Errorf("artifact strict source/terms malformed")
	}
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	if e != nil || f.Build != ref || f.Physical.Identity != a.Identity || a.OriginalPhysical.Identity != a.Identity || a.OriginalPhysical.Match(f.Physical) != nil {
		return fmt.Errorf("artifact original physical source differs")
	}
	expected, e := normalInflightCalculateCPUFixed(f, ref, uint64(a.MaximumVCPUs))
	if e != nil || !reflect.DeepEqual(expected, t) {
		return fmt.Errorf("artifact inflight terms differ from exact kernel source")
	}
	leaves, e := normalInflightSum(uint64(a.PrivateMemoryMaxBytes), uint64(a.ArraysMemoryMaxBytes))
	if e != nil {
		return e
	}
	plan, e := normalInflightPlanCPUFixed(f, normalInflightPlanningInput{uint64(a.MaximumVMBytes), leaves, uint64(a.CurrentLimits.SystemMemoryMaxBytes)}, ref, uint64(a.MaximumVCPUs))
	if e != nil {
		return e
	}
	outside, e := qualificationOutside(uint64(a.CurrentLimits.SystemMemoryMaxBytes), plan, t)
	if e != nil || outside != a.Outside || a.MinimumNormalBytes != control.Uint64(plan.ConditionalNormalTargetBytes) {
		return fmt.Errorf("artifact exact physical budget terms differ")
	}
	return nil
}

// MatchView不把格式校验当认证；调用方必须先从原认证Root/Owner通道取得原响应。
func (a GuestQualificationArtifact) MatchView(v View) error {
	if a.Validate() != nil || v.Validate() != nil || a.Identity != v.Identity || a.ObservedGuestMemTotalBytes != v.Actual.GuestMemTotalBytes {
		return fmt.Errorf("artifact belongs to another actual Root")
	}
	limits, e := v.ConfirmedLimits()
	if e != nil || !reflect.DeepEqual(limits, a.CurrentLimits) {
		return fmt.Errorf("artifact hard limits changed")
	}
	h, e := qualificationStateSHA(v)
	if e != nil || h != a.RootStateSHA256 {
		return fmt.Errorf("artifact original Root journal changed")
	}
	raw, e := json.Marshal(v.Bootstrap)
	if e != nil || physicalDigest(raw) != a.RootBootstrapSHA256 {
		return fmt.Errorf("artifact original bootstrap changed")
	}
	return nil
}
func (a *GuestQualificationArtifact) UnmarshalJSON(raw []byte) error {
	type plain GuestQualificationArtifact
	var v plain
	if e := control.UnmarshalObjectBounded(raw, &v, 32<<10, 4096); e != nil {
		return e
	}
	n := GuestQualificationArtifact(v)
	if e := n.Validate(); e != nil {
		return e
	}
	*a = n
	return nil
}
