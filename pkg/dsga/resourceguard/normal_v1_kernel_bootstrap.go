//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const (
	kernelPrepScope        = "pre-origin-kernel-preparation-only"
	kernelPrepIntentName   = "kernel-bootstrap-intent.json"
	kernelPrepCompleteName = "kernel-bootstrap-complete.json"
)

// NormalV1KernelAllocatorControls 全部来自原只读policy；没有按current/idle猜出的默认值。
// PIDMax必须等于新内核原early只读hard值，晚写普通pid_max不能补签历史。
type NormalV1KernelAllocatorControls struct {
	PIDMax                     uint32         `json:"pid_max"`
	MinFreeKBytes              control.Uint64 `json:"min_free_kbytes"`
	WatermarkScaleFactor       uint32         `json:"watermark_scale_factor"`
	WatermarkBoostFactor       uint32         `json:"watermark_boost_factor"`
	PerCPUPagelistHighFraction uint32         `json:"percpu_pagelist_high_fraction"`
	CompactionProactiveness    uint32         `json:"compaction_proactiveness"`
}

func (c NormalV1KernelAllocatorControls) Validate() error {
	if c.PIDMax < 301 || c.PIDMax > 4096 || c.MinFreeKBytes == 0 || c.MinFreeKBytes > 0x7fffffff || c.WatermarkScaleFactor == 0 || c.WatermarkScaleFactor > 3000 || c.WatermarkBoostFactor != 0 || c.PerCPUPagelistHighFraction < 8 || c.PerCPUPagelistHighFraction > 0x7fffffff || c.CompactionProactiveness != 0 {
		return fmt.Errorf("invalid explicit kernel bootstrap controls")
	}
	return nil
}
func (c *NormalV1KernelAllocatorControls) UnmarshalJSON(raw []byte) error {
	type plain NormalV1KernelAllocatorControls
	var value plain
	if err := control.UnmarshalObject(raw, &value); err != nil {
		return err
	}
	next := NormalV1KernelAllocatorControls(value)
	if err := next.Validate(); err != nil {
		return err
	}
	*c = next
	return nil
}

// NormalV1KernelBootstrapPolicy 只接受固定ISO原字节的引用；所有值须原创建者显式冻结。
// 这不是完整FS/Root/在途资格，准备成功也不授予Bootstrap、PG出生或VM许可。
type NormalV1KernelBootstrapPolicy struct {
	ProtocolVersion            uint32                          `json:"protocol_version"`
	ProfileID                  string                          `json:"profile_id"`
	RuntimeDocumentSHA256      string                          `json:"runtime_document_sha256"`
	PhysicalOriginPolicySHA256 string                          `json:"physical_origin_policy_sha256"`
	Controls                   NormalV1KernelAllocatorControls `json:"controls"`
	ModulesDisabled            uint32                          `json:"modules_disabled"`
	NUMABalancing              uint32                          `json:"numa_balancing"`
	DemotionEnabled            bool                            `json:"demotion_enabled"`
}

func (p NormalV1KernelBootstrapPolicy) Validate() error {
	if p.ProtocolVersion != 1 || p.ProfileID != NormalSplitV1PhysicalKernelProfileID || p.ModulesDisabled != 1 || p.NUMABalancing != 0 || p.DemotionEnabled || !physicalDigestValid(p.RuntimeDocumentSHA256) || !physicalDigestValid(p.PhysicalOriginPolicySHA256) {
		return fmt.Errorf("kernel preparation requires exact original PID-capped policy")
	}
	ref, err := LookupPhysicalKernelBuild(p.ProfileID)
	if err != nil || !ref.StrictPageCounterIRQBoundary() {
		return fmt.Errorf("kernel preparation requires exact strict counter build")
	}
	return p.Controls.Validate()
}
func (p *NormalV1KernelBootstrapPolicy) UnmarshalJSON(raw []byte) error {
	type plain NormalV1KernelBootstrapPolicy
	var value plain
	if err := control.UnmarshalObject(raw, &value); err != nil {
		return err
	}
	next := NormalV1KernelBootstrapPolicy(value)
	if err := next.Validate(); err != nil {
		return err
	}
	*p = next
	return nil
}

type kernelPrepRawInputs struct {
	RuntimeRaw        []byte `json:"runtime_raw"`
	PhysicalPolicyRaw []byte `json:"physical_policy_raw"`
	PolicyRaw         []byte `json:"policy_raw"`
}
type kernelPrepDecoded struct {
	document RuntimeDocument
	source   SourceConfig
	physical PhysicalKernelOriginPolicy
	policy   NormalV1KernelBootstrapPolicy
}

func (in kernelPrepRawInputs) decode(config Config) (kernelPrepDecoded, error) {
	var d kernelPrepDecoded
	if err := json.Unmarshal(in.RuntimeRaw, &d.document); err != nil {
		return d, err
	}
	source, err := d.document.Source()
	if err != nil {
		return d, err
	}
	d.source = source
	if source.Config != config {
		return d, fmt.Errorf("kernel preparation original resource config differs")
	}
	if err := json.Unmarshal(in.PhysicalPolicyRaw, &d.physical); err != nil {
		return d, err
	}
	if err := json.Unmarshal(in.PolicyRaw, &d.policy); err != nil {
		return d, err
	}
	if d.physical.ProfileID != NormalSplitV1PhysicalKernelProfileID || d.policy.ProfileID != d.physical.ProfileID || d.physical.RuntimeDocumentSHA256 != physicalDigest(in.RuntimeRaw) || d.policy.RuntimeDocumentSHA256 != physicalDigest(in.RuntimeRaw) || d.policy.PhysicalOriginPolicySHA256 != physicalDigest(in.PhysicalPolicyRaw) {
		return d, fmt.Errorf("kernel preparation full raw source binding differs")
	}
	return d, nil
}

// kernelPrepSnapshot与完整Observation分开：写前PIDMax允许观察原大值，不能全局放宽
// Observation的<=4096规则。真正J仍只能来自新准确内核的原early hard值。
type kernelPrepSnapshot struct {
	Identity          Identity                          `json:"identity"`
	KernelRelease     string                            `json:"kernel_release"`
	KernelNotesSHA256 string                            `json:"kernel_notes_sha256"`
	CommandLine       string                            `json:"command_line"`
	PossibleCPUs      uint32                            `json:"possible_cpus"`
	PIDMax            uint32                            `json:"pid_max"`
	HardPIDMax        uint32                            `json:"hard_pid_max"`
	Capabilities      PhysicalKernelRuntimeCapabilities `json:"capabilities"`
	Zones             []PhysicalKernelZone              `json:"zones"`
	ModulesDisabled   uint32                            `json:"modules_disabled"`
	LoadedModules     []string                          `json:"loaded_modules"`
	NUMABalancing     uint32                            `json:"numa_balancing"`
	DemotionEnabled   bool                              `json:"demotion_enabled"`
	Namespaces        [4]control.Uint64                 `json:"namespaces"`
	StateDevice       control.Uint64                    `json:"state_device"`
	StateInode        control.Uint64                    `json:"state_inode"`
}

func (s kernelPrepSnapshot) controls() NormalV1KernelAllocatorControls {
	c := s.Capabilities
	return NormalV1KernelAllocatorControls{PIDMax: s.PIDMax, MinFreeKBytes: c.MinFreeKBytes, WatermarkScaleFactor: c.WatermarkScaleFactor, WatermarkBoostFactor: c.WatermarkBoostFactor, PerCPUPagelistHighFraction: c.PerCPUPagelistHighFraction, CompactionProactiveness: c.CompactionProactiveness}
}
func (s kernelPrepSnapshot) validate(d kernelPrepDecoded) error {
	if s.Identity.Validate() != nil || s.Identity.VMUID != d.source.Config.VMUID || s.Identity.ComputeInstanceID != d.source.Config.ComputeInstanceID || s.KernelRelease != d.physical.KernelRelease || s.KernelNotesSHA256 != d.physical.KernelNotesSHA256 || s.PossibleCPUs == 0 || s.PossibleCPUs > 1024 || s.PIDMax != s.HardPIDMax || s.HardPIDMax != d.policy.Controls.PIDMax || s.Capabilities.Validate() != nil || s.Capabilities.HardPIDMaxLimit == nil || *s.Capabilities.HardPIDMaxLimit != s.HardPIDMax || s.ModulesDisabled > 1 || s.NUMABalancing > 3 || len(s.LoadedModules) != 0 || s.StateDevice == 0 || s.StateInode == 0 {
		return fmt.Errorf("invalid raw kernel preparation source")
	}
	k, err := physicalNonMovable(s.Zones)
	if err != nil {
		return err
	}
	for _, n := range s.Namespaces {
		if n == 0 {
			return fmt.Errorf("initial namespace missing")
		}
	}
	// 显式minfree也必须装得进原nonMovable managed几何；不采样free或造安全百分比。
	if uint64(d.policy.Controls.MinFreeKBytes) > k/1024 {
		return fmt.Errorf("explicit minfree exceeds fixed managed geometry")
	}
	return physicalPIDCappedBootPolicy(s.CommandLine, s.HardPIDMax)
}
func (s kernelPrepSnapshot) zeroMovable() bool {
	for _, zone := range s.Zones {
		if zone.Name == "Movable" && zone.PresentPages != 0 {
			return false
		}
	}
	return true
}
func (s kernelPrepSnapshot) fixedMatch(current kernelPrepSnapshot) error {
	a, b := s, current
	onlyFixed := func(zones []PhysicalKernelZone) []PhysicalKernelZone {
		out := make([]PhysicalKernelZone, 0, len(zones))
		for _, z := range zones {
			if z.Name != "Movable" {
				out = append(out, z)
			}
		}
		return out
	}
	a.Zones, b.Zones = onlyFixed(a.Zones), onlyFixed(b.Zones)
	// 这些明确是准备阶段可变的控制值；其余Boot/build/域/几何/只读hard来源全部相等。
	a.PIDMax, b.PIDMax = 0, 0
	a.ModulesDisabled, b.ModulesDisabled = 0, 0
	a.NUMABalancing, b.NUMABalancing = 0, 0
	a.DemotionEnabled, b.DemotionEnabled = false, false
	a.Capabilities.MinFreeKBytes, b.Capabilities.MinFreeKBytes = 0, 0
	a.Capabilities.WatermarkScaleFactor, b.Capabilities.WatermarkScaleFactor = 0, 0
	a.Capabilities.WatermarkBoostFactor, b.Capabilities.WatermarkBoostFactor = 0, 0
	a.Capabilities.PerCPUPagelistHighFraction, b.Capabilities.PerCPUPagelistHighFraction = 0, 0
	a.Capabilities.CompactionProactiveness, b.Capabilities.CompactionProactiveness = 0, 0
	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("kernel preparation fixed original source drifted")
	}
	return nil
}

type kernelPrepIntent struct {
	ProtocolVersion uint32              `json:"protocol_version"`
	Nonce           string              `json:"nonce"`
	Inputs          kernelPrepRawInputs `json:"inputs"`
	Before          kernelPrepSnapshot  `json:"before"`
}

// NormalV1KernelBootstrapEvidence只确认原控制写入组件。AllSourcesClosed固定false，
// 不把PID/allocator准备改名为完整private、FS、Root、I或目标Movable资格。
type NormalV1KernelBootstrapEvidence struct {
	// 只描述本次原文件同步成功；不写入规范证据，Observe/重复Prepare均false。
	// 完整旧文件可能来自前次fsync失败，只有显式Replay重新同步才为true。
	durabilityConfirmed        bool
	ProtocolVersion            uint32             `json:"protocol_version"`
	Scope                      string             `json:"scope"`
	VMShrinkPermit             bool               `json:"vm_shrink_permit"`
	AllSourcesClosed           bool               `json:"all_sources_closed"`
	Nonce                      string             `json:"nonce"`
	IntentSHA256               string             `json:"intent_sha256"`
	PolicySHA256               string             `json:"policy_sha256"`
	RuntimeDocumentSHA256      string             `json:"runtime_document_sha256"`
	PhysicalOriginPolicySHA256 string             `json:"physical_origin_policy_sha256"`
	After                      kernelPrepSnapshot `json:"after"`
}

func (e NormalV1KernelBootstrapEvidence) ReferenceSHA256() (string, error) {
	if e.ProtocolVersion != 1 || e.Scope != kernelPrepScope || e.VMShrinkPermit || e.AllSourcesClosed || e.Nonce == "00000000000000000000000000000000" || control.ValidateIncarnation(e.Nonce) != nil || !physicalDigestValid(e.IntentSHA256) || !physicalDigestValid(e.PolicySHA256) || !physicalDigestValid(e.RuntimeDocumentSHA256) || !physicalDigestValid(e.PhysicalOriginPolicySHA256) {
		return "", fmt.Errorf("invalid component-only kernel preparation evidence")
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	return physicalDigest(raw), nil
}

type kernelPreparationBackend interface {
	kernelPrepRead() (kernelPrepSnapshot, error)
	kernelPrepPGEmpty() error
	kernelPrepWrite(string, uint64) error
	kernelPrepCensus(uint32) error
}
type kernelPrepOperation uint8

const (
	kernelPrepPrepare kernelPrepOperation = iota
	kernelPrepObserve
	kernelPrepReplay
)

func (g *Guard) normalKernelPreparation(ctx context.Context, in kernelPrepRawInputs, op kernelPrepOperation) (NormalV1KernelBootstrapEvidence, error) {
	var zero NormalV1KernelBootstrapEvidence
	// 生产只从固定ISO loader到此；测试显式注入locked temp store，不改系统固定目录。
	if _, err := in.decode(g.config); err != nil {
		return zero, err
	}
	store, err := lockState(ctx, g.config.StateDir)
	if err != nil {
		return zero, err
	}
	defer store.close()
	return g.normalKernelPreparationLocked(ctx, in, op, store)
}

// 与Origin/PreSys/出生/资源写共用唯一flock，不跨服务取锁。unknown只原nonce原source
// 重放；Observe不写，pending Prepare不补写，完整旧准备仅fsync原文件重确认而不改knobs。
func (g *Guard) normalKernelPreparationLocked(ctx context.Context, in kernelPrepRawInputs, op kernelPrepOperation, store *lockedState) (result NormalV1KernelBootstrapEvidence, err error) {
	var zero NormalV1KernelBootstrapEvidence
	d, err := in.decode(g.config)
	if err != nil {
		return zero, err
	}
	if op > kernelPrepReplay {
		return zero, fmt.Errorf("unknown kernel preparation operation")
	}
	backend, ok := g.kernel.(kernelPreparationBackend)
	if !ok {
		return zero, fmt.Errorf("kernel preparation backend unavailable")
	}
	raw, exists, err := store.readPhysicalFile(kernelPrepIntentName)
	if err != nil {
		return zero, err
	}
	completeRaw, complete, err := store.readPhysicalFile(kernelPrepCompleteName)
	if err != nil {
		return zero, err
	}
	if complete && !exists {
		return zero, fmt.Errorf("%w: completion without original intent", ErrUnknown)
	}
	pending := exists && !complete
	defer func() {
		if err != nil && pending && !errors.Is(err, ErrUnknown) {
			err = fmt.Errorf("%w: pending kernel preparation: %v", ErrUnknown, err)
		}
	}()
	current, err := backend.kernelPrepRead()
	if err != nil {
		return zero, err
	}
	var locked unix.Stat_t
	if err = unix.Fstat(int(store.dir.Fd()), &locked); err != nil {
		return zero, err
	}
	if current.StateDevice != control.Uint64(locked.Dev) || current.StateInode != control.Uint64(locked.Ino) {
		return zero, fmt.Errorf("kernel preparation state path differs from locked inode")
	}
	if err = current.validate(d); err != nil {
		return zero, err
	}
	boundary := func() error {
		record, err := store.load()
		if err != nil {
			return err
		}
		if record != nil {
			return fmt.Errorf("kernel preparation cannot acquire historical resource journal")
		}
		fd, err := unix.Openat(int(store.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		directory := os.NewFile(uintptr(fd), "original-kernel-state")
		entries, err := directory.ReadDir(-1)
		directory.Close()
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.Name() != "lock" && entry.Name() != kernelPrepIntentName && entry.Name() != kernelPrepCompleteName {
				return fmt.Errorf("kernel preparation found other original-stage state")
			}
		}
		if !current.zeroMovable() {
			return fmt.Errorf("kernel preparation requires original zero MOVABLE phase")
		}
		return backend.kernelPrepPGEmpty()
	}
	var intent kernelPrepIntent
	if !exists {
		if op != kernelPrepPrepare {
			return zero, fmt.Errorf("%w: original kernel preparation missing", ErrUnknown)
		}
		if err = boundary(); err != nil {
			return zero, err
		}
		// 已禁模块却无原intent，无法排除旧模块生命周期或先前未绑定准备，拒绝补签。
		if current.ModulesDisabled != 0 {
			return zero, fmt.Errorf("modules already disabled without original intent")
		}
		if err = ctx.Err(); err != nil {
			return zero, err
		}
		var nonce [16]byte
		if _, err = rand.Read(nonce[:]); err != nil {
			return zero, err
		}
		if hex.EncodeToString(nonce[:]) == "00000000000000000000000000000000" {
			return zero, fmt.Errorf("reserved zero original nonce")
		}
		intent = kernelPrepIntent{ProtocolVersion: 1, Nonce: hex.EncodeToString(nonce[:]), Inputs: in, Before: current}
		raw, err = json.Marshal(intent)
		if err != nil {
			return zero, err
		}
		// O_EXCL+file/dir fsync在第一项sysctl写前。失败残留文件不覆盖、不新建nonce。
		pending = true
		if err = store.createPhysicalFile(kernelPrepIntentName, raw); err != nil {
			return zero, fmt.Errorf("%w: save kernel intent: %v", ErrUnknown, err)
		}
	} else {
		if err = control.UnmarshalObject(raw, &intent); err != nil {
			return zero, fmt.Errorf("%w: invalid original kernel intent: %v", ErrUnknown, err)
		}
		canonical, marshalErr := json.Marshal(intent)
		if marshalErr != nil || !bytes.Equal(canonical, raw) {
			return zero, fmt.Errorf("%w: noncanonical original kernel intent", ErrUnknown)
		}
		if intent.ProtocolVersion != 1 || intent.Nonce == "00000000000000000000000000000000" || control.ValidateIncarnation(intent.Nonce) != nil || !bytes.Equal(intent.Inputs.RuntimeRaw, in.RuntimeRaw) || !bytes.Equal(intent.Inputs.PhysicalPolicyRaw, in.PhysicalPolicyRaw) || !bytes.Equal(intent.Inputs.PolicyRaw, in.PolicyRaw) {
			return zero, fmt.Errorf("kernel preparation original raw source differs")
		}
		if err = intent.Before.validate(d); err != nil {
			return zero, err
		}
		if intent.Before.ModulesDisabled != 0 || !intent.Before.zeroMovable() {
			return zero, fmt.Errorf("original kernel intent birth phase differs")
		}
		if err = intent.Before.fixedMatch(current); err != nil {
			return zero, err
		}
		if !complete && op != kernelPrepReplay {
			return zero, fmt.Errorf("%w: explicit original kernel replay required", ErrUnknown)
		}
	}
	if complete {
		var e NormalV1KernelBootstrapEvidence
		if err = control.UnmarshalObject(completeRaw, &e); err != nil {
			return zero, fmt.Errorf("%w: invalid kernel completion: %v", ErrUnknown, err)
		}
		if _, err = e.ReferenceSHA256(); err != nil {
			return zero, err
		}
		if e.Nonce != intent.Nonce || e.IntentSHA256 != physicalDigest(raw) || e.PolicySHA256 != physicalDigest(in.PolicyRaw) || e.RuntimeDocumentSHA256 != physicalDigest(in.RuntimeRaw) || e.PhysicalOriginPolicySHA256 != physicalDigest(in.PhysicalPolicyRaw) || !e.After.target(d.policy) {
			return zero, fmt.Errorf("original kernel completion differs")
		}
		if err = e.After.validate(d); err != nil {
			return zero, err
		}
		if err = e.After.fixedMatch(current); err != nil {
			return zero, err
		}
		if !current.target(d.policy) {
			return zero, fmt.Errorf("kernel bootstrap controls drifted; no automatic repair")
		}
		canonical, marshalErr := json.Marshal(e)
		if marshalErr != nil || !bytes.Equal(canonical, completeRaw) {
			return zero, fmt.Errorf("%w: noncanonical kernel completion", ErrUnknown)
		}
		if op == kernelPrepReplay {
			if err = store.syncPhysicalFile(kernelPrepIntentName); err != nil {
				return zero, err
			}
			if err = store.syncPhysicalFile(kernelPrepCompleteName); err != nil {
				return zero, err
			}
			e.durabilityConfirmed = true
		}
		return e, nil
	}
	if err = boundary(); err != nil {
		return zero, err
	}
	if op == kernelPrepReplay {
		if (current.ModulesDisabled != intent.Before.ModulesDisabled && current.ModulesDisabled != 1) || (current.NUMABalancing != intent.Before.NUMABalancing && current.NUMABalancing != 0) || (current.DemotionEnabled != intent.Before.DemotionEnabled && current.DemotionEnabled) {
			return zero, fmt.Errorf("unknown force-control drift")
		}
		before, target, now := intent.Before.controls(), d.policy.Controls, current.controls()
		old := []uint64{uint64(before.PIDMax), uint64(before.MinFreeKBytes), uint64(before.WatermarkScaleFactor), uint64(before.WatermarkBoostFactor), uint64(before.PerCPUPagelistHighFraction), uint64(before.CompactionProactiveness)}
		want := []uint64{uint64(target.PIDMax), uint64(target.MinFreeKBytes), uint64(target.WatermarkScaleFactor), uint64(target.WatermarkBoostFactor), uint64(target.PerCPUPagelistHighFraction), uint64(target.CompactionProactiveness)}
		actual := []uint64{uint64(now.PIDMax), uint64(now.MinFreeKBytes), uint64(now.WatermarkScaleFactor), uint64(now.WatermarkBoostFactor), uint64(now.PerCPUPagelistHighFraction), uint64(now.CompactionProactiveness)}
		for i := range old {
			if actual[i] != old[i] && actual[i] != want[i] {
				return zero, fmt.Errorf("unknown third-party kernel control drift")
			}
		}
		if err = store.syncPhysicalFile(kernelPrepIntentName); err != nil {
			return zero, err
		}
	}
	target := d.policy.Controls
	// 先PID与禁boost/proactive，再显式PCP/scale/minfree；不回滚放宽原硬源。
	writes := []struct {
		name  string
		value uint64
	}{
		{"modules_disabled", 1}, {"numa_balancing", 0}, {"demotion_enabled", 0},
		{"watermark_boost_factor", 0}, {"compaction_proactiveness", 0},
		{"percpu_pagelist_high_fraction", uint64(target.PerCPUPagelistHighFraction)}, {"watermark_scale_factor", uint64(target.WatermarkScaleFactor)}, {"min_free_kbytes", uint64(target.MinFreeKBytes)},
	}
	for _, write := range writes {
		if err = ctx.Err(); err != nil {
			return zero, fmt.Errorf("%w: interrupted original kernel writes: %v", ErrUnknown, err)
		}
		if err = backend.kernelPrepWrite(write.name, write.value); err != nil {
			return zero, fmt.Errorf("%w: kernel control %s: %v", ErrUnknown, write.name, err)
		}
	}
	if err = backend.kernelPrepCensus(target.PIDMax); err != nil {
		return zero, fmt.Errorf("%w: kernel PID observation: %v", ErrUnknown, err)
	}
	current, err = backend.kernelPrepRead()
	if err != nil {
		return zero, fmt.Errorf("%w: kernel readback: %v", ErrUnknown, err)
	}
	if err = current.validate(d); err != nil {
		return zero, err
	}
	if err = intent.Before.fixedMatch(current); err != nil {
		return zero, err
	}
	if !current.zeroMovable() || !current.target(d.policy) {
		return zero, fmt.Errorf("%w: kernel preparation target differs", ErrUnknown)
	}
	if err = backend.kernelPrepPGEmpty(); err != nil {
		return zero, err
	}
	evidence := NormalV1KernelBootstrapEvidence{ProtocolVersion: 1, Scope: kernelPrepScope, Nonce: intent.Nonce, IntentSHA256: physicalDigest(raw), PolicySHA256: physicalDigest(in.PolicyRaw), RuntimeDocumentSHA256: physicalDigest(in.RuntimeRaw), PhysicalOriginPolicySHA256: physicalDigest(in.PhysicalPolicyRaw), After: current}
	if err = ctx.Err(); err != nil {
		return zero, err
	}
	done, err := json.Marshal(evidence)
	if err != nil {
		return zero, err
	}
	if err = store.createPhysicalFile(kernelPrepCompleteName, done); err != nil {
		return zero, fmt.Errorf("%w: kernel completion persistence: %v", ErrUnknown, err)
	}
	evidence.durabilityConfirmed = true
	return evidence, nil
}

func (s kernelPrepSnapshot) target(p NormalV1KernelBootstrapPolicy) bool {
	return s.controls() == p.Controls && s.ModulesDisabled == 1 && s.NUMABalancing == 0 && !s.DemotionEnabled && len(s.LoadedModules) == 0
}

// DurabilityConfirmed只描述本次调用的原文件同步，不进入规范证据JSON。
func (e NormalV1KernelBootstrapEvidence) DurabilityConfirmed() bool { return e.durabilityConfirmed }
