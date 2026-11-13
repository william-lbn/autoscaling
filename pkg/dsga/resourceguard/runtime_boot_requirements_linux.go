//go:build linux

package resourceguard

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func deriveRuntimeBootRequirementsPlatform(next RuntimeBootRequirements, document RuntimeDocument, source SourceConfig, assets map[string][]byte) (RuntimeBootRequirements, error) {
	var zero RuntimeBootRequirements
	// v2 carries the resource document plus five original policy assets. v3
	// additionally carries the public-only Compute startup config; it is part
	// of the same immutable source and must reach the Guest ISO unchanged.
	wantAssets := 1 + len(runtimePolicyAssetOrder)
	if document.ComputeStartup != nil {
		wantAssets++
	}
	if len(assets) != wantAssets {
		return zero, fmt.Errorf("runtime v%d boot requires the complete policy prefix and original public assets", document.ProtocolVersion)
	}
	_, hasPublic := assets[RuntimeComputePublicAssetName]
	if hasPublic != (document.ComputeStartup != nil) {
		return zero, fmt.Errorf("runtime Compute public asset differs from original startup selection")
	}
	var physical PhysicalKernelOriginPolicy
	var kernel NormalV1KernelBootstrapPolicy
	var pre prebootstrapSystemPolicy
	var split NormalSplitTopologyPolicy
	if err := json.Unmarshal(assets["dsga-physical-kernel.json"], &physical); err != nil {
		return zero, err
	}
	if physical.ProfileID == NormalSplitMetadataV1PhysicalKernelProfileID {
		return deriveMetadataRuntimeBootRequirements(next, source, assets, physical)
	}
	if err := json.Unmarshal(assets["dsga-kernel-bootstrap.json"], &kernel); err != nil {
		return zero, err
	}
	if err := json.Unmarshal(assets["dsga-prebootstrap-system.json"], &pre); err != nil {
		return zero, err
	}
	if err := json.Unmarshal(assets["dsga-normal-split.json"], &split); err != nil {
		return zero, err
	}
	if split.PhysicalProfileID != physical.ProfileID {
		return zero, fmt.Errorf("memory origin plan changed original physical profile")
	}
	next.memoryOriginProtocol = split.MemoryOriginProtocolVersion
	next.kernelPreparationProtocol = 1
	ref, err := LookupPhysicalKernelBuild(physical.ProfileID)
	if err != nil || !ref.StrictPageCounterIRQBoundary() || physical.ProfileID != NormalSplitV1PhysicalKernelProfileID || kernel.ProfileID != physical.ProfileID || pre.PhysicalProfileID != physical.ProfileID || !source.Config.SystemCgroup {
		return zero, fmt.Errorf("runtime boot needs original canonical normal-split-v1 source")
	}
	// BootUpper只来自原policy。当前QMP base、zero-Movable、K、任务和实际PID hard
	// 仍须由原Guest读者检查；不能用上界选择base或伪造它已被观测。
	next.profile = ref.ProfileID
	next.build = ref
	next.bootUpper = uint64(pre.BootMemoryUpperBytes)
	next.pidLimit = kernel.Controls.PIDMax
	next.args = []string{ManagedKernelArg, NormalV1KernelBootstrapArg, PhysicalKernelOriginArg, PrebootstrapSystemNormalSplitKernelArg, NormalSplitTopologyKernelArg,
		"neonvm.pid_max_limit=" + strconv.FormatUint(uint64(next.pidLimit), 10), "memhp_default_state=online_movable", "cgroup.memory=nokmem,nosocket,nobpf"}
	cmdline := strings.Join(next.args, " ")
	if err := next.ValidateKernelCommandLine(cmdline); err != nil {
		return zero, err
	}
	// 直接复用实际reader的严格marker检查，保持生成值与生产消费者相同。
	for _, err := range []error{kernelBootstrapMarker(cmdline), normalSplitBootMarker(cmdline), preSystemBootMarker(cmdline, pre), physicalPIDCappedBootPolicy(cmdline, next.pidLimit)} {
		if err != nil {
			return zero, err
		}
	}
	return next, nil
}

// 精确新政策族；不按任意recognized profile放宽旧入口。
func deriveMetadataRuntimeBootRequirements(next RuntimeBootRequirements, source SourceConfig, assets map[string][]byte, physical PhysicalKernelOriginPolicy) (RuntimeBootRequirements, error) {
	var zero RuntimeBootRequirements
	in := kernelPrepRawInputs{RuntimeRaw: assets[RuntimeResourceAssetName], PhysicalPolicyRaw: assets["dsga-physical-kernel.json"], PolicyRaw: assets["dsga-kernel-bootstrap.json"]}
	// assets的resource在Derive层已精确核；这里用原runtime SHA定位同一原raw。
	if len(in.RuntimeRaw) == 0 {
		return zero, fmt.Errorf("metadata boot original runtime asset absent")
	}
	decoded, e := decodeNormalKernelPrepV2(in, source.Config)
	if e != nil {
		return zero, e
	}
	var pre prebootstrapSystemPolicy
	var split NormalSplitTopologyPolicy
	if e = json.Unmarshal(assets["dsga-prebootstrap-system.json"], &pre); e != nil {
		return zero, e
	}
	if e = json.Unmarshal(assets["dsga-normal-split.json"], &split); e != nil {
		return zero, e
	}
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	if e != nil || physical.ProfileID != ref.ProfileID || decoded.policy.ProfileID != ref.ProfileID || !ref.ControlMetadataGuard() || !ref.StrictPageCounterIRQBoundary() || pre.ProtocolVersion != 3 || pre.PhysicalProfileID != ref.ProfileID || split.ProtocolVersion != 4 || split.PhysicalProfileID != ref.ProfileID || split.MemoryOriginProtocolVersion != 2 || !source.Config.SystemCgroup {
		return zero, fmt.Errorf("metadata boot requires exact Kernel2/PreSystem3/Topology4/memory2 family")
	}
	stableMinFree, e := NormalMetadataStableMinFreeKBytes(uint64(pre.BootMemoryUpperBytes))
	if e != nil || uint64(decoded.policy.Controls.MinFreeKBytes) < stableMinFree {
		return zero, fmt.Errorf("metadata min_free_kbytes must cover original boot-upper hotplug recommendation (need %d KiB, derive error %v)", stableMinFree, e)
	}
	next.version = 2
	next.profile = ref.ProfileID
	next.build = ref
	next.bootUpper = uint64(pre.BootMemoryUpperBytes)
	next.pidLimit = decoded.policy.Controls.PIDMax
	next.memoryOriginProtocol = 2
	next.kernelPreparationProtocol = 2
	next.initialWALFamily = normalInitialWALFamilyKernelPrepV2
	next.args = []string{ManagedKernelArg, NormalV2KernelBootstrapArg, PhysicalKernelOriginArg, PrebootstrapSystemMetadataKernelArg, NormalSplitMetadataTopologyKernelArg, "neonvm.pid_max_limit=" + strconv.FormatUint(uint64(next.pidLimit), 10), "memhp_default_state=online_movable", "cgroup.memory=nokmem,nosocket,nobpf"}
	cmd := strings.Join(next.args, " ")
	if e = next.ValidateKernelCommandLine(cmd); e != nil {
		return zero, e
	}
	for _, e := range []error{normalKernelPrepV2Marker(cmd), normalSplitBootMarkerForPolicy(cmd, split), preSystemBootMarker(cmd, pre), physicalPIDCappedBootPolicy(cmd, next.pidLimit)} {
		if e != nil {
			return zero, e
		}
	}
	return next, nil
}
