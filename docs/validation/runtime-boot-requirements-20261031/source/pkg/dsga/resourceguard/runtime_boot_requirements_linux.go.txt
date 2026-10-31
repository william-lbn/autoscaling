//go:build linux

package resourceguard

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func deriveRuntimeBootRequirementsPlatform(next RuntimeBootRequirements, _ RuntimeDocument, source SourceConfig, assets map[string][]byte) (RuntimeBootRequirements, error) {
	var zero RuntimeBootRequirements
	if len(assets) != 6 {
		return zero, fmt.Errorf("runtime v2 boot requires the complete five-policy prefix")
	}
	var physical PhysicalKernelOriginPolicy
	var kernel NormalV1KernelBootstrapPolicy
	var pre prebootstrapSystemPolicy
	if err := json.Unmarshal(assets["dsga-physical-kernel.json"], &physical); err != nil {
		return zero, err
	}
	if err := json.Unmarshal(assets["dsga-kernel-bootstrap.json"], &kernel); err != nil {
		return zero, err
	}
	if err := json.Unmarshal(assets["dsga-prebootstrap-system.json"], &pre); err != nil {
		return zero, err
	}
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
