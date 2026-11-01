//go:build linux

package resourceguard

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// 不重入resource flock、不读PGEmpty、不重放KernelPrep。由原内核启动标记或
// 固定ISO policy的存在选择normal（包括损坏/不可读的policy）；缺失/歧义不可
// 因record-v3尚未出现而退回旧writer。旧未选择normal的VM路径保持原语义。
func (k *linuxKernel) normalImmutableBounds(target Limits) error {
	line, err := kernelText("/proc/cmdline")
	if err != nil {
		return errors.Join(ErrUnknown, err)
	}
	selected := false
	for _, token := range strings.Fields(line) {
		key, _, _ := strings.Cut(token, "=")
		if key == "neonvm.dsga.normal_split" {
			selected = true
		}
	}
	if _, err := os.Lstat(FixedNormalSplitTopologyPolicyPath); err == nil || !os.IsNotExist(err) {
		selected = true
	}
	if !selected {
		return nil
	}
	source, raw, assets, err := initialGuestChannelInputs()
	if err != nil {
		return errors.Join(ErrUnknown, fmt.Errorf("immutable normal original ISO: %w", err))
	}
	if source.Config != k.config {
		return errors.Join(ErrUnknown, fmt.Errorf("immutable normal original config differs"))
	}
	plan, err := DeriveRuntimeBootRequirements(raw, assets)
	if err != nil || (plan.ProfileID() != NormalSplitV1PhysicalKernelProfileID && plan.ProfileID() != NormalSplitMetadataV1PhysicalKernelProfileID) {
		return errors.Join(ErrUnknown, fmt.Errorf("immutable normal exact original profile missing"), err)
	}
	return normalImmutableBoundsTarget(plan, source, target)
}
