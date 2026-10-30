package resourceguard

import (
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"math"
	"reflect"
	"strings"
)

// PhysicalKernelRuntimeCapabilities 仅为新准确构建的实际只读能力/调优来源。
// UMH 编译依据在准确 config；不冒充测量整个内核镜像、FS 规则已生效或未来写者封闭。
// allocator 原值封存并在 Match 拒漂移；默认 fraction=0/proactive=20 可以被组件观察，
// 完整 normal profile 仍须原出生强制 fraction>=8、proactive=0、boost=0 等全部条件。
type PhysicalKernelRuntimeCapabilities struct {
	ActiveLSMs                 []string       `json:"active_lsms"`
	LandlockABI                uint32         `json:"landlock_abi"`
	InitialPIDNamespaceDevice  control.Uint64 `json:"initial_pid_namespace_device"`
	InitialPIDNamespaceInode   control.Uint64 `json:"initial_pid_namespace_inode"`
	MemoryHigh                 string         `json:"memory_high"`
	MinFreeKBytes              control.Uint64 `json:"min_free_kbytes"`
	WatermarkScaleFactor       uint32         `json:"watermark_scale_factor"`
	WatermarkBoostFactor       uint32         `json:"watermark_boost_factor"`
	PerCPUPagelistHighFraction uint32         `json:"percpu_pagelist_high_fraction"`
	CompactionProactiveness    uint32         `json:"compaction_proactiveness"`
}

func (c PhysicalKernelRuntimeCapabilities) Validate() error {
	wantLSM := []string{"lockdown", "capability", "landlock", "yama", "apparmor"}
	if !reflect.DeepEqual(c.ActiveLSMs, wantLSM) || c.LandlockABI != 6 || c.InitialPIDNamespaceDevice == 0 || c.InitialPIDNamespaceInode != 0xEFFFFFFC || c.MemoryHigh != "max" || c.MinFreeKBytes > math.MaxInt32 || c.WatermarkScaleFactor == 0 || c.WatermarkScaleFactor > 3000 || c.WatermarkBoostFactor > math.MaxInt32 || (c.PerCPUPagelistHighFraction != 0 && c.PerCPUPagelistHighFraction < 8) || c.PerCPUPagelistHighFraction > math.MaxInt32 || c.CompactionProactiveness > 100 {
		return fmt.Errorf("normal-v1 runtime capability or bounded allocator source differs")
	}
	return nil
}

func (c *PhysicalKernelRuntimeCapabilities) UnmarshalJSON(b []byte) error {
	type plain PhysicalKernelRuntimeCapabilities
	var value plain
	// 零值 boost/proactive 也必须显式出现；缺字段/null不能变成“已经关闭”。
	if err := control.UnmarshalObject(b, &value); err != nil {
		return err
	}
	next := PhysicalKernelRuntimeCapabilities(value)
	if err := next.Validate(); err != nil {
		return err
	}
	*c = next
	return nil
}

func (o *PhysicalKernelObservation) UnmarshalJSON(b []byte) error {
	type plain PhysicalKernelObservation
	var value plain
	if err := control.UnmarshalObject(b, &value); err != nil {
		return err
	}
	next := PhysicalKernelObservation(value)
	if err := next.Validate(); err != nil {
		return err
	}
	*o = next
	return nil
}

func physicalKernelObservationBuild(release, notes string) (string, error) {
	if release == physicalKernelRelease && physicalDigestValid(notes) {
		return "pg17-linux-6.12.26-physical-component-v1", nil
	}
	ref, err := LookupPhysicalKernelBuild(NormalV1PhysicalKernelProfileID)
	if err == nil && release == ref.KernelRelease && notes == ref.KernelNotesSHA256 {
		return ref.ProfileID, nil
	}
	return "", fmt.Errorf("physical kernel release/notes do not match reviewed build")
}

func physicalNormalV1BootPolicy(cmdline string) error {
	if err := physicalBootPolicy(cmdline); err != nil {
		return err
	}
	for _, token := range strings.Fields(cmdline) {
		key, _, _ := strings.Cut(token, "=")
		if key == "lsm" || key == "security" {
			// 只接受已审编译名单，不因当前 landlock 出现就收养不同或重复 boot 选择。
			return fmt.Errorf("normal-v1 disallows boot LSM selection overrides")
		}
	}
	return nil
}

var _ json.Unmarshaler = (*PhysicalKernelRuntimeCapabilities)(nil)
