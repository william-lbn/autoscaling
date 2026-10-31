package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"math"
)

// 明确首版base选择；BootUpper仅限制此选择，不是观测事实。真正base仍由原QMP核验。
// 不从当前RSS、spec.Use、环境变量或现有requested-size推导初始Origin。
const managedInitialBaseBytes uint64 = 256 << 20

func initialBootMemoryArguments(cfg *Config, spec *vmv1.VirtualMachineSpec) ([]string, error) {
	if cfg == nil || spec == nil {
		return nil, fmt.Errorf("original Runner inputs missing")
	}
	enabled := cfg.runtimeBoot != nil && cfg.runtimeBoot.Enabled()
	if !enabled {
		if cfg.initialBootstrap != nil {
			return nil, fmt.Errorf("initial mode without original boot requirements")
		}
		return nil, nil
	}
	initial := cfg.initialBootstrap
	if spec.DSGABootstrap == nil || cfg.architecture != architectureAmd64 || cfg.cpuScalingMode != vmv1.CpuScalingModeSysfs || initial == nil || initial.OriginalBaseBytes != managedInitialBaseBytes {
		return nil, fmt.Errorf("new boot requires original managed amd64 sysfs mode and fixed base")
	}
	hash := sha256.Sum256(initial.RuntimeJSON)
	if hex.EncodeToString(hash[:]) != cfg.runtimeBoot.RuntimeDocumentSHA256() || initial.OriginalBaseBytes > cfg.runtimeBoot.BootMemoryUpperBoundBytes() {
		return nil, fmt.Errorf("initial mode differs from original runtime bytes")
	}
	slot, exact := spec.Guest.MemorySlotSize.AsInt64()
	slots := spec.Guest.MemorySlots
	if !exact || slot <= 0 || slots.Min < 1 || slots.Max < slots.Min || uint64(slots.Max) > math.MaxInt64/uint64(slot) {
		return nil, fmt.Errorf("initial memory geometry overflows")
	}
	minimum, maximum := uint64(slots.Min)*uint64(slot), uint64(slots.Max)*uint64(slot)
	base := initial.OriginalBaseBytes
	if minimum < base || maximum <= base || base%vmguard.VirtioBlockBytes != 0 || (minimum-base)%vmguard.VirtioBlockBytes != 0 || (maximum-base)%vmguard.VirtioBlockBytes != 0 {
		return nil, fmt.Errorf("initial base or target is outside original block geometry")
	}
	return []string{"-m", fmt.Sprintf("size=%db,slots=1,maxmem=%db", base, maximum), "-object", fmt.Sprintf("memory-backend-ram,id=vmem0,size=%db", maximum-base), "-device", "virtio-mem-pci,id=vm0,memdev=vmem0,node=0,block-size=8M,requested-size=0"}, nil
}
