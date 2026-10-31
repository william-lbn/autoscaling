//go:build linux

package resourceguard

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// InspectPhysicalKernelComponent 仅做固定内核路径的只读观察，当前无 launch、预算、
// qualification 或降限消费者。使用原 resource flock 只保证读取与同一日志身份相符，
// 不会封存“PG 出生前”的历史，也不会阻止未来特权拓扑写入；取消不写 WAL/资源，
// 原未知意图、Held 和 boot 不变。完整 profile 完成这两项前，本组件不能进入预算。
func (g *Guard) InspectPhysicalKernelComponent(ctx context.Context) (PhysicalKernelObservation, error) {
	var o PhysicalKernelObservation
	err := g.withRecord(ctx, func(r *record, _ *lockedState) error {
		var err error
		o, err = readPhysicalKernelComponent(r.Identity)
		return err
	})
	return o, err
}

func readPhysicalKernelComponent(id Identity) (PhysicalKernelObservation, error) {
	var o PhysicalKernelObservation
	if runtime.GOARCH != "amd64" || os.Getpagesize() != 4096 {
		return o, fmt.Errorf("physical profile requires Linux amd64 base pages")
	}
	release, err := kernelText("/proc/sys/kernel/osrelease")
	if err != nil {
		return o, err
	}
	normal, err := LookupPhysicalKernelBuild(NormalV1PhysicalKernelProfileID)
	pidCapped, pidErr := LookupPhysicalKernelBuild(NormalV1PIDCappedPhysicalKernelProfileID)
	split, splitErr := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if err != nil || pidErr != nil || splitErr != nil || (release != physicalKernelRelease && release != normal.KernelRelease && release != pidCapped.KernelRelease && release != split.KernelRelease) {
		return o, fmt.Errorf("physical profile requires an exact reviewed kernel artifact release")
	}
	boot, err := kernelText("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return o, err
	}
	if boot != id.GuestBootID {
		return o, fmt.Errorf("physical kernel Guest boot differs")
	}
	cmdline, err := kernelText("/proc/cmdline")
	if err != nil {
		return o, err
	}
	if err := physicalBootPolicy(cmdline); err != nil {
		return o, err
	}
	online, err := kernelText("/sys/devices/system/memory/auto_online_blocks")
	if err != nil {
		return o, err
	}
	if online != "online_movable" {
		return o, fmt.Errorf("physical hotplug policy drifted from MOVABLE")
	}
	for _, path := range []string{"/sys/kernel/mm/transparent_hugepage", "/sys/kernel/mm/ksm"} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			return o, fmt.Errorf("physical profile requires compiled-out THP/KSM at %s", path)
		}
	}
	swaps, err := kernelText("/proc/swaps")
	if err != nil {
		return o, err
	}
	lines := strings.Split(swaps, "\n")
	if len(lines) != 1 || strings.Join(strings.Fields(lines[0]), " ") != "Filename Type Size Used Priority" {
		return o, fmt.Errorf("physical profile requires no Guest swap devices")
	}
	possible, err := kernelText("/sys/devices/system/cpu/possible")
	if err != nil {
		return o, err
	}
	cpus, err := physicalPossibleCPUs(possible)
	if err != nil {
		return o, err
	}
	pidMax, err := readNumber("/proc/sys/kernel/pid_max", false)
	if err != nil {
		return o, err
	}
	if pidMax < 301 || pidMax > 4096 {
		return o, fmt.Errorf("physical profile requires explicitly bounded Guest PID space")
	}
	text, err := kernelText("/proc/zoneinfo")
	if err != nil {
		return o, err
	}
	zones, err := parsePhysicalZoneInfo(text)
	if err != nil {
		return o, err
	}
	k, err := physicalNonMovable(zones)
	if err != nil {
		return o, err
	}
	notes, err := physicalKernelNotes()
	if err != nil {
		return o, err
	}
	o = PhysicalKernelObservation{ProtocolVersion: 1, Scope: physicalKernelObservationScope, Identity: id, KernelRelease: release, KernelNotesSHA256: physicalDigest(notes), CommandLineSHA256: physicalDigest([]byte(cmdline)), PageSizeBytes: 4096, PossibleCPUs: cpus, PIDMax: uint32(pidMax), Zones: zones, NonMovableUpperBytes: control.Uint64(k), CPUChargeStockUpperBytes: control.Uint64(cpus) * 64 * 4096}
	if release == normal.KernelRelease || release == pidCapped.KernelRelease || release == split.KernelRelease {
		if err := physicalNormalV1BootPolicy(cmdline); err != nil {
			return PhysicalKernelObservation{}, err
		}
		capabilities, err := readPhysicalKernelRuntimeCapabilities()
		if err != nil {
			return PhysicalKernelObservation{}, err
		}
		if release == pidCapped.KernelRelease || release == split.KernelRelease {
			hard, err := readPhysicalKernelPIDHardLimit(cmdline)
			if err != nil {
				return PhysicalKernelObservation{}, err
			}
			capabilities.HardPIDMaxLimit = &hard
		}
		o.RuntimeCapabilities = &capabilities
	}
	return o, o.Validate()
}

func physicalKernelNotes() ([]byte, error) {
	f, err := rootFile("/sys/kernel/notes", false)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 1024*1024+1))
	if err != nil {
		return nil, err
	}
	if len(b) == 0 || len(b) > 1024*1024 {
		return nil, fmt.Errorf("missing or excessive running kernel notes")
	}
	return b, nil
}
