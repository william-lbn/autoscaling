//go:build linux && dsga_native_kernel_profile

package resourceguard

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// TestPG17NativePhysicalKernelComponent is an explicitly selected, isolated KVM
// component probe. It observes real sources; it does not start PG, write memory
// limits, resize a VM, issue capacity, or certify a production Kube identity.
func TestPG17NativePhysicalKernelComponent(t *testing.T) {
	const (
		optIn       = "isolated-kvm-component-only-v1"
		cgroupPath  = "/sys/fs/cgroup/pg17-native-empty"
		kernelSHA   = "0ab3f71456c783452a49d50752a9b1945c2da399588febed409637d7af6891a0"
		harnessVM   = "test-harness-pg17-native-kernel-component"
		harnessComp = "test-harness-isolated-kvm-observer"
	)
	for key, expected := range map[string]string{
		"PG17_NATIVE_KERNEL_COMPONENT": optIn,
		"PG17_NATIVE_EMPTY_PG_CGROUP":  cgroupPath,
		"PG17_NATIVE_KERNEL_SHA256":    kernelSHA,
	} {
		if os.Getenv(key) != expected {
			t.Fatalf("explicit isolated native probe environment required: %s", key)
		}
	}
	if os.Geteuid() != 0 {
		t.Fatal("isolated Guest root required")
	}
	read := func(path string) string {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read real source %s: %v", path, err)
		}
		return strings.TrimSpace(string(b))
	}
	cmdline := read("/proc/cmdline")
	if !strings.Contains(" "+cmdline+" ", " pg17_kernel_component=1 ") {
		t.Fatal("dedicated isolated kernel boot marker required")
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(cgroupPath, &fs); err != nil || uint64(fs.Type) != 0x63677270 {
		t.Fatalf("real cgroup2 filesystem required: type=%x err=%v", fs.Type, err)
	}
	info, err := os.Lstat(cgroupPath)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("original real empty cgroup required: %v", err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != 0 || st.Ino == 0 {
		t.Fatal("original root-owned cgroup dev/inode required")
	}
	if read(filepath.Join(cgroupPath, "cgroup.procs")) != "" ||
		read(filepath.Join(cgroupPath, "cgroup.threads")) != "" {
		t.Fatal("component-only PG placeholder must stay empty")
	}
	memoryMax := read(filepath.Join(cgroupPath, "memory.max"))
	id := Identity{
		VMUID: harnessVM, ComputeInstanceID: harnessComp,
		GuestBootID:  read("/proc/sys/kernel/random/boot_id"),
		CgroupDevice: control.Uint64(st.Dev), CgroupInode: control.Uint64(st.Ino),
	}
	if err := id.Validate(); err != nil {
		t.Fatal(err)
	}
	observation, err := readPhysicalKernelComponent(id)
	if err != nil {
		t.Fatalf("real physical kernel observation: %v", err)
	}
	if observation.PossibleCPUs != 2 || observation.PIDMax != 512 || observation.VMShrinkPermit {
		t.Fatalf("unexpected isolated probe geometry/permission: %+v", observation)
	}
	movable := false
	for _, zone := range observation.Zones {
		if zone.Name == "Movable" && zone.PresentPages > 0 {
			movable = true
		}
	}
	if !movable {
		t.Fatal("real populated Movable zone required for this component probe")
	}
	again, err := readPhysicalKernelComponent(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := observation.Match(again); err != nil {
		t.Fatalf("same-boot read-only sources drifted: %v", err)
	}
	after, err := os.Stat(cgroupPath)
	if err != nil {
		t.Fatal(err)
	}
	afterStat := after.Sys().(*syscall.Stat_t)
	if st.Dev != afterStat.Dev || st.Ino != afterStat.Ino ||
		read(filepath.Join(cgroupPath, "cgroup.procs")) != "" ||
		read(filepath.Join(cgroupPath, "cgroup.threads")) != "" ||
		memoryMax != read(filepath.Join(cgroupPath, "memory.max")) {
		t.Fatal("original empty cgroup identity/population/limit changed")
	}
	notes, err := physicalKernelNotes()
	if err != nil {
		t.Fatal(err)
	}
	blocks := map[string]map[string]string{}
	entries, err := os.ReadDir("/sys/devices/system/memory")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		suffix := strings.TrimPrefix(entry.Name(), "memory")
		if suffix == entry.Name() || suffix == "" || strings.IndexFunc(suffix, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
			continue
		}
		values := map[string]string{}
		for _, field := range []string{"state", "valid_zones", "removable"} {
			b, err := os.ReadFile(filepath.Join("/sys/devices/system/memory", entry.Name(), field))
			if err != nil {
				values[field+"_read_error"] = err.Error()
			} else {
				values[field] = strings.TrimSpace(string(b))
			}
		}
		blocks[entry.Name()] = values
	}
	receipt := struct {
		Classification       string                       `json:"classification"`
		ProductionIdentity   bool                         `json:"production_identity"`
		PGStarted            bool                         `json:"pg_started"`
		MemoryLimitWritten   bool                         `json:"memory_limit_written"`
		VMResized            bool                         `json:"vm_resized"`
		E2E                  bool                         `json:"e2e"`
		Observation          PhysicalKernelObservation    `json:"observation"`
		KernelArtifactSHA    string                       `json:"declared_kernel_artifact_sha256"`
		CommandLine          string                       `json:"command_line"`
		KernelNotesBase64    string                       `json:"kernel_notes_base64"`
		ZoneInfo             string                       `json:"zoneinfo"`
		MemInfo              string                       `json:"meminfo"`
		EmptyCgroupMemoryMax string                       `json:"empty_cgroup_memory_max"`
		MemoryBlockSize      string                       `json:"memory_block_size_hex"`
		MemoryBlocks         map[string]map[string]string `json:"memory_blocks_read_only"`
	}{
		Classification: "PG17-S-KERNEL-001 observation-only component",
		Observation:    observation, KernelArtifactSHA: kernelSHA,
		CommandLine: cmdline, KernelNotesBase64: base64.StdEncoding.EncodeToString(notes),
		ZoneInfo: read("/proc/zoneinfo"), MemInfo: read("/proc/meminfo"),
		EmptyCgroupMemoryMax: memoryMax,
		MemoryBlockSize:      read("/sys/devices/system/memory/block_size_bytes"),
		MemoryBlocks:         blocks,
	}
	b, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("PG17_NATIVE_KERNEL_COMPONENT=%s\n", b)
}
