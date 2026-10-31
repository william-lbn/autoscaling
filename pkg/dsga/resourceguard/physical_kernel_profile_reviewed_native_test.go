//go:build linux && dsga_native_reviewed_kernel_profile

package resourceguard

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 同新准确3b/369内核的真实Guest观察组件；旧552测试与历史快照原样保留。
// 这里不运行PG、不创建Origin、不写allocator/sysctl、不授予normal/VM许可。
func TestNativeReviewedPhysicalKernelComponent(t *testing.T) {
	profile := os.Getenv("DSGA_NATIVE_REVIEWED_KERNEL_PROFILE")
	if !physicalKernelHasPIDHardSource(profile) {
		t.Fatal("explicit reviewed hard-PID profile required")
	}
	ref, err := LookupPhysicalKernelBuild(profile)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"DSGA_NATIVE_REVIEWED_KERNEL_COMPONENT": "isolated-kvm-reviewed-reader-v1",
		"DSGA_NATIVE_REVIEWED_KERNEL_SHA256":    ref.KernelArtifactSHA256,
		"DSGA_NATIVE_REVIEWED_CONFIG_SHA256":    ref.KernelConfigSHA256,
	} {
		if os.Getenv(key) != want {
			t.Fatalf("explicit exact reviewed build required: %s", key)
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
	if !strings.Contains(" "+read("/proc/cmdline")+" ", " dsga_reviewed_kernel_reader=1 ") {
		t.Fatal("dedicated isolated boot marker required")
	}
	if read("/proc/sys/kernel/osrelease") != ref.KernelRelease {
		t.Fatal("reviewed normal-v1 build differs")
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(pgCgroup, &fs); err != nil || uint64(fs.Type) != 0x63677270 {
		t.Fatalf("actual cgroup2 required: %v", err)
	}
	info, err := os.Lstat(pgCgroup)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("actual original empty PG placeholder required: %v", err)
	}
	st := info.Sys().(*syscall.Stat_t)
	if st.Uid != 0 || st.Ino == 0 || read(pgCgroup+"/cgroup.procs") != "" ||
		read(pgCgroup+"/cgroup.threads") != "" {
		t.Fatal("root-owned empty PG placeholder required")
	}
	high := read(pgCgroup + "/memory.high")
	id := Identity{
		VMUID:             "test-harness-pg17-normal-kernel-reader",
		ComputeInstanceID: "test-harness-isolated-component",
		GuestBootID:       read("/proc/sys/kernel/random/boot_id"),
		CgroupDevice:      control.Uint64(st.Dev), CgroupInode: control.Uint64(st.Ino),
	}
	if err := id.Validate(); err != nil {
		t.Fatal(err)
	}
	observation, err := readPhysicalKernelComponent(id)
	if err != nil {
		t.Fatalf("actual normal-v1 reader: %v", err)
	}
	if observation.KernelNotesSHA256 != ref.KernelNotesSHA256 || observation.RuntimeCapabilities == nil ||
		observation.PIDMax != 512 || observation.PossibleCPUs != 2 || observation.VMShrinkPermit ||
		observation.RuntimeCapabilities.HardPIDMaxLimit == nil || *observation.RuntimeCapabilities.HardPIDMaxLimit != 512 {
		t.Fatalf("actual normal-v1 observation differs: %+v", observation)
	}
	if ref.StrictPageCounterIRQBoundary() != (profile == NormalSplitV1PhysicalKernelProfileID) {
		t.Fatal("exact compiled strict IRQ property differs")
	}
	hard, err := readPhysicalKernelPIDHardLimit(read("/proc/cmdline"))
	if err != nil || hard != 512 {
		t.Fatal("actual readonly hard PID source differs", hard, err)
	}
	if err := observation.Validate(); err != nil {
		t.Fatal(err)
	}
	again, err := readPhysicalKernelComponent(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := observation.Match(again); err != nil {
		t.Fatalf("same-boot real sources drifted: %v", err)
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		t.Fatal(err)
	}
	var decoded PhysicalKernelObservation
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := observation.Match(decoded); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(pgCgroup)
	if err != nil {
		t.Fatal(err)
	}
	as := after.Sys().(*syscall.Stat_t)
	if st.Dev != as.Dev || st.Ino != as.Ino || high != read(pgCgroup+"/memory.high") ||
		read(pgCgroup+"/cgroup.procs") != "" || read(pgCgroup+"/cgroup.threads") != "" {
		t.Fatal("original empty cgroup identity/limits/population changed")
	}
	receipt := struct {
		Case                                string                       `json:"case"`
		Build                               PhysicalKernelBuildReference `json:"reviewed_build_reference"`
		Observation                         PhysicalKernelObservation    `json:"observation"`
		ProductionIdentity                  bool                         `json:"production_identity"`
		RootEnrolled                        bool                         `json:"root_enrolled"`
		PGStarted                           bool                         `json:"pg_started"`
		RuntimeControlsWritten              bool                         `json:"runtime_controls_written"`
		FullAllocatorQualificationPerformed bool                         `json:"full_allocator_qualification_performed"`
		NormalDecreaseAuthorized            bool                         `json:"normal_decrease_authorized"`
		E2E                                 bool                         `json:"e2e"`
	}{Case: "PG17-S-KERNEL-003 actual PID-capped reader component", Build: ref, Observation: observation}
	if profile == NormalSplitV1PhysicalKernelProfileID {
		receipt.Case = "PG17-S-KERNEL-004 actual normal-split reader component"
	}
	b, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("DSGA_NATIVE_REVIEWED_KERNEL_COMPONENT=%s\n", b)
}
