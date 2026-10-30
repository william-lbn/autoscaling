//go:build linux && dsga_native_normal_kernel_profile

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

// TestPG17NativeNormalKernelComponent observes the exact normal-v1 kernel in an
// isolated Guest. It does not enroll Root, run PG, tune sysctls, or permit resize.
func TestPG17NativeNormalKernelComponent(t *testing.T) {
	const artifact = "552ffabd096736e38d2d4b6c119533e4a26c2b9a47f6e04a8e1795a084f8d85d"
	const config = "935e43773879d6a4f5c27c2ef966314de7e0e463a4a9bebefa3dc5dcf283ce5b"
	for key, want := range map[string]string{
		"PG17_NATIVE_NORMAL_KERNEL_COMPONENT": "isolated-kvm-normal-reader-v1",
		"PG17_NATIVE_NORMAL_KERNEL_SHA256":    artifact,
		"PG17_NATIVE_NORMAL_CONFIG_SHA256":    config,
	} {
		if os.Getenv(key) != want {
			t.Fatalf("explicit isolated normal reader environment required: %s", key)
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
	if !strings.Contains(" "+read("/proc/cmdline")+" ", " pg17_normal_reader=1 ") {
		t.Fatal("dedicated isolated boot marker required")
	}
	ref, err := LookupPhysicalKernelBuild(NormalV1PhysicalKernelProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if ref.KernelArtifactSHA256 != artifact || ref.KernelConfigSHA256 != config ||
		read("/proc/sys/kernel/osrelease") != ref.KernelRelease {
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
		observation.PIDMax != 512 || observation.PossibleCPUs != 2 || observation.VMShrinkPermit {
		t.Fatalf("actual normal-v1 observation differs: %+v", observation)
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
	}{Case: "PG17-S-KERNEL-002 actual normal-v1 reader component", Build: ref, Observation: observation}
	b, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("PG17_NATIVE_NORMAL_KERNEL_COMPONENT=%s\n", b)
}
