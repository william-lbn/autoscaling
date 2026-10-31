//go:build linux && dsga_native_guest_memory_maximum

package resourceguard

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

// 只在隔离369真实KVM执行。这里只验只读Guest来源及测试装置QMP数据比较，
// 不产生原Host/Runner/Origin身份，不启PG，不授normal或完整I资格。
func TestNativeGuestMemoryMaximumComponent(t *testing.T) {
	ref, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if e != nil {
		t.Fatal(e)
	}
	if os.Geteuid() != 0 || os.Getenv("DSGA_NATIVE_GUEST_MAXIMUM_COMPONENT") != "isolated-369-readonly-region-v1" || os.Getenv("DSGA_NATIVE_GUEST_MAXIMUM_KERNEL_SHA256") != ref.KernelArtifactSHA256 || os.Getenv("DSGA_NATIVE_GUEST_MAXIMUM_CONFIG_SHA256") != ref.KernelConfigSHA256 {
		t.Fatal("explicit isolated accurate369 fixture required")
	}
	cmd, e := os.ReadFile("/proc/cmdline")
	if e != nil || !strings.Contains(" "+strings.TrimSpace(string(cmd))+" ", " dsga_guest_maximum_reader=1 ") {
		t.Fatal("dedicated isolated marker required", e)
	}
	b, e := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if e != nil {
		t.Fatal(e)
	}
	boot := strings.TrimSpace(string(b))
	o, e := readGuestMemoryMaximumComponent(boot)
	if e != nil {
		t.Fatal(e)
	}
	again, e := readGuestMemoryMaximumComponent(boot)
	if e != nil || o.match(again) != nil {
		t.Fatal("real source drifted", e)
	}
	raw, e := json.Marshal(o)
	if e != nil {
		t.Fatal(e)
	}
	var decoded guestMemoryMaximumObservation
	if e = json.Unmarshal(raw, &decoded); e != nil || o.match(decoded) != nil {
		t.Fatal("roundtrip differs", e)
	}
	if _, e = readGuestMemoryMaximumComponent("ffffffff-ffff-ffff-ffff-ffffffffffff"); e == nil {
		t.Fatal("wrong boot accepted")
	}
	// 下列值仅描述测试装置，正式同实例QMP生产互证留给原Runner消费者。
	if o.Address != 4294967296 || o.RegionBytes != 805306368 || o.AllSourcesClosed || o.NormalDecreaseAuthorized {
		t.Fatalf("fixture geometry differs: %+v", o)
	}
	if _, e = compareGuestMaximumGeometry(o, 268435456, 1073741824, 4294967296, 805306368); e != nil {
		t.Fatal(e)
	}
	fmt.Printf("DSGA_NATIVE_GUEST_MAXIMUM_COMPONENT=%s\n", raw)
}
