package resourceguard

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func guestMaximumFixture(t *testing.T) guestMemoryMaximumObservation {
	t.Helper()
	ref, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if e != nil {
		t.Fatal(e)
	}
	return guestMemoryMaximumObservation{ProtocolVersion: 1, Scope: guestMemoryMaximumScope, Build: ref, GuestBootID: "11111111-2222-3333-4444-555555555555", DeviceName: "virtio0", CanonicalPath: "/sys/devices/pci0000:00/0000:00:02.0/virtio0", SysfsDevice: 23, SysfsInode: 98, DeviceID: 24, VendorID: 0x1af4, DriverPath: "/sys/bus/virtio/drivers/virtio_mem", Address: 4294967296, RegionBytes: 805306368, NotIndependentlyObserved: [2]string{"device_block_size", "device_nid"}}
}

func TestGuestMaximumIOMemResource(t *testing.T) {
	valid := "00000000-00000fff : Reserved\n00100000-0ffdefff : System RAM\n100000000-12fffffff : virtio0\n180000000-97fffffff : PCI Bus 0000:00\n"
	for name, text := range map[string]string{"zero-plugged-region": valid, "online-child-independent": strings.Replace(valid, "100000000-12fffffff : virtio0\n", "100000000-12fffffff : virtio0\n  100000000-107ffffff : System RAM\n", 1)} {
		t.Run(name, func(t *testing.T) {
			a, s, e := parseGuestMaximumIOMem(text, "virtio0")
			if e != nil || a != 4294967296 || s != 805306368 {
				t.Fatalf("%d %d %v", a, s, e)
			}
		})
	}
	for name, text := range map[string]string{
		"missing": "00000000-00000fff : Reserved\n", "masked": "00000000-00000000 : virtio0\n", "nested": "  100000000-12fffffff : virtio0\n", "duplicate": valid + "100000000-12fffffff : virtio0\n", "other-device": valid + "140000000-14fffffff : virtio1\n", "reversed": "12fffffff-100000000 : virtio0\n", "unaligned-base": "100000001-12fffffff : virtio0\n", "unaligned-length": "100000000-12ffffffe : virtio0\n", "overflow": "00000000-ffffffffffffffff : virtio0\n", "last-address-overflow": "fffffffffffff000-ffffffffffffffff : virtio0\n", "malformed": "not a resource\n", "odd-indent": " 100000000-12fffffff : virtio0\n", "uppercase": "100000000-12FFFFFFF : virtio0\n", "excessive": strings.Repeat("x", 256*1024+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, e := parseGuestMaximumIOMem(text, "virtio0"); e == nil {
				t.Fatal("unsafe resource accepted")
			}
		})
	}
}

func TestGuestMaximumObservationValidation(t *testing.T) {
	o := guestMaximumFixture(t)
	if e := o.validate(); e != nil {
		t.Fatal(e)
	}
	for name, change := range map[string]func(*guestMemoryMaximumObservation){
		"version": func(o *guestMemoryMaximumObservation) { o.ProtocolVersion = 2 }, "scope": func(o *guestMemoryMaximumObservation) { o.Scope = "authorized" }, "boot": func(o *guestMemoryMaximumObservation) { o.GuestBootID = "" }, "historical-build": func(o *guestMemoryMaximumObservation) {
			o.Build, _ = LookupPhysicalKernelBuild(NormalV1PIDCappedPhysicalKernelProfileID)
		}, "mixed-config": func(o *guestMemoryMaximumObservation) { o.Build.KernelConfigSHA256 = strings.Repeat("a", 64) }, "inode": func(o *guestMemoryMaximumObservation) { o.SysfsInode = 0 }, "device": func(o *guestMemoryMaximumObservation) { o.DeviceID = 1 }, "vendor": func(o *guestMemoryMaximumObservation) { o.VendorID = 0 }, "driver": func(o *guestMemoryMaximumObservation) { o.DriverPath = "/fake/virtio_mem" }, "canonical": func(o *guestMemoryMaximumObservation) { o.CanonicalPath = "/sys/devices/../virtio0" }, "zero-region": func(o *guestMemoryMaximumObservation) { o.RegionBytes = 0 }, "unaligned": func(o *guestMemoryMaximumObservation) { o.RegionBytes++ }, "overflow": func(o *guestMemoryMaximumObservation) { o.Address = control.Uint64(math.MaxUint64 - 4095) }, "pretend-block": func(o *guestMemoryMaximumObservation) { o.NotIndependentlyObserved[0] = "" }, "full-source": func(o *guestMemoryMaximumObservation) { o.AllSourcesClosed = true }, "normal-permit": func(o *guestMemoryMaximumObservation) { o.NormalDecreaseAuthorized = true },
	} {
		t.Run(name, func(t *testing.T) {
			x := o
			change(&x)
			if x.validate() == nil {
				t.Fatal("invalid observation accepted")
			}
		})
	}
	raw, e := json.Marshal(o)
	if e != nil {
		t.Fatal(e)
	}
	var decoded guestMemoryMaximumObservation
	if e = json.Unmarshal(raw, &decoded); e != nil || o.match(decoded) != nil {
		t.Fatal("observation roundtrip differs", e)
	}
}

func TestGuestMaximumObservationMatch(t *testing.T) {
	o := guestMaximumFixture(t)
	if e := o.match(o); e != nil {
		t.Fatal(e)
	}
	for name, change := range map[string]func(*guestMemoryMaximumObservation){
		"boot": func(o *guestMemoryMaximumObservation) { o.GuestBootID = "22222222-2222-3333-4444-555555555555" }, "inode": func(o *guestMemoryMaximumObservation) { o.SysfsInode++ }, "device-filesystem": func(o *guestMemoryMaximumObservation) { o.SysfsDevice++ }, "path": func(o *guestMemoryMaximumObservation) {
			o.CanonicalPath = "/sys/devices/pci0000:00/0000:00:03.0/virtio0"
		}, "address": func(o *guestMemoryMaximumObservation) { o.Address += 4096 }, "maximum": func(o *guestMemoryMaximumObservation) { o.RegionBytes += 4096 },
	} {
		t.Run(name, func(t *testing.T) {
			x := o
			change(&x)
			if x.validate() != nil {
				t.Fatal("drift fixture should itself be valid")
			}
			if o.match(x) == nil {
				t.Fatal("source drift accepted")
			}
		})
	}
}

func TestGuestMaximumGeometryComparison(t *testing.T) {
	o := guestMaximumFixture(t)
	if n, e := compareGuestMaximumGeometry(o, 268435456, 1073741824, 4294967296, 805306368); e != nil || n != 1073741824 {
		t.Fatal(n, e)
	}
	for name, in := range map[string][4]uint64{
		"missing-base": {0, 1073741824, 4294967296, 805306368}, "unaligned-base": {268435457, 1073741825, 4294967296, 805306368}, "K-not-base": {260046848, 1073741824, 4294967296, 805306368}, "different-machine": {268435456, 2147483648, 4294967296, 805306368}, "different-address": {268435456, 1073741824, 4294971392, 805306368}, "different-region": {268435456, 1073745920, 4294967296, 805310464}, "base-overlap": {8589934592, 9395240960, 4294967296, 805306368}, "addition-overflow": {math.MaxUint64 - 4095, 805302272, 4294967296, 805306368},
	} {
		t.Run(name, func(t *testing.T) {
			if _, e := compareGuestMaximumGeometry(o, in[0], in[1], in[2], in[3]); e == nil {
				t.Fatal("inconsistent arithmetic accepted")
			}
		})
	}
	t.Run("invalid-source", func(t *testing.T) {
		o.NormalDecreaseAuthorized = true
		if _, e := compareGuestMaximumGeometry(o, 268435456, 1073741824, 4294967296, 805306368); e == nil {
			t.Fatal("invalid source accepted")
		}
	})
}

func TestGuestMaximumCanonicalDevice(t *testing.T) {
	for name, p := range map[string]string{"virtio0": "/sys/devices/pci0000:00/0000:00:02.0/virtio0", "virtio12": "/sys/devices/pci0000:00/0000:00:03.0/virtio12"} {
		if !guestMaximumCanonicalPath(name, p) {
			t.Fatal(name, p)
		}
	}
	for name, p := range map[string]string{"virtio01": "/sys/devices/x/virtio01", "../virtio0": "/sys/devices/x/virtio0", "virtio0": "/tmp/virtio0", "virtio1": "/sys/devices/x/../virtio1", "virtio2": "/sys/devices/a b/virtio2", "virtio3": "/sys/devices/x/virtio4"} {
		if guestMaximumCanonicalPath(name, p) {
			t.Fatal("unsafe device path accepted", name, p)
		}
	}
}
