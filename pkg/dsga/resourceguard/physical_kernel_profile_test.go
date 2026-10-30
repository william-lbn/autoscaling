package resourceguard

import (
	"math"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func physicalFixture(t *testing.T) PhysicalKernelObservation {
	t.Helper()
	zones, err := parsePhysicalZoneInfo("Node 0, zone DMA32\n  present 65536\n  managed 50000\nNode 0, zone Movable\n  present 262144\n")
	if err != nil {
		t.Fatal(err)
	}
	o := PhysicalKernelObservation{ProtocolVersion: 1, Scope: physicalKernelObservationScope, Identity: Identity{VMUID: "vm-physical", ComputeInstanceID: "compute-physical", GuestBootID: "11111111-2222-3333-4444-555555555555", CgroupDevice: 1, CgroupInode: 2}, KernelRelease: physicalKernelRelease, KernelNotesSHA256: physicalDigest([]byte("notes")), CommandLineSHA256: physicalDigest([]byte("boot")), PageSizeBytes: 4096, PossibleCPUs: 4, PIDMax: 512, Zones: zones, NonMovableUpperBytes: 256 << 20, CPUChargeStockUpperBytes: 1 << 20}
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
	return o
}

func TestPhysicalKernelObservationParsers(t *testing.T) {
	t.Run("present-not-managed-or-free", func(t *testing.T) {
		o := physicalFixture(t)
		if o.NonMovableUpperBytes != 256<<20 {
			t.Fatal(o)
		}
		// 即使当前 artifact 未开 DMA，观察层也必须保守计入真实 DMA 页，
		// 同时允许完整 DMA/DMA32/Normal/Movable 四区而不漏算或无必要拒绝。
		zones, err := parsePhysicalZoneInfo("Node 0, zone DMA\n present 1\nNode 0, zone DMA32\n present 2\nNode 0, zone Normal\n present 3\nNode 0, zone Movable\n present 100\n")
		if err != nil {
			t.Fatal(err)
		}
		k, err := physicalNonMovable(zones)
		if err != nil || k != 6*4096 {
			t.Fatal(k, err)
		}
		o.Zones, o.NonMovableUpperBytes = zones, control.Uint64(k)
		if err := o.Validate(); err != nil {
			t.Fatal(err)
		}
	})
	for name, text := range map[string]string{
		"missing-present":       "Node 0, zone Normal\n managed 64\n",
		"duplicate-present":     "Node 0, zone Normal\n present 64\n present 65\n",
		"duplicate-zone":        "Node 0, zone Normal\n present 64\nNode 0, zone Normal\n present 64\n",
		"numa-out-of-profile":   "Node 1, zone Normal\n present 64\n",
		"device-out-of-profile": "Node 0, zone Device\n present 64\n",
		"only-movable":          "Node 0, zone Movable\n present 64\n",
		"overflow":              "Node 0, zone Normal\n present 18446744073709551615\n",
		"unbound-present":       "present 64\n",
		"noncanonical":          "Node 0, zone Normal\n present 064\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parsePhysicalZoneInfo(text); err == nil {
				t.Fatal("accepted incomplete/unsupported zone source")
			}
		})
	}
	t.Run("cpu-possible-not-online", func(t *testing.T) {
		n, err := physicalPossibleCPUs("0-7")
		if err != nil || n != 8 {
			t.Fatal(n, err)
		}
		for _, s := range []string{"", "1-7", "0-0", "0-2,4", "0-1024", "0-01"} {
			if _, err := physicalPossibleCPUs(s); err == nil {
				t.Fatalf("accepted %q", s)
			}
		}
	})
	t.Run("boot-explicit-and-unambiguous", func(t *testing.T) {
		good := "console=ttyS0 cgroup.memory=nosocket,nobpf,nokmem memhp_default_state=online_movable"
		if err := physicalBootPolicy(good); err != nil {
			t.Fatal(err)
		}
		for _, s := range []string{"", strings.Replace(good, "nobpf,", "", 1), good + " memhp_default_state=online", good + " cgroup.memory=nokmem", strings.Replace(good, "online_movable", "online", 1), strings.Replace(good, "nokmem", "nokmem,nokmem", 1)} {
			if err := physicalBootPolicy(s); err == nil {
				t.Fatalf("accepted %q", s)
			}
		}
	})
}

func TestPhysicalKernelObservationMatchRejectsDrift(t *testing.T) {
	original := physicalFixture(t)
	t.Run("movable-may-grow-or-shrink", func(t *testing.T) {
		for _, n := range []control.Uint64{0, 524288} {
			current := physicalFixture(t)
			current.Zones[1].PresentPages = n
			if err := original.Match(current); err != nil {
				t.Fatal(err)
			}
		}
	})
	for name, change := range map[string]func(*PhysicalKernelObservation){
		"guest-boot": func(o *PhysicalKernelObservation) { o.Identity.GuestBootID = "11111111-2222-3333-4444-666666666666" },
		"cgroup":     func(o *PhysicalKernelObservation) { o.Identity.CgroupInode++ },
		"notes":      func(o *PhysicalKernelObservation) { o.KernelNotesSHA256 = physicalDigest([]byte("other-kernel")) },
		"boot-controls": func(o *PhysicalKernelObservation) {
			o.CommandLineSHA256 = physicalDigest([]byte("other-boot-controls"))
		},
		"possible-cpu":    func(o *PhysicalKernelObservation) { o.PossibleCPUs = 8; o.CPUChargeStockUpperBytes = 2 << 20 },
		"pid-space":       func(o *PhysicalKernelObservation) { o.PIDMax = 1024 },
		"physical-growth": func(o *PhysicalKernelObservation) { o.Zones[0].PresentPages++; o.NonMovableUpperBytes += 4096 },
		"physical-shrink": func(o *PhysicalKernelObservation) { o.Zones[0].PresentPages--; o.NonMovableUpperBytes -= 4096 },
		"forged-total":    func(o *PhysicalKernelObservation) { o.NonMovableUpperBytes-- },
		"forged-stock":    func(o *PhysicalKernelObservation) { o.CPUChargeStockUpperBytes-- },
		"wrong-release":   func(o *PhysicalKernelObservation) { o.KernelRelease = "6.12.26-william-other-build" },
		"permit":          func(o *PhysicalKernelObservation) { o.VMShrinkPermit = true },
	} {
		t.Run(name, func(t *testing.T) {
			current := physicalFixture(t)
			change(&current)
			if err := original.Match(current); err == nil {
				t.Fatal("accepted physical source drift")
			}
		})
	}
}

func TestPhysicalKernelObservationRejectsSummedCapacityOverflow(t *testing.T) {
	o := physicalFixture(t)
	o.Zones = []PhysicalKernelZone{{Node: 0, Name: "DMA32", PresentPages: control.Uint64(math.MaxUint64 / 4096)}, {Node: 0, Name: "Normal", PresentPages: 1}}
	if _, err := physicalNonMovable(o.Zones); err == nil {
		t.Fatal("accepted summed capacity overflow")
	}
}
