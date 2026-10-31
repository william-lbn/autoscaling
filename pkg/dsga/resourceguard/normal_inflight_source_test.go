//go:build linux

package resourceguard

import (
	"math"
	"reflect"
	"testing"
)

func normalInflightTestFacts(t *testing.T) normalInflightSourceFacts {
	t.Helper()
	o := reviewedHardPIDFixture(t, NormalSplitV1PhysicalKernelProfileID)
	ref, err := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if err != nil {
		t.Fatal(err)
	}
	o.KernelRelease = ref.KernelRelease
	o.KernelNotesSHA256 = ref.KernelNotesSHA256
	o.PIDMax = 512
	c := *o.RuntimeCapabilities
	hard := uint32(512)
	c.HardPIDMaxLimit = &hard
	c.WatermarkBoostFactor = 0
	c.CompactionProactiveness = 0
	c.PerCPUPagelistHighFraction = 128
	o.RuntimeCapabilities = &c
	return normalInflightSourceFacts{Build: ref, Physical: o, NonMovableManagedPages: 10000, NoBalloonDevice: true, NoXenHypervisor: true, VirtioInventorySHA256: physicalDigest([]byte("model only virtio inventory")), ModulesDisabled: true, ModulesEmpty: true, NUMABalancingDisabled: true, DemotionDisabled: true, MemmapOnMemoryDisabled: true, HugeStatesZero: true, MountInfoSHA256: physicalDigest([]byte("model-only mount inventory")), MountFilesystems: []string{"ext4", "tmpfs"}}
}
func TestNormalInflightSourceExactTerms(t *testing.T) {
	f := normalInflightTestFacts(t)
	v, err := normalInflightCalculate(f)
	if err != nil {
		t.Fatal(err)
	}
	if v.MigrationDestinationsBytes != 1<<30 || v.TaskRetireBytes != 512*33*4096 || v.CompactionIsolatedFreeReserveBytes != 512*1535*4096 || v.CompiledCPUCeiling != 64 || v.CounterHierarchyGapBytes != 128<<20 {
		t.Fatalf("exact source equations differ: %+v", v)
	}
	if v.IRQContextCeiling != 18944 || v.IRQConditionalOccupiedBytes != 18944*35*4096 {
		t.Fatal("x86 fixed stack bound differs")
	}
	if v.AllSourcesClosed || v.NormalDecreaseAuthorized || len(v.MissingSources) != 8 {
		t.Fatal("partial source cannot authorize normal")
	}
	if v.KnownConditionalOccupiedBytes != v.MigrationDestinationsBytes+v.TaskPrechargeBytes+v.TaskRetireBytes+v.TaskSpeculativeLookupBytes+v.LateExitSingleRetireBytes+v.IRQConditionalOccupiedBytes {
		t.Fatal("mixed paid stock/free pool into occupied I")
	}
	before := v
	f.Physical.PossibleCPUs = 4
	f.Physical.CPUChargeStockUpperBytes = 4 * 64 * 4096
	v, err = normalInflightCalculate(f)
	if err != nil {
		t.Fatal(err)
	}
	before.CurrentPossibleCPUs = 4
	if !reflect.DeepEqual(before, v) {
		t.Fatal("current possible CPUs silently became original hard max")
	}
}
func TestNormalInflightSourceRejectDrift(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*normalInflightSourceFacts)
	}{
		{"other build", func(f *normalInflightSourceFacts) {
			f.Build, _ = LookupPhysicalKernelBuild(NormalV1PIDCappedPhysicalKernelProfileID)
		}},
		{"missing early hard PID", func(f *normalInflightSourceFacts) { f.Physical.RuntimeCapabilities.HardPIDMaxLimit = nil }},
		{"late PID differs", func(f *normalInflightSourceFacts) { f.Physical.PIDMax = 511 }},
		{"balloon", func(f *normalInflightSourceFacts) { f.NoBalloonDevice = false }},
		{"Xen", func(f *normalInflightSourceFacts) { f.NoXenHypervisor = false }},
		{"device source missing", func(f *normalInflightSourceFacts) { f.VirtioInventorySHA256 = "" }},
		{"module loaded", func(f *normalInflightSourceFacts) { f.ModulesEmpty = false }},
		{"modules writable", func(f *normalInflightSourceFacts) { f.ModulesDisabled = false }},
		{"numa automatic", func(f *normalInflightSourceFacts) { f.NUMABalancingDisabled = false }},
		{"demotion", func(f *normalInflightSourceFacts) { f.DemotionDisabled = false }},
		{"memmap movable", func(f *normalInflightSourceFacts) { f.MemmapOnMemoryDisabled = false }},
		{"Huge", func(f *normalInflightSourceFacts) { f.HugeStatesZero = false }},
		{"boost", func(f *normalInflightSourceFacts) { f.Physical.RuntimeCapabilities.WatermarkBoostFactor = 1 }},
		{"proactive", func(f *normalInflightSourceFacts) { f.Physical.RuntimeCapabilities.CompactionProactiveness = 1 }},
		{"PCP", func(f *normalInflightSourceFacts) { f.Physical.RuntimeCapabilities.PerCPUPagelistHighFraction = 0 }},
		{"FUSE", func(f *normalInflightSourceFacts) { f.MountFilesystems = []string{"fuse"} }},
		{"overlay", func(f *normalInflightSourceFacts) { f.MountFilesystems = []string{"overlay"} }},
		{"missing mounts", func(f *normalInflightSourceFacts) { f.MountInfoSHA256 = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := normalInflightTestFacts(t)
			tc.mutate(&f)
			if _, err := normalInflightCalculate(f); err == nil {
				t.Fatal("drift accepted")
			}
		})
	}
}
func TestNormalInflightSourceCheckedArithmetic(t *testing.T) {
	if _, e := normalInflightMultiply(math.MaxUint64, 2); e == nil {
		t.Fatal("multiply overflow")
	}
	if _, e := normalInflightSum(math.MaxUint64, 1); e == nil {
		t.Fatal("sum overflow")
	}
	if n, e := normalInflightMultiply(0, math.MaxUint64); e != nil || n != 0 {
		t.Fatal(n, e)
	}
}

func TestNormalInflightSourceMountParser(t *testing.T) {
	good := "1 0 0:1 / / rw - tmpfs tmpfs rw\n2 1 8:1 / /data rw - ext4 /dev/vda rw"
	if fs, e := normalInflightMountTypes(good); e != nil || len(fs) != 2 || fs[0] != "ext4" {
		t.Fatal(fs, e)
	}
	for _, bad := range []string{"", "1 - ext4", "1 0 8:1 / /data rw - ext4 /dev/vda rw,dax", "1 0 8:1 / /data rw - ext4 /dev/vda rw,dax=always", "1 0 8:1 / /data rw - ext4 /dev/vda rw,dax=inode"} {
		if _, e := normalInflightMountTypes(bad); e == nil {
			t.Fatal("malformed/DAX mount accepted")
		}
	}
}

func TestNormalInflightSourceCandidateBudget(t *testing.T) {
	f := normalInflightTestFacts(t)
	f.Physical.Zones = []PhysicalKernelZone{{Node: 0, Name: "Movable", PresentPages: 1}, {Node: 0, Name: "Normal", PresentPages: 131072}}
	f.Physical.NonMovableUpperBytes = 512 << 20
	f.NonMovableManagedPages = 100000
	f.Physical.RuntimeCapabilities.PerCPUPagelistHighFraction = 1024
	in := normalInflightPlanningInput{MaximumBytes: 12 << 30, NormalLeavesBytes: 1536 << 20, SystemBytes: 512 << 20}
	r, e := normalInflightPlan(f, in)
	if e != nil {
		t.Fatal(e)
	}
	if !r.CandidateFits || r.CandidateSpareBytes < 650<<20 || !r.CandidateOnly || r.MmaxAuthorized || r.NormalDecreaseAuthorized {
		t.Fatalf("12GiB conditional model differs: %+v", r)
	}
	in.MaximumBytes = 8 << 30
	r, e = normalInflightPlan(f, in)
	if e != nil {
		t.Fatal(e)
	}
	if r.CandidateFits || r.CandidateSpareBytes != 0 {
		t.Fatal("64CPU baseline falsely fits8GiB")
	}
	for _, bad := range []normalInflightPlanningInput{{}, {MaximumBytes: 1, NormalLeavesBytes: 1, SystemBytes: 1}, {MaximumBytes: math.MaxUint64 - 4095, NormalLeavesBytes: 1, SystemBytes: 1}} {
		if _, e := normalInflightPlan(f, bad); e == nil {
			t.Fatal("invalid/overflow plan accepted")
		}
	}
}
func TestNormalInflightSourceManagedParser(t *testing.T) {
	raw := "Node 0, zone DMA32\n managed 100\nNode 0, zone Movable\n managed 999\nNode 0, zone Normal\n managed 0"
	if n, e := normalInflightManagedLowPages(raw); e != nil || n != 100 {
		t.Fatal(n, e)
	}
	for _, s := range []string{"", "managed 1", "Node 0, zone Normal", raw + "\nmanaged 1"} {
		if _, e := normalInflightManagedLowPages(s); e == nil {
			t.Fatal("bad managed source accepted")
		}
	}
}
