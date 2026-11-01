//go:build linux

package resourceguard

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"os"
	"strings"
	"testing"
)

func TestOrdinaryUdevV2MaximumGeometryPreservesHolesAndOriginalBase(t *testing.T) {
	c := &normalOrdinaryUdevV2Catalog{memoryBlock: 128 << 20, initialRAM: []normalOrdinaryUdevV2Range{{1 << 20, 2 << 30}}, device: normalOrdinaryUdevV2Range{4 << 30, 5 << 30}, entries: map[string]*normalOrdinaryUdevV2CatalogEntry{"/devices/system/memory/memory0": {subsystem: "memory", name: "memory0"}}}
	require.True(t, c.memoryIndex(0))
	require.False(t, c.memoryIndex(16), "hole after base cannot become future memory")
	require.False(t, c.memoryIndex(31))
	require.True(t, c.memoryIndex(32))
	require.True(t, c.memoryIndex(39))
	require.False(t, c.memoryIndex(40))
	require.False(t, c.memoryIndex(^uint32(0)))
}
func TestOrdinaryUdevV2KindThreeUsesOriginalTicketAndUnknownRulesFail(t *testing.T) {
	c := &normalOrdinaryUdevV2Catalog{entries: map[string]*normalOrdinaryUdevV2CatalogEntry{"/devices/virtual/test0": {path: "/devices/virtual/test0", subsystem: "test", name: "test0", properties: map[string]string{}, projection: normalOrdinaryUdevV2DeviceProjection{Properties: map[string]string{}}}}}
	p := ordinaryUdevV2TestProperties()
	ticket := &normalOrdinaryUdevV2Ticket{sequence: 8, properties: p}
	q, e := decodeNormalOrdinaryUdevV2Request(ordinaryUdevV2TestRequest(t, 3, 0, p))
	require.NoError(t, e)
	require.NoError(t, c.processed(ticket, q))
	q.Properties["DEVPATH"] = "/devices/forged"
	require.ErrorIs(t, c.processed(ticket, q), ErrUnknown)
	q.Properties = ordinaryUdevV2TestProperties()
	q.Properties["ID_UNKNOWN"] = "untrusted"
	require.ErrorIs(t, c.processed(ticket, q), ErrUnknown)
	c.unresolved = []string{"original known rule IMPORT unknown"}
	delete(q.Properties, "ID_UNKNOWN")
	require.ErrorIs(t, c.processed(ticket, q), ErrUnknown)
}

func TestOrdinaryUdevV2NonEventEvidenceRequiresKernelEmptyAndAbsentSubsystem(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
		err  error
		good bool
	}{
		{"filtered-empty", nil, unix.ENOENT, true},
		{"wrapped-absent", nil, &os.PathError{Op: "readlink", Path: "subsystem", Err: unix.ENOENT}, true},
		{"nonempty-no-subsystem", []byte("DRIVER=pci\n"), unix.ENOENT, false},
		{"newline-is-not-empty", []byte("\n"), unix.ENOENT, false},
		{"empty-with-subsystem", nil, nil, false},
		{"permission-failure", nil, unix.EACCES, false},
		{"wrong-object-type", nil, unix.EINVAL, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := normalOrdinaryUdevV2NonEventEvidence(tc.raw, tc.err)
			if tc.good {
				require.NoError(t, e)
			} else {
				require.ErrorIs(t, e, ErrUnknown)
			}
		})
	}
	// 即使伪造了有效内存几何，非事件容器也不进入原kernel ticket目标表。
	c := &normalOrdinaryUdevV2Catalog{entries: map[string]*normalOrdinaryUdevV2CatalogEntry{}}
	_, e := c.originalTicket(&normalOrdinaryUdevV2Ticket{properties: map[string]string{"DEVPATH": "/devices/pci0000:00", "SUBSYSTEM": "pci"}})
	require.ErrorIs(t, e, ErrUnknown)
}
func TestOrdinaryUdevV2NonEventContainerRejectsNonSysfsAndClosedOriginal(t *testing.T) {
	dir := t.TempDir()
	f, e := os.Open(dir)
	require.NoError(t, e)
	defer f.Close()
	ue, e := os.CreateTemp(dir, "uevent")
	require.NoError(t, e)
	defer ue.Close()
	dobj, e := normalFilesystemFDObject(int(f.Fd()))
	require.NoError(t, e)
	uobj, e := normalFilesystemFDObject(int(ue.Fd()))
	require.NoError(t, e)
	c := &normalOrdinaryUdevV2NonEventContainer{directory: f, uevent: ue, directoryObject: dobj, ueventObject: uobj}
	require.ErrorIs(t, c.verify(), ErrUnknown, "ordinary empty file + ENOENT cannot certify kernel-filtered state")
	require.NoError(t, ue.Close())
	require.ErrorIs(t, c.verify(), ErrUnknown)
	require.ErrorIs(t, (*normalOrdinaryUdevV2NonEventContainer)(nil).verify(), ErrUnknown)
}

func TestOrdinaryUdevV2KernelStaticCPUExtraNewlineIsNarrowAndBounded(t *testing.T) {
	const path = "/sys/devices/system/cpu/cpu0"
	const raw = "DRIVER=processor\nMODALIAS=cpu:type:x86,ven0000fam0006mod0006:feature:,0001\n\n"
	for _, tc := range []struct {
		name, path, subsystem, raw string
		good                       bool
	}{
		{"kernel-cpu-original", path, "cpu", raw, true},
		{"kernel-cpu-single-lf", path, "cpu", raw[:len(raw)-1], true},
		{"ordinary-kernel-order", "/sys/devices/test0", "test", "Z=last\nA=first\n", true},
		{"wrong-subsystem", path, "pci", raw, false},
		{"wrong-object", "/sys/devices/pci0000:00", "cpu", raw, false},
		{"noncanonical-index", "/sys/devices/system/cpu/cpu00", "cpu", raw, false},
		{"child-object", path + "/other", "cpu", raw, false},
		{"unknown-empty-line", path, "cpu", "DRIVER=processor\n\n", false},
		{"extra-empty-line", path, "cpu", raw + "\n", false},
		{"interior-empty-line", path, "cpu", "DRIVER=processor\n\nMODALIAS=cpu:type:x86\n\n", false},
		{"duplicate-key", path, "cpu", "DRIVER=processor\nDRIVER=forged\nMODALIAS=cpu:type:x86\n\n", false},
		{"overlong-property", path, "cpu", "MODALIAS=cpu:type:" + strings.Repeat("x", 1024) + "\n\n", false},
		{"control-value", path, "cpu", "DRIVER=\tbad\nMODALIAS=cpu:type:x86\n\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, e := normalOrdinaryUdevV2KernelStaticProperties(tc.path, tc.subsystem, []byte(tc.raw))
			if tc.good {
				require.NoError(t, e)
				require.NotEmpty(t, p)
			} else {
				require.Error(t, e)
			}
		})
	}
	// 此兼容只适用于内核原sysfs；DB及wire仍不能把双换行冒作规范属性。
	_, e := normalOrdinaryUdevV2StaticProperties([]byte(raw))
	require.Error(t, e)
	_, e = normalOrdinaryUdevV2Properties([]byte("Z=last\x00A=first\x00"), true)
	require.Error(t, e)
}
