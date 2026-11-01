//go:build linux

package resourceguard

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const normalOrdinaryStockUdevManifestScope = "normal-ordinary-stock-udev-manifest-v2"
const normalOrdinaryStockUdevACLPath = "/etc/udev/rules.d/99-neon-stock-acl.rules"

type normalOrdinaryStockUdevFile struct {
	Path   string         `json:"path"`
	Kind   string         `json:"kind"`
	Code   bool           `json:"code"`
	Bytes  control.Uint64 `json:"bytes"`
	SHA256 string         `json:"sha256"`
}
type normalOrdinaryStockUdevManifest struct {
	ProtocolVersion   uint32                        `json:"protocol_version"`
	Scope             string                        `json:"scope"`
	PackageLockSHA256 string                        `json:"package_lock_sha256"`
	Files             []normalOrdinaryStockUdevFile `json:"files"`
}

// 这只是静态清单，绝不从JSON生成Root/FS/VM/预算能力。Root后继生产者仍须
// 真实只读FD/ELF闭包/原package-lock与prep生命周期；manifest不含循环的runtime/System SHA。
// 所有必填键显式出现且非null，拒Go标准库允许的大小写别名/省略false与重复拼写。
func normalOrdinaryStockUdevExactKeys(raw []byte, want ...string) error {
	var fields map[string]json.RawMessage
	if e := json.Unmarshal(raw, &fields); e != nil || fields == nil {
		return ErrUnknown
	}
	if len(fields) != len(want) {
		return ErrUnknown
	}
	for _, name := range want {
		b, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
			return ErrUnknown
		}
	}
	return nil
}
func (v *normalOrdinaryStockUdevFile) UnmarshalJSON(raw []byte) error {
	if e := normalOrdinaryStockUdevExactKeys(raw, "path", "kind", "code", "bytes", "sha256"); e != nil {
		return e
	}
	type plain normalOrdinaryStockUdevFile
	var n plain
	if e := control.UnmarshalObject(raw, &n); e != nil {
		return e
	}
	*v = normalOrdinaryStockUdevFile(n)
	return nil
}
func decodeNormalOrdinaryStockUdevManifest(raw []byte) (normalOrdinaryStockUdevManifest, error) {
	var zero normalOrdinaryStockUdevManifest
	fail := func(e error) (normalOrdinaryStockUdevManifest, error) { return zero, errors.Join(ErrUnknown, e) }
	if len(raw) == 0 || len(raw) > 64<<10 {
		return fail(fmt.Errorf("stock udev manifest size differs"))
	}
	if e := normalOrdinaryStockUdevExactKeys(raw, "protocol_version", "scope", "package_lock_sha256", "files"); e != nil {
		return fail(e)
	}
	type plain normalOrdinaryStockUdevManifest
	var n plain
	if e := control.UnmarshalObject(raw, &n); e != nil {
		return fail(e)
	}
	m := normalOrdinaryStockUdevManifest(n)
	if m.ProtocolVersion != 2 || m.Scope != normalOrdinaryStockUdevManifestScope || !normalOrdinaryStockUdevDigest(m.PackageLockSHA256) || len(m.Files) > 100 {
		return fail(fmt.Errorf("stock udev manifest contract differs"))
	}
	required := normalOrdinaryStockUdevRequiredInventory()
	seen := map[string]bool{}
	var total uint64
	previous := ""
	dsos := 0
	for _, f := range m.Files {
		if f.Path <= previous || path.Clean(f.Path) != f.Path || !strings.HasPrefix(f.Path, "/") || strings.ContainsAny(f.Path, "\x00\r\n\t ") || !normalOrdinaryStockUdevDigest(f.SHA256) || f.Bytes == 0 {
			return fail(fmt.Errorf("stock udev inventory path/order/hash differs"))
		}
		previous = f.Path
		kind, limit, ok := normalOrdinaryStockUdevFileClass(f.Path)
		if !ok || f.Kind != kind || f.Code != (kind == "program" || kind == "helper" || kind == "dso") || uint64(f.Bytes) > limit {
			return fail(fmt.Errorf("stock udev fixed file class differs"))
		}
		total += uint64(f.Bytes)
		if total > 256<<20 {
			return fail(fmt.Errorf("stock udev inventory total exceeds bound"))
		}
		if kind == "dso" {
			dsos++
		}
		seen[f.Path] = true
	}
	for name := range required {
		if !seen[name] {
			return fail(fmt.Errorf("stock udev required fixed inventory absent"))
		}
	}
	if dsos == 0 {
		return fail(fmt.Errorf("stock udev dynamic loader closure inventory absent"))
	}
	return m, nil
}
func normalOrdinaryStockUdevDigest(s string) bool {
	return physicalDigestValid(s) && s != strings.Repeat("0", 64)
}

// 必须完整保留原27规则、28hwdb文本与udev.conf/原passwd/group；不是按现场命中规则缩小库存。
// 返回独立map；不能通过改全局切片/清单替换新Root可执行程序。
func normalOrdinaryStockUdevRequiredInventory() map[string]string {
	m := map[string]string{"/neonvm/bin/udevd": "program", "/neonvm/bin/udevadm": "program", normalOrdinaryStockUdevACLPath: "acl-override", "/etc/udev/udev.conf": "config", "/etc/passwd": "config", "/etc/group": "config"}
	for _, n := range []string{"ata_id", "cdrom_id", "collect", "dmi_memory_id", "fido_id", "mtd_probe", "scsi_id", "v4l_id"} {
		m["/lib/udev/"+n] = "helper"
	}
	m["/lib/udev/rules.d/99-virtio-ports.rules"] = "rule" // 原prepare-root生成log alias规则，不是新增vendor规则。
	for _, n := range []string{"50-udev-default.rules", "60-autosuspend.rules", "60-block.rules", "60-cdrom_id.rules", "60-drm.rules", "60-evdev.rules", "60-fido-id.rules", "60-input-id.rules", "60-persistent-alsa.rules", "60-persistent-input.rules", "60-persistent-storage-tape.rules", "60-persistent-storage.rules", "60-persistent-v4l.rules", "60-sensor.rules", "60-serial.rules", "64-btrfs.rules", "70-camera.rules", "70-joystick.rules", "70-memory.rules", "70-mouse.rules", "70-touchpad.rules", "75-net-description.rules", "75-probe_mtd.rules", "78-sound-card.rules", "80-drivers.rules", "80-net-name-slot.rules", "81-net-dhcp.rules"} {
		m["/lib/udev/rules.d/"+n] = "rule"
	}
	for _, n := range []string{"20-OUI.hwdb", "20-acpi-vendor.hwdb", "20-bluetooth-vendor-product.hwdb", "20-dmi-id.hwdb", "20-net-ifname.hwdb", "20-pci-classes.hwdb", "20-pci-vendor-model.hwdb", "20-sdio-classes.hwdb", "20-sdio-vendor-model.hwdb", "20-usb-classes.hwdb", "20-usb-vendor-model.hwdb", "20-vmbus-class.hwdb", "60-autosuspend-fingerprint-reader.hwdb", "60-autosuspend.hwdb", "60-evdev.hwdb", "60-input-id.hwdb", "60-keyboard.hwdb", "60-seat.hwdb", "60-sensor.hwdb", "70-analyzers.hwdb", "70-av-production.hwdb", "70-cameras.hwdb", "70-joystick.hwdb", "70-mouse.hwdb", "70-pda.hwdb", "70-pointingstick.hwdb", "70-touchpad.hwdb", "80-ieee1394-unit-function.hwdb"} {
		m["/etc/udev/hwdb.d/"+n] = "hwdb-text"
	}
	return m
}

// hwdb.bin若真实存在，Root inventory/lookup必须把原文件列入本清单；纯parser
// 无法证明文件不存在，也不把省略bin字段当原provider未安装。两个实际lookup孔固定。
func normalOrdinaryStockUdevOptionalHWDBPaths() []string {
	return []string{"/etc/udev/hwdb.bin", "/lib/udev/hwdb.bin"}
}
func normalOrdinaryStockUdevFileClass(name string) (string, uint64, bool) {
	if kind, ok := normalOrdinaryStockUdevRequiredInventory()[name]; ok {
		switch kind {
		case "program", "helper":
			return kind, 16 << 20, true
		case "rule":
			return kind, 64 << 10, true
		case "acl-override", "config":
			return kind, 64 << 10, true
		case "hwdb-text":
			return kind, 8 << 20, true
		}
	}
	for _, n := range normalOrdinaryStockUdevOptionalHWDBPaths() {
		if name == n {
			return "hwdb-bin", 64 << 20, true
		}
	}
	// 这里只允许原包的有限DSO名；实际DT_NEEDED/解释器/只读祖先与全文件闭包仍
	// 必须由Root ELF读者核验，合法文件名本身不是可执行授权。
	for _, dir := range []string{"/lib/", "/usr/lib/"} {
		for _, base := range []string{"ld-musl-x86_64.so.1", "libc.musl-x86_64.so.1", "libudev.so.1", "libudev.so.1.6.3", "libblkid.so.1", "libuuid.so.1", "libkmod.so.2", "libz.so.1", "liblzma.so.5", "libzstd.so.1", "libcrypto.so.3", "libgcc_s.so.1"} {
			if name == dir+base {
				return "dso", 64 << 20, true
			}
		}
	}
	return "", 0, false
}
func normalOrdinaryStockUdevSortedRequiredPaths() []string {
	m := normalOrdinaryStockUdevRequiredInventory()
	out := make([]string, 0, len(m))
	for n := range m {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
