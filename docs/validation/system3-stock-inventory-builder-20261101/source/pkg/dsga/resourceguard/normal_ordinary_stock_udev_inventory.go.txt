package resourceguard

import (
	"fmt"
	"sort"
)

// 构建器与Linux原输入读者使用同一份静态路径表；没有新增运行期授权协议。
// 固定MF位于原17mount图的RO根，不可放入/etc/neon-dsga持久RW孔。
const NormalSystemStockUdevManifestPath = "/neonvm/config/dsga-ordinary-stock-udev-manifest.json"
const normalOrdinaryStockUdevManifestPath = NormalSystemStockUdevManifestPath
const normalOrdinaryStockUdevACLPath = "/etc/udev/rules.d/99-neon-stock-acl.rules"

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

func normalSystemStockUdevFileKind(name string) (string, bool) {
	if name == NormalSystemStockUdevManifestPath {
		return "config", true
	}
	class, _, ok := normalOrdinaryStockUdevFileClass(name)
	if !ok {
		return "", false
	}
	switch class {
	case "program", "helper":
		return "exec", true
	case "dso":
		return "loader", true
	default:
		return "config", true
	}
}

// 返回新stock扩展的必要库存副本，不把路径/类别表称为ELF或来源资格。
func NormalSystemSourceStockUdevRequiredFiles() map[string]string {
	out := map[string]string{NormalSystemStockUdevManifestPath: "config"}
	for name := range normalOrdinaryStockUdevRequiredInventory() {
		kind, _ := normalSystemStockUdevFileKind(name)
		out[name] = kind
	}
	return out
}
func normalSystemStockUdevSelected(m NormalSystemSourceManifest) bool {
	if m.ProtocolVersion != 3 {
		return false
	}
	for _, f := range m.Files {
		if f.Path == NormalSystemStockUdevManifestPath {
			return true
		}
	}
	return false
}

// 原v1/v2及没有stock清单的原v3仍是四棵目录；完整stock扩展再纳hwdb。
// 此函数仅选择检查范围，调用者仍须Validate并逐FD验证，不能返回授权布尔。
func NormalSystemSourceConfigDirectoriesForManifest(m NormalSystemSourceManifest) []string {
	out := NormalSystemSourceConfigDirectories()
	if normalSystemStockUdevSelected(m) {
		out = append(out, "/etc/udev/hwdb.d")
	}
	return out
}
func normalSystemStockUdevValidateInventory(m NormalSystemSourceManifest) error {
	if !normalSystemStockUdevSelected(m) {
		return nil
	}
	wanted := NormalSystemSourceStockUdevRequiredFiles()
	for _, f := range m.Files {
		if kind, ok := wanted[f.Path]; ok {
			if f.Kind != kind {
				return fmt.Errorf("stock System3 file class differs: %s", f.Path)
			}
			if f.Path == NormalSystemStockUdevManifestPath && f.CanonicalPath != f.Path {
				return fmt.Errorf("stock manifest alias is unsupported")
			}
			delete(wanted, f.Path)
		}
	}
	if len(wanted) != 0 {
		return fmt.Errorf("stock System3 requires complete fixed inventory")
	}
	return nil
}
func normalSystemStockUdevHelperAlias(path, canonical string) bool {
	kind, _, ok := normalOrdinaryStockUdevFileClass(path)
	if !ok || kind != "helper" {
		return false
	}
	return canonical == "/usr"+path
}

// 旧version级入口原样保留，新增权限范围必须由同一份manifest中的固定MF选中。
// 不允许仅version=3就单独加入helper/账户/hwdb而缺少其余准备库存。
func normalSystemAllowedManifestFile(m NormalSystemSourceManifest, name, kind string) bool {
	if normalSystemAllowedVersionFile(m.ProtocolVersion, name, kind) {
		return true
	}
	if !normalSystemStockUdevSelected(m) {
		return false
	}
	expected, ok := normalSystemStockUdevFileKind(name)
	return ok && expected == kind
}

// 精确eudev split-usr构建的静态规则查找根；/run那一根由独立RuntimeDirs保护。
// alternate根只能是原已覆盖目录的同canonical alias，或原RO缺失/空目录；
// 此路径表不证明目录实际不存在、只读或没有新增规则。
func NormalSystemStockUdevStaticRuleDirectories() []string {
	return []string{"/etc/udev/rules.d", "/lib/udev/rules.d", "/usr/lib/udev/rules.d", "/usr/local/lib/udev/rules.d"}
}
