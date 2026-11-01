//go:build linux

package resourceguard

import (
	"bytes"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type normalOrdinaryStockUdevProjection struct {
	Properties                   map[string]string
	MonitorBytes                 []byte
	DatabasePresent, Initialized bool
	DevlinkPriority              int32
	WatchHandle                  int32
}

// 纯DB→属性投影：不读运行期文件/60004可写目录，不产Root/FS/票据/Source。
// kernel必须由Root原kernel/sysfs producer给出；普通map或DB字节本身不能认证它。
// 官方3.2.14 device.c597/1357/1864与device-private.c104为本有限合同来源。
func projectNormalOrdinaryStockUdevDB(kernel map[string]string, db []byte, present bool) (normalOrdinaryStockUdevProjection, error) {
	var zero normalOrdinaryStockUdevProjection
	fail := func(e error) (normalOrdinaryStockUdevProjection, error) { return zero, errors.Join(ErrUnknown, e) }
	if len(kernel) == 0 || len(kernel) > 126 || len(db) > 64<<10 || !present && len(db) != 0 {
		return fail(fmt.Errorf("stock udev projection input bound differs"))
	}
	props := map[string]string{}
	for k, v := range kernel {
		if !normalOrdinaryStockUdevKernelKey(k) || !normalOrdinaryStockUdevScalar(v) || len(v) > 4095 {
			return fail(fmt.Errorf("stock udev original kernel property unsupported"))
		}
		props[k] = v
	}
	if !normalOrdinaryStockUdevAbsolute(props["DEVPATH"], "") || !normalOrdinaryStockUdevWord(props["SUBSYSTEM"]) {
		return fail(fmt.Errorf("stock udev original path/subsystem differs"))
	}
	if value, ok := props["DEVNAME"]; ok {
		if !strings.HasPrefix(value, "/") {
			value = "/dev/" + value
		}
		if !normalOrdinaryStockUdevAbsolute(value, "/dev/") {
			return fail(fmt.Errorf("stock udev devname escapes fixed namespace"))
		}
		props["DEVNAME"] = value
	}
	major, minor := uint64(0), uint64(0)
	_, hasMajor := props["MAJOR"]
	_, hasMinor := props["MINOR"]
	if hasMajor != hasMinor {
		return fail(fmt.Errorf("stock udev incomplete devnum"))
	}
	if hasMajor {
		var e error
		major, e = normalOrdinaryStockUdevUnsigned(props["MAJOR"], 32)
		if e != nil {
			return fail(e)
		}
		minor, e = normalOrdinaryStockUdevUnsigned(props["MINOR"], 32)
		if e != nil {
			return fail(e)
		}
	}
	_ = minor
	ifindex := uint64(0)
	if value, yes := props["IFINDEX"]; yes {
		var e error
		ifindex, e = normalOrdinaryStockUdevUnsigned(value, 31)
		if e != nil || ifindex == 0 {
			return fail(fmt.Errorf("stock udev invalid original ifindex"))
		}
	}
	if !present && (major > 0 || ifindex > 0) {
		return fail(fmt.Errorf("stock udev device with devnum/ifindex lacks database"))
	}
	out := normalOrdinaryStockUdevProjection{Properties: props, DatabasePresent: present, Initialized: present, WatchHandle: -1}
	var links, tags []string
	seen := map[string]bool{}
	if len(db) > 0 {
		if db[len(db)-1] != '\n' || bytes.IndexByte(db, 0) >= 0 {
			return fail(fmt.Errorf("stock udev DB is truncated or contains NUL"))
		}
		for _, line := range strings.Split(string(db[:len(db)-1]), "\n") {
			if len(line) < 3 || len(line) >= 16384 || line[1] != ':' {
				return fail(fmt.Errorf("stock udev unsupported DB record"))
			}
			kind, value := line[0], line[2:]
			if !normalOrdinaryStockUdevScalar(value) {
				return fail(fmt.Errorf("stock udev DB control character"))
			}
			key := string(kind)
			switch kind {
			case 'S':
				if major == 0 || !normalOrdinaryStockUdevRelative(value) {
					return fail(fmt.Errorf("stock udev devlink requires devnum/canonical relative dev path"))
				}
				key += value
				links = append(links, "/dev/"+value)
			case 'G':
				if !normalOrdinaryStockUdevWord(value) {
					return fail(fmt.Errorf("stock udev tag differs"))
				}
				key += value
				tags = append(tags, value)
			case 'E':
				name, v, ok := strings.Cut(value, "=")
				if !ok || !normalOrdinaryStockUdevDBKey(name) || v == "" || len(v) > 4095 {
					return fail(fmt.Errorf("stock udev DB property unsupported"))
				}
				if _, conflict := props[name]; conflict {
					return fail(fmt.Errorf("stock udev DB cannot override original property"))
				}
				key += name
				props[name] = v
			case 'I':
				n, e := normalOrdinaryStockUdevUnsigned(value, 64)
				if e != nil || n == 0 {
					return fail(fmt.Errorf("stock udev initialized time noncanonical"))
				}
				props["USEC_INITIALIZED"] = value
			case 'L', 'W':
				if major == 0 {
					return fail(fmt.Errorf("stock udev link/watch internal value requires devnum"))
				}
				n, e := strconv.ParseInt(value, 10, 32)
				if e != nil || strconv.FormatInt(n, 10) != value || kind == 'W' && n < 0 {
					return fail(fmt.Errorf("stock udev link/watch integer noncanonical"))
				}
				if kind == 'L' {
					out.DevlinkPriority = int32(n)
				} else {
					out.WatchHandle = int32(n)
				}
			default:
				return fail(fmt.Errorf("stock udev unknown DB record"))
			}
			if seen[key] {
				return fail(fmt.Errorf("stock udev duplicate/conflicting DB record"))
			}
			seen[key] = true
		}
	}
	// 原libudev unique list按name排序；DEVLINKS与TAGS不是原S/G行串或伪造E。
	sort.Strings(links)
	sort.Strings(tags)
	if len(links) > 0 {
		value := strings.Join(links, " ")
		if len(value) >= 1024 {
			return fail(fmt.Errorf("stock udev DEVLINKS would truncate original 1024 buffer"))
		}
		props["DEVLINKS"] = value
	}
	if len(tags) > 0 {
		value := ":" + strings.Join(tags, ":") + ":"
		if len(value) >= 1024 {
			return fail(fmt.Errorf("stock udev TAGS would truncate original 1024 buffer"))
		}
		props["TAGS"] = value
	}
	names := make([]string, 0, len(props))
	for name := range props {
		if !strings.HasPrefix(name, ".") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	// 官方envp128槽在i++后检查i+1>=128，所以最多126属性，而非127。
	if len(names) > 126 {
		return fail(fmt.Errorf("stock udev monitor envp bound"))
	}
	var b bytes.Buffer
	for _, name := range names {
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(props[name])
		b.WriteByte(0)
	}
	if b.Len() > 4096 {
		return fail(fmt.Errorf("stock udev monitor buffer bound"))
	}
	out.MonitorBytes = append([]byte(nil), b.Bytes()...)
	return out, nil
}
func normalOrdinaryStockUdevScalar(v string) bool {
	if !utf8.ValidString(v) {
		return false
	}
	for _, r := range v {
		if unicode.IsControl(r) || r == '\u2028' || r == '\u2029' {
			return false
		}
	}
	return true
}
func normalOrdinaryStockUdevWord(v string) bool {
	if len(v) == 0 || len(v) > 64 {
		return false
	}
	for _, c := range v {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func normalOrdinaryStockUdevRelative(v string) bool {
	if len(v) == 0 || len(v) > 1023 || strings.HasPrefix(v, "/") || path.Clean(v) != v || v == "." || v == ".." || strings.HasPrefix(v, "../") || strings.ContainsAny(v, " \t\r\n\x00") {
		return false
	}
	return normalOrdinaryStockUdevScalar(v)
}
func normalOrdinaryStockUdevAbsolute(v, prefix string) bool {
	return strings.HasPrefix(v, "/") && v != "/" && path.Clean(v) == v && len(v) <= 1023 && !strings.ContainsAny(v, " \t\r\n\x00") && normalOrdinaryStockUdevScalar(v) && (prefix == "" || strings.HasPrefix(v, prefix))
}
func normalOrdinaryStockUdevUnsigned(v string, bits int) (uint64, error) {
	n, e := strconv.ParseUint(v, 10, bits)
	if e != nil || strconv.FormatUint(n, 10) != v {
		return 0, ErrUnknown
	}
	return n, nil
}
func normalOrdinaryStockUdevKernelKey(name string) bool {
	switch name {
	case "ACTION", "DEVPATH", "SUBSYSTEM", "DEVNAME", "DEVTYPE", "MAJOR", "MINOR", "DRIVER", "MODALIAS", "SEQNUM", "IFINDEX", "INTERFACE", "DEVPATH_OLD", "INTERFACE_OLD", "SYNTH_UUID", "DISKSEQ", "DEVUID", "DEVGID", "DEVMODE", "PRODUCT", "TYPE", "PHYSDEVPATH", "PHYSDEVBUS", "PHYSDEVDRIVER", "PCI_CLASS", "PCI_ID", "PCI_SUBSYS_ID", "PCI_SLOT_NAME", "DMI_MODALIAS", "HID_ID", "HID_NAME", "HID_PHYS", "HID_UNIQ", "NAME", "SERIO_TYPE", "SERIO_PROTO", "SERIO_ID", "SERIO_EXTRA", "OF_NAME", "OF_FULLNAME", "OF_COMPATIBLE_0", "OF_COMPATIBLE_N", "PHYS_INDEX", "MEMORY_BLOCK_SIZE_BYTES", "NUMA_NODE":
		return true
	}
	return false
}
func normalOrdinaryStockUdevDMIProperty(name string) bool {
	if name == "MEMORY_ARRAY_MAX_CAPACITY" {
		return true
	}
	const prefix = "MEMORY_DEVICE_"
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	index, suffix, ok := strings.Cut(strings.TrimPrefix(name, prefix), "_")
	if !ok {
		return false
	}
	if _, e := normalOrdinaryStockUdevUnsigned(index, 32); e != nil {
		return false
	}
	// 来自官方dmi_memory_id.c:type17所有格式化后缀。这里只解析unsigned索引
	// 的元数据语法，不用此数值认领memory block/原Geometry或任何未来写权限。
	switch suffix {
	case "TOTAL_WIDTH", "DATA_WIDTH", "SIZE", "PRESENT", "FORM_FACTOR", "SET", "LOCATOR", "BANK_LOCATOR", "TYPE", "TYPE_DETAIL", "SPEED_MTS", "MANUFACTURER", "SERIAL_NUMBER", "ASSET_TAG", "PART_NUMBER", "RANK", "CONFIGURED_SPEED_MTS", "MINIMUM_VOLTAGE", "MAXIMUM_VOLTAGE", "CONFIGURED_VOLTAGE", "MEMORY_TECHNOLOGY", "MEMORY_OPERATING_MODE_CAPABILITY", "FIRMWARE_VERSION", "MODULE_MANUFACTURER_ID", "MODULE_PRODUCT_ID", "MEMORY_SUBSYSTEM_CONTROLLER_MANUFACTURER_ID", "MEMORY_SUBSYSTEM_CONTROLLER_PRODUCT_ID", "NON_VOLATILE_SIZE", "VOLATILE_SIZE", "CACHE_SIZE", "LOGICAL_SIZE":
		return true
	}
	return false
}

func normalOrdinaryStockUdevDBKey(name string) bool {
	if normalOrdinaryStockUdevDMIProperty(name) {
		return true
	}
	// 精确官方字面/原27规则名单；没有HasPrefix("ID_")放行。
	switch name {
	case ".BSG_DEV", ".ID_PORT", ".INPUT_CLASS", "DISK_EJECT_REQUEST", "DISK_MEDIA_CHANGE", "ID_ATA", "ID_ATA_CFA", "ID_ATA_DOWNLOAD_MICROCODE", "ID_ATA_FEATURE_SET_AAM", "ID_ATA_FEATURE_SET_AAM_CURRENT_VALUE", "ID_ATA_FEATURE_SET_AAM_ENABLED", "ID_ATA_FEATURE_SET_AAM_VENDOR_RECOMMENDED_VALUE", "ID_ATA_FEATURE_SET_APM", "ID_ATA_FEATURE_SET_APM_CURRENT_VALUE", "ID_ATA_FEATURE_SET_APM_ENABLED", "ID_ATA_FEATURE_SET_HPA", "ID_ATA_FEATURE_SET_HPA_ENABLED", "ID_ATA_FEATURE_SET_PM", "ID_ATA_FEATURE_SET_PM_ENABLED", "ID_ATA_FEATURE_SET_PUIS", "ID_ATA_FEATURE_SET_PUIS_ENABLED", "ID_ATA_FEATURE_SET_SECURITY", "ID_ATA_FEATURE_SET_SECURITY_ENABLED", "ID_ATA_FEATURE_SET_SECURITY_ENHANCED_ERASE_UNIT_MIN", "ID_ATA_FEATURE_SET_SECURITY_ERASE_UNIT_MIN", "ID_ATA_FEATURE_SET_SECURITY_EXPIRE", "ID_ATA_FEATURE_SET_SECURITY_FROZEN", "ID_ATA_FEATURE_SET_SECURITY_LEVEL", "ID_ATA_FEATURE_SET_SECURITY_LOCKED", "ID_ATA_FEATURE_SET_SMART", "ID_ATA_FEATURE_SET_SMART_ENABLED", "ID_ATA_ROTATION_RATE_RPM", "ID_ATA_SATA", "ID_ATA_SATA_SIGNAL_RATE_GEN1", "ID_ATA_SATA_SIGNAL_RATE_GEN2", "ID_ATA_WRITE_CACHE", "ID_ATA_WRITE_CACHE_ENABLED", "ID_AUTOSUSPEND", "ID_BTRFS_READY", "ID_BUS", "ID_CDROM", "ID_CDROM_BD", "ID_CDROM_BD_R", "ID_CDROM_BD_RE", "ID_CDROM_CD", "ID_CDROM_CD_R", "ID_CDROM_CD_RW", "ID_CDROM_DVD", "ID_CDROM_DVD_PLUS_R", "ID_CDROM_DVD_PLUS_RW", "ID_CDROM_DVD_PLUS_RW_DL", "ID_CDROM_DVD_PLUS_R_DL", "ID_CDROM_DVD_R", "ID_CDROM_DVD_RAM", "ID_CDROM_DVD_RW", "ID_CDROM_HDDVD", "ID_CDROM_HDDVD_R", "ID_CDROM_HDDVD_RW", "ID_CDROM_MEDIA", "ID_CDROM_MEDIA_BD", "ID_CDROM_MEDIA_BD_R", "ID_CDROM_MEDIA_BD_RE", "ID_CDROM_MEDIA_CD", "ID_CDROM_MEDIA_CD_R", "ID_CDROM_MEDIA_CD_RW", "ID_CDROM_MEDIA_DVD", "ID_CDROM_MEDIA_DVD_PLUS_R", "ID_CDROM_MEDIA_DVD_PLUS_RW", "ID_CDROM_MEDIA_DVD_PLUS_RW_DL", "ID_CDROM_MEDIA_DVD_PLUS_R_DL", "ID_CDROM_MEDIA_DVD_R", "ID_CDROM_MEDIA_DVD_RAM", "ID_CDROM_MEDIA_DVD_RW", "ID_CDROM_MEDIA_HDDVD", "ID_CDROM_MEDIA_HDDVD_R", "ID_CDROM_MEDIA_HDDVD_RW", "ID_CDROM_MEDIA_MO", "ID_CDROM_MEDIA_MRW", "ID_CDROM_MEDIA_MRW_W", "ID_CDROM_MEDIA_SESSION_COUNT", "ID_CDROM_MEDIA_SESSION_LAST_OFFSET", "ID_CDROM_MEDIA_SESSION_NEXT", "ID_CDROM_MEDIA_STATE", "ID_CDROM_MEDIA_TRACK_COUNT", "ID_CDROM_MEDIA_TRACK_COUNT_AUDIO", "ID_CDROM_MEDIA_TRACK_COUNT_DATA", "ID_CDROM_MO", "ID_CDROM_MRW", "ID_CDROM_MRW_W", "ID_FS_APPLICATION_ID", "ID_FS_BOOT_SYSTEM_ID", "ID_FS_LABEL", "ID_FS_LABEL_ENC", "ID_FS_PUBLISHER_ID", "ID_FS_SYSTEM_ID", "ID_FS_TYPE", "ID_FS_USAGE", "ID_FS_UUID", "ID_FS_UUID_ENC", "ID_FS_UUID_SUB", "ID_FS_UUID_SUB_ENC", "ID_FS_VERSION", "ID_ID", "ID_INPUT", "ID_INPUT_ACCELEROMETER", "ID_INPUT_HEIGHT_MM", "ID_INPUT_JOYSTICK", "ID_INPUT_KEY", "ID_INPUT_KEYBOARD", "ID_INPUT_MOUSE", "ID_INPUT_POINTINGSTICK", "ID_INPUT_SWITCH", "ID_INPUT_TABLET", "ID_INPUT_TOUCHPAD", "ID_INPUT_TOUCHSCREEN", "ID_INPUT_WIDTH_MM", "ID_INSTANCE", "ID_MODEL", "ID_MODEL_ENC", "ID_MODEL_FROM_DATABASE", "ID_MODEL_ID", "ID_NAME", "ID_NET_DHCP_BROADCAST", "ID_NET_DRIVER", "ID_NET_LABEL_ONBOARD", "ID_NET_NAME_MAC", "ID_NET_NAME_ONBOARD", "ID_NET_NAME_PATH", "ID_NET_NAME_SLOT", "ID_PART_ENTRY_DISK", "ID_PART_ENTRY_FLAGS", "ID_PART_ENTRY_NAME", "ID_PART_ENTRY_NUMBER", "ID_PART_ENTRY_OFFSET", "ID_PART_ENTRY_SCHEME", "ID_PART_ENTRY_SIZE", "ID_PART_ENTRY_TYPE", "ID_PART_ENTRY_UUID", "ID_PART_TABLE_TYPE", "ID_PART_TABLE_UUID", "ID_PATH", "ID_PATH_ATA_COMPAT", "ID_PATH_TAG", "ID_PERSIST", "ID_REVISION", "ID_SCSI", "ID_SCSI_SERIAL", "ID_SECURITY_TOKEN", "ID_SERIAL", "ID_SERIAL_SHORT", "ID_SMARTCARD_READER", "ID_SYSFS_ATTRIBUTE_MODEL", "ID_TARGET_PORT", "ID_TYPE", "ID_USB_DRIVER", "ID_USB_INTERFACES", "ID_USB_INTERFACE_NUM", "ID_VENDOR", "ID_VENDOR_ENC", "ID_VENDOR_FROM_DATABASE", "ID_VENDOR_ID", "ID_WWN", "ID_WWN_VENDOR_EXTENSION", "ID_WWN_WITH_EXTENSION", "IEEE1394_UNIT_FUNCTION_AUDIO", "IEEE1394_UNIT_FUNCTION_MIDI", "IEEE1394_UNIT_FUNCTION_VIDEO", "MEMORY_ARRAY_EC_TYPE", "MEMORY_ARRAY_LOCATION", "MEMORY_ARRAY_NUM_DEVICES", "MTD_FTL", "SOUND_CLASS", "SOUND_FORM_FACTOR", "SOUND_INITIALIZED", "SYSTEMD_MOUNT_DEVICE_BOUND", "SYSTEMD_READY", "TIFM_CARD_TYPE", "UDEV_DISABLE_PERSISTENT_STORAGE_RULES_FLAG":
		return true
	}
	return false
}
