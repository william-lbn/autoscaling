package resourceguard

import (
	"bufio"
	"fmt"
	"math"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const guestMemoryMaximumScope = "guest-immutable-virtio-region-observation-only"

// 此私有结果只记Guest实际观察事实。region是probe时驱动保留的完整上限，
// 不是online/requested/plugged。没有全Guest最大值、原出生、Host保留或normal票据。
// block/node没有独立Guest接口，明确留为未观察；不从QMP请求补填伪值。
type guestMemoryMaximumObservation struct {
	ProtocolVersion          uint32                       `json:"protocol_version"`
	Scope                    string                       `json:"scope"`
	Build                    PhysicalKernelBuildReference `json:"reviewed_build_reference"`
	GuestBootID              string                       `json:"guest_boot_id"`
	DeviceName               string                       `json:"device_name"`
	CanonicalPath            string                       `json:"canonical_path"`
	SysfsDevice              control.Uint64               `json:"sysfs_device"`
	SysfsInode               control.Uint64               `json:"sysfs_inode"`
	DeviceID                 uint32                       `json:"device_id"`
	VendorID                 uint32                       `json:"vendor_id"`
	DriverPath               string                       `json:"driver_path"`
	Address                  control.Uint64               `json:"address"`
	RegionBytes              control.Uint64               `json:"maximum_region_bytes"`
	NotIndependentlyObserved [2]string                    `json:"not_independently_observed"`
	AllSourcesClosed         bool                         `json:"all_sources_closed"`
	NormalDecreaseAuthorized bool                         `json:"normal_decrease_authorized"`
}

var guestMaximumDeviceName = regexp.MustCompile(`^virtio(0|[1-9][0-9]*)$`)
var guestMaximumResourceLine = regexp.MustCompile(`^( *)([0-9a-f]{8,16})-([0-9a-f]{8,16}) : (.+)$`)

func guestMaximumCanonicalPath(name, canonical string) bool {
	return guestMaximumDeviceName.MatchString(name) && len(name) <= 32 &&
		strings.HasPrefix(canonical, "/sys/devices/") && path.Clean(canonical) == canonical &&
		path.Base(canonical) == name && !strings.ContainsAny(canonical, "\x00\r\n\t ")
}

func (o guestMemoryMaximumObservation) validate() error {
	ref, err := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if err != nil || o.Build != ref || o.ProtocolVersion != 1 || o.Scope != guestMemoryMaximumScope ||
		!validGuestBoot(o.GuestBootID) || !guestMaximumCanonicalPath(o.DeviceName, o.CanonicalPath) ||
		o.SysfsDevice == 0 || o.SysfsInode == 0 || o.DeviceID != 24 || o.VendorID != 0x1af4 ||
		o.DriverPath != "/sys/bus/virtio/drivers/virtio_mem" || o.Address == 0 || o.RegionBytes == 0 ||
		o.Address%4096 != 0 || o.RegionBytes%4096 != 0 || uint64(o.Address) > math.MaxUint64-uint64(o.RegionBytes) ||
		o.NotIndependentlyObserved != [2]string{"device_block_size", "device_nid"} ||
		o.AllSourcesClosed || o.NormalDecreaseAuthorized {
		return fmt.Errorf("invalid Guest immutable-region observation")
	}
	return nil
}

// 只比较原内核/boot/真实设备及上限。普通online子资源不入结果，不会将热插拔
// 的当前状态冒充最大值。相等不封闭未来特权设备写者，也不补签Origin历史。
func (o guestMemoryMaximumObservation) match(now guestMemoryMaximumObservation) error {
	if err := o.validate(); err != nil {
		return err
	}
	if err := now.validate(); err != nil {
		return err
	}
	if !reflect.DeepEqual(o, now) {
		return fmt.Errorf("Guest immutable-region source drifted")
	}
	return nil
}

// Linux6.12.26 virtio_mem_create_resource按驱动缓存addr/region_size保留整个
// iomem顶层资源，名称为真实virtio设备名；其下System RAM可随online改变。
// 必须唯一且未遮罩，拒绝同名嵌套/第二个virtio资源/错误几何；原文保留缩进。
func parseGuestMaximumIOMem(text, name string) (uint64, uint64, error) {
	fail := func() (uint64, uint64, error) {
		return 0, 0, fmt.Errorf("Guest virtio immutable iomem region missing or ambiguous")
	}
	if !guestMaximumDeviceName.MatchString(name) || len(name) > 32 || len(text) == 0 || len(text) > 256*1024 {
		return fail()
	}
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 4096), 256*1024)
	var address, size uint64
	found := false
	for scanner.Scan() {
		line := scanner.Text()
		m := guestMaximumResourceLine.FindStringSubmatch(line)
		if m == nil {
			return fail()
		}
		start, e1 := strconv.ParseUint(m[2], 16, 64)
		end, e2 := strconv.ParseUint(m[3], 16, 64)
		if e1 != nil || e2 != nil || end < start || len(m[1])%2 != 0 {
			return fail()
		}
		if !guestMaximumDeviceName.MatchString(m[4]) {
			continue
		}
		if m[4] != name || m[1] != "" || found || start == 0 || end-start == math.MaxUint64 {
			return fail()
		}
		address = start
		size = end - start + 1
		found = true
		if address%4096 != 0 || size == 0 || size%4096 != 0 || address > math.MaxUint64-size {
			return fail()
		}
	}
	if scanner.Err() != nil || !found {
		return fail()
	}
	return address, size, nil
}

// 仅为后继跨包实物互证提供私有算术比较；这些参数本身不是可信Host来源。
// 正式调用必须另绑定原Runner/UUID/QMP帧/原Origin与进程生命周期，尚未接线。
// base必须取实际QMP pc.ram/machine交叉值，绝不能用Kpresent替代base上界。
// 返回数字不产生持久Source、完整I、原出生或normal许可。
func compareGuestMaximumGeometry(o guestMemoryMaximumObservation, base, machineMaximum, deviceAddress, deviceMaximum uint64) (uint64, error) {
	if err := o.validate(); err != nil {
		return 0, err
	}
	if base == 0 || base%4096 != 0 || deviceAddress != uint64(o.Address) || deviceMaximum != uint64(o.RegionBytes) ||
		base > deviceAddress || base > math.MaxUint64-deviceMaximum || base+deviceMaximum != machineMaximum {
		return 0, fmt.Errorf("Host/Guest maximum-region geometry differs or overflows")
	}
	return machineMaximum, nil
}
