//go:build linux

package resourceguard

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

// 固定真实proc/sysfs读取，不接受请求路径，不写sysctl/设备/online/WAL。
// rootFile检查所有父目录和O_NOFOLLOW；这里再核打开文件的真实伪文件系统。
func readGuestMaximumFile(path string, fsType int64) (string, error) {
	f, err := rootFile(path, false)
	if err != nil {
		return "", err
	}
	defer f.Close()
	var fs unix.Statfs_t
	if err = unix.Fstatfs(int(f.Fd()), &fs); err != nil {
		return "", err
	}
	if int64(fs.Type) != fsType {
		return "", fmt.Errorf("Guest maximum source filesystem differs")
	}
	raw, err := io.ReadAll(io.LimitReader(f, 256*1024+1))
	if err != nil {
		return "", err
	}
	if len(raw) == 0 || len(raw) > 256*1024 {
		return "", fmt.Errorf("Guest maximum source size invalid")
	}
	return string(raw), nil
}

func guestMaximumDirectory(path string) (*os.File, unix.Stat_t, error) {
	var st unix.Stat_t
	if err := secureParents(filepath.Dir(path)); err != nil {
		return nil, st, err
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, st, err
	}
	f := os.NewFile(uintptr(fd), path)
	if err = unix.Fstat(fd, &st); err != nil {
		f.Close()
		return nil, st, err
	}
	var fs unix.Statfs_t
	if err = unix.Fstatfs(fd, &fs); err != nil {
		f.Close()
		return nil, st, err
	}
	if st.Uid != 0 || st.Mode&0022 != 0 || st.Ino == 0 || fs.Type != unix.SYSFS_MAGIC {
		f.Close()
		return nil, st, fmt.Errorf("Guest virtio source is not root-controlled sysfs")
	}
	return f, st, nil
}

// 这里只观察当前Guest，不认证原Boot/原Host的来源。expectedBoot必须由后继原
// Source消费者提供；独立测试用真实boot构造fixture，不能当生产原身份。
// 准确369构建没有新能力补签，也没有从当前online量反推上限。
func readGuestMemoryMaximumComponent(expectedBoot string) (guestMemoryMaximumObservation, error) {
	var zero guestMemoryMaximumObservation
	if os.Geteuid() != 0 || runtime.GOARCH != "amd64" || os.Getpagesize() != 4096 || !validGuestBoot(expectedBoot) {
		return zero, fmt.Errorf("Guest maximum reader requires isolated supported root/boot")
	}
	ref, err := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if err != nil {
		return zero, err
	}
	read := func(path string, fs int64) (string, error) {
		s, e := readGuestMaximumFile(path, fs)
		return strings.TrimSpace(s), e
	}
	release, err := read("/proc/sys/kernel/osrelease", unix.PROC_SUPER_MAGIC)
	if err != nil || release != ref.KernelRelease {
		return zero, fmt.Errorf("Guest maximum reviewed kernel release differs: %v", err)
	}
	boot, err := read("/proc/sys/kernel/random/boot_id", unix.PROC_SUPER_MAGIC)
	if err != nil || boot != expectedBoot {
		return zero, fmt.Errorf("Guest maximum boot differs: %v", err)
	}
	notes, err := readGuestMaximumFile("/sys/kernel/notes", unix.SYSFS_MAGIC)
	if err != nil || physicalDigest([]byte(notes)) != ref.KernelNotesSHA256 {
		return zero, fmt.Errorf("Guest maximum running kernel notes differ: %v", err)
	}
	entries, err := os.ReadDir("/sys/bus/virtio/devices")
	if err != nil {
		return zero, err
	}
	if len(entries) > 1024 {
		return zero, fmt.Errorf("too many Guest virtio devices")
	}
	var observation guestMemoryMaximumObservation
	for _, entry := range entries {
		name := entry.Name()
		if !guestMaximumDeviceName.MatchString(name) || len(name) > 32 {
			return zero, fmt.Errorf("unexpected Guest virtio device name")
		}
		canonical, err := filepath.EvalSymlinks("/sys/bus/virtio/devices/" + name)
		if err != nil || !guestMaximumCanonicalPath(name, canonical) {
			return zero, fmt.Errorf("Guest virtio canonical path differs: %v", err)
		}
		dir, st, err := guestMaximumDirectory(canonical)
		if err != nil {
			return zero, err
		}
		device, err := read(canonical+"/device", unix.SYSFS_MAGIC)
		if err != nil {
			dir.Close()
			return zero, err
		}
		if device != "0x0018" {
			dir.Close()
			continue
		}
		if observation.DeviceName != "" {
			dir.Close()
			return zero, fmt.Errorf("multiple Guest virtio memory devices")
		}
		vendor, e1 := read(canonical+"/vendor", unix.SYSFS_MAGIC)
		status, e2 := read(canonical+"/status", unix.SYSFS_MAGIC)
		driver, e3 := filepath.EvalSymlinks(canonical + "/driver")
		if e1 != nil || e2 != nil || e3 != nil || vendor != "0x1af4" || status != "0x0000000f" || driver != "/sys/bus/virtio/drivers/virtio_mem" {
			dir.Close()
			return zero, fmt.Errorf("Guest virtio memory device not bound to supported live driver")
		}
		iomem, err := readGuestMaximumFile("/proc/iomem", unix.PROC_SUPER_MAGIC)
		if err != nil {
			dir.Close()
			return zero, err
		}
		addr, size, err := parseGuestMaximumIOMem(iomem, name)
		if err != nil {
			dir.Close()
			return zero, err
		}
		// 同一个目录FD留存到事实读取后，重核路径与FD身份，拒绝明显解绑/换设备。
		// 这不是阻止未来Root/Host写者的能力；原生命周期封存仍是外层必需条件。
		canonical2, e := filepath.EvalSymlinks("/sys/bus/virtio/devices/" + name)
		var after unix.Stat_t
		err = unix.Stat(canonical, &after)
		driver2, de := filepath.EvalSymlinks(canonical + "/driver")
		dir.Close()
		if e != nil || err != nil || de != nil || canonical2 != canonical || st.Dev != after.Dev || st.Ino != after.Ino || driver2 != driver {
			return zero, fmt.Errorf("Guest virtio device changed while reading")
		}
		observation = guestMemoryMaximumObservation{ProtocolVersion: 1, Scope: guestMemoryMaximumScope, Build: ref, GuestBootID: boot, DeviceName: name, CanonicalPath: canonical, SysfsDevice: control.Uint64(st.Dev), SysfsInode: control.Uint64(st.Ino), DeviceID: 24, VendorID: 0x1af4, DriverPath: driver, Address: control.Uint64(addr), RegionBytes: control.Uint64(size), NotIndependentlyObserved: [2]string{"device_block_size", "device_nid"}}
	}
	if observation.DeviceName == "" {
		return zero, fmt.Errorf("Guest virtio memory device absent")
	}
	// 跨整次枚举再核boot/notes；外层原资源锁和受信启动拓扑仍须独立接线。
	boot2, e := read("/proc/sys/kernel/random/boot_id", unix.PROC_SUPER_MAGIC)
	notes2, ne := readGuestMaximumFile("/sys/kernel/notes", unix.SYSFS_MAGIC)
	if e != nil || ne != nil || boot2 != boot || physicalDigest([]byte(notes2)) != ref.KernelNotesSHA256 {
		return zero, fmt.Errorf("Guest maximum kernel identity changed")
	}
	iomem2, ie := readGuestMaximumFile("/proc/iomem", unix.PROC_SUPER_MAGIC)
	addr2, size2, pe := parseGuestMaximumIOMem(iomem2, observation.DeviceName)
	if ie != nil || pe != nil || addr2 != uint64(observation.Address) || size2 != uint64(observation.RegionBytes) {
		return zero, fmt.Errorf("Guest immutable resource changed while reading")
	}
	return observation, observation.validate()
}
