//go:build linux

package resourceguard

import (
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
	"strings"
)

// 没有 mount/sysctl/cgroup 写入。securityfs 须由原可信 provision 在首次 Origin 前
// 准备；只读观察不自动安装能力、不追补原历史。全部错误保留旧 WAL/来源。
func readPhysicalKernelRuntimeCapabilities() (PhysicalKernelRuntimeCapabilities, error) {
	var c PhysicalKernelRuntimeCapabilities
	text, err := kernelText("/sys/kernel/security/lsm")
	if err != nil {
		return c, err
	}
	c.ActiveLSMs = strings.Split(text, ",")
	abi, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, 0, 0, unix.LANDLOCK_CREATE_RULESET_VERSION)
	if errno != 0 {
		return c, fmt.Errorf("read actual Landlock ABI: %w", errno)
	}
	if abi != 6 {
		return c, fmt.Errorf("actual Landlock ABI is not the reviewed kernel ABI")
	}
	c.LandlockABI = uint32(abi)
	// 固定 procfs namespace 句柄是内核 magic-link：有意跟随后 fstat 验 nsfs。
	// 不接请求路径；current 与 PID1 都必须是准确 initial PID namespace，不能将
	// 容器视图当全 Guest pid_max 源。这仍不证明未来 Root 无 namespace 写路径。
	var namespace unix.Stat_t
	for i, path := range []string{"/proc/self/ns/pid", "/proc/1/ns/pid"} {
		fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if err != nil {
			return c, err
		}
		var st unix.Stat_t
		var fs unix.Statfs_t
		statErr := unix.Fstat(fd, &st)
		fsErr := unix.Fstatfs(fd, &fs)
		closeErr := unix.Close(fd)
		if statErr != nil || fsErr != nil || closeErr != nil || fs.Type != unix.NSFS_MAGIC || st.Ino != 0xEFFFFFFC {
			return c, fmt.Errorf("physical source is not exact initial PID namespace")
		}
		if i != 0 && (st.Dev != namespace.Dev || st.Ino != namespace.Ino) {
			return c, fmt.Errorf("procfs and current PID namespace differ")
		}
		namespace = st
	}
	c.InitialPIDNamespaceDevice, c.InitialPIDNamespaceInode = control.Uint64(namespace.Dev), control.Uint64(namespace.Ino)
	c.MemoryHigh, err = kernelText(pgCgroup + "/memory.high")
	if err != nil {
		return c, err
	}
	values := []struct {
		path string
		out  *uint32
	}{
		{"/proc/sys/vm/watermark_scale_factor", &c.WatermarkScaleFactor},
		{"/proc/sys/vm/watermark_boost_factor", &c.WatermarkBoostFactor},
		{"/proc/sys/vm/percpu_pagelist_high_fraction", &c.PerCPUPagelistHighFraction},
		{"/proc/sys/vm/compaction_proactiveness", &c.CompactionProactiveness},
	}
	minFree, err := readNumber("/proc/sys/vm/min_free_kbytes", false)
	if err != nil {
		return c, err
	}
	c.MinFreeKBytes = control.Uint64(minFree)
	for _, value := range values {
		n, err := readNumber(value.path, false)
		if err != nil {
			return c, err
		}
		if n > 0x7fffffff {
			return c, fmt.Errorf("allocator source exceeds reviewed integer range")
		}
		*value.out = uint32(n)
	}
	return c, c.Validate()
}
