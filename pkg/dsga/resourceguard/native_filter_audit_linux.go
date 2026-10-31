//go:build linux

package resourceguard

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

const ptraceSeccompGetFilter = 0x420c

// 这只是原Root listener在已核peer/原PID-start/原WAL且已ptrace-stop之后的读回组件。
// 不在此接受网络传入PID，不进行SEIZE/CONT/DETACH，不创建出生或normal许可。
// tracerTID必须是同一原SEIZE线程；锁定后再核，避免Go迁线程导致ESRCH或读错归属。
func readAndCompareNativeKernelFilters(pid, tracerTID int, bundle nativeFilterAuditBundle, phase nativeFilterPhase) error {
	if pid <= 1 || tracerTID <= 0 {
		return fmt.Errorf("original stopped native target missing")
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if unix.Gettid() != tracerTID {
		return fmt.Errorf("native tracer OS thread differs from original SEIZE owner")
	}
	if err := nativeFilterAuditCaller(); err != nil {
		return err
	}
	reader := func(offset uint32, dst []nativeFilterInstruction) (int, error) {
		if len(dst) > nativeAuditMaxInstructions {
			return 0, fmt.Errorf("native instruction destination too large")
		}
		// struct sock_filter在支持的amd64 ABI中为8B；用显式kernel类型，随后逐字段
		// 拷贝，不能把Go struct布局或native端hash当实际内核指令。
		actual := make([]unix.SockFilter, len(dst))
		var address *unix.SockFilter
		if len(actual) > 0 {
			address = &actual[0]
		}
		n, _, errno := unix.Syscall6(unix.SYS_PTRACE, ptraceSeccompGetFilter, uintptr(pid), uintptr(offset), uintptr(unsafe.Pointer(address)), 0, 0)
		runtime.KeepAlive(actual)
		if errno != 0 {
			return 0, errno
		}
		if n > nativeAuditMaxInstructions {
			return 0, fmt.Errorf("kernel native program exceeds bounded ABI")
		}
		if len(dst) > 0 {
			if int(n) != len(dst) {
				return 0, fmt.Errorf("kernel native program length changed")
			}
			for i, x := range actual {
				dst[i] = nativeFilterInstruction{x.Code, x.Jt, x.Jf, x.K}
			}
		}
		return int(n), nil
	}
	return compareNativeInstalledFilters(bundle, phase, reader, func(err error) bool { return errors.Is(err, unix.ENOENT) })
}

func nativeFilterAuditCaller() error {
	if runtime.GOARCH != "amd64" || os.Getuid() != 0 || os.Geteuid() != 0 {
		return fmt.Errorf("native filter audit requires real root on supported amd64")
	}
	mode, _, errno := unix.Syscall6(unix.SYS_PRCTL, unix.PR_GET_SECCOMP, 0, 0, 0, 0, 0)
	if errno != 0 || mode != 0 {
		return fmt.Errorf("native filter auditor itself must have seccomp disabled")
	}
	// 固定self kernel对象；使用当前tracer线程能力；不能用通用rootFile拒绝/proc/thread-self这个内核魔术链接，
	// 也不能从调用者传入的status文本补签。最终CAP/ptrace权限仍由真实内核再次强制。
	raw, err := os.ReadFile("/proc/thread-self/status")
	if err != nil {
		return err
	}
	var count int
	var caps uint64
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "CapEff:") {
			continue
		}
		count++
		fields := strings.Fields(line)
		if len(fields) != 2 || len(fields[1]) != 16 {
			return fmt.Errorf("invalid actual auditor effective capability source")
		}
		caps, err = strconv.ParseUint(fields[1], 16, 64)
		if err != nil {
			return err
		}
	}
	want := uint64(1)<<uint(unix.CAP_SYS_ADMIN) | uint64(1)<<uint(unix.CAP_SYS_PTRACE)
	if count != 1 || caps&want != want {
		return fmt.Errorf("native filter auditor lacks actual SYS_ADMIN/SYS_PTRACE")
	}
	return nil
}
