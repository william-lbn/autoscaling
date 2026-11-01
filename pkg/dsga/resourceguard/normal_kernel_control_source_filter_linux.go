//go:build linux && amd64

package resourceguard

import (
	"fmt"
	"golang.org/x/sys/unix"
	"sort"
)

// 完整固定BPF由Root在真实exec-stop逐指令读取；没有自报hash或请求中的允许标志。
// cap/Landlock覆盖文件写；此表封闭绕过路径的内存迁移/新namespace/mount/注入入口。
func normalKernelControlDeniedSyscalls() []uint32 {
	a := []uint32{
		unix.SYS_MIGRATE_PAGES, unix.SYS_MOVE_PAGES, unix.SYS_MBIND, unix.SYS_SET_MEMPOLICY, 450, // set_mempolicy_home_node
		unix.SYS_SWAPON, unix.SYS_SWAPOFF, 156, // obsolete _sysctl remains explicitly denied
		unix.SYS_CLONE3,
		unix.SYS_MOUNT, unix.SYS_UMOUNT2, unix.SYS_PIVOT_ROOT, unix.SYS_CHROOT, unix.SYS_UNSHARE, unix.SYS_SETNS,
		428, 429, 430, 431, 432, 433, 442, // open_tree/move_mount/fsopen/fsconfig/fsmount/fspick/mount_setattr
		unix.SYS_BPF, unix.SYS_PERF_EVENT_OPEN, unix.SYS_USERFAULTFD,
		unix.SYS_PTRACE, unix.SYS_PROCESS_VM_WRITEV, unix.SYS_PROCESS_MADVISE,
		unix.SYS_INIT_MODULE, unix.SYS_FINIT_MODULE, unix.SYS_DELETE_MODULE,
		unix.SYS_KEXEC_LOAD, unix.SYS_KEXEC_FILE_LOAD, unix.SYS_IOPL, unix.SYS_IOPERM,
		unix.SYS_IO_URING_SETUP, unix.SYS_IO_URING_ENTER, unix.SYS_IO_URING_REGISTER,
	}
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	out := a[:0]
	for _, nr := range a {
		if len(out) == 0 || out[len(out)-1] != nr {
			out = append(out, nr)
		}
	}
	return out
}
func normalKernelControlFilter() []unix.SockFilter {
	// x86-64 only；x32/其他arch直接kill，不能借另一系统调用号绕固定表。
	p := []unix.SockFilter{
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 4},
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.AUDIT_ARCH_X86_64, Jt: 1},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_KILL_PROCESS},
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0},
		{Code: unix.BPF_JMP | unix.BPF_JGE | unix.BPF_K, K: 0x40000000, Jf: 1},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_KILL_PROCESS},
	}
	// clone3的不可检查指针结构禁用并返ENOSYS，让既有libc走可检查flags的clone。
	for _, nr := range normalKernelControlDeniedSyscalls() {
		errno := uint32(unix.EPERM)
		if nr == unix.SYS_CLONE3 {
			errno = uint32(unix.ENOSYS)
		}
		p = append(p, unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: nr, Jf: 1}, unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | errno})
	}
	// 禁止clone直接创建新namespace，保普通fork和Go原线程创建。
	p = append(p,
		unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.SYS_CLONE, Jf: 6},
		unix.SockFilter{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 20},
		unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: 0, Jt: 1},
		unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)},
		unix.SockFilter{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 16},
		unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JSET | unix.BPF_K, K: 0x7e020000, Jf: 1},
		unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)})
	return append(p, unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW})
}
func normalKernelControlCheckFilter(program []unix.SockFilter) error {
	wanted := normalKernelControlFilter()
	if len(program) != len(wanted) {
		return fmt.Errorf("ordinary complete filter length differs")
	}
	for i := range wanted {
		if program[i] != wanted[i] {
			return fmt.Errorf("ordinary complete filter instruction differs")
		}
	}
	return nil
}
