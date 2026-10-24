//go:build linux

package resourceguard

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"runtime"
	"strconv"
	"syscall"

	"golang.org/x/sys/unix"
)

// ExecPostgres是受限SUID入口：只加入固定组并降权exec固定PG文件。非root真实
// UID绝不能进入LoadConfig/Open及任何资源写动作；不采用sudo父进程，以保持
// Compute Child PID、信号、wait状态与真正Postmaster一致。实际安装需root拥有、
// 不可由postgres修改的静态CGO_ENABLED=0程序，且目标文件系统允许SUID。
func ExecPostgres(args []string) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("managed PG launcher requires effective root")
	}
	account, err := user.Lookup("postgres")
	if err != nil {
		return fmt.Errorf("trusted postgres account unavailable")
	}
	uid, err := strconv.Atoi(account.Uid)
	if err != nil || uid <= 0 {
		return fmt.Errorf("invalid postgres UID")
	}
	gid, err := strconv.Atoi(account.Gid)
	if err != nil || gid <= 0 {
		return fmt.Errorf("invalid postgres GID")
	}
	if os.Getuid() != 0 && os.Getuid() != uid {
		return fmt.Errorf("PG birth caller is not postgres or root")
	}
	if len(args) == 0 || len(args) > 4096 {
		return fmt.Errorf("invalid PG argument count")
	}
	bytes := 0
	for _, arg := range args {
		bytes += len(arg) + 1
	}
	if bytes > 256*1024 {
		return fmt.Errorf("PG arguments exceed bound")
	}
	// 固定root配置，不接受调用者指定--config或环境覆盖。解析发生在降权前，但
	// 只含本VM资源身份与状态目录；不执行配置文本、用户程序或可选shell。
	source, err := sealedRuntimeSource()
	if err != nil {
		return err
	}
	config := source.Config
	if err := secureParents(config.StateDir); err != nil {
		return err
	}
	g := &Guard{config: config, kernel: &linuxKernel{config: config}}
	return g.launchWithSource(context.Background(), &source, func() error {
		// prctl能力/NO_NEW_PRIVS属于线程属性，降权与exec之间固定当前OS线程；
		// Setres*仍覆盖全部线程，不能只依赖LockOSThread来完成进程整体降权。
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		if err := writeNumber(pgCgroup+"/cgroup.procs", uint64(os.Getpid())); err != nil {
			return err
		}
		membership, err := os.ReadFile("/proc/" + strconv.Itoa(os.Getpid()) + "/cgroup")
		if err != nil || string(membership) != "0::/neon-postgres\n" {
			return fmt.Errorf("launcher failed to enter exact PG cgroup")
		}
		// 不继承可保留root能力的开关；降组→降UID覆盖所有Go OS线程，再确认真实/
		// 有效身份。用标准库Set*，其Linux实现对纯Go采用AllThreadsSyscall，不能
		// 换成只改当前线程的RawSyscall；否则另一线程仍可能带root身份执行exec。
		if err := unix.Prctl(unix.PR_SET_KEEPCAPS, 0, 0, 0, 0); err != nil {
			return err
		}
		if err := unix.Prctl(unix.PR_CAP_AMBIENT, unix.PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0); err != nil {
			return err
		}
		if err := syscall.Setgroups([]int{gid}); err != nil {
			return err
		}
		if err := syscall.Setresgid(gid, gid, gid); err != nil {
			return err
		}
		if err := syscall.Setresuid(uid, uid, uid); err != nil {
			return err
		}
		if os.Getuid() != uid || os.Geteuid() != uid || os.Getgid() != gid || os.Getegid() != gid {
			return fmt.Errorf("PG privilege drop did not complete")
		}
		if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
			return err
		}
		// 降权完成后才解析/执行PG路径（官方路径可以是root提供的版本符号链接），
		// PG参数与存储token环境仅交给postgres身份。不打印args/env，防止泄漏token。
		argv := append([]string{FixedPostgresPath}, args...)
		return syscall.Exec(FixedPostgresPath, argv, os.Environ())
	})
}
