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

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
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
	server, err := launchInvocation(args)
	if err != nil {
		return err
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
	var birth *ProcessIdentity
	if server {
		id, err := g.InspectIdentity()
		if err != nil {
			return err
		}
		stat, err := os.ReadFile("/proc/self/stat")
		if err != nil {
			return err
		}
		ticks, err := postmasterTicks(string(stat), os.Getpid())
		if err != nil {
			return err
		}
		birth = &ProcessIdentity{ProtocolVersion: 1, Identity: id, PostmasterPID: os.Getpid(), PostmasterStartTicks: control.Uint64(ticks), AddressSpaceMaxBytes: source.Limits.AddressSpaceMaxBytes}
		// Linux6.12 fs/exec.c::de_thread在非leader线程exec时保留原TGID和
		// leader start_boottime；/proc stat使用此值生成第22字段。这里记录
		// 进程PID而非当前Go线程TID；真实内核验证仍列入Linux目标验收。
	}
	return g.launchWithBirth(context.Background(), &source, birth, func() error {
		// prctl能力/NO_NEW_PRIVS属于线程属性，降权与exec之间固定当前OS线程；
		// Setres*仍覆盖全部线程，不能只依赖LockOSThread来完成进程整体降权。
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		// Go helper有多个OS线程；cgroup迁移不受pids.max的fork限制，可临时超过N。
		// 整个入组/降权/exec仍持原资源文件锁，其他root读者不能在此窗口发证。
		// exec失败不清Birth/原预算；后续InspectProcess必须见真正PG文件，实际
		// 任务数还须<=原N才可确认，不能将helper迁入当成PG已满足任务上限。
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
		// 由原生PG在main最早阶段落实AS；不能在仍有GC/多线程的Go helper里
		// 压低整个进程AS再继续分配。root原创建覆盖同名环境，普通调用者不能
		// 放宽、删除或选择另一额度。原生旧PG忽略此项时，Root真实读回会拒绝。
		environment := addressSpaceEnvironment(os.Environ(), source.Limits.AddressSpaceMaxBytes)
		return syscall.Exec(FixedPostgresPath, argv, environment)
	})
}
