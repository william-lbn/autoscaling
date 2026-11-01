//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// SO_PEERPIDFD只能选择原socket kernel对象，随后钉住该proc目录。真实UID与
// effective/saved/fs UID都为0才接受，PG SUID的ruid=1000/euid=0明确拒绝。
func normalSystemRoleServicePeer(conn *net.UnixConn, boot string) (*normalSystemOriginalTask, error) {
	peer, pidfd, e := arraySocketPeer(conn)
	if e != nil {
		return nil, e
	}
	original := os.NewFile(uintptr(pidfd), "original-System-role-peer-pidfd")
	defer original.Close()
	if peer == nil || peer.Pid <= 1 || peer.Uid != 0 || peer.Gid != 0 {
		return nil, fmt.Errorf("fixed System role peer requires original real root")
	}
	if e = arrayRegistryPIDFDAlive(pidfd); e != nil {
		return nil, e
	}
	fd, e := unix.Open("/proc/"+strconv.Itoa(int(peer.Pid)), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	proc := os.NewFile(uintptr(fd), "original-System-role-peer-proc")
	defer proc.Close()
	raw, e := nativeTraceProcText(fd, "stat")
	if e != nil {
		return nil, e
	}
	ticks, e := postmasterTicks(raw, int(peer.Pid))
	if e != nil {
		return nil, e
	}
	task, e := newNormalSystemOriginalTask(normalSystemOriginalTaskSource{PID: int(peer.Pid), StartTicks: ticks, UID: 0, GID: 0, GuestBootID: boot, PIDFD: original, ProcDirectory: proc})
	if e != nil {
		return nil, e
	}
	if e = normalSystemRoleServicePrivileges(task.procfd); e != nil {
		task.close()
		return nil, e
	}
	return task, nil
}
func normalSystemRoleServiceVerifyTask(ctx context.Context, d *normalSystemRoleDeclarationSource, ns *normalSystemRoleNamespaceSource, t *normalSystemOriginalTask, argv []string) error {
	if ctx == nil || ctx.Err() != nil || d == nil || ns == nil || t == nil {
		return ErrUnknown
	}
	if e := d.verify(ctx); e != nil {
		return e
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if e := t.verify(); e != nil {
		return e
	}
	if e := normalSystemRoleServicePrivileges(t.procfd); e != nil {
		return e
	}
	if e := ns.verify(t.procfd); e != nil {
		return e
	}
	group, e := nativeTraceProcText(t.procfd, "cgroup")
	if e != nil || strings.TrimSpace(group) != "0::"+strings.TrimPrefix(systemCgroup, "/sys/fs/cgroup") {
		return errors.Join(ErrUnknown, fmt.Errorf("original System role peer is outside fixed domain"))
	}
	node, e := splitReadNode(systemCgroup)
	if e != nil || !node.RootControlled || uint64(node.Device) != d.source.binding.SystemDevice || uint64(node.Inode) != d.source.binding.SystemInode || uint64(node.MemoryMaxBytes) != d.source.binding.SystemMemoryMaxBytes || node.SwapMaxBytes != 0 || !node.MemoryHighUnlimited {
		return errors.Join(ErrUnknown, e, fmt.Errorf("original System role controls differ"))
	}
	fd, e := unix.Openat(t.procfd, "exe", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	exe := os.NewFile(uintptr(fd), "original-System-role-service-exe")
	defer exe.Close()
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Dev != d.executableDevice || st.Ino != d.executableInode {
		return fmt.Errorf("original System role peer executable differs")
	}
	sha, e := nativeBirthFileHash(exe)
	if e != nil || sha != d.declaration.ExecutableSHA256 {
		return fmt.Errorf("original System role peer executable bytes differ")
	}
	expectedArgv, e := normalSystemVectorSHA(argv)
	if e != nil {
		return e
	}
	expectedEnv, _ := normalSystemVectorSHA(normalSystemRoleServiceExpectedEnvironment(argv))
	actualArgv, e := normalSystemReadVector(t.procfd, "cmdline")
	if e != nil {
		return e
	}
	actualEnv, e := normalSystemReadVector(t.procfd, "environ")
	if e != nil {
		return e
	}
	a, e := normalSystemVectorSHA(actualArgv)
	if e != nil {
		return e
	}
	b, e := normalSystemVectorSHA(actualEnv)
	if e != nil {
		return e
	}
	if a != expectedArgv || b != expectedEnv {
		return fmt.Errorf("fixed System role peer argv/environment differs")
	}
	return t.verify()
}

// 只比较真正跨实例不变的来源字段；old/new RootConsumer由单独原spawn绑定，
// 不能靠此函数把新nonce当旧Root实例，也不把raw binding当授权对象。
func normalSystemRoleImmutableBindingsEqual(a, b normalSystemLifecycleBinding) bool {
	a.RootConsumer = nativeBirthConsumer{}
	b.RootConsumer = nativeBirthConsumer{}
	return a == b
}

// UID0不足以通过：普通角色的NNP1/cap0必须拒绝。Root审计动作还需原
// SYS_ADMIN/SYS_PTRACE/KILL，并保持未过滤调用方，不能借sudo/ruid归一化补签。
func normalSystemRoleServicePrivileges(procfd int) error {
	text, e := nativeTraceProcText(procfd, "status")
	if e != nil {
		return e
	}
	return normalSystemRoleServicePrivilegeStatus(text)
}
func normalSystemRoleServicePrivilegeStatus(text string) error {
	var e error
	for _, name := range []string{"NoNewPrivs", "Seccomp"} {
		v, e := nativeTraceStatusValues(text, name, 1)
		if e != nil || v[0] != 0 {
			return fmt.Errorf("original role client privilege stage differs")
		}
	}
	var effective uint64
	found := false
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "CapEff:") {
			parts := strings.Fields(line)
			if len(parts) != 2 {
				return fmt.Errorf("original role client capability format differs")
			}
			effective, e = strconv.ParseUint(parts[1], 16, 64)
			if e != nil {
				return fmt.Errorf("original role client capability format differs")
			}
			found = true
		}
	}
	mask := uint64(1<<unix.CAP_KILL | 1<<unix.CAP_SYS_ADMIN | 1<<unix.CAP_SYS_PTRACE)
	if !found || effective&mask != mask {
		return fmt.Errorf("original role client lacks fixed Root capabilities")
	}
	return nil
}
func normalSystemRoleServiceCaller() error {
	if e := normalSystemRoleCaller(); e != nil {
		return e
	}
	fd, e := unix.Open("/proc/thread-self", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	defer unix.Close(fd)
	return normalSystemRoleServicePrivileges(fd)
}

func normalSystemRoleServicePID1Client(t *normalSystemOriginalTask) error {
	if t == nil {
		return ErrUnknown
	}
	raw, e := nativeTraceProcText(t.procfd, "stat")
	if e != nil {
		return e
	}
	if _, e = postmasterTicks(raw, t.source.PID); e != nil {
		return e
	}
	right := strings.LastIndex(raw, ")")
	if right < 0 {
		return fmt.Errorf("fixed role client stat differs")
	}
	fields := strings.Fields(raw[right+1:])
	if len(fields) < 2 || fields[1] != "1" {
		return fmt.Errorf("fixed role client was not launched by original namespace PID1")
	}
	return t.verify()
}

// 仅固定exec-system-role客户端为空环境；initialRoot/probe/broker与目标
// ordinary/compute继续原PATHonly。nil不是空环境向量，必须显式[]才可匹配。
func normalSystemRoleServiceExpectedEnvironment(argv []string) []string {
	if len(argv) == 3 && argv[0] == normalSystemRoleBrokerExecutable && argv[1] == "exec-system-role" {
		return normalSystemV3ClientEnvironment()
	}
	return normalSystemRoleBrokerEnvironment()
}
func normalSystemRoleServiceClientEnvironment(actual []string) error {
	expected := normalSystemV3ClientEnvironment()
	want, e := normalSystemVectorSHA(expected)
	if e != nil {
		return e
	}
	got, e := normalSystemVectorSHA(actual)
	if e != nil || got != want {
		return fmt.Errorf("fixed role client environment differs")
	}
	return nil
}
