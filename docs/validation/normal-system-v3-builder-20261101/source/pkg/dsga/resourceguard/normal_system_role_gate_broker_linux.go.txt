//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

const normalSystemRoleBrokerKind = "initial-role-broker"

// broker不是普通socket角色枚举；必须额外由原RO声明固定其特殊父生命周期。
// 只此角色在可信Gate中清Pdeathsig。其它角色仍由原creator M租约覆盖到真Wait。
func normalSystemRoleGateLaunchPlan(ctx context.Context, s *normalSystemLifecycleSource, role string) (normalSystemRoleLaunchSpec, error) {
	if role != normalSystemRoleBrokerKind {
		return newNormalSystemRoleLaunchSpec(ctx, s, role)
	}
	d, e := readNormalSystemRoleDeclarationOriginal(ctx, s)
	if e != nil {
		return normalSystemRoleLaunchSpec{}, e
	}
	defer d.close()
	sha, e := s.referenceSHA256()
	if e != nil {
		return normalSystemRoleLaunchSpec{}, e
	}
	return normalSystemRoleLaunchSpec{role: role, argv: normalSystemRoleBrokerArgv(), env: normalSystemRoleBrokerEnvironment(), binding: s.binding, sourceSHA: sha}, nil
}
func normalSystemRoleGateInspectTarget(ctx context.Context, s *normalSystemLifecycleSource, task *normalSystemOriginalTask, plan normalSystemRoleLaunchSpec) (normalSystemRoleObservation, error) {
	if plan.role != normalSystemRoleBrokerKind {
		return s.inspectOriginalTask(ctx, task, plan.role, plan.argv, plan.env)
	}
	d, e := readNormalSystemRoleDeclarationOriginal(ctx, s)
	if e != nil {
		return normalSystemRoleObservation{}, e
	}
	defer d.close()
	return s.inspectOriginalBrokerTask(ctx, task, plan.role, plan.argv, plan.env)
}

// 仅原StartBroker内部writer可提供这个固定启动通道。普通role请求没有FD/路径
// 参数，也不能将其升级为broker。通道在目标prelude继续完成原pair/ready/commit。
type normalSystemRoleBrokerGateFileProvider interface{ originalSystemRoleBrokerGateChannel() (*os.File, error) }

func normalSystemRoleBrokerGateFile(writer normalSystemRoleStoppedWriter) (*os.File, error) {
	provider, ok := writer.(normalSystemRoleBrokerGateFileProvider)
	if !ok {
		return nil, fmt.Errorf("original broker startup channel source missing")
	}
	f, e := provider.originalSystemRoleBrokerGateChannel()
	if e != nil || f == nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	typ, e := unix.GetsockoptInt(int(f.Fd()), unix.SOL_SOCKET, unix.SO_TYPE)
	if e != nil || typ != unix.SOCK_SEQPACKET {
		return nil, fmt.Errorf("original broker startup channel type differs")
	}
	cred, e := unix.GetsockoptUcred(int(f.Fd()), unix.SOL_SOCKET, unix.SO_PEERCRED)
	if e != nil || cred.Uid != 0 || cred.Gid != 0 || int(cred.Pid) != os.Getpid() {
		return nil, fmt.Errorf("original broker startup channel owner differs")
	}
	return f, nil
}

// 下列完整检查保留原resource角色检查的全部身份/控制/RO/摘要门槛，只把
// 角色固定为独立broker声明；不冒用resource/serve、不放宽旧Lifecycle API。
func (s *normalSystemLifecycleSource) inspectOriginalBrokerTask(ctx context.Context, task *normalSystemOriginalTask, role string, argv, env []string) (normalSystemRoleObservation, error) {
	var zero normalSystemRoleObservation
	fail := func(e error) (normalSystemRoleObservation, error) { return zero, errors.Join(ErrUnknown, e) }
	if s == nil || task == nil || ctx == nil || ctx.Err() != nil {
		return fail(fmt.Errorf("original System role context missing"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task.mu.Lock()
	defer task.mu.Unlock()
	if s.closed || task.closed || task.source.GuestBootID != s.binding.GuestBootID {
		return fail(fmt.Errorf("original System role source unavailable"))
	}
	consumer, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || consumer != s.binding.RootConsumer {
		return fail(fmt.Errorf("original System Root consumer changed"))
	}
	ah, e := normalSystemVectorSHA(argv)
	if e != nil {
		return fail(e)
	}
	eh, e := normalSystemVectorSHA(env)
	if e != nil {
		return fail(e)
	}
	if e = normalSystemCleanEnvironment(env); e != nil {
		return fail(e)
	}
	if role != normalSystemRoleBrokerKind {
		return fail(fmt.Errorf("original fixed broker kind differs"))
	}
	fixedSHA, _ := normalSystemVectorSHA(normalSystemRoleBrokerArgv())
	fixedEnvSHA, _ := normalSystemVectorSHA(normalSystemRoleBrokerEnvironment())
	if ah != fixedSHA || eh != fixedEnvSHA {
		return fail(fmt.Errorf("original fixed broker vectors differ"))
	}
	path := s.logical[normalSystemRoleBrokerExecutable]
	wantUID, wantGID := uint32(0), uint32(0)
	expected := s.files[path]
	if path == "" || expected == nil {
		return fail(fmt.Errorf("original role executable not in System closure"))
	}
	if task.source.UID != wantUID || task.source.GID != wantGID {
		return fail(fmt.Errorf("original System role UID/GID differs"))
	}
	if e = task.verify(); e != nil {
		return fail(e)
	}
	group, e := nativeTraceProcText(task.procfd, "cgroup")
	if e != nil || strings.TrimSpace(group) != "0::"+strings.TrimPrefix(systemCgroup, "/sys/fs/cgroup") {
		return fail(fmt.Errorf("original System role left fixed domain"))
	}
	var st unix.Stat_t
	if e = unix.Stat(systemCgroup, &st); e != nil || st.Dev != s.binding.SystemDevice || st.Ino != s.binding.SystemInode {
		return fail(fmt.Errorf("original System cgroup inode differs"))
	}
	currentSystem, e := splitReadNode(systemCgroup)
	if e != nil || !currentSystem.RootControlled || uint64(currentSystem.MemoryMaxBytes) != s.binding.SystemMemoryMaxBytes || currentSystem.SwapMaxBytes != 0 || !currentSystem.MemoryHighUnlimited {
		return fail(fmt.Errorf("original System controls drifted"))
	}
	fd, e := unix.Openat(task.procfd, "exe", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return fail(e)
	}
	actual := os.NewFile(uintptr(fd), "original-System-exe")
	defer actual.Close()
	var got, want unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(fd, &got) != nil || unix.Fstat(int(expected.Fd()), &want) != nil || unix.Fstatfs(fd, &fs) != nil || fs.Flags&unix.ST_RDONLY == 0 || got.Dev != want.Dev || got.Ino != want.Ino {
		return fail(fmt.Errorf("original System executable inode differs"))
	}
	gotSHA, e := nativeBirthFileHash(actual)
	if e != nil {
		return fail(e)
	}
	wantSHA, e := nativeBirthFileHash(expected)
	if e != nil || gotSHA != wantSHA {
		return fail(fmt.Errorf("original System executable bytes differ"))
	}
	actualArgv, e := normalSystemReadVector(task.procfd, "cmdline")
	if e != nil {
		return fail(e)
	}
	gotArgv, e := normalSystemVectorSHA(actualArgv)
	if e != nil || gotArgv != ah {
		return fail(fmt.Errorf("original System argv differs"))
	}
	actualEnv, e := normalSystemReadVector(task.procfd, "environ")
	if e != nil {
		return fail(e)
	}
	gotEnv, e := normalSystemVectorSHA(actualEnv)
	if e != nil || gotEnv != eh {
		return fail(fmt.Errorf("original System environment differs"))
	}
	if e = ctx.Err(); e != nil {
		return fail(e)
	}
	if e = task.verify(); e != nil {
		return fail(e)
	}
	var pf unix.Stat_t
	if e = unix.Fstat(task.pidfd, &pf); e != nil {
		return fail(e)
	}
	return normalSystemRoleObservation{Role: role, PID: task.source.PID, StartTicks: task.source.StartTicks, UID: task.source.UID, GID: task.source.GID, ExecutableSHA256: gotSHA, ArgvSHA256: ah, EnvironmentSHA256: eh, PIDFDDevice: pf.Dev, PIDFDInode: pf.Ino, Binding: s.binding, OriginalTaskObserved: true, FixedArgvEnvironmentObserved: true}, nil
}
