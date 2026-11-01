//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

type normalSystemRoleGateCheckpointKey struct{}
type normalSystemRoleGateCheckpoint func(string, int, int)

// 仅包内机制测试可在Context中提供此私有类型；固定service入口不会构造它，
// 不能由请求、环境、JSON或声明选择。生产状态机始终执行同一核验/持久顺序。
func normalSystemRoleGateCheck(ctx context.Context, stage string, pidfd, tid int) {
	if hook, ok := ctx.Value(normalSystemRoleGateCheckpointKey{}).(normalSystemRoleGateCheckpoint); ok {
		hook(stage, pidfd, tid)
	}
}

func normalSystemRoleGateVerifyStop(ctx context.Context, s *normalSystemLifecycleSource, ns *normalSystemRoleNamespaceSource, task *normalSystemOriginalTask, tid int) error {
	if e := task.verify(); e != nil {
		return e
	}
	if task.source.UID != 0 || task.source.GID != 0 {
		return fmt.Errorf("original Gate birth identity differs")
	}
	if e := ns.verify(task.procfd); e != nil {
		return e
	}
	status, e := nativeTraceProcText(task.procfd, "status")
	if e != nil {
		return e
	}
	for key, want := range map[string]uint64{"TracerPid": uint64(tid), "Threads": 1} {
		v, e := nativeTraceStatusValues(status, key, 1)
		if e != nil || v[0] != want {
			return fmt.Errorf("original Gate tracer/leader state differs")
		}
	}
	group, e := nativeTraceProcText(task.procfd, "cgroup")
	if e != nil || strings.TrimSpace(group) != "0::"+strings.TrimPrefix(systemCgroup, "/sys/fs/cgroup") {
		return fmt.Errorf("original Gate birth is outside System")
	}
	s.mu.Lock()
	expected := s.files[s.logical[normalSystemRoleGateArgv()[0]]]
	meta := s.metadata[s.logical[normalSystemRoleGateArgv()[0]]]
	s.mu.Unlock()
	if expected == nil || meta.Interpreter != "" || len(meta.Needed) != 0 {
		return fmt.Errorf("original Gate executable is not fixed static guard")
	}
	fd, e := unix.Openat(task.procfd, "exe", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(fd), "original-static-role-Gate")
	defer f.Close()
	var a, b unix.Stat_t
	if unix.Fstat(fd, &a) != nil || unix.Fstat(int(expected.Fd()), &b) != nil || a.Dev != b.Dev || a.Ino != b.Ino {
		return fmt.Errorf("original Gate executable inode differs")
	}
	x, e := nativeBirthFileHash(f)
	if e != nil {
		return e
	}
	y, e := nativeBirthFileHash(expected)
	if e != nil || x != y {
		return fmt.Errorf("original Gate executable bytes differ")
	}
	argv, e := normalSystemReadVector(task.procfd, "cmdline")
	if e != nil {
		return e
	}
	env, e := normalSystemReadVector(task.procfd, "environ")
	if e != nil {
		return e
	}
	ah, e := normalSystemVectorSHA(argv)
	if e != nil {
		return e
	}
	eh, e := normalSystemVectorSHA(env)
	if e != nil {
		return e
	}
	wa, _ := normalSystemVectorSHA(normalSystemRoleGateArgv())
	we, _ := normalSystemVectorSHA(normalSystemRoleGateEnvironment())
	if ah != wa || eh != we {
		return fmt.Errorf("original Gate vectors differ")
	}
	return ctx.Err()
}

// 原pidfd不变，仅可信Gate允许真实UID从0下降到原计划非root。没有改旧PG
// tracer的UID合同，也不重写siginfo；每个真实事件必须是这两个已知UID之一。
func normalSystemRoleGatePoll(k *nativeTraceOriginalControl, target uint32) (int, unix.WaitStatus, error) {
	var info [128]byte
	_, _, errno := unix.Syscall6(unix.SYS_WAITID, 3, uintptr(k.pidfd), uintptr(unsafe.Pointer(&info[0])), unix.WEXITED|unix.WSTOPPED|unix.WNOHANG|unix.WALL, 0, 0)
	runtime.KeepAlive(info)
	if errno != 0 {
		return 0, 0, errno
	}
	uid := binary.LittleEndian.Uint32(info[20:24])
	if uid != 0 && uid != target {
		return 0, 0, fmt.Errorf("original Gate UID transition differs")
	}
	pid, status, e := decodeNativeOriginalWait(info, k.pid, uid)
	if e == nil && pid != 0 {
		k.uid = uid
	}
	return pid, status, e
}
func normalSystemRoleGateWaitExec(ctx context.Context, k *nativeTraceOriginalControl, target uint32) error {
	for {
		if e := ctx.Err(); e != nil {
			return e
		}
		pid, status, e := normalSystemRoleGatePoll(k, target)
		if errors.Is(e, unix.EINTR) {
			continue
		}
		if e != nil {
			return e
		}
		if pid == 0 {
			time.Sleep(time.Millisecond)
			continue
		}
		if status.Exited() || status.Signaled() {
			k.observedExit = true
			k.attached = false
			return fmt.Errorf("original Gate exited before target exec")
		}
		if !status.Stopped() {
			return fmt.Errorf("original Gate unknown event")
		}
		k.stopped = true
		k.rememberStop(status)
		event := uint32(status) >> 16
		if event == unix.PTRACE_EVENT_EXEC && status.StopSignal() == unix.SIGTRAP {
			if k.uid != target {
				return fmt.Errorf("original target exec UID differs")
			}
			return nil
		}
		if event != 0 {
			return fmt.Errorf("original Gate unexpected ptrace event")
		}
		// 保留Gate本身收到的真实signal-delivery信号（含Go async preempt），不把
		// 它当目标exec，也不吞掉后声称成功。异常stop仍只原pidfd取消。
		signal := status.StopSignal()
		if signal <= 0 || signal > 64 {
			return fmt.Errorf("original Gate stop signal differs")
		}
		if e = nativeTracePtrace(unix.PTRACE_CONT, k.pid, uintptr(signal)); e != nil {
			return e
		}
		k.stopped = false
	}
}
func normalSystemRoleGateCleanup(ctx context.Context, k *nativeTraceOriginalControl, target uint32) (bool, error) {
	if k.observedExit || !k.attached {
		return false, nil
	}
	e := unix.PidfdSendSignal(k.pidfd, unix.SIGKILL, nil, 0)
	if e != nil && !errors.Is(e, unix.ESRCH) {
		return true, e
	}
	for {
		if e := ctx.Err(); e != nil {
			return true, e
		}
		pid, status, e := normalSystemRoleGatePoll(k, target)
		if errors.Is(e, unix.EINTR) {
			continue
		}
		if e != nil {
			return true, e
		}
		if pid == 0 {
			time.Sleep(time.Millisecond)
			continue
		}
		if status.Exited() || status.Signaled() {
			k.observedExit = true
			k.attached = false
			return false, nil
		}
	}
}
func normalSystemRoleGateExitObserved(fd int) (bool, error) {
	p := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	n, e := unix.Poll(p, 0)
	if e != nil {
		return false, e
	}
	if p[0].Revents&(unix.POLLERR|unix.POLLNVAL) != 0 {
		return false, fmt.Errorf("original child exit descriptor differs")
	}
	return n == 1 && p[0].Revents&(unix.POLLIN|unix.POLLHUP) != 0, nil
}
