//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// 仅原clone/PidFD或Root固定socket的SO_PEERPIDFD生产者移交；没有numeric PID
// 重新open的构造路径。原proc目录必须已由该同一出生/peer握手钉住。这里允许
// Root System角色，不改旧PG/helper tracer对非root的限制，也不迁组/ptrace/kill。
type normalSystemOriginalTaskSource struct {
	PID                  int
	StartTicks           uint64
	UID, GID             uint32
	GuestBootID          string
	PIDFD, ProcDirectory *os.File
}
type normalSystemOriginalTask struct {
	mu            sync.Mutex
	source        normalSystemOriginalTaskSource
	pidfd, procfd int
	closed        bool
}

func newNormalSystemOriginalTask(in normalSystemOriginalTaskSource) (*normalSystemOriginalTask, error) {
	if in.PID <= 1 || in.StartTicks == 0 || !validGuestBoot(in.GuestBootID) || in.PIDFD == nil || in.ProcDirectory == nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original System task source incomplete"))
	}
	t := &normalSystemOriginalTask{source: in, pidfd: -1, procfd: -1}
	ok := false
	defer func() {
		if !ok {
			t.close()
		}
	}()
	var e error
	t.pidfd, e = unix.FcntlInt(in.PIDFD.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	t.procfd, e = unix.FcntlInt(in.ProcDirectory.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	var st unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(t.procfd, &st) != nil || unix.Fstatfs(t.procfd, &fs) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || fs.Type != unix.PROC_SUPER_MAGIC {
		return nil, fmt.Errorf("original System proc source is not procfs")
	}
	if e = t.verify(); e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	ok = true
	return t, nil
}
func (t *normalSystemOriginalTask) close() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		if t.pidfd >= 0 {
			unix.Close(t.pidfd)
		}
		if t.procfd >= 0 {
			unix.Close(t.procfd)
		}
		t.closed = true
	}
}
func (t *normalSystemOriginalTask) verify() error {
	if t.closed {
		return fmt.Errorf("original System task closed")
	}
	boot, e := kernelText("/proc/sys/kernel/random/boot_id")
	if e != nil || boot != t.source.GuestBootID {
		return fmt.Errorf("original System Guest differs")
	}
	if e = unix.PidfdSendSignal(t.pidfd, 0, nil, 0); e != nil {
		return e
	}
	if e = arrayRegistryPIDFDAlive(t.pidfd); e != nil {
		return e
	}
	info, e := os.ReadFile("/proc/self/fdinfo/" + strconv.Itoa(t.pidfd))
	if e != nil || len(info) > 4096 {
		return fmt.Errorf("original System pidfd info missing")
	}
	values, e := nativeTraceStatusValues(string(info), "Pid", 1)
	if e != nil || values[0] != uint64(t.source.PID) {
		return fmt.Errorf("original System pidfd object differs")
	}
	stat, e := nativeTraceProcText(t.procfd, "stat")
	if e != nil {
		return e
	}
	ticks, e := postmasterTicks(stat, t.source.PID)
	if e != nil || ticks != t.source.StartTicks {
		return fmt.Errorf("original System start differs")
	}
	status, e := nativeTraceProcText(t.procfd, "status")
	if e != nil {
		return e
	}
	for name, want := range map[string][]uint64{"Pid": {uint64(t.source.PID)}, "Tgid": {uint64(t.source.PID)}, "Uid": {uint64(t.source.UID), uint64(t.source.UID), uint64(t.source.UID), uint64(t.source.UID)}, "Gid": {uint64(t.source.GID), uint64(t.source.GID), uint64(t.source.GID), uint64(t.source.GID)}} {
		actual, e := nativeTraceStatusValues(status, name, len(want))
		if e != nil {
			return e
		}
		for i, v := range want {
			if actual[i] != v {
				return fmt.Errorf("original System task credentials differ")
			}
		}
	}
	return arrayRegistryPIDFDAlive(t.pidfd)
}
func normalSystemReadVector(fd int, name string) ([]string, error) {
	n, e := unix.Openat(fd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(n), "original-System-vector")
	defer f.Close()
	raw, e := io.ReadAll(io.LimitReader(f, 64<<10+1))
	if e == nil && name == "environ" && len(raw) == 0 {
		return []string{}, nil
	}
	if e != nil || len(raw) > 64<<10 || len(raw) == 0 || raw[len(raw)-1] != 0 {
		return nil, fmt.Errorf("System exec vector missing or exceeds bound")
	}
	raw = raw[:len(raw)-1]
	parts := bytes.Split(raw, []byte{0})
	out := make([]string, len(parts))
	for i, b := range parts {
		out[i] = string(b)
	}
	if _, e = normalSystemVectorSHA(out); e != nil {
		return nil, e
	}
	return out, nil
}

// expected向量不是权限：固定角色必须逐字相等；Compute两摘要必须原manifest
// 相等。此batch不生成expected env、不执行新程序；后续固定launcher才真正
// env-i和绑定每次respawn。即使实际一次观察成功，continuous资格仍Unknown。
func (s *normalSystemLifecycleSource) inspectOriginalTask(ctx context.Context, task *normalSystemOriginalTask, role string, argv, env []string) (normalSystemRoleObservation, error) {
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
	path := ""
	wantUID, wantGID := uint32(0), uint32(0)
	if role == "compute" {
		if len(argv) == 0 || argv[0] != s.manifest.ComputePath || ah != s.manifest.ComputeArgvSHA256 || eh != s.manifest.ComputeEnvironmentSHA256 {
			return fail(fmt.Errorf("original Compute vectors differ"))
		}
		account, e := user.Lookup("postgres")
		if e != nil {
			return fail(e)
		}
		uid, e := strconv.ParseUint(account.Uid, 10, 32)
		if e != nil || uid == 0 {
			return fail(fmt.Errorf("original Compute identity unsupported"))
		}
		gid, e := strconv.ParseUint(account.Gid, 10, 32)
		if e != nil || gid == 0 {
			return fail(fmt.Errorf("original Compute group unsupported"))
		}
		wantUID, wantGID = uint32(uid), uint32(gid)
		path = s.logical[s.manifest.ComputePath]
	} else {
		fixed, e := normalSystemFixedRoleArgv(role)
		if e != nil {
			return fail(e)
		}
		fixedSHA, _ := normalSystemVectorSHA(fixed)
		fixedEnvSHA, _ := normalSystemVectorSHA([]string{"PATH=/neonvm/bin"})
		if ah != fixedSHA || eh != fixedEnvSHA {
			return fail(fmt.Errorf("original fixed System vectors differ"))
		}
		path = s.logical[fixed[0]]
	}
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

// 仅给原调度点摘要，不把向量内容或env值写入WAL/日志。
func (s *normalSystemLifecycleSource) referenceSHA256() (string, error) {
	o, e := s.snapshot()
	if e != nil {
		return "", e
	}
	b, e := json.Marshal(o)
	if e != nil {
		return "", e
	}
	return physicalDigest(b), nil
}
