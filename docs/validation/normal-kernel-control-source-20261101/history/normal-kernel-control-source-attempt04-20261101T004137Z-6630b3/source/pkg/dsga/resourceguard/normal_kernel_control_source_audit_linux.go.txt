//go:build linux && amd64

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

type normalKernelRoleControlObservation struct {
	Role                string                       `json:"role"`
	PID                 int                          `json:"pid"`
	StartTicks          uint64                       `json:"start_ticks"`
	Binding             normalSystemLifecycleBinding `json:"binding"`
	ManifestSHA256      string                       `json:"manifest_sha256"`
	SourceSHA256        string                       `json:"source_sha256"`
	BPFProgramSHA256    string                       `json:"bpf_program_sha256"`
	Capabilities        uint64                       `json:"capabilities"`
	TracerTID           int                          `json:"tracer_tid"`
	NoNewPrivileges     uint64                       `json:"no_new_privileges"`
	OriginalExecStopped bool                         `json:"original_exec_stopped"`
	// Landlock和securebits无独立proc查询；此项精确表示由原RO Gate固定、
	// 全部checked成功且在其后唯一装入的BPF顺序见证，不能说读出了规则。
	LandlockFromFixedGateSequence bool `json:"landlock_from_fixed_gate_sequence"`
	NormalDecreaseAuthorized      bool `json:"normal_decrease_authorized"`
}
type normalKernelRoleControlSource struct {
	plan        *normalKernelOrdinaryControlPlan
	task        *normalSystemOriginalTask
	observation normalKernelRoleControlObservation
}

func normalKernelControlStatusHex(raw, key string) (uint64, error) {
	count := 0
	var n uint64
	for _, line := range strings.Split(raw, "\n") {
		if !strings.HasPrefix(line, key+":") {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 2 || len(f[1]) != 16 {
			return 0, fmt.Errorf("ordinary capability status malformed")
		}
		v, e := strconv.ParseUint(f[1], 16, 64)
		if e != nil {
			return 0, e
		}
		n = v
		count++
	}
	if count != 1 {
		return 0, fmt.Errorf("ordinary capability status absent/duplicate")
	}
	return n, nil
}
func normalKernelControlCheckStatus(raw string, keep uint64, tracerTID int) error {
	for _, key := range []string{"CapEff", "CapPrm", "CapInh", "CapBnd", "CapAmb"} {
		n, e := normalKernelControlStatusHex(raw, key)
		if e != nil || n != keep {
			return fmt.Errorf("ordinary capability set differs: %s", key)
		}
	}
	for key, want := range map[string]uint64{"NoNewPrivs": 1, "Seccomp": 2, "Seccomp_filters": 1, "Threads": 1, "TracerPid": uint64(tracerTID)} {
		n, e := nativeTraceStatusValues(raw, key, 1)
		if e != nil || n[0] != want {
			return fmt.Errorf("ordinary original stop control differs: %s", key)
		}
	}
	return nil
}
func normalKernelControlReadFullFilter(pid, tracerTID int) ([]unix.SockFilter, error) {
	if pid <= 1 || tracerTID <= 0 {
		return nil, fmt.Errorf("original ordinary stopped target absent")
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if unix.Gettid() != tracerTID {
		return nil, fmt.Errorf("original ordinary tracer thread differs")
	}
	if e := nativeFilterAuditCaller(); e != nil {
		return nil, e
	}
	n, _, errno := unix.Syscall6(unix.SYS_PTRACE, ptraceSeccompGetFilter, uintptr(pid), 0, 0, 0, 0)
	if errno != 0 || n == 0 || n > 4096 {
		return nil, errors.Join(errno, fmt.Errorf("ordinary complete kernel filter length unavailable"))
	}
	actual := make([]unix.SockFilter, int(n))
	got, _, errno := unix.Syscall6(unix.SYS_PTRACE, ptraceSeccompGetFilter, uintptr(pid), 0, uintptr(unsafe.Pointer(&actual[0])), 0, 0)
	runtime.KeepAlive(actual)
	if errno != 0 || got != n {
		return nil, fmt.Errorf("ordinary complete kernel filter changed: %v", errno)
	}
	if e := normalKernelControlCheckFilter(actual); e != nil {
		return nil, e
	}
	// 明确要求唯一新filter；额外继承filter不是“更安全所以忽略”的来源。
	_, _, errno = unix.Syscall6(unix.SYS_PTRACE, ptraceSeccompGetFilter, uintptr(pid), 1, 0, 0, 0)
	if errno != unix.ENOENT {
		return nil, fmt.Errorf("ordinary unexpected inherited filter")
	}
	return actual, nil
}
func normalKernelControlCheckExecFDs(procfd int) error {
	fd, e := unix.Openat(procfd, "fd", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(fd), "original-ordinary-exec-fds")
	entries, e := f.ReadDir(-1)
	f.Close()
	if e != nil {
		return e
	}
	if len(entries) != 3 {
		return fmt.Errorf("ordinary target inherited nonstdio descriptor")
	}
	for _, entry := range entries {
		n, e := strconv.Atoi(entry.Name())
		if e != nil || n < 0 || n > 2 {
			return fmt.Errorf("ordinary target inherited foreign descriptor")
		}
		object, e := unix.Openat(procfd, "fd/"+entry.Name(), unix.O_PATH|unix.O_CLOEXEC, 0)
		if e != nil {
			return e
		}
		var st unix.Stat_t
		e = unix.Fstat(object, &st)
		unix.Close(object)
		if e != nil {
			return e
		}
		typ := st.Mode & unix.S_IFMT
		if typ == unix.S_IFIFO {
			continue
		}
		if typ != unix.S_IFCHR {
			return fmt.Errorf("ordinary stdio is not original console/null/pipe")
		}
		major, minor := unix.Major(uint64(st.Rdev)), unix.Minor(uint64(st.Rdev))
		if !(major == 1 && minor == 3 || major == 5 && minor == 1) {
			return fmt.Errorf("ordinary stdio contains raw/foreign device")
		}
	}
	return nil
}

// caller必须仍持原exec tracer M和原resource flock，且尚未detach。
// 本函数不SEIZE/CONT/DETACH、不从numeric PID打开新对象，也不修改原P2 journal。
func inspectNormalKernelOriginalRole(ctx context.Context, s *normalSystemLifecycleSource, task *normalSystemOriginalTask, p *normalKernelOrdinaryControlPlan, tracerTID int) (*normalKernelRoleControlSource, error) {
	if p == nil || p.source != s || task == nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("ordinary original opaque inputs absent"))
	}
	if e := p.recheck(ctx); e != nil {
		return nil, e
	}
	argv, e := normalSystemFixedRoleArgv(p.rule.Role)
	if e != nil {
		return nil, e
	}
	observed, e := s.inspectOriginalTask(ctx, task, p.rule.Role, argv, []string{"PATH=/neonvm/bin"})
	if e != nil {
		return nil, e
	}
	if observed.Binding != p.binding {
		return nil, fmt.Errorf("ordinary source binding differs")
	}
	task.mu.Lock()
	defer task.mu.Unlock()
	if task.closed {
		return nil, ErrUnknown
	}
	if e = task.verify(); e != nil {
		return nil, e
	}
	status, e := nativeTraceProcText(task.procfd, "status")
	if e != nil {
		return nil, e
	}
	if e = normalKernelControlCheckStatus(status, p.rule.KeepCapabilities, tracerTID); e != nil {
		return nil, e
	}
	program, e := normalKernelControlReadFullFilter(task.source.PID, tracerTID)
	if e != nil {
		return nil, e
	}
	if e = normalKernelControlCheckExecFDs(task.procfd); e != nil {
		return nil, e
	}
	if e = task.verify(); e != nil {
		return nil, e
	}
	// 与原P2 task生命周期分离持有FD，防外层Wait关闭后被错误同名PID顶替。
	pf, e := unix.FcntlInt(uintptr(task.pidfd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	a := os.NewFile(uintptr(pf), "ordinary-original-pidfd")
	defer a.Close()
	proc, e := unix.FcntlInt(uintptr(task.procfd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	b := os.NewFile(uintptr(proc), "ordinary-original-proc")
	defer b.Close()
	// newNormalSystemOriginalTask再dup；各临时FD只有一个os.File关闭所有者。
	original := task.source
	original.PIDFD = a
	original.ProcDirectory = b
	copy, e := newNormalSystemOriginalTask(original)
	if e != nil {
		return nil, e
	}
	raw, _ := json.Marshal(program)
	return &normalKernelRoleControlSource{plan: p, task: copy, observation: normalKernelRoleControlObservation{
		Role: p.rule.Role, PID: task.source.PID, StartTicks: task.source.StartTicks, Binding: p.binding, ManifestSHA256: p.manifestSHA, SourceSHA256: p.sourceSHA, BPFProgramSHA256: physicalDigest(raw), Capabilities: p.rule.KeepCapabilities, TracerTID: tracerTID, NoNewPrivileges: 1, OriginalExecStopped: true, LandlockFromFixedGateSequence: true,
	}}, nil
}
func (s *normalKernelRoleControlSource) close() {
	if s != nil && s.task != nil {
		s.task.close()
	}
}
