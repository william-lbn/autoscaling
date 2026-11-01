//go:build linux && amd64

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	"golang.org/x/sys/unix"
)

// 仅原真实第二exec-stop可产生；它不包含可反序列化的verified许可。
// Landlock没有规则读回接口，证据是原RO Gate的唯一checked安装顺序与不可移除BPF。
type normalSystemOrdinaryV2InstalledControlSource struct {
	plan        *normalSystemOrdinaryV2ControlPlan
	task        *normalSystemOriginalTask
	observation normalSystemRoleObservation
	filterSHA   string
}

func normalSystemOrdinaryV2ObserveTask(ctx context.Context, s *normalSystemLifecycleSource, task *normalSystemOriginalTask, p *normalSystemOrdinaryV2ControlPlan) (normalSystemRoleObservation, error) {
	var zero normalSystemRoleObservation
	if p == nil || s == nil || task == nil || p.source != s || ctx == nil || ctx.Err() != nil {
		return zero, ErrUnknown
	}
	if e := p.recheck(ctx); e != nil {
		return zero, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task.mu.Lock()
	defer task.mu.Unlock()
	if s.closed || task.closed || p.binding != s.binding || task.source.GuestBootID != s.binding.GuestBootID || task.source.UID != p.rule.UID || task.source.GID != p.rule.GID {
		return zero, ErrUnknown
	}
	current, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || current != s.binding.RootConsumer {
		return zero, errors.Join(ErrUnknown, e)
	}
	if e = task.verify(); e != nil {
		return zero, e
	}
	group, e := nativeTraceProcText(task.procfd, "cgroup")
	if e != nil || strings.TrimSpace(group) != "0::"+strings.TrimPrefix(systemCgroup, "/sys/fs/cgroup") {
		return zero, ErrUnknown
	}
	var st unix.Stat_t
	if unix.Stat(systemCgroup, &st) != nil || st.Dev != s.binding.SystemDevice || st.Ino != s.binding.SystemInode {
		return zero, ErrUnknown
	}
	node, e := splitReadNode(systemCgroup)
	if e != nil || !node.RootControlled || uint64(node.MemoryMaxBytes) != s.binding.SystemMemoryMaxBytes || node.SwapMaxBytes != 0 || !node.MemoryHighUnlimited {
		return zero, ErrUnknown
	}
	if len(p.rule.Argv) == 0 {
		return zero, ErrUnknown
	}
	expected := s.files[s.logical[p.rule.Argv[0]]]
	if expected == nil {
		return zero, ErrUnknown
	}
	fd, e := unix.Openat(task.procfd, "exe", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return zero, e
	}
	actual := os.NewFile(uintptr(fd), "original-ordinary2-current-ELF")
	defer actual.Close()
	var got, want unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(fd, &got) != nil || unix.Fstat(int(expected.Fd()), &want) != nil || unix.Fstatfs(fd, &fs) != nil || fs.Flags&unix.ST_RDONLY == 0 || got.Dev != want.Dev || got.Ino != want.Ino {
		return zero, ErrUnknown
	}
	hash, e := nativeBirthFileHash(actual)
	if e != nil {
		return zero, e
	}
	original, e := nativeBirthFileHash(expected)
	if e != nil || hash != original {
		return zero, ErrUnknown
	}
	argv, e := normalSystemReadVector(task.procfd, "cmdline")
	if e != nil {
		return zero, e
	}
	env, e := normalSystemReadVector(task.procfd, "environ")
	if e != nil {
		return zero, e
	}
	if !reflect.DeepEqual(argv, p.rule.Argv) || !reflect.DeepEqual(env, p.rule.Environment) {
		return zero, fmt.Errorf("ordinary2 actual original vectors differ")
	}
	ah, _ := normalSystemVectorSHA(argv)
	eh, _ := normalSystemVectorSHA(env)
	var pid unix.Stat_t
	if unix.Fstat(task.pidfd, &pid) != nil {
		return zero, ErrUnknown
	}
	if e = task.verify(); e != nil {
		return zero, e
	}
	return normalSystemRoleObservation{Role: p.rule.Role, PID: task.source.PID, StartTicks: task.source.StartTicks, UID: p.rule.UID, GID: p.rule.GID, ExecutableSHA256: hash, ArgvSHA256: ah, EnvironmentSHA256: eh, PIDFDDevice: pid.Dev, PIDFDInode: pid.Ino, Binding: s.binding, OriginalTaskObserved: true, FixedArgvEnvironmentObserved: true}, nil
}

// 本函数不SEIZE/CONT/DETACH；必须仍在原tracer M、原resource flock内。
// control的原pidfd与phaseTask比较同kernel对象，绝不按numeric PID重开或补签。
func inspectNormalSystemOrdinaryV2InstalledControls(ctx context.Context, s *normalSystemLifecycleSource, task *normalSystemOriginalTask, p *normalSystemOrdinaryV2ControlPlan, control *nativeTraceOriginalControl, tracerTID int) (*normalSystemOrdinaryV2InstalledControlSource, error) {
	if control == nil || p == nil || task == nil || !control.attached || !control.stopped || control.observedExit || control.lastStop.StopSignal() != unix.SIGTRAP || uint32(control.lastStop)>>16 != unix.PTRACE_EVENT_EXEC {
		return nil, ErrUnknown
	}
	if e := normalSystemOrdinaryV2ExactBuild(s); e != nil {
		return nil, e
	}
	task.mu.Lock()
	same := control.pid == task.source.PID
	match := arrayMatchPIDFD(control.pidfd, task.pidfd)
	task.mu.Unlock()
	if !same || match != nil {
		return nil, errors.Join(ErrUnknown, match)
	}
	observed, e := normalSystemOrdinaryV2ObserveTask(ctx, s, task, p)
	if e != nil {
		return nil, e
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
	if e = normalSystemOrdinaryV2ExecStatus(status, p.rule); e != nil {
		return nil, e
	}
	if e = normalKernelControlCheckStatus(status, uint64(p.rule.KeepCapabilities), tracerTID); e != nil {
		return nil, e
	}
	filter, e := normalKernelControlReadFullFilter(task.source.PID, tracerTID)
	if e != nil {
		return nil, e
	}
	if e = normalKernelControlCheckExecFDs(task.procfd); e != nil {
		return nil, e
	}
	if e = task.verify(); e != nil {
		return nil, e
	}
	// 独立pin保留原phase身份；不是引用原UID0 Gate对象，也不延用会被Wait关闭的fd。
	pf, e := unix.FcntlInt(uintptr(task.pidfd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	pid := os.NewFile(uintptr(pf), "original-ordinary2-stop-pidfd")
	defer pid.Close()
	df, e := unix.FcntlInt(uintptr(task.procfd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	proc := os.NewFile(uintptr(df), "original-ordinary2-stop-proc")
	defer proc.Close()
	source := task.source
	source.PIDFD = pid
	source.ProcDirectory = proc
	copy, e := newNormalSystemOriginalTask(source)
	if e != nil {
		return nil, e
	}
	raw, _ := json.Marshal(filter)
	return &normalSystemOrdinaryV2InstalledControlSource{plan: p, task: copy, observation: observed, filterSHA: physicalDigest(raw)}, nil
}
func (o *normalSystemOrdinaryV2InstalledControlSource) close() {
	if o != nil && o.task != nil {
		o.task.close()
	}
}

func normalSystemRoleV2InspectTarget(ctx context.Context, s *normalSystemLifecycleSource, task *normalSystemOriginalTask, spec normalSystemRoleLaunchSpec) (normalSystemRoleObservation, error) {
	if spec.roleSourceProtocol != 2 {
		return normalSystemRoleObservation{}, ErrUnknown
	}
	if spec.role == "compute" {
		return s.inspectOriginalTask(ctx, task, spec.role, spec.argv, spec.env)
	}
	plan, e := newNormalSystemOrdinaryV2ControlPlan(ctx, s, spec.role)
	if e != nil {
		return normalSystemRoleObservation{}, e
	}
	if spec.uid != plan.rule.UID || spec.gid != plan.rule.GID || !reflect.DeepEqual(spec.argv, plan.rule.Argv) || !reflect.DeepEqual(spec.env, plan.rule.Environment) || spec.binding != plan.binding || spec.sourceSHA != plan.sourceSHA {
		return normalSystemRoleObservation{}, ErrUnknown
	}
	return normalSystemOrdinaryV2ObserveTask(ctx, s, task, plan)
}

// 后继读取不重新PTRACE，也不从当前census创造安装资格。完整filter已在真实
// loader第一条指令前读回；NNP+不可移除seccomp/LL和固定原exec链保留该历史。
func (o *normalSystemOrdinaryV2InstalledControlSource) recheckOriginalRoleControlHistory(ctx context.Context) error {
	if o == nil || o.plan == nil || o.task == nil || !o.observation.OriginalTaskObserved || !o.observation.FixedArgvEnvironmentObserved {
		return ErrUnknown
	}
	if e := o.plan.recheck(ctx); e != nil {
		return e
	}
	raw, _ := json.Marshal(normalKernelControlFilter())
	if physicalDigest(raw) != o.filterSHA || o.observation.Binding != o.plan.binding || o.observation.Role != o.plan.rule.Role {
		return ErrUnknown
	}
	return nil
}
func (o *normalSystemOrdinaryV2InstalledControlSource) recheckOriginalRoleControlLive(ctx context.Context) error {
	if e := o.recheckOriginalRoleControlHistory(ctx); e != nil {
		return e
	}
	observed, e := normalSystemOrdinaryV2ObserveTask(ctx, o.plan.source, o.task, o.plan)
	if e != nil || !reflect.DeepEqual(observed, o.observation) {
		return errors.Join(ErrUnknown, e)
	}
	o.task.mu.Lock()
	defer o.task.mu.Unlock()
	status, e := nativeTraceProcText(o.task.procfd, "status")
	if e != nil {
		return e
	}
	// 服务可创建同域线程，或主动进一步降低cap；不得增加cap、重新获得Root或移除filter。
	for _, key := range []string{"CapEff", "CapPrm", "CapInh", "CapBnd", "CapAmb"} {
		v, e := normalKernelControlStatusHex(status, key)
		if e != nil || v & ^uint64(o.plan.rule.KeepCapabilities) != 0 {
			return ErrUnknown
		}
	}
	for key, min := range map[string]uint64{"Threads": 1, "Seccomp_filters": 1} {
		v, e := nativeTraceStatusValues(status, key, 1)
		if e != nil || v[0] < min {
			return ErrUnknown
		}
	}
	for key, want := range map[string]uint64{"NoNewPrivs": 1, "Seccomp": 2} {
		v, e := nativeTraceStatusValues(status, key, 1)
		if e != nil || v[0] != want {
			return ErrUnknown
		}
	}
	return o.task.verify()
}

// Compute没有普通2 LL/NNP/filter；只证明原固定非Root主任务与stdio出生链。
// 任何PG资源权限仍由原Birth/完整NativeFinal/Root预算另行核验。
type normalSystemRoleV2ComputeControlSource struct {
	source      *normalSystemLifecycleSource
	task        *normalSystemOriginalTask
	spec        normalSystemRoleLaunchSpec
	observation normalSystemRoleObservation
	bounding    uint64
}

func (o *normalSystemRoleV2ComputeControlSource) close() {
	if o != nil && o.task != nil {
		o.task.close()
	}
}
func (o *normalSystemRoleV2ComputeControlSource) recheckOriginalRoleControlHistory(ctx context.Context) error {
	if o == nil || o.source == nil || o.task == nil || o.spec.role != "compute" || o.spec.roleSourceProtocol != 2 {
		return ErrUnknown
	}
	spec, e := normalSystemRoleGateLaunchPlan(ctx, o.source, "compute")
	if e != nil {
		return e
	}
	a, e := spec.referenceSHA256()
	if e != nil {
		return e
	}
	b, e := o.spec.referenceSHA256()
	if e != nil || a != b {
		return ErrUnknown
	}
	return nil
}
func (o *normalSystemRoleV2ComputeControlSource) recheckOriginalRoleControlLive(ctx context.Context) error {
	if e := o.recheckOriginalRoleControlHistory(ctx); e != nil {
		return e
	}
	observed, e := o.source.inspectOriginalTask(ctx, o.task, "compute", o.spec.argv, o.spec.env)
	if e != nil || !reflect.DeepEqual(observed, o.observation) {
		return errors.Join(ErrUnknown, e)
	}
	o.task.mu.Lock()
	defer o.task.mu.Unlock()
	status, e := nativeTraceProcText(o.task.procfd, "status")
	if e != nil {
		return e
	}
	for _, key := range []string{"CapEff", "CapPrm", "CapInh", "CapAmb"} {
		v, e := normalKernelControlStatusHex(status, key)
		if e != nil || v != 0 {
			return ErrUnknown
		}
	}
	b, e := normalKernelControlStatusHex(status, "CapBnd")
	if e != nil || b != o.bounding {
		return ErrUnknown
	}
	for key, want := range map[string]uint64{"NoNewPrivs": 0, "Seccomp": 0} {
		v, e := nativeTraceStatusValues(status, key, 1)
		if e != nil || v[0] != want {
			return ErrUnknown
		}
	}
	return o.task.verify()
}
func inspectNormalSystemRoleV2ControlsAtStop(ctx context.Context, s *normalSystemLifecycleSource, task *normalSystemOriginalTask, spec normalSystemRoleLaunchSpec, control *nativeTraceOriginalControl, tid int) (normalSystemRoleV2ControlWitness, error) {
	if spec.roleSourceProtocol != 2 || control == nil || task == nil || !control.attached || !control.stopped || control.observedExit || control.lastStop.StopSignal() != unix.SIGTRAP || uint32(control.lastStop)>>16 != unix.PTRACE_EVENT_EXEC || unix.Gettid() != tid {
		return nil, ErrUnknown
	}
	if spec.role != "compute" {
		p, e := newNormalSystemOrdinaryV2ControlPlan(ctx, s, spec.role)
		if e != nil {
			return nil, e
		}
		return inspectNormalSystemOrdinaryV2InstalledControls(ctx, s, task, p, control, tid)
	}
	observed, e := normalSystemRoleV2InspectTarget(ctx, s, task, spec)
	if e != nil {
		return nil, e
	}
	if e = arrayMatchPIDFD(control.pidfd, task.pidfd); e != nil {
		return nil, e
	}
	status, e := nativeTraceProcText(task.procfd, "status")
	if e != nil {
		return nil, e
	}
	for key, want := range map[string]uint64{"TracerPid": uint64(tid), "Threads": 1, "NoNewPrivs": 0, "Seccomp": 0} {
		v, e := nativeTraceStatusValues(status, key, 1)
		if e != nil || v[0] != want {
			return nil, ErrUnknown
		}
	}
	for _, key := range []string{"CapEff", "CapPrm", "CapInh", "CapAmb"} {
		v, e := normalKernelControlStatusHex(status, key)
		if e != nil || v != 0 {
			return nil, ErrUnknown
		}
	}
	b, e := normalKernelControlStatusHex(status, "CapBnd")
	if e != nil {
		return nil, e
	}
	original, e := duplicateNormalSystemOriginalTask(task)
	if e != nil {
		return nil, e
	}
	return &normalSystemRoleV2ComputeControlSource{source: s, task: original, spec: spec, observation: observed, bounding: b}, nil
}
