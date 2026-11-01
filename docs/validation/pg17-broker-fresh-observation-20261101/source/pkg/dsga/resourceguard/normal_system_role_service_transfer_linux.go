//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

// SCM接收使用内核MSG_CMSG_CLOEXEC；不是先收普通FD再在另一线程spawn可能
// 竞态的窗口补fcntl。旧版恰好4个、新版恰好5个对象，所有错误都关闭已收FD，不接受额外控制消息。
func normalSystemRoleServiceWriteTransfer(conn *net.UnixConn, t normalSystemRoleServiceTransfer, files []*os.File, ctx context.Context) error {
	if t.validate() != nil || len(files) != t.descriptorCount() {
		return ErrUnknown
	}
	fds := make([]int, len(files))
	for i, f := range files {
		if f == nil {
			return ErrUnknown
		}
		fds[i] = int(f.Fd())
	}
	raw, e := json.Marshal(t)
	if e != nil || len(raw) > normalSystemRoleServiceStartupBytes-12 {
		return fmt.Errorf("original broker transfer exceeds bound")
	}
	frame := make([]byte, normalSystemRoleServiceStartupBytes)
	copy(frame, t.transferMagic())
	binary.BigEndian.PutUint32(frame[8:12], uint32(len(raw)))
	copy(frame[12:], raw)
	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("bounded original transfer required")
	}
	if e = conn.SetWriteDeadline(deadline); e != nil {
		return e
	}
	oob := unix.UnixRights(fds...)
	n, on, e := conn.WriteMsgUnix(frame, oob, nil)
	if e != nil || n != len(frame) || on != len(oob) {
		return errors.Join(ErrUnknown, e, fmt.Errorf("original broker transfer unconfirmed"))
	}
	return nil
}
func normalSystemRoleServiceReadTransfer(conn *net.UnixConn, ctx context.Context) (t normalSystemRoleServiceTransfer, files []*os.File, err error) {
	fail := func(e error) (normalSystemRoleServiceTransfer, []*os.File, error) {
		for _, f := range files {
			f.Close()
		}
		return normalSystemRoleServiceTransfer{}, nil, errors.Join(ErrUnknown, e)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return fail(fmt.Errorf("bounded original transfer required"))
	}
	if e := conn.SetReadDeadline(deadline); e != nil {
		return fail(e)
	}
	raw := make([]byte, normalSystemRoleServiceStartupBytes)
	oob := make([]byte, unix.CmsgSpace(20))
	var n, on, flags int
	var receive error
	fd, e := conn.SyscallConn()
	if e != nil {
		return fail(e)
	}
	e = fd.Read(func(s uintptr) bool {
		n, on, flags, _, receive = unix.Recvmsg(int(s), raw, oob, unix.MSG_DONTWAIT|unix.MSG_CMSG_CLOEXEC)
		return receive != unix.EAGAIN && receive != unix.EWOULDBLOCK
	})
	if e != nil {
		return fail(e)
	}
	if receive != nil {
		return fail(receive)
	}
	controls, e := unix.ParseSocketControlMessage(oob[:on])
	if e != nil {
		return fail(e)
	}
	valid := len(controls) == 1
	for _, c := range controls {
		if c.Header.Level != unix.SOL_SOCKET || c.Header.Type != unix.SCM_RIGHTS {
			valid = false
			continue
		}
		rights, e := unix.ParseUnixRights(&c)
		if e != nil {
			return fail(e)
		}
		for _, r := range rights {
			files = append(files, os.NewFile(uintptr(r), "original broker received kernel object"))
		}
	}
	if !valid || n != len(raw) || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || string(raw[:8]) != "NSYSBF01" && string(raw[:8]) != "NSYSBF02" {
		return fail(fmt.Errorf("original broker fixed FD frame differs"))
	}
	size := int(binary.BigEndian.Uint32(raw[8:12]))
	if size <= 0 || size > len(raw)-12 {
		return fail(fmt.Errorf("original transfer length differs"))
	}
	for _, v := range raw[12+size:] {
		if v != 0 {
			return fail(fmt.Errorf("original transfer padding differs"))
		}
	}
	if e = json.Unmarshal(raw[12:12+size], &t); e != nil {
		return fail(e)
	}
	if e = t.validate(); e != nil {
		return fail(e)
	}
	if len(files) != t.descriptorCount() || string(raw[:8]) != t.transferMagic() {
		return fail(fmt.Errorf("original versioned FD count/header differs"))
	}
	for _, f := range files {
		flags, e := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
		if e != nil || flags&unix.FD_CLOEXEC == 0 {
			return fail(fmt.Errorf("original transfer FD lacks atomic CLOEXEC"))
		}
	}
	return t, files, nil
}

func normalSystemRoleServiceReadGate(state *lockedState, t normalSystemRoleServiceTransfer, stage string, sha string) (normalSystemRoleJournalEntry, error) {
	var out normalSystemRoleJournalEntry
	raw, found, e := state.readPhysicalFile(normalSystemRoleServiceGateName(t.Origin.Nonce, stage))
	if e != nil || !found || physicalDigest(raw) != sha {
		return out, errors.Join(ErrUnknown, e, fmt.Errorf("original broker Gate prefix changed"))
	}
	if e = control.UnmarshalObject(raw, &out); e != nil {
		return out, e
	}
	return out, nil
}
func normalSystemRoleServiceValidateTransferLocked(ctx context.Context, state *lockedState, t normalSystemRoleServiceTransfer) error {
	if ctx == nil || ctx.Err() != nil || t.validate() != nil {
		return ErrUnknown
	}
	var st unix.Stat_t
	if unix.Fstat(int(state.dir.Fd()), &st) != nil || st.Dev != t.Origin.StateDevice || st.Ino != t.Origin.StateInode {
		return fmt.Errorf("original broker state directory changed")
	}
	if _, e := normalSystemRoleServiceReadStage(state, t.Origin, "intent", t.IntentSHA256); e != nil {
		return e
	}
	pending, e := normalSystemRoleServiceReadGate(state, t, normalSystemRoleBrokerGatePending, t.GateSHA256[0])
	if e != nil {
		return e
	}
	stopped, e := normalSystemRoleServiceReadGate(state, t, normalSystemRoleBrokerGateExec, t.GateSHA256[1])
	if e != nil {
		return e
	}
	released, e := normalSystemRoleServiceReadGate(state, t, normalSystemRoleBrokerGateReleased, t.GateSHA256[2])
	if e != nil {
		return e
	}
	if pending.ProtocolVersion != 1 || pending.Scope != normalSystemRoleJournalScope || pending.Stage != "pending" || pending.Nonce != t.Origin.Nonce || pending.PreviousSHA256 != "" || pending.Task != nil || pending.OriginalTaskExitObserved || pending.Role != normalSystemRoleBrokerKind || pending.Binding != t.Origin.Parent || pending.ResourceIdentity != t.Origin.ResourceIdentity || pending.ResourceFloor != t.Origin.ResourceFloor || pending.StateDevice != t.Origin.StateDevice || pending.StateInode != t.Origin.StateInode || !reflect.DeepEqual(pending.Namespace, t.Origin.Namespace) || !normalSystemRoleDeclarationDigest(pending.PlanSHA256) {
		return fmt.Errorf("original broker Gate pending binding differs")
	}
	original := pending
	original.Stage = "exec-stopped"
	original.PreviousSHA256 = t.GateSHA256[0]
	original.Task = &t.Task
	if !reflect.DeepEqual(original, stopped) {
		return fmt.Errorf("original broker Gate exec prefix differs")
	}
	original.Stage = "released"
	original.PreviousSHA256 = t.GateSHA256[1]
	if !reflect.DeepEqual(original, released) {
		return fmt.Errorf("original broker Gate release prefix differs")
	}
	return nil
}

// 只能由固定启动通道、原clone PIDFD+原procFD以及原FS两FD构造。raw receipt、
// 当前self pidfd或numeric PID不能独立生产本对象。持有FD直到broker退出。
type normalSystemRoleBrokerExecProof struct {
	kernelPreparation                              *os.File
	mu                                             sync.Mutex
	closed                                         bool
	system                                         *normalSystemLifecycleSource
	declaration                                    *normalSystemRoleDeclarationSource
	transfer                                       normalSystemRoleServiceTransfer
	namespace, root                                *os.File
	task, parent                                   *normalSystemOriginalTask
	binding                                        normalSystemLifecycleBinding
	witnesses                                      map[string]normalSystemRoleFileWitness
	stateDevice, stateInode, lockDevice, lockInode uint64
}

func (p *normalSystemRoleBrokerExecProof) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		if p.kernelPreparation != nil {
			p.kernelPreparation.Close()
		}
		p.closed = true
		if p.namespace != nil {
			p.namespace.Close()
		}
		if p.root != nil {
			p.root.Close()
		}
		if p.task != nil {
			p.task.close()
		}
		if p.parent != nil {
			p.parent.close()
		}
	}
}
func (p *normalSystemRoleBrokerExecProof) verifyLocked(ctx context.Context, state *lockedState, system *normalSystemLifecycleSource) error {
	_, err := p.verifyOriginalObservationLocked(ctx, state, system)
	return err
}

// 此core每次重新完成原私有事实校验，只返回本调用栈的新Observation，不保存缓存。
// 唯一完整task/ELF检查放在所有声明、权限、WAL和ns/root检查之后。
func (p *normalSystemRoleBrokerExecProof) verifyOriginalObservationLocked(ctx context.Context, state *lockedState, system *normalSystemLifecycleSource) (normalSystemRoleObservation, error) {
	var zero normalSystemRoleObservation
	if p == nil || system == nil || state == nil || p.task == nil || p.declaration == nil || ctx == nil || ctx.Err() != nil {
		return zero, ErrUnknown
	}
	if p.closed {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("original broker proof closed"))
	}
	if system != p.system || system.binding != p.binding {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("original broker proof System differs"))
	}
	current, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || current != p.binding.RootConsumer {
		return zero, fmt.Errorf("original broker consumer changed")
	}
	if e = p.task.verify(); e != nil {
		return zero, e
	}
	if len(p.witnesses) != 4 {
		return zero, fmt.Errorf("original broker durable witnesses incomplete")
	}
	if p.task.source.PID != current.PID || p.task.source.StartTicks != uint64(current.StartTicks) {
		return zero, fmt.Errorf("original broker task is not this process")
	}
	if e = p.declaration.verify(ctx); e != nil {
		return zero, e
	}
	if e = normalSystemRoleServicePrivileges(p.task.procfd); e != nil {
		return zero, e
	}
	var dir, lock unix.Stat_t
	if unix.Fstat(int(state.dir.Fd()), &dir) != nil || unix.Fstat(int(state.lock.Fd()), &lock) != nil || dir.Dev != p.stateDevice || dir.Ino != p.stateInode || lock.Dev != p.lockDevice || lock.Ino != p.lockInode {
		return zero, fmt.Errorf("original broker resource lock object changed")
	}
	if e = normalSystemRoleServiceValidateTransferLocked(ctx, state, p.transfer); e != nil {
		return zero, e
	}
	for name, wanted := range p.witnesses {
		actual, e := readNormalSystemRoleFileWitness(state, name)
		if e != nil || actual != wanted {
			return zero, errors.Join(ErrUnknown, e, fmt.Errorf("original broker durable source prefix changed"))
		}
	}
	ns, e := normalFilesystemFDObject(int(p.namespace.Fd()))
	if e != nil || ns != p.transfer.Origin.Filesystem.Namespace {
		return zero, fmt.Errorf("original broker namespace FD changed")
	}
	root, e := normalFilesystemFDObject(int(p.root.Fd()))
	if e != nil || root != p.transfer.Origin.Filesystem.Root {
		return zero, fmt.Errorf("original broker root FD changed")
	}
	meter := beginNormalBrokerInspectionDEV(ctx)
	observed, e := system.inspectOriginalBrokerTask(ctx, p.task, normalSystemRoleBrokerKind, normalSystemRoleBrokerArgv(), normalSystemRoleBrokerEnvironment())
	meter.finish()
	if e != nil {
		return zero, e
	}
	expected := p.transfer.Task
	expected.Binding = system.binding
	if observed != expected {
		return zero, fmt.Errorf("original broker task differs from original exec-stop observation")
	}
	return observed, nil
}
func (p *normalSystemRoleBrokerExecProof) originalTaskForFilesystem(ctx context.Context, system *normalSystemLifecycleSource) (*normalSystemOriginalTask, error) {
	if p == nil {
		return nil, ErrUnknown
	}
	source, e := sealedRuntimeSource()
	if e != nil {
		return nil, e
	}
	state, e := lockState(ctx, source.Config.StateDir)
	if e != nil {
		return nil, e
	}
	defer state.close()
	return p.originalTaskForFilesystemLocked(ctx, state, system)
}

// FS adapter调用时已经持原flock，不能再次lockState；Root可直接用此私有变体。
func (p *normalSystemRoleBrokerExecProof) originalTaskForFilesystemLocked(ctx context.Context, state *lockedState, system *normalSystemLifecycleSource) (*normalSystemOriginalTask, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	observed, e := p.verifyOriginalObservationLocked(ctx, state, system)
	if e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	p.task.mu.Lock()
	defer p.task.mu.Unlock()
	// 只消费刚完成的完整Observation，仍以原Task/pidfd核身份再dup；不是按PID重开。
	if observed.PID != p.task.source.PID || observed.StartTicks != p.task.source.StartTicks || observed.Binding != system.binding {
		return nil, ErrUnknown
	}
	if e = p.task.verify(); e != nil {
		return nil, e
	}
	source := p.task.source
	pidfd, e := unix.FcntlInt(uintptr(p.task.pidfd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	pid := os.NewFile(uintptr(pidfd), "original broker task handoff pidfd")
	defer pid.Close()
	procfd, e := unix.FcntlInt(uintptr(p.task.procfd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	proc := os.NewFile(uintptr(procfd), "original broker task handoff proc")
	defer proc.Close()
	source.PIDFD = pid
	source.ProcDirectory = proc
	return newNormalSystemOriginalTask(source)
}

func receiveNormalSystemRoleBrokerExecProof(ctx context.Context, g *Guard, system *normalSystemLifecycleSource, declaration *normalSystemRoleDeclarationSource, conn *net.UnixConn) (*normalSystemRoleBrokerExecProof, error) {
	fail := func(e error) (*normalSystemRoleBrokerExecProof, error) { return nil, errors.Join(ErrUnknown, e) }
	if e := normalSystemRoleServiceCaller(); e != nil {
		return fail(e)
	}
	if system == nil || system.manifest.ProtocolVersion != 3 {
		return fail(fmt.Errorf("production broker requires fixed System v3"))
	}
	parent, e := normalSystemRoleServicePeer(conn, system.binding.GuestBootID)
	if e != nil {
		return fail(e)
	}
	accepted := false
	defer func() {
		if !accepted {
			parent.close()
		}
	}()
	parentArgs, e := normalSystemReadVector(parent.procfd, "cmdline")
	if e != nil || len(parentArgs) != 2 || parentArgs[0] != normalSystemRoleBrokerExecutable || parentArgs[1] != "initial-boot" && parentArgs[1] != "recover-initial-boot" {
		return fail(fmt.Errorf("original broker parent action differs"))
	}
	namespace, e := captureNormalSystemRoleNamespaces()
	if e != nil {
		return fail(e)
	}
	defer namespace.close()
	if e = normalSystemRoleServiceVerifyTask(ctx, declaration, namespace, parent, parentArgs); e != nil {
		return fail(e)
	}
	transfer, files, e := normalSystemRoleServiceReadTransfer(conn, ctx)
	if e != nil {
		return fail(e)
	}
	owned := false
	defer func() {
		if !owned {
			for _, f := range files {
				f.Close()
			}
		}
	}()
	if e = transfer.matchesDeclaration(declaration.declaration); e != nil {
		return fail(e)
	}
	if transfer.KernelPreparation != nil && transfer.KernelPreparation.Build != system.manifest.Kernel {
		return fail(ErrUnknown)
	}
	if transfer.Origin.DeclarationSHA256 != declaration.rawSHA || !normalSystemRoleImmutableBindingsEqual(transfer.Origin.Parent, system.binding) || transfer.Origin.Parent.RootConsumer == system.binding.RootConsumer || parent.source.PID != transfer.Origin.Parent.RootConsumer.PID || parent.source.StartTicks != uint64(transfer.Origin.Parent.RootConsumer.StartTicks) || transfer.Task.PID != os.Getpid() || transfer.Task.StartTicks != uint64(system.binding.RootConsumer.StartTicks) {
		return fail(fmt.Errorf("original initialRoot/broker pair differs"))
	}
	task, e := newNormalSystemOriginalTask(normalSystemOriginalTaskSource{PID: transfer.Task.PID, StartTicks: transfer.Task.StartTicks, UID: 0, GID: 0, GuestBootID: transfer.Origin.Parent.GuestBootID, PIDFD: files[0], ProcDirectory: files[1]})
	if e != nil {
		return fail(e)
	}
	files[0].Close()
	files[1].Close()
	proof := &normalSystemRoleBrokerExecProof{system: system, declaration: declaration, transfer: transfer, namespace: files[2], root: files[3], task: task, parent: parent, binding: system.binding, witnesses: map[string]normalSystemRoleFileWitness{}}
	if transfer.ProtocolVersion == 2 {
		proof.kernelPreparation = files[4]
	}
	owned = true
	success := false
	defer func() {
		if !success {
			proof.close()
		}
	}()
	e = g.withRecord(ctx, func(r *record, state *lockedState) error {
		recordRaw, e := json.Marshal(r)
		if e != nil || physicalDigest(recordRaw) != transfer.Origin.ResourceStageSHA256 {
			return fmt.Errorf("original broker complete resource record changed")
		}
		if r.Identity != transfer.Origin.ResourceIdentity || r.Floor != transfer.Origin.ResourceFloor {
			return fmt.Errorf("original broker resource context changed")
		}
		if e := normalSystemRoleServiceValidateTransferLocked(ctx, state, transfer); e != nil {
			return e
		}
		var dir, lock unix.Stat_t
		if unix.Fstat(int(state.dir.Fd()), &dir) != nil || unix.Fstat(int(state.lock.Fd()), &lock) != nil {
			return fmt.Errorf("original broker resource object unavailable")
		}
		proof.stateDevice, proof.stateInode, proof.lockDevice, proof.lockInode = dir.Dev, dir.Ino, lock.Dev, lock.Ino
		names := []string{normalSystemRoleServiceStageName(transfer.Origin.Nonce, "intent"), normalSystemRoleServiceGateName(transfer.Origin.Nonce, normalSystemRoleBrokerGatePending), normalSystemRoleServiceGateName(transfer.Origin.Nonce, normalSystemRoleBrokerGateExec), normalSystemRoleServiceGateName(transfer.Origin.Nonce, normalSystemRoleBrokerGateReleased)}
		// 显式原Root移交接收阶段：输入/FD/引用先核，再逐file+dir sync重建新consumer
		// 的私有durability见证；ordinary角色请求不执行这些写入/补签。
		for _, name := range names {
			if e := state.syncPhysicalFile(name); e != nil {
				return e
			}
			w, e := readNormalSystemRoleFileWitness(state, name)
			if e != nil {
				return e
			}
			proof.witnesses[name] = w
		}
		return proof.verifyLocked(ctx, state, system)
	})
	if e != nil {
		return fail(e)
	}
	accepted = true
	success = true
	return proof, nil
}

// 仅DEV ctx可安装的固定字段聚合，没有可替换函数或业务选择器。生产Background无此值。
// rchar是整个R2进程逻辑读取，包含采样自身开销与其它goroutine，绝非exact ELF io.Copy n。
type normalBrokerInspectionDEVTraceKey struct{}
type normalBrokerInspectionDEVSummary struct {
	Calls                  uint64 `json:"calls"`
	DurationNS             uint64 `json:"duration_ns"`
	WholeProcessRcharDelta uint64 `json:"whole_R2_rchar_delta"`
	MissingIOSamples       uint64 `json:"missing_or_nonmonotonic_io_samples"`
}
type normalBrokerInspectionDEVTrace struct {
	mu      sync.Mutex
	summary normalBrokerInspectionDEVSummary
}

func (t *normalBrokerInspectionDEVTrace) snapshot() normalBrokerInspectionDEVSummary {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.summary
}

type normalBrokerInspectionDEVMeter struct {
	trace   *normalBrokerInspectionDEVTrace
	started time.Time
	before  uint64
	sampled bool
}

func normalBrokerInspectionDEVReadChars() (uint64, bool) {
	f, e := os.Open("/proc/self/io")
	if e != nil {
		return 0, false
	}
	defer f.Close()
	raw, e := nativeBirthReadBounded(f, 4096)
	if e != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "rchar:" {
			n, e := strconv.ParseUint(fields[1], 10, 64)
			return n, e == nil
		}
	}
	return 0, false
}
func beginNormalBrokerInspectionDEV(ctx context.Context) normalBrokerInspectionDEVMeter {
	trace, _ := ctx.Value(normalBrokerInspectionDEVTraceKey{}).(*normalBrokerInspectionDEVTrace)
	if trace == nil {
		return normalBrokerInspectionDEVMeter{}
	}
	before, ok := normalBrokerInspectionDEVReadChars()
	return normalBrokerInspectionDEVMeter{trace: trace, started: time.Now(), before: before, sampled: ok}
}
func (m normalBrokerInspectionDEVMeter) finish() {
	if m.trace == nil {
		return
	}
	elapsed := uint64(time.Since(m.started).Nanoseconds())
	after, ok := normalBrokerInspectionDEVReadChars()
	t := m.trace
	t.mu.Lock()
	defer t.mu.Unlock()
	add := func(value *uint64, n uint64) {
		if ^uint64(0)-*value < n {
			*value = ^uint64(0)
		} else {
			*value += n
		}
	}
	add(&t.summary.Calls, 1)
	add(&t.summary.DurationNS, elapsed)
	if !ok || !m.sampled || after < m.before {
		add(&t.summary.MissingIOSamples, 1)
	} else {
		add(&t.summary.WholeProcessRcharDelta, after-m.before)
	}
}
