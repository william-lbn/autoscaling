//go:build linux

package resourceguard

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
)

const normalRoleStdioRingBytes = 32 << 10
const normalRoleStdioReadBytes = 4 << 10
const normalRoleStdioMaxRoles = 10

// 有界收集器不调用任意sink、不生成路径，也不把目标日志正文写入来源/WAL。
// 此处的32KiB只界定本缓冲，不声称包含Go栈/堆或pipe全部内核成本。
type normalRoleStdioDrain struct {
	mu           sync.Mutex
	ring         [normalRoleStdioRingBytes]byte
	next, filled int
	total        uint64
	done         chan struct{}
	err          error
	sink         *os.File
	sinkErr      error
}

func (d *normalRoleStdioDrain) append(raw []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if uint64(len(raw)) > ^uint64(0)-d.total {
		d.total = ^uint64(0)
	} else {
		d.total += uint64(len(raw))
	}
	for len(raw) > 0 {
		n := copy(d.ring[d.next:], raw)
		d.next = (d.next + n) % len(d.ring)
		d.filled += n
		if d.filled > len(d.ring) {
			d.filled = len(d.ring)
		}
		raw = raw[n:]
	}
}
func (d *normalRoleStdioDrain) summary() (uint64, int, [32]byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	h := sha256.New()
	if d.filled < len(d.ring) {
		h.Write(d.ring[:d.filled])
	} else {
		h.Write(d.ring[d.next:])
		h.Write(d.ring[:d.next])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return d.total, d.filled, out
}
func (d *normalRoleStdioDrain) run(f *os.File) {
	defer close(d.done)
	var block [normalRoleStdioReadBytes]byte
	for {
		n, e := f.Read(block[:])
		if n > 0 {
			d.append(block[:n])
			// 此goroutine不持resource/factory锁；只向typed factory原console写有界块。
			if d.sink != nil {
				_, se := d.sink.Write(block[:n])
				if se != nil {
					d.mu.Lock()
					d.sinkErr = se
					d.mu.Unlock()
				}
			}
		}
		if e != nil {
			d.mu.Lock()
			d.err = e
			d.mu.Unlock()
			return
		}
	}
}

// objects只有本进程真实新建的/dev/null OFD和两条pipe端点。独立创建不产
// Source；只有原R2 factory纳管、第二exec-stop核同OFD后才有stdio-only来源。
type normalRoleStdioObjects struct {
	mu        sync.Mutex
	closed    bool
	originals [3]*os.File
	readers   [2]*os.File
	drains    [2]*normalRoleStdioDrain
	closeOnce sync.Once
}

func newNormalRoleStdioObjects() (*normalRoleStdioObjects, error) {
	return newNormalRoleStdioObjectsForRootSink(nil)
}

// sink只由typed factory传入其原固定console；nil仅用于不产Source的机制测试。
func newNormalRoleStdioObjectsForRootSink(sink *os.File) (out *normalRoleStdioObjects, err error) {
	out = &normalRoleStdioObjects{}
	ok := false
	defer func() {
		if !ok {
			out.close()
		}
	}()
	fd, e := unix.Open("/dev/null", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	out.originals[0] = os.NewFile(uintptr(fd), "original-Root-stdin-null")
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFCHR || unix.Major(uint64(st.Rdev)) != 1 || unix.Minor(uint64(st.Rdev)) != 3 {
		return nil, ErrUnknown
	}
	for i := 0; i < 2; i++ {
		var pair [2]int
		if e = unix.Pipe2(pair[:], unix.O_CLOEXEC); e != nil {
			return nil, e
		}
		// 只有读端改NONBLOCK供Go poller取消；目标写端保持普通阻塞stdio语义。
		if _, e = unix.FcntlInt(uintptr(pair[0]), unix.F_SETFL, unix.O_NONBLOCK); e != nil {
			unix.Close(pair[0])
			unix.Close(pair[1])
			return nil, e
		}
		out.readers[i] = os.NewFile(uintptr(pair[0]), "original-Root-log-read")
		out.originals[i+1] = os.NewFile(uintptr(pair[1]), "original-Root-log-write")
		capacity, e := unix.FcntlInt(uintptr(pair[0]), unix.F_GETPIPE_SZ, 0)
		if e != nil || capacity <= 0 || capacity > 64<<10 {
			return nil, errors.Join(ErrUnknown, e)
		}
		out.drains[i] = &normalRoleStdioDrain{done: make(chan struct{}), sink: sink}
	}
	for i := range out.readers {
		go out.drains[i].run(out.readers[i])
	}
	ok = true
	return out, nil
}
func (o *normalRoleStdioObjects) close() {
	if o == nil {
		return
	}
	o.closeOnce.Do(func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.closed = true
		for _, f := range o.originals {
			if f != nil {
				f.Close()
			}
		}
		for _, f := range o.readers {
			if f != nil {
				f.Close()
			}
		}
	})
}
func (o *normalRoleStdioObjects) spawnFiles() ([3]*os.File, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out [3]*os.File
	if o.closed {
		return out, ErrUnknown
	}
	for i, f := range o.originals {
		if f == nil {
			closeNormalRoleStdioFiles(out)
			return out, ErrUnknown
		}
		n, e := unix.FcntlInt(f.Fd(), unix.F_DUPFD_CLOEXEC, 0)
		if e != nil {
			closeNormalRoleStdioFiles(out)
			return out, e
		}
		out[i] = os.NewFile(uintptr(n), "original-Root-stdio-spawn-dup")
	}
	return out, nil
}
func closeNormalRoleStdioFiles(a [3]*os.File) {
	for _, f := range a {
		if f != nil {
			f.Close()
		}
	}
}

// 工厂只来自本Root进程原同g已COMMIT Authority及其原4FD owner；没有Config、
// 路径、SELF pidfd或raw bool重建入口。后继Guard应把本对象持至原role退出处理完。
type normalSystemRoleStdioFactory struct {
	mu         sync.Mutex
	authority  *normalSystemResourceAuthority
	filesystem *normalFilesystemOriginSource
	proof      *normalSystemRoleBrokerExecProof
	originalFS normalFilesystemOriginReference
	console    *os.File
	actor      *normalSystemOriginalTask
	roles      map[string]*normalSystemRoleStdio
	closed     atomic.Bool
}
type normalSystemRoleStdio struct {
	factory     *normalSystemRoleStdioFactory
	role, nonce string
	objects     *normalRoleStdioObjects
	closed      bool
	retired     bool
	installed   *normalSystemRoleInstalledMain
}

func newNormalSystemRoleStdioFactory(ctx context.Context, g *Guard, filesystem *normalFilesystemOriginSource, proof *normalSystemRoleBrokerExecProof) (*normalSystemRoleStdioFactory, error) {
	if ctx == nil || ctx.Err() != nil || g == nil || filesystem == nil || proof == nil {
		return nil, ErrUnknown
	}
	if e := normalSystemRoleServiceCaller(); e != nil {
		return nil, e
	}
	var out *normalSystemRoleStdioFactory
	e := g.withRecord(ctx, func(r *record, s *lockedState) error {
		a := lookupNormalSystemAuthority(g)
		if a == nil || a.admission.failed.Load() || proof.system != a.source || !reflect.DeepEqual(proof.transfer.Origin, a.origin) {
			return ErrUnknown
		}
		originalFS, e := filesystem.referenceLocked(ctx, s)
		if e != nil || !reflect.DeepEqual(originalFS, a.origin.Filesystem) {
			return errors.Join(ErrUnknown, e)
		}
		actor, e := proof.originalTaskForFilesystemLocked(ctx, s, a.source)
		if e != nil {
			return e
		}
		f := &normalSystemRoleStdioFactory{authority: a, actor: actor, filesystem: filesystem, proof: proof, originalFS: originalFS, roles: map[string]*normalSystemRoleStdio{}}
		if e = f.checkExternalLocked(ctx, s, r, false); e != nil {
			actor.close()
			return e
		}
		console, e := f.openOriginalConsole()
		if e != nil {
			actor.close()
			return e
		}
		f.console = console
		out = f
		return nil
	})
	if e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	return out, nil
}

// 调用者已持resource锁；先FS/proof核验后才能取得factory.mu，避免反调FS。
// candidate只可建工厂，不可准备角色；prepare/exec-stop固定传requireCommit=true。
func (f *normalSystemRoleStdioFactory) checkExternalLocked(ctx context.Context, s *lockedState, r *record, requireCommit bool) error {
	if f == nil || f.closed.Load() || f.authority == nil || f.actor == nil || ctx == nil || ctx.Err() != nil || s == nil || r == nil {
		return ErrUnknown
	}
	a := f.authority
	if a.admission.failed.Load() || requireCommit && !a.admission.businessAllowed() || r.Identity != a.origin.ResourceIdentity || r.Floor < a.origin.ResourceFloor || r.Bootstrap == nil || !r.Bootstrap.Applied || !r.Bootstrap.SystemPlacementObserved {
		return ErrUnknown
	}
	fsref, e := f.filesystem.referenceLocked(ctx, s)
	if e != nil || !reflect.DeepEqual(fsref, f.originalFS) {
		return errors.Join(ErrUnknown, e)
	}
	actor, e := f.proof.originalTaskForFilesystemLocked(ctx, s, a.source)
	if e != nil {
		return e
	}
	defer actor.close()
	if e = arrayMatchPIDFD(actor.pidfd, f.actor.pidfd); e != nil {
		return e
	}
	nsfd, e := unix.Open("/proc/thread-self/ns/mnt", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	nsobj, e := normalFilesystemFDObject(nsfd)
	unix.Close(nsfd)
	if e != nil || nsobj != f.originalFS.Namespace {
		return ErrUnknown
	}
	rootfd, e := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	rootobj, e := normalFilesystemFDObject(rootfd)
	unix.Close(rootfd)
	if e != nil || rootobj != f.originalFS.Root {
		return ErrUnknown
	}
	current, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || current != a.source.binding.RootConsumer {
		return ErrUnknown
	}
	var st unix.Stat_t
	if unix.Fstat(int(s.dir.Fd()), &st) != nil || st.Dev != a.origin.StateDevice || st.Ino != a.origin.StateInode {
		return ErrUnknown
	}
	f.actor.mu.Lock()
	defer f.actor.mu.Unlock()
	return f.actor.verify()
}
func normalRoleStdioFixedRole(role string) bool {
	switch role {
	case "allow-start", "kernel-version", "network", "udev", "udev-init", "acpi", "logs", "clock", "daemon", "compute":
		return true
	}
	return false
}
func (f *normalSystemRoleStdioFactory) prepare(ctx context.Context, role string) (*normalSystemRoleStdio, error) {
	if f == nil || f.authority == nil {
		return nil, ErrUnknown
	}
	var out *normalSystemRoleStdio
	e := f.authority.guard.withRecord(ctx, func(r *record, s *lockedState) error {
		var e error
		out, e = f.prepareOriginalLocked(ctx, s, r, role)
		return e
	})
	return out, e
}
func (f *normalSystemRoleStdioFactory) prepareOriginalLocked(ctx context.Context, s *lockedState, r *record, role string) (*normalSystemRoleStdio, error) {
	if f == nil || f.authority == nil || !normalRoleStdioFixedRole(role) {
		return nil, ErrUnknown
	}
	if e := f.checkExternalLocked(ctx, s, r, true); e != nil {
		return nil, e
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed.Load() {
		return nil, ErrUnknown
	}
	if old := f.roles[role]; old != nil {
		// 只有原Wait/creator退役已确认且全部管道读到EOF的旧代才能重生。
		if !old.retired || old.objects == nil {
			return nil, ErrUnknown
		}
		for _, d := range old.objects.drains {
			select {
			case <-d.done:
			default:
				return nil, ErrUnknown
			}
			d.mu.Lock()
			e := d.err
			d.mu.Unlock()
			if !errors.Is(e, io.EOF) {
				return nil, ErrUnknown
			}
		}
		old.objects.close()
	} else if len(f.roles) >= normalRoleStdioMaxRoles {
		return nil, ErrUnknown
	}
	nonce, e := newNativeBirthNonce()
	if e != nil {
		return nil, e
	}
	objects, e := newNormalRoleStdioObjectsForRootSink(f.console)
	if e != nil {
		return nil, e
	}
	out := &normalSystemRoleStdio{factory: f, role: role, nonce: nonce, objects: objects}
	f.roles[role] = out
	return out, nil
}
func (f *normalSystemRoleStdioFactory) close() {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed.Load() {
		return
	}
	f.closed.Store(true)
	for _, p := range f.roles {
		p.closed = true
		p.objects.close()
	}
	f.actor.close()
	if f.console != nil {
		f.console.Close()
	}
}

// 这是stdio-only原对象事实，不能替代role lease、控制安装、PG Birth或normal资格。
type normalSystemRoleOriginalStdio struct {
	factory     *normalSystemRoleStdioFactory
	endpoints   *normalSystemRoleStdio
	task        *normalSystemOriginalTask
	nonce, role string
}

func (o *normalSystemRoleOriginalStdio) close() {
	if o != nil && o.task != nil {
		o.task.close()
	}
}
func normalRoleStdioSameOFD(held, other int) error {
	n, _, e := unix.Syscall6(unix.SYS_KCMP, uintptr(os.Getpid()), uintptr(os.Getpid()), 0, uintptr(held), uintptr(other), 0)
	if e != 0 {
		return errors.Join(ErrUnknown, e)
	}
	if n != 0 {
		return errors.Join(ErrUnknown, fmt.Errorf("stdio OFD differs"))
	}
	return nil
}
func normalRoleStdioOnlyThreeFDs(procfd int) error {
	fd, e := unix.Openat(procfd, "fd", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	d := os.NewFile(uintptr(fd), "original-target-stdio-only-directory")
	defer d.Close()
	names, e := d.Readdirnames(4)
	if e != nil && !errors.Is(e, io.EOF) || len(names) != 3 {
		return errors.Join(ErrUnknown, e)
	}
	seen := [3]bool{}
	for _, name := range names {
		n, e := strconv.Atoi(name)
		if e != nil || n < 0 || n > 2 || seen[n] {
			return ErrUnknown
		}
		seen[n] = true
	}
	return nil
}
func (p *normalSystemRoleStdio) inspectOriginalStoppedTargetLocked(ctx context.Context, state *lockedState, record *record, control *nativeTraceOriginalControl, task *normalSystemOriginalTask) (*normalSystemRoleOriginalStdio, error) {
	if p == nil || p.factory == nil || task == nil || control == nil {
		return nil, ErrUnknown
	}
	f := p.factory
	var out *normalSystemRoleOriginalStdio
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if e := f.checkExternalLocked(ctx, state, record, true); e != nil {
		return nil, e
	}
	e := func() error {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.closed.Load() {
			return ErrUnknown
		}
		if p.closed || f.roles[p.role] != p || !control.stopped || control.observedExit || control.pid != task.source.PID || control.lastStop.StopSignal() != unix.SIGTRAP || uint32(control.lastStop)>>16 != unix.PTRACE_EVENT_EXEC {
			return ErrUnknown
		}
		task.mu.Lock()
		defer task.mu.Unlock()
		if e := task.verify(); e != nil {
			return e
		}
		if e := arrayMatchPIDFD(control.pidfd, task.pidfd); e != nil {
			return e
		}
		status, e := nativeTraceProcText(task.procfd, "status")
		if e != nil {
			return e
		}
		tracer, e := nativeTraceStatusValues(status, "TracerPid", 1)
		if e != nil || tracer[0] != uint64(unix.Gettid()) {
			return ErrUnknown
		}
		threads, e := nativeTraceStatusValues(status, "Threads", 1)
		if e != nil || threads[0] != 1 {
			return ErrUnknown
		}
		if e = normalRoleStdioOnlyThreeFDs(task.procfd); e != nil {
			return e
		}
		for i, held := range p.objects.originals {
			dup, e := unix.PidfdGetfd(task.pidfd, i, 0)
			if e != nil {
				return e
			}
			e = normalRoleStdioSameOFD(int(held.Fd()), dup)
			unix.Close(dup)
			if e != nil {
				return e
			}
		}
		if e = task.verify(); e != nil {
			return e
		}
		// task.mu不能递归进入duplicate；原fd先dup，构造器在下方独立阶段再核。
		pf, e := unix.FcntlInt(uintptr(task.pidfd), unix.F_DUPFD_CLOEXEC, 0)
		if e != nil {
			return e
		}
		pidfile := os.NewFile(uintptr(pf), "original-stdio-task-pidfd")
		defer pidfile.Close()
		df, e := unix.FcntlInt(uintptr(task.procfd), unix.F_DUPFD_CLOEXEC, 0)
		if e != nil {
			return e
		}
		procfile := os.NewFile(uintptr(df), "original-stdio-task-procfd")
		defer procfile.Close()
		source := task.source
		source.PIDFD = pidfile
		source.ProcDirectory = procfile
		// os.File拥有临时FD，避免其finalizer与裸Close双重关闭。
		copy, e := newNormalSystemOriginalTask(source)
		if e != nil {
			return e
		}
		out = &normalSystemRoleOriginalStdio{factory: f, endpoints: p, task: copy, nonce: p.nonce, role: p.role}
		return nil
	}()
	if e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	return out, nil
}

// 只在已核原typedFS和exec proof的R2 namespace打开固定console；不是沿用stderr。
// Sink只有Root持有，不作为任何target stdio；写日志不得发生在resource锁内。
func (f *normalSystemRoleStdioFactory) openOriginalConsole() (*os.File, error) {
	f.actor.mu.Lock()
	defer f.actor.mu.Unlock()
	raw, e := nativeTraceProcText(f.actor.procfd, "mountinfo")
	if e != nil {
		return nil, e
	}
	mounts, e := normalFilesystemParseMounts([]byte(raw))
	if e != nil {
		return nil, e
	}
	found := false
	for _, m := range mounts {
		if m.Point == "/dev" {
			if m.FSType != "devtmpfs" {
				return nil, ErrUnknown
			}
			found = true
		}
	}
	if !found {
		return nil, ErrUnknown
	}
	fd, e := unix.Open("/dev/console", unix.O_WRONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	if e != nil {
		return nil, e
	}
	file := os.NewFile(uintptr(fd), "original-Root-fixed-console-sink")
	good := false
	defer func() {
		if !good {
			file.Close()
		}
	}()
	var st unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(fd, &st) != nil || unix.Fstatfs(fd, &fs) != nil || fs.Type != unix.TMPFS_MAGIC || st.Uid != 0 || st.Mode&unix.S_IFMT != unix.S_IFCHR || unix.Major(uint64(st.Rdev)) != 5 || unix.Minor(uint64(st.Rdev)) != 1 {
		return nil, ErrUnknown
	}
	flags, e := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if e != nil || flags&unix.O_ACCMODE != unix.O_WRONLY || flags&unix.O_NONBLOCK == 0 {
		return nil, ErrUnknown
	}
	clo, e := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if e != nil || clo&unix.FD_CLOEXEC == 0 {
		return nil, ErrUnknown
	}
	good = true
	return file, nil
}

// 已有真实stop witness才可重核；历史路径不要求已退出task继续alive，也不重开OFD。
func (o *normalSystemRoleOriginalStdio) recheckOriginalHistoryLocked(ctx context.Context, state *lockedState) error {
	if o == nil || o.factory == nil || o.endpoints == nil || o.task == nil || state == nil {
		return ErrUnknown
	}
	return o.recheckOriginalFactoryLocked(ctx, state)
}
func (o *normalSystemRoleOriginalStdio) recheckOriginalFactoryLocked(ctx context.Context, state *lockedState) error {
	if o == nil || o.factory == nil || o.endpoints == nil || o.task == nil {
		return ErrUnknown
	}
	f := o.factory
	// 此检查不重新读取业务record；当前sameg/floor由Installed/P1原resource读者逐次核。
	if f.closed.Load() || f.authority == nil || !f.authority.admission.businessAllowed() {
		return ErrUnknown
	}
	fs, e := f.filesystem.referenceLocked(ctx, state)
	if e != nil || !reflect.DeepEqual(fs, f.originalFS) {
		return errors.Join(ErrUnknown, e)
	}
	actor, e := f.proof.originalTaskForFilesystemLocked(ctx, state, f.authority.source)
	if e != nil {
		return e
	}
	defer actor.close()
	if e = arrayMatchPIDFD(actor.pidfd, f.actor.pidfd); e != nil {
		return e
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if (!o.endpoints.retired && f.roles[o.role] != o.endpoints) || o.nonce != o.endpoints.nonce || o.endpoints.closed || o.endpoints.objects == nil || (!o.endpoints.retired && o.endpoints.objects.closed) {
		return ErrUnknown
	}
	return nil
}
func (o *normalSystemRoleOriginalStdio) recheckOriginalLiveLocked(ctx context.Context, state *lockedState) error {
	if e := o.recheckOriginalFactoryLocked(ctx, state); e != nil {
		return e
	}
	o.task.mu.Lock()
	defer o.task.mu.Unlock()
	if e := o.task.verify(); e != nil {
		return e
	}
	for n, held := range o.endpoints.objects.originals {
		fd, e := unix.PidfdGetfd(o.task.pidfd, n, 0)
		if e != nil {
			return e
		}
		e = normalRoleStdioSameOFD(int(held.Fd()), fd)
		unix.Close(fd)
		if e != nil {
			return e
		}
	}
	return o.task.verify()
}

// 只能由原run在唯一Wait及原creator真正退役之后、同flock的exited fsync调用。
// 先关Root持写端，让仍持管道的后代阻止EOF；不借main退出声称后代或物理页已free。
func (o *normalSystemRoleOriginalStdio) completeOriginalWaitLocked(ctx context.Context, state *lockedState) error {
	if o == nil || o.endpoints == nil || o.endpoints.installed == nil {
		return ErrUnknown
	}
	i := o.endpoints.installed
	if _, e := i.child.originalRoleWaitConfirmed(i.creator); e != nil {
		return e
	}
	if e := o.recheckOriginalHistoryLocked(ctx, state); e != nil {
		return e
	}
	e := func() error {
		f := o.factory
		f.mu.Lock()
		defer f.mu.Unlock()
		if o.endpoints.retired {
			return nil
		}
		if f.roles[o.role] != o.endpoints {
			return ErrUnknown
		}
		o.endpoints.objects.mu.Lock()
		for _, file := range o.endpoints.objects.originals {
			if file != nil {
				file.Close()
			}
		}
		o.endpoints.objects.mu.Unlock()
		o.endpoints.retired = true
		return nil
	}()
	if e != nil {
		return e
	}
	return normalSystemRoleV2CheckedCheckpoint(ctx, "wait-confirmed", i.publication)
}
