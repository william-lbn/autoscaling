//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

type nativeTraceThreadMeta struct {
	TID                 int
	Retired, Unexpected bool
}
type nativeTraceTypedWorkerBody[T any] func(int) (T, error, bool)
type nativeTraceTypedWorkerResult[T any] struct {
	Value  T
	Err    error
	Thread *os.File
	Meta   nativeTraceThreadMeta
}

// Birth和固定helper共用此唯一专用M寿命实现。必须避开m0；失败不得把仍有
// ptrace关系的M还给普通调度器。返回channel后仍须核原thread对象实际消失。
func nativeTraceRunTypedLockedWorker[T any](body func(int) (T, error, bool)) (T, error, nativeTraceThreadMeta) {
	results := make(chan nativeTraceTypedWorkerResult[T], 1)
	go nativeTraceTypedWorker(body, results, nil)
	result := <-results
	if result.Thread != nil {
		defer result.Thread.Close()
		until := time.Now().Add(5 * time.Second)
		for {
			_, err := nativeTraceProcText(int(result.Thread.Fd()), "status")
			if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ESRCH) {
				result.Meta.Retired = true
				break
			}
			if err != nil || !time.Now().Before(until) {
				result.Err = errors.Join(result.Err, ErrUnknown, fmt.Errorf("original tracer thread retirement not confirmed"))
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
	return result.Value, result.Err, result.Meta
}
func nativeTraceTypedWorker[T any](body func(int) (T, error, bool), results chan<- nativeTraceTypedWorkerResult[T], ready chan<- struct{}) {
	runtime.LockOSThread()
	if unix.Gettid() == os.Getpid() {
		started := make(chan struct{})
		go nativeTraceTypedWorker(body, results, started)
		<-started
		runtime.UnlockOSThread()
		return
	}
	if ready != nil {
		close(ready)
	}
	var thread *os.File
	sent := false
	defer func() {
		_ = recover()
		if !sent {
			if thread == nil {
				runtime.UnlockOSThread()
			}
			results <- nativeTraceTypedWorkerResult[T]{Err: errors.Join(ErrUnknown, fmt.Errorf("unexpected native tracer worker termination")), Thread: thread, Meta: nativeTraceThreadMeta{TID: unix.Gettid(), Unexpected: true}}
		}
	}()
	if body == nil {
		runtime.UnlockOSThread()
		sent = true
		results <- nativeTraceTypedWorkerResult[T]{Err: fmt.Errorf("original tracer body required")}
		return
	}
	if err := nativeTraceCaller(); err != nil {
		runtime.UnlockOSThread()
		sent = true
		results <- nativeTraceTypedWorkerResult[T]{Err: err}
		return
	}
	fd, err := unix.Open("/proc/thread-self", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		runtime.UnlockOSThread()
		sent = true
		results <- nativeTraceTypedWorkerResult[T]{Err: err}
		return
	}
	thread = os.NewFile(uintptr(fd), "original-native-tracer-thread")
	value, err, retire := body(unix.Gettid())
	result := nativeTraceTypedWorkerResult[T]{Value: value, Err: err, Meta: nativeTraceThreadMeta{TID: unix.Gettid()}}
	if !retire {
		_ = thread.Close()
		runtime.UnlockOSThread()
	} else {
		result.Thread = thread
	}
	// retire时故意不Unlock；非leader G返回让runtime退出该M，外层再核TID终态。
	sent = true
	results <- result
}

// 仅承载原pidfd绑定的通用ptrace状态。Birth和helper调用同一套实现，不能
// 对未确认numeric目标设置EXITKILL、发kill或fallback到Wait4/P_PID。
type nativeTraceOriginalControl struct {
	pid                             int
	uid                             uint32
	pidfd                           int
	attached, stopped, observedExit bool
	undeliveredSignal               int
	lastStop                        unix.WaitStatus
}

// 以下控制方法从已审Birth tracer逐方法抽出；Birth保留同名promoted方法。
func (k *nativeTraceOriginalControl) seize() error {
	if err := nativeTracePtrace(unix.PTRACE_SEIZE, k.pid, 0); err != nil {
		return err
	}
	k.attached = true
	return nil
}
func (k *nativeTraceOriginalControl) interrupt() error {
	return nativeTracePtrace(unix.PTRACE_INTERRUPT, k.pid, 0)
}
func (k *nativeTraceOriginalControl) exitKill() error {
	return nativeTracePtrace(unix.PTRACE_SETOPTIONS, k.pid, unix.PTRACE_O_EXITKILL)
}
func (k *nativeTraceOriginalControl) cont() error {
	if err := nativeTracePtrace(unix.PTRACE_CONT, k.pid, 0); err != nil {
		return err
	}
	k.stopped = false
	return nil
}
func (k *nativeTraceOriginalControl) detach() error {
	if err := nativeTracePtrace(unix.PTRACE_DETACH, k.pid, 0); err != nil {
		return err
	}
	k.attached, k.stopped = false, false
	return nil
}

// 不确认numeric新target就绝不kill/EXITKILL；只尝试停止后detach。期限不确定则
// 退役专用M，让kernel auto-detach。确认原对象后只能原peer pidfd发SIGKILL，
// 严格wait exit；发出kill而未见exit仍是Unknown，账本不得据此归零或重放。
func (k *nativeTraceOriginalControl) cleanup(ctx context.Context, confirmed bool) (bool, string, error) {
	if !k.attached || k.observedExit {
		return false, "original-exit-observed", nil
	}
	if !confirmed {
		if !k.stopped {
			if err := k.interrupt(); err == nil {
				_ = k.waitAnyStop(ctx)
			}
		}
		if k.observedExit {
			return false, "tentative-exit-observed", nil
		}
		if k.stopped && k.undeliveredSignal != int(unix.SIGKILL) {
			// 不确定target的真实signal-delivery-stop必须保留原信号，
			// 不能用最终原生门禁的detach0规则吞掉其SIGTERM等信号。
			if err := nativeTracePtrace(unix.PTRACE_DETACH, k.pid, uintptr(k.undeliveredSignal)); err == nil {
				k.attached, k.stopped = false, false
				return false, "tentative-detached-without-kill", nil
			}
		}
		return true, "tentative-retire-without-exitkill", fmt.Errorf("tentative detach unconfirmed; retire original tracer thread")
	}
	err := unix.PidfdSendSignal(k.pidfd, unix.SIGKILL, nil, 0)
	if err != nil && !errors.Is(err, unix.ESRCH) {
		return true, "original-pidfd-kill-unknown", err
	}
	for {
		if err := ctx.Err(); err != nil {
			return true, "original-pidfd-kill-exit-unconfirmed", err
		}
		pid, status, err := k.waitOriginal()
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return true, "original-pidfd-kill-wait-unknown", err
		}
		if pid == 0 {
			time.Sleep(time.Millisecond)
			continue
		}
		if pid != k.pid {
			return true, "original-wait-identity-unknown", fmt.Errorf("unexpected original wait target")
		}
		if status.Exited() || status.Signaled() {
			k.observedExit = true
			k.attached = false
			return false, "original-pidfd-killed-and-exit-observed", nil
		}
		// 不通过numeric PID发kill。原pidfd上的SIGKILL在内核排队，stop/迟到事件
		// 不是退出证明；只继续读wait，期限到则Unknown且退役原tracer M。
	}
}
func (k *nativeTraceOriginalControl) waitAnyStop(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pid, status, err := k.waitOriginal()
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return err
		}
		if pid == 0 {
			time.Sleep(time.Millisecond)
			continue
		}
		if status.Exited() || status.Signaled() {
			k.observedExit = true
			k.attached = false
			return nil
		}
		if status.Stopped() {
			k.stopped = true
			k.rememberStop(status)
			return nil
		}
		return fmt.Errorf("unknown tentative trace wait state")
	}
}

// SEIZE的group/event stop带非零高16位；只有signal-delivery-stop保留信号。
func (k *nativeTraceOriginalControl) rememberStop(status unix.WaitStatus) {
	k.lastStop = status
	k.undeliveredSignal = 0
	if status.Stopped() && uint32(status)>>16 == 0 {
		k.undeliveredSignal = int(status.StopSignal())
	}
}

func (k *nativeTraceOriginalControl) waitOriginal() (int, unix.WaitStatus, error) {
	return nativePollOriginalWait(k.pidfd, k.pid, k.uid,
		func(which, id int, info *[128]byte, options int) error {
			_, _, errno := unix.Syscall6(unix.SYS_WAITID, uintptr(which), uintptr(id),
				uintptr(unsafe.Pointer(&info[0])), uintptr(options), 0, 0)
			runtime.KeepAlive(info)
			if errno != 0 {
				return errno
			}
			return nil
		})
}

// 这是Root内部固定来源，不接HTTP/配置帧。PIDFD必须来自原spawn CLONE_PIDFD
// 或原SO_PEERPIDFD；ProcDirectory是当时钉住的proc对象，禁止numeric重开替代。
type nativeTraceOriginalTargetSource struct {
	PID                              int
	StartTicks                       uint64
	UID, GID                         uint32
	GuestBootID                      string
	PIDFD, ProcDirectory, Executable *os.File
	ExecutableSHA256                 [32]byte
}
type nativeTraceOriginalStoppedTarget struct {
	nativeTraceOriginalControl
	source     nativeTraceOriginalTargetSource
	procfd     int
	executable *os.File
	mu         sync.Mutex
	used       bool
}

func newNativeTraceOriginalStoppedTarget(source nativeTraceOriginalTargetSource) (*nativeTraceOriginalStoppedTarget, error) {
	if source.PID <= 1 || source.StartTicks == 0 || source.UID == 0 || source.GID == 0 || !validGuestBoot(source.GuestBootID) || source.PIDFD == nil || source.ProcDirectory == nil || source.Executable == nil || source.ExecutableSHA256 == [32]byte{} {
		return nil, fmt.Errorf("original stopped target source incomplete")
	}
	target := &nativeTraceOriginalStoppedTarget{nativeTraceOriginalControl: nativeTraceOriginalControl{pid: source.PID, uid: source.UID, pidfd: -1}, source: source, procfd: -1}
	ok := false
	defer func() {
		if !ok {
			target.close()
		}
	}()
	var err error
	target.pidfd, err = unix.FcntlInt(source.PIDFD.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	runtime.KeepAlive(source.PIDFD)
	if err != nil {
		return nil, err
	}
	target.procfd, err = unix.FcntlInt(source.ProcDirectory.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	runtime.KeepAlive(source.ProcDirectory)
	if err != nil {
		return nil, err
	}
	fd, err := unix.FcntlInt(source.Executable.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	runtime.KeepAlive(source.Executable)
	if err != nil {
		return nil, err
	}
	target.executable = os.NewFile(uintptr(fd), "original-stopped-target-executable")
	var fs unix.Statfs_t
	var st unix.Stat_t
	if unix.Fstatfs(target.procfd, &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC || unix.Fstat(target.procfd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR {
		return nil, fmt.Errorf("original target proc directory must be actual procfs")
	}
	if unix.Fstatfs(fd, &fs) != nil || fs.Flags&unix.ST_RDONLY == 0 || unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Size <= 0 || st.Size > 256<<20 {
		return nil, fmt.Errorf("original target executable must be bounded readonly file")
	}
	sum := sha256.New()
	if _, err = io.Copy(sum, io.NewSectionReader(target.executable, 0, st.Size)); err != nil {
		return nil, err
	}
	if !bytes.Equal(sum.Sum(nil), source.ExecutableSHA256[:]) {
		return nil, fmt.Errorf("original target executable SHA differs")
	}
	if err = target.verify(0); err != nil {
		return nil, err
	}
	ok = true
	return target, nil
}
func (t *nativeTraceOriginalStoppedTarget) close() {
	if t.executable != nil {
		_ = t.executable.Close()
	}
	if t.procfd >= 0 {
		_ = unix.Close(t.procfd)
	}
	if t.pidfd >= 0 {
		_ = unix.Close(t.pidfd)
	}
}
func (t *nativeTraceOriginalStoppedTarget) verify(tracer int) error {
	boot, err := kernelText("/proc/sys/kernel/random/boot_id")
	if err != nil || boot != t.source.GuestBootID {
		return fmt.Errorf("original stopped target Guest changed")
	}
	if err = unix.PidfdSendSignal(t.pidfd, 0, nil, 0); err != nil {
		return fmt.Errorf("original target pidfd not live: %w", err)
	}
	alive := func() error {
		p := []unix.PollFd{{Fd: int32(t.pidfd), Events: unix.POLLIN}}
		n, e := unix.Poll(p, 0)
		if e != nil {
			return e
		}
		if n != 0 || p[0].Revents != 0 {
			return fmt.Errorf("original target pidfd exited")
		}
		return nil
	}
	if err = alive(); err != nil {
		return err
	}
	// fdinfo上的Pid来自原struct pid；固定proc对象上的start/exe必须同时吻合。
	info, err := os.ReadFile("/proc/self/fdinfo/" + strconv.Itoa(t.pidfd))
	if err != nil || len(info) > 4096 {
		return fmt.Errorf("original target pidfd metadata missing")
	}
	values, err := nativeTraceStatusValues(string(info), "Pid", 1)
	if err != nil || values[0] != uint64(t.pid) {
		return fmt.Errorf("original pidfd namespace identity differs")
	}
	stat, err := nativeTraceProcText(t.procfd, "stat")
	if err != nil {
		return err
	}
	ticks, err := postmasterTicks(stat, t.pid)
	if err != nil || ticks != t.source.StartTicks {
		return fmt.Errorf("original stopped target start differs")
	}
	status, err := nativeTraceProcText(t.procfd, "status")
	if err != nil {
		return err
	}
	for name, want := range map[string][]uint64{"Uid": {uint64(t.source.UID), uint64(t.source.UID), uint64(t.source.UID), uint64(t.source.UID)}, "Gid": {uint64(t.source.GID), uint64(t.source.GID), uint64(t.source.GID), uint64(t.source.GID)}, "Pid": {uint64(t.pid)}, "Tgid": {uint64(t.pid)}, "TracerPid": {uint64(tracer)}, "Threads": {1}} {
		values, e := nativeTraceStatusValues(status, name, len(want))
		if e != nil {
			return e
		}
		for i, n := range want {
			if values[i] != n {
				return fmt.Errorf("original target %s differs", name)
			}
		}
	}
	exe, err := unix.Openat(t.procfd, "exe", unix.O_PATH|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	var a, b unix.Stat_t
	ea := unix.Fstat(exe, &a)
	_ = unix.Close(exe)
	eb := unix.Fstat(int(t.executable.Fd()), &b)
	if ea != nil || eb != nil || a.Dev != b.Dev || a.Ino != b.Ino {
		return fmt.Errorf("original target executable identity differs")
	}
	stat, err = nativeTraceProcText(t.procfd, "stat")
	if err != nil {
		return err
	}
	end, err := postmasterTicks(stat, t.pid)
	if err != nil || end != ticks {
		return fmt.Errorf("original target changed during verification")
	}
	return alive()
}
func (t *nativeTraceOriginalStoppedTarget) waitInterruptStop(ctx context.Context) error {
	if err := t.waitAnyStop(ctx); err != nil {
		return err
	}
	if t.observedExit || !t.stopped || t.lastStop.StopSignal() != unix.SIGTRAP || t.lastStop.TrapCause() != unix.PTRACE_EVENT_STOP {
		return fmt.Errorf("original target did not reach exact interrupt event stop")
	}
	return nil
}

type nativeTraceStoppedResult struct {
	TracerTID                                                         int
	OriginalConfirmed, ExitKillInstalled, CallbackCompleted, Detached bool
	Cleanup                                                           string
	TracerThreadRetired                                               bool
}
type nativeTraceStoppedDriver interface {
	verify(int) error
	seize() error
	interrupt() error
	waitInterruptStop(context.Context) error
	exitKill() error
	detach() error
	cleanup(context.Context, bool) (bool, string, error)
}

// 仅私有固定源实现和明确模型U共用顺序。生产入口不从配置/网络接driver。
func nativeTraceOriginalStoppedFlow(ctx context.Context, ops nativeTraceStoppedDriver, tid int, callback func(context.Context) error) (tr nativeTraceStoppedResult, err error, retire bool) {
	tr.TracerTID = tid
	seized, confirmed := false, false
	defer func() {
		if err != nil && seized {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var e error
			retire, tr.Cleanup, e = ops.cleanup(cleanup, confirmed)
			err = errors.Join(err, ErrUnknown, e)
		}
	}()
	if err = ctx.Err(); err != nil {
		return
	}
	if err = ops.verify(0); err != nil {
		return
	}
	if err = ops.seize(); err != nil {
		return
	}
	seized = true
	if err = ops.interrupt(); err != nil {
		return
	}
	if err = ops.waitInterruptStop(ctx); err != nil {
		return
	}
	if err = ops.verify(tid); err != nil {
		return
	}
	confirmed = true
	tr.OriginalConfirmed = true
	if err = ops.exitKill(); err != nil {
		return
	}
	tr.ExitKillInstalled = true
	if err = ctx.Err(); err != nil {
		return
	}
	if err = callback(ctx); err != nil {
		return
	}
	tr.CallbackCompleted = true
	if err = ctx.Err(); err != nil {
		return
	}
	if err = ops.verify(tid); err != nil {
		return
	}
	if err = ops.detach(); err != nil {
		return
	}
	seized = false
	tr.Detached = true
	return
}

// 回调只能由helper固定调用点构造，在原已确认stop内做固定A迁移/复核/持久。
// 返回Detached之后才能GO；任何partial仍保原marker/预算，绝不发normal许可。
func nativeTraceWithOriginalStoppedTarget(ctx context.Context, target *nativeTraceOriginalStoppedTarget, onConfirmedStopped func(context.Context, *nativeTraceOriginalStoppedTarget) error) (nativeTraceStoppedResult, error) {
	var zero nativeTraceStoppedResult
	if ctx == nil || target == nil || onConfirmedStopped == nil {
		return zero, fmt.Errorf("original bounded stopped target callback required")
	}
	deadline, bounded := ctx.Deadline()
	if !bounded || !deadline.After(time.Now()) || deadline.After(time.Now().Add(30*time.Second)) {
		return zero, fmt.Errorf("bounded stopped target deadline required")
	}
	target.mu.Lock()
	if target.used {
		target.mu.Unlock()
		return zero, fmt.Errorf("original stopped target is single-use")
	}
	target.used = true
	target.mu.Unlock()
	tr, err, meta := nativeTraceRunTypedLockedWorker(func(tid int) (nativeTraceStoppedResult, error, bool) {
		return nativeTraceOriginalStoppedFlow(ctx, target, tid, func(ctx context.Context) error { return onConfirmedStopped(ctx, target) })
	})
	tr.TracerThreadRetired = meta.Retired
	if meta.Unexpected {
		tr.TracerTID = meta.TID
		tr.Cleanup = "unexpected-worker-exit-unknown"
	}
	return tr, err
}
