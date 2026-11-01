//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// 仅一次性pre-ordinary准备使用的固定程序，不是任意Root执行接口。
// 构造者只接受原System的RO ELF对象；业务来源仍须后继同g opaque闭合。
type normalOrdinaryStockUdevPrograms struct {
	system  *normalSystemLifecycleSource
	files   [2]*os.File
	objects [2]normalFilesystemObject
	hashes  [2]string
}

var normalOrdinaryStockUdevProgramPaths = [2]string{"/neonvm/bin/udevd", "/neonvm/bin/udevadm"}

func pinNormalOrdinaryStockUdevPrograms(system *normalSystemLifecycleSource) (_ *normalOrdinaryStockUdevPrograms, err error) {
	if system == nil {
		return nil, ErrUnknown
	}
	system.mu.Lock()
	defer system.mu.Unlock()
	if system.closed {
		return nil, ErrUnknown
	}
	p := &normalOrdinaryStockUdevPrograms{system: system}
	defer func() {
		if err != nil {
			p.close()
		}
	}()
	for i, path := range normalOrdinaryStockUdevProgramPaths {
		found := false
		for _, entry := range system.manifest.Files {
			if entry.Path != path {
				continue
			}
			if found || entry.Kind != "exec" || !physicalDigestValid(entry.SHA256) {
				return nil, ErrUnknown
			}
			found = true
			original := system.files[entry.CanonicalPath]
			if original == nil {
				return nil, ErrUnknown
			}
			fd, e := unix.FcntlInt(original.Fd(), unix.F_DUPFD_CLOEXEC, 0)
			if e != nil {
				return nil, e
			}
			p.files[i] = os.NewFile(uintptr(fd), "original-stock-udev-program")
			p.objects[i], e = normalFilesystemFDObject(fd)
			if e != nil {
				return nil, e
			}
			p.hashes[i] = entry.SHA256
		}
		if !found {
			return nil, errors.Join(ErrUnknown, fmt.Errorf("fixed stock program absent from original System"))
		}
	}
	if err = p.verify(); err != nil {
		return nil, err
	}
	return p, nil
}
func (p *normalOrdinaryStockUdevPrograms) close() {
	if p == nil {
		return
	}
	for i, f := range p.files {
		if f != nil {
			_ = f.Close()
			p.files[i] = nil
		}
	}
}
func (p *normalOrdinaryStockUdevPrograms) verify() error {
	if p == nil || p.system == nil {
		return ErrUnknown
	}
	for i, path := range normalOrdinaryStockUdevProgramPaths {
		if p.files[i] == nil {
			return ErrUnknown
		}
		if e := nativeBirthReadonlyPath(path); e != nil {
			return e
		}
		f, e := nativeBirthOpenRO(path)
		if e != nil {
			return e
		}
		object, e := normalFilesystemFDObject(int(f.Fd()))
		h, he := nativeBirthFileHash(f)
		_ = f.Close()
		pinned, pe := normalFilesystemFDObject(int(p.files[i].Fd()))
		if e != nil || he != nil || pe != nil || object != p.objects[i] || pinned != p.objects[i] || h != p.hashes[i] {
			return errors.Join(ErrUnknown, e, he, pe, fmt.Errorf("original fixed stock executable changed"))
		}
	}
	return nil
}

// Root实际exec和kmod输入读者共用此固定环境；返回副本，无caller覆盖。
func normalOrdinaryStockUdevEnvironment() []string {
	return []string{"PATH=/neonvm/bin:/usr/sbin:/usr/bin:/sbin:/bin", "LANG=C", "LC_ALL=C"}
}

type normalOrdinaryStockUdevStage uint8

const (
	normalOrdinaryStockUdevDaemon normalOrdinaryStockUdevStage = iota + 1
	normalOrdinaryStockUdevControlReady
	normalOrdinaryStockUdevTriggerDevices
	normalOrdinaryStockUdevSettle
)

func normalOrdinaryStockUdevArgv(stage normalOrdinaryStockUdevStage) ([]string, error) {
	switch stage {
	case normalOrdinaryStockUdevDaemon:
		// 不传--daemon；新privsep版本未获ordinary标记，明确执行原Root分支。
		return []string{normalOrdinaryStockUdevProgramPaths[0], "--children-max=1"}, nil
	case normalOrdinaryStockUdevControlReady:
		// 原eudev3.2.14无--ping。固定timeout先于动作；该原控制命令
		// send/poll失败会非0，但连接关闭不是内容ACK，更不代表queue空。
		return []string{normalOrdinaryStockUdevProgramPaths[1], "control", "--timeout=5", "--children-max=1"}, nil
	case normalOrdinaryStockUdevTriggerDevices:
		return []string{normalOrdinaryStockUdevProgramPaths[1], "trigger", "--type=devices", "--action=add"}, nil
	case normalOrdinaryStockUdevSettle:
		return []string{normalOrdinaryStockUdevProgramPaths[1], "settle", "--timeout=10"}, nil
	default:
		return nil, ErrUnknown
	}
}

// 原eudevd在argv后再解析内核cmdline，可能覆盖worker/延时/日志等行为。
// 新explicit2准备不接受任何udev.*覆盖；不用字符串采样冒单worker证明。
// 这里故意保守拒绝含该命名域的参数，包括引号/嵌入值，不实现另一套宽松parser。
func normalOrdinaryStockUdevCommandLine(raw string) error {
	if len(raw) == 0 || len(raw) > 64<<10 || strings.ContainsAny(raw, "\x00\r\n") || strings.Contains(raw, "udev.") {
		return errors.Join(ErrUnknown, fmt.Errorf("fixed stock preparation rejects kernel udev overrides"))
	}
	return nil
}

// 始终吞吐输出但只保留32KiB，溢出使本次准备失败，不阻塞子进程输出。
type normalOrdinaryStockUdevLog struct {
	mu       sync.Mutex
	data     []byte
	overflow bool
}

func (b *normalOrdinaryStockUdevLog) Write(raw []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	remaining := (32 << 10) - len(b.data)
	n := len(raw)
	if n > remaining {
		b.overflow = true
		raw = raw[:remaining]
	}
	b.data = append(b.data, raw...)
	return n, nil
}
func (b *normalOrdinaryStockUdevLog) snapshot() ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.data...), b.overflow
}

type normalOrdinaryStockUdevWait struct {
	Stage                   normalOrdinaryStockUdevStage
	PID                     int
	PIDFSDevice, PIDFSInode uint64
	ExitCode                int
	WaitError               string
}
type normalOrdinaryStockUdevProcess struct {
	cmd     *exec.Cmd
	pidfd   *os.File
	witness normalOrdinaryStockUdevWait
	done    chan error
	waited  bool
}

func (p *normalOrdinaryStockUdevPrograms) start(stage normalOrdinaryStockUdevStage, log *normalOrdinaryStockUdevLog) (*normalOrdinaryStockUdevProcess, error) {
	if e := p.verify(); e != nil {
		return nil, e
	}
	argv, e := normalOrdinaryStockUdevArgv(stage)
	if e != nil {
		return nil, e
	}
	fd := -1
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = normalOrdinaryStockUdevEnvironment()
	cmd.Dir = "/"
	cmd.Stdout, cmd.Stderr = log, log
	cmd.WaitDelay = time.Second
	// clone原生返回pidfd，不按PID重开；Pdeathsig防原R2死亡遗留Root主进程。
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &fd, Pdeathsig: syscall.SIGKILL}
	if e = cmd.Start(); e != nil {
		return nil, e
	}
	q := &normalOrdinaryStockUdevProcess{cmd: cmd, done: make(chan error, 1), witness: normalOrdinaryStockUdevWait{Stage: stage, PID: cmd.Process.Pid, ExitCode: -1}}
	if fd >= 0 {
		q.pidfd = os.NewFile(uintptr(fd), "original-stock-clone-pidfd")
		var st unix.Stat_t
		var fs unix.Statfs_t
		e = unix.Fstat(fd, &st)
		if e == nil {
			e = unix.Fstatfs(fd, &fs)
		}
		if e == nil && (uint64(fs.Type) != 0x50494446 || st.Ino == 0) {
			e = ErrUnknown
		}
		if e == nil {
			q.witness.PIDFSDevice, q.witness.PIDFSInode = uint64(st.Dev), st.Ino
		}
	} else {
		e = ErrUnknown
	}
	// 唯一cmd.Wait；没有其他Wait4(-1)抢这个直属子进程。
	go func() { q.done <- cmd.Wait() }()
	// 即使原pin验证失败也交回已启动对象，外层唯一清理者必须跟踪它。
	// 不在此丢失原Wait后再让subreaper与cmd.Wait抢同一个孩子。
	if e != nil {
		return q, errors.Join(ErrUnknown, e)
	}
	return q, nil
}
func (p *normalOrdinaryStockUdevProcess) signal(sig unix.Signal) error {
	if p == nil || p.pidfd == nil {
		return ErrUnknown
	}
	return unix.PidfdSendSignal(int(p.pidfd.Fd()), sig, nil, 0)
}
func (p *normalOrdinaryStockUdevProcess) wait(ctx context.Context) error {
	if p == nil || p.waited {
		return ErrUnknown
	}
	select {
	case e := <-p.done:
		p.waited = true
		if p.cmd == nil || p.cmd.ProcessState == nil {
			return errors.Join(ErrUnknown, e)
		}
		p.witness.ExitCode = p.cmd.ProcessState.ExitCode()
		if e != nil {
			p.witness.WaitError = e.Error()
		}
		return e
	case <-ctx.Done():
		return ctx.Err()
	}
}

// 子进程窗口仅由原R2在pre-ordinary阶段打开。subreaper令双fork/setsid也不能
// 越过本R2逃到PID1；所有直属命令先由其唯一Wait收尾，再清退收养后代。
var normalOrdinaryStockUdevSupervisorMu sync.Mutex

func normalOrdinaryStockUdevChildren() ([]int, error) {
	threads, e := os.ReadDir("/proc/self/task")
	if e != nil || len(threads) > 4096 {
		return nil, errors.Join(ErrUnknown, e)
	}
	seen := map[int]bool{}
	var out []int
	for _, thread := range threads {
		raw, e := os.ReadFile(filepath.Join("/proc/self/task", thread.Name(), "children"))
		if errors.Is(e, os.ErrNotExist) {
			continue
		} // 已退出的runtime线程不再有直属孩子。
		if e != nil || len(raw) > 65536 {
			return nil, errors.Join(ErrUnknown, e)
		}
		for _, field := range strings.Fields(string(raw)) {
			pid, e := strconv.Atoi(field)
			if e != nil || pid <= 1 || pid == os.Getpid() || seen[pid] || len(out) >= 4096 {
				return nil, ErrUnknown
			}
			seen[pid] = true
			out = append(out, pid)
		}
	}
	return out, nil
}
func reapNormalOrdinaryStockUdevOrphans(ctx context.Context) ([]normalOrdinaryStockUdevWait, error) {
	var out []normalOrdinaryStockUdevWait
	for {
		children, e := normalOrdinaryStockUdevChildren()
		if e != nil {
			return out, e
		}
		if len(children) == 0 {
			return out, nil
		}
		for _, pid := range children {
			// 这些是已结束直属cmd唯一Wait之后、本subreaper的未reap孩子；
			// 本窗口没有其他Wait者，故PID在本次Wait前不会被回收再利用。
			fd, e := unix.PidfdOpen(pid, 0)
			if e != nil {
				return out, e
			}
			var st unix.Stat_t
			e = unix.Fstat(fd, &st)
			if e == nil {
				e = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
			}
			_ = unix.Close(fd)
			if e != nil && !errors.Is(e, unix.ESRCH) {
				return out, e
			}
			var status unix.WaitStatus
			got, e := unix.Wait4(pid, &status, unix.WNOHANG, nil)
			if e != nil {
				return out, e
			}
			if got == pid {
				out = append(out, normalOrdinaryStockUdevWait{PID: pid, PIDFSDevice: uint64(st.Dev), PIDFSInode: st.Ino, ExitCode: status.ExitStatus()})
			}
		}
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// PR_GET_CHILD_SUBREAPER把结果写到arg2指向的int，而非prctl返回值。
// 直接Syscall6保留指针活性；不以retint/空指针调用后忽略EFAULT。
func normalOrdinaryStockUdevSubreaper() (int, error) {
	var value int32
	_, _, e := syscall.Syscall6(unix.SYS_PRCTL, unix.PR_GET_CHILD_SUBREAPER, uintptr(unsafe.Pointer(&value)), 0, 0, 0, 0)
	runtime.KeepAlive(&value)
	if e != 0 {
		return 0, e
	}
	if value != 0 && value != 1 {
		return 0, ErrUnknown
	}
	return int(value), nil
}

// 返回原Wait证据而非ready布尔；调用者还必须核三actor、原g/FS/WAL、规则及封存投影。
// 此函数本身不产生normalOrdinaryUdevV2Preparation或任何Source资格。
func (p *normalOrdinaryStockUdevPrograms) run(ctx context.Context) (waits []normalOrdinaryStockUdevWait, logRaw []byte, err error) {
	normalOrdinaryStockUdevSupervisorMu.Lock()
	defer normalOrdinaryStockUdevSupervisorMu.Unlock()
	// Linux Pdeathsig绑定fork创建线程而非整个TGID。原Wait前保持同一M，
	// 防Go runtime退休创建M时将仍合法的stock daemon误杀。
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if ctx == nil || ctx.Err() != nil {
		return nil, nil, ErrUnknown
	}
	if e := normalKernelPrepV2R2Caller(); e != nil {
		return nil, nil, e
	}
	cmdline, e := kernelText("/proc/cmdline")
	if e != nil {
		return nil, nil, e
	}
	if e = normalOrdinaryStockUdevCommandLine(cmdline); e != nil {
		return nil, nil, e
	}
	children, e := normalOrdinaryStockUdevChildren()
	if e != nil || len(children) != 0 {
		return nil, nil, errors.Join(ErrUnknown, e, fmt.Errorf("preparation cannot acquire an existing child"))
	}
	previous, e := normalOrdinaryStockUdevSubreaper()
	if e != nil {
		return nil, nil, e
	}
	if e = unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); e != nil {
		return nil, nil, e
	}
	call, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	log := &normalOrdinaryStockUdevLog{}
	var started []*normalOrdinaryStockUdevProcess
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		allWaited := true
		for _, process := range started {
			if !process.waited {
				se := process.signal(unix.SIGKILL)
				if process.pidfd == nil && process.cmd != nil && process.cmd.Process != nil {
					// Go1.24 Process持自己的原生clone pidfd；仅pin失败清理用，
					// 该路径既有ErrUnknown，绝不会生成成功准备凭据。
					se = process.cmd.Process.Kill()
				}
				we := process.wait(cleanup)
				err = errors.Join(err, se, we)
			}
			allWaited = allWaited && process.waited
			waits = append(waits, process.witness)
		}
		// 仍有原cmd未Wait时绝不与其竞争Wait4，也绝不签后代已归零。
		if allWaited {
			reaped, re := reapNormalOrdinaryStockUdevOrphans(cleanup)
			waits = append(waits, reaped...)
			err = errors.Join(err, re)
			// settle和daemon自身Wait之后仍有Root后代，虽清退也不能冒充
			// 原规则全部正常完成；本次Source必须失败，等待显式恢复。
			if len(reaped) != 0 {
				err = errors.Join(err, fmt.Errorf("stock preparation left adopted descendants"))
			}
			if re == nil {
				err = errors.Join(err, unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, uintptr(previous), 0, 0, 0))
			}
		} else {
			err = errors.Join(err, ErrUnknown)
		}
		for _, process := range started {
			if process.pidfd != nil {
				_ = process.pidfd.Close()
			}
		}
		var overflow bool
		logRaw, overflow = log.snapshot()
		if overflow {
			err = errors.Join(err, fmt.Errorf("fixed stock log exceeded 32KiB"))
		}
		if err != nil {
			err = errors.Join(ErrUnknown, err)
		}
	}()
	// 原control必须不存在；不删除/收养任何旧对象或旧daemon。
	if _, e = os.Lstat("/run/udev/control"); !errors.Is(e, os.ErrNotExist) {
		return nil, nil, errors.Join(ErrUnknown, e, fmt.Errorf("preexisting stock control"))
	}
	daemon, e := p.start(normalOrdinaryStockUdevDaemon, log)
	if daemon != nil {
		started = append(started, daemon)
	}
	if e != nil {
		return nil, nil, e
	}
	for {
		st, e := os.Lstat("/run/udev/control")
		if e == nil {
			if st.Mode()&os.ModeSocket == 0 {
				return nil, nil, ErrUnknown
			}
			break
		}
		if !errors.Is(e, os.ErrNotExist) {
			return nil, nil, e
		}
		select {
		case <-call.Done():
			return nil, nil, call.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	for _, stage := range []normalOrdinaryStockUdevStage{normalOrdinaryStockUdevControlReady, normalOrdinaryStockUdevTriggerDevices, normalOrdinaryStockUdevSettle} {
		if e = arrayRegistryPIDFDAlive(int(daemon.pidfd.Fd())); e != nil {
			return nil, nil, e
		}
		child, e := p.start(stage, log)
		if child != nil {
			started = append(started, child)
		}
		if e != nil {
			return nil, nil, e
		}
		if e = child.wait(call); e != nil {
			return nil, nil, e
		}
	}
	if e = arrayRegistryPIDFDAlive(int(daemon.pidfd.Fd())); e != nil {
		return nil, nil, e
	}
	if e = daemon.signal(unix.SIGTERM); e != nil {
		return nil, nil, e
	}
	if e = daemon.wait(call); e != nil {
		return nil, nil, e
	}
	if e = p.verify(); e != nil {
		return nil, nil, e
	}
	return waits, nil, nil
}
