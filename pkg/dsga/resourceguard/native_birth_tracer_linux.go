//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// 本文件是私有出生会话组件，不接HTTP/provision，不产生normal/VM许可。
// accepted conn、原ProcessIdentity和expected均须由原Root Pending提供；
// BinarySHA是原不可变构建manifest的PG ELF摘要，绝不能用帧内SourceSHA替代。
// 完整ELF依赖/FS/I与持原resource flock的WAL consumer由外层另行证明。
type nativeBirthTraceConfig struct {
	Original         ProcessIdentity
	PeerUID, PeerGID uint32
	Expected         nativeBirthExpected
	AuditBinary      *os.File
	BinarySHA256     [32]byte
	Persist          func(context.Context, nativeFilterPhase, nativeBirthTraceTranscript) error
}

type nativeBirthTraceTranscript struct {
	Original                      ProcessIdentity
	Request                       nativeBirthRequest
	AuditSHA256                   [32]byte
	BinarySHA256                  [32]byte
	TracerTID                     int
	BaseVerified, FinalVerified   bool
	BasePersisted, FinalPersisted bool
	DetachedNonAuthorizing        bool
	Cleanup                       string
	TracerThreadRetired           bool
}

type nativeBirthTraceResult struct {
	transcript nativeBirthTraceTranscript
	err        error
	thread     *os.File
}

// 整个会话在专用非leader G/M上锁线程；ctx取消只能触发同步cleanup，不能
// select抢先返回遗留写者。成功必须detach；不确定清理的M必须退役且观察原
// thread目录消失。Go1.24.3的m0只wedge不退出，因此m0永不进行SEIZE。
func runNativeBirthTraceSession(ctx context.Context, conn *net.UnixConn, cfg nativeBirthTraceConfig) (nativeBirthTraceTranscript, error) {
	var zero nativeBirthTraceTranscript
	if ctx == nil {
		return zero, fmt.Errorf("bounded native trace context required")
	}
	deadline, bounded := ctx.Deadline()
	if conn == nil || cfg.Original.Validate() != nil || cfg.PeerUID == 0 || cfg.PeerGID == 0 ||
		cfg.Persist == nil || cfg.AuditBinary == nil || cfg.BinarySHA256 == [32]byte{} ||
		uint64(cfg.Original.AddressSpaceMaxBytes) != cfg.Expected.AddressSpaceBytes ||
		!bounded || !deadline.After(time.Now()) || deadline.After(time.Now().Add(30*time.Second)) {
		return zero, fmt.Errorf("original bounded private native trace source required")
	}
	if _, _, err := cfg.Expected.geometry(); err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	// body只由本私有入口从固定kernel driver构造；不是配置/网络可注入hook。
	defer conn.Close()
	return nativeTraceRunLockedWorker(func(tid int) (nativeBirthTraceTranscript, error, bool) {
		driver, err := newNativeBirthTraceKernel(conn, cfg, tid)
		if err != nil {
			return nativeBirthTraceTranscript{}, err, false
		}
		defer driver.close()
		return nativeBirthTraceFlow(ctx, cfg, driver, tid)
	})
}

type nativeTraceWorkerBody func(int) (nativeBirthTraceTranscript, error, bool)

// 单一线程寿命实现，供生产私有会话及明确native机械组件测试复用。
// 不能从cfg/HTTP/native帧取得body；callback不构成Root/normal资格。
func nativeTraceRunLockedWorker(body nativeTraceWorkerBody) (nativeBirthTraceTranscript, error) {
	results := make(chan nativeBirthTraceResult, 1)
	go nativeBirthTraceWorker(body, results, nil)
	result := <-results
	if result.thread != nil {
		defer result.thread.Close()
		until := time.Now().Add(5 * time.Second)
		for {
			_, err := nativeTraceProcText(int(result.thread.Fd()), "status")
			if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ESRCH) {
				result.transcript.TracerThreadRetired = true
				break
			}
			if err != nil || !time.Now().Before(until) {
				result.err = errors.Join(result.err, ErrUnknown, fmt.Errorf("original tracer thread retirement not confirmed"))
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
	return result.transcript, result.err
}

func nativeBirthTraceWorker(body nativeTraceWorkerBody, results chan<- nativeBirthTraceResult, ready chan<- struct{}) {
	runtime.LockOSThread()
	if unix.Gettid() == os.Getpid() {
		// 占住m0，第二G只能在另一M锁定；第一G从不碰ptrace。
		started := make(chan struct{})
		go nativeBirthTraceWorker(body, results, started)
		<-started
		runtime.UnlockOSThread()
		return
	}
	if ready != nil {
		close(ready)
	}
	var thread *os.File
	sent := false
	// 持久callback的panic/Goexit也不能让调用者永等channel；意外路径不
	// Unlock受污染M，按原真实EXITKILL状态退役。不得回显潜在敏感panic值。
	defer func() {
		_ = recover()
		if !sent {
			if thread == nil {
				runtime.UnlockOSThread()
			}
			results <- nativeBirthTraceResult{
				transcript: nativeBirthTraceTranscript{TracerTID: unix.Gettid(), Cleanup: "unexpected-worker-exit-unknown"},
				err:        errors.Join(ErrUnknown, fmt.Errorf("unexpected native tracer worker termination")), thread: thread,
			}
		}
	}()
	if err := nativeTraceCaller(); err != nil {
		runtime.UnlockOSThread()
		sent = true
		results <- nativeBirthTraceResult{err: err}
		return
	}
	fd, err := unix.Open("/proc/thread-self", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		runtime.UnlockOSThread()
		sent = true
		results <- nativeBirthTraceResult{err: err}
		return
	}
	thread = os.NewFile(uintptr(fd), "original-native-tracer-thread")
	transcript, flowErr, retire := body(unix.Gettid())
	if !retire {
		_ = thread.Close()
		runtime.UnlockOSThread()
		sent = true
		results <- nativeBirthTraceResult{transcript: transcript, err: flowErr}
		return
	}
	// 故意不Unlock：专用G返回让runtime销毁非m0的M；kernel按真实已设置的
	// EXITKILL决定杀已确认原target，否则仅auto-detach不确定numeric新target。
	// 接收channel还不算终态；外层必须核固定旧thread对象真正消失。
	sent = true
	results <- nativeBirthTraceResult{transcript: transcript, err: flowErr, thread: thread}
}

// interface只隔离明确模型U；生产入口永远构造真实kernel driver，不从配置/帧
// 接收reader、PID替代器、expected bool或任意ptrace回调。
type nativeBirthTraceDriver interface {
	verify(*nativeBirthRequest, int) error
	receive(context.Context) ([]byte, error)
	compile(context.Context, nativeBirthRequest) (nativeFilterAuditBundle, error)
	seize() error
	interrupt() error
	waitStop(context.Context, bool) error
	exitKill() error
	filters(nativeFilterAuditBundle, nativeFilterPhase) error
	cont() error
	ack(context.Context, nativeBirthExpected) error
	closed198() error
	detach() error
	cleanup(context.Context, bool) (bool, string, error)
}

func nativeBirthTraceFlow(ctx context.Context, cfg nativeBirthTraceConfig, ops nativeBirthTraceDriver, tid int) (tr nativeBirthTraceTranscript, err error, retire bool) {
	tr.Original, tr.BinarySHA256, tr.TracerTID = cfg.Original, cfg.BinarySHA256, tid
	seized, confirmed := false, false
	defer func() {
		if err == nil || !seized {
			return
		}
		// 原ctx已取消也必须同步收尾；错误不投影成NoEffect或释放额度。
		cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var cleanupErr error
		retire, tr.Cleanup, cleanupErr = ops.cleanup(cleanCtx, confirmed)
		err = errors.Join(err, ErrUnknown, cleanupErr)
	}()
	if err = ctx.Err(); err != nil {
		return
	}
	if err = ops.verify(nil, 0); err != nil {
		return
	}
	var raw []byte
	if raw, err = ops.receive(ctx); err != nil {
		return
	}
	if tr.Request, err = decodeNativeBirthRequest(raw, cfg.Expected); err != nil {
		return
	}
	var bundle nativeFilterAuditBundle
	if bundle, err = ops.compile(ctx, tr.Request); err != nil {
		return
	}
	tr.AuditSHA256 = bundle.EncodedSHA256
	if err = ctx.Err(); err != nil {
		return
	}
	// SEIZE无pidfd版本：先不带EXITKILL。若numeric PID已复用，不能因Root
	// 退出而误杀新进程；停下后核原pidfd/proc/start/exe才可设EXITKILL。
	if err = ops.seize(); err != nil {
		return
	}
	seized = true
	if err = ops.interrupt(); err != nil {
		return
	}
	if err = ops.waitStop(ctx, false); err != nil {
		return
	}
	if err = ops.verify(&tr.Request, tid); err != nil {
		return
	}
	confirmed = true
	if err = ops.exitKill(); err != nil {
		return
	}
	if err = ops.filters(bundle, nativeFilterPMBase); err != nil {
		return
	}
	tr.BaseVerified = true
	if err = cfg.Persist(ctx, nativeFilterPMBase, tr); err != nil {
		return
	}
	tr.BasePersisted = true
	if err = ctx.Err(); err != nil {
		return
	}
	if err = ops.verify(&tr.Request, tid); err != nil {
		return
	}
	if err = ops.filters(bundle, nativeFilterPMBase); err != nil {
		return
	}
	if err = ops.cont(); err != nil {
		return
	}
	if err = ops.ack(ctx, cfg.Expected); err != nil {
		return
	}
	if err = ops.waitStop(ctx, true); err != nil {
		return
	}
	if err = ops.verify(&tr.Request, tid); err != nil {
		return
	}
	if err = ops.closed198(); err != nil {
		return
	}
	if err = ops.filters(bundle, nativeFilterPMFinal); err != nil {
		return
	}
	tr.FinalVerified = true
	if err = cfg.Persist(ctx, nativeFilterPMFinal, tr); err != nil {
		return
	}
	tr.FinalPersisted = true
	if err = ctx.Err(); err != nil {
		return
	}
	if err = ops.verify(&tr.Request, tid); err != nil {
		return
	}
	if err = ops.closed198(); err != nil {
		return
	}
	if err = ops.filters(bundle, nativeFilterPMFinal); err != nil {
		return
	}
	if err = ops.detach(); err != nil {
		return
	}
	seized = false
	tr.DetachedNonAuthorizing = true
	return
}

func nativeTraceCaller() error {
	if err := nativeFilterAuditCaller(); err != nil {
		return err
	}
	header := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3, Pid: 0}
	data := [2]unix.CapUserData{}
	if err := unix.Capget(&header, &data[0]); err != nil {
		return err
	}
	if data[unix.CAP_KILL/32].Effective&(uint32(1)<<uint(unix.CAP_KILL%32)) == 0 {
		return fmt.Errorf("actual native tracer thread lacks cleanup CAP_KILL")
	}
	return nil
}

const nativeTracePeerPIDFD = 77 // Linux6.12.26 include/uapi/asm-generic/socket.h SO_PEERPIDFD.

type nativeBirthTraceKernel struct {
	conn                            *net.UnixConn
	cfg                             nativeBirthTraceConfig
	tid                             int
	pidfd, procfd                   int
	binary                          *os.File
	attached, stopped, observedExit bool
	undeliveredSignal               int
}

func newNativeBirthTraceKernel(conn *net.UnixConn, cfg nativeBirthTraceConfig, tid int) (*nativeBirthTraceKernel, error) {
	k := &nativeBirthTraceKernel{conn: conn, cfg: cfg, tid: tid, pidfd: -1, procfd: -1}
	ok := false
	defer func() {
		if !ok {
			k.close()
		}
	}()
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	var inner error
	err = raw.Control(func(fd uintptr) {
		var peer *unix.Ucred
		var kind int
		peer, inner = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if inner != nil {
			return
		}
		kind, inner = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
		if inner != nil {
			return
		}
		if peer == nil || peer.Pid != int32(cfg.Original.PostmasterPID) || peer.Uid != cfg.PeerUID || peer.Gid != cfg.PeerGID || kind != unix.SOCK_SEQPACKET {
			inner = fmt.Errorf("accepted native kernel peer differs from original identity")
			return
		}
		// 只能取kernel原peer pidfd；旧内核不支持就关门，不race fallback pidfd_open(PID)。
		k.pidfd, inner = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, nativeTracePeerPIDFD)
	})
	if err != nil {
		return nil, err
	}
	if inner != nil {
		return nil, inner
	}
	if k.pidfd < 0 {
		return nil, fmt.Errorf("kernel did not return original peer pidfd")
	}
	if _, err := unix.FcntlInt(uintptr(k.pidfd), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		return nil, err
	}
	proc, err := unix.Open("/proc", unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	var fs unix.Statfs_t
	if err := unix.Fstatfs(proc, &fs); err != nil || fs.Type != unix.PROC_SUPER_MAGIC {
		_ = unix.Close(proc)
		return nil, fmt.Errorf("actual procfs required")
	}
	boot, bootErr := nativeTraceProcText(proc, "sys/kernel/random/boot_id")
	if bootErr != nil || strings.TrimSpace(boot) != cfg.Original.Identity.GuestBootID {
		_ = unix.Close(proc)
		return nil, fmt.Errorf("original native process belongs to another Guest boot")
	}
	k.procfd, err = unix.Openat(proc, strconv.Itoa(cfg.Original.PostmasterPID), unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	_ = unix.Close(proc)
	if err != nil {
		return nil, err
	}
	dup, err := unix.FcntlInt(cfg.AuditBinary.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	runtime.KeepAlive(cfg.AuditBinary)
	if err != nil {
		return nil, err
	}
	k.binary = os.NewFile(uintptr(dup), "original-pinned-native-audit-binary")
	info, err := k.binary.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 256<<20 {
		return nil, fmt.Errorf("bounded original native ELF required")
	}
	if err := unix.Fstatfs(dup, &fs); err != nil || fs.Flags&unix.ST_RDONLY == 0 {
		return nil, fmt.Errorf("original native audit binary must be on readonly source mount")
	}
	sum := sha256.New()
	if _, err := io.Copy(sum, io.NewSectionReader(k.binary, 0, info.Size())); err != nil {
		return nil, err
	}
	if !bytes.Equal(sum.Sum(nil), cfg.BinarySHA256[:]) {
		return nil, fmt.Errorf("fixed native ELF differs from original build manifest")
	}
	ok = true
	return k, nil
}

func (k *nativeBirthTraceKernel) close() {
	if k.binary != nil {
		_ = k.binary.Close()
	}
	if k.procfd >= 0 {
		_ = unix.Close(k.procfd)
	}
	if k.pidfd >= 0 {
		_ = unix.Close(k.pidfd)
	}
	_ = k.conn.Close()
}

func nativeTraceProcText(dir int, name string) (string, error) {
	fd, err := unix.Openat(dir, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return "", err
	}
	f := os.NewFile(uintptr(fd), "pinned-proc-"+name)
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil {
		return "", err
	}
	if len(b) > 64*1024 {
		return "", fmt.Errorf("pinned native process source exceeds bound")
	}
	return string(b), nil
}

func nativeTraceStatusValues(status, field string, count int) ([]uint64, error) {
	var result []uint64
	for _, line := range strings.Split(status, "\n") {
		if !strings.HasPrefix(line, field+":") {
			continue
		}
		if result != nil {
			return nil, fmt.Errorf("duplicate native process status field")
		}
		fields := strings.Fields(strings.TrimPrefix(line, field+":"))
		if len(fields) != count {
			return nil, fmt.Errorf("native process status field count differs")
		}
		result = make([]uint64, count)
		for i, value := range fields {
			n, err := canonicalUint(value)
			if err != nil || n > uint64(^uint32(0)) {
				return nil, fmt.Errorf("native process status value invalid")
			}
			result[i] = n
		}
	}
	if result == nil {
		return nil, fmt.Errorf("native process status field missing")
	}
	return result, nil
}

func (k *nativeBirthTraceKernel) alive() error {
	p := []unix.PollFd{{Fd: int32(k.pidfd), Events: unix.POLLIN}}
	n, err := unix.Poll(p, 0)
	if err != nil {
		return err
	}
	if n != 0 || p[0].Revents != 0 {
		return fmt.Errorf("original peer pidfd is exited or unknown")
	}
	return nil
}

func (k *nativeBirthTraceKernel) verify(request *nativeBirthRequest, tracer int) error {
	if err := k.alive(); err != nil {
		return err
	}
	first, err := nativeTraceProcText(k.procfd, "stat")
	if err != nil {
		return err
	}
	ticks, err := postmasterTicks(first, k.cfg.Original.PostmasterPID)
	if err != nil || ticks != uint64(k.cfg.Original.PostmasterStartTicks) {
		return fmt.Errorf("pinned original process start changed")
	}
	status, err := nativeTraceProcText(k.procfd, "status")
	if err != nil {
		return err
	}
	if err := processUID(status, uint64(k.cfg.PeerUID)); err != nil {
		return err
	}
	for field, want := range map[string][]uint64{
		"Gid": {uint64(k.cfg.PeerGID), uint64(k.cfg.PeerGID), uint64(k.cfg.PeerGID), uint64(k.cfg.PeerGID)},
		"Pid": {uint64(k.cfg.Original.PostmasterPID)}, "Tgid": {uint64(k.cfg.Original.PostmasterPID)},
		"TracerPid": {uint64(tracer)}, "Threads": {1},
	} {
		got, err := nativeTraceStatusValues(status, field, len(want))
		if err != nil {
			return err
		}
		for i := range want {
			if got[i] != want[i] {
				return fmt.Errorf("original native %s identity differs", field)
			}
		}
	}
	limits, err := nativeTraceProcText(k.procfd, "limits")
	if err != nil {
		return err
	}
	hard, _, err := processAddressSpace(limits, status, k.cfg.Original.AddressSpaceMaxBytes)
	if err != nil {
		return fmt.Errorf("native original AS/NNP/capability facts differ: %w", err)
	}
	if hard != k.cfg.Original.AddressSpaceMaxBytes {
		return fmt.Errorf("native original AS hard ceiling differs")
	}
	exact := 0
	for _, line := range strings.Split(limits, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && strings.Join(fields[:3], " ") == "Max address space" {
			if len(fields) != 6 || fields[3] != strconv.FormatUint(uint64(hard), 10) || fields[4] != fields[3] {
				return fmt.Errorf("native hard/soft AS must equal original source")
			}
			exact++
		}
	}
	if exact != 1 {
		return fmt.Errorf("native exact AS source missing")
	}
	actual, err := unix.Openat(k.procfd, "exe", unix.O_PATH|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	var a, b unix.Stat_t
	ae := unix.Fstat(actual, &a)
	_ = unix.Close(actual)
	be := unix.Fstat(int(k.binary.Fd()), &b)
	if ae != nil || be != nil || a.Dev != b.Dev || a.Ino != b.Ino {
		return fmt.Errorf("pinned native executable differs from fixed original binary")
	}
	if request != nil {
		maps, err := nativeTraceProcText(k.procfd, "maps")
		if err != nil {
			return err
		}
		if err := nativeTraceReservationMaps(maps, *request); err != nil {
			return err
		}
	}
	second, err := nativeTraceProcText(k.procfd, "stat")
	if err != nil {
		return err
	}
	end, err := postmasterTicks(second, k.cfg.Original.PostmasterPID)
	if err != nil || end != ticks {
		return fmt.Errorf("native process changed during pinned verification")
	}
	return k.alive()
}

func nativeTraceReservationMaps(raw string, request nativeBirthRequest) error {
	type span struct {
		left, right uint64
		reservation bool
	}
	var spans []span
	var previous uint64
	for _, line := range strings.Split(strings.TrimSuffix(raw, "\n"), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			return fmt.Errorf("native maps line malformed")
		}
		ends := strings.Split(fields[0], "-")
		if len(ends) != 2 {
			return fmt.Errorf("native maps range malformed")
		}
		left, e1 := strconv.ParseUint(ends[0], 16, 64)
		right, e2 := strconv.ParseUint(ends[1], 16, 64)
		if e1 != nil || e2 != nil || left < previous || left >= right {
			return fmt.Errorf("native maps overlap or range invalid")
		}
		previous = right
		spans = append(spans, span{left, right, fields[1] == "---p" && fields[2] == "00000000" && fields[3] == "00:00" && fields[4] == "0" && len(fields) == 5})
	}
	for _, r := range request.Ranges {
		cursor := r.Base
		for _, s := range spans {
			if s.left <= cursor && cursor < s.right {
				if !s.reservation {
					return fmt.Errorf("native R is not original anonymous PROT_NONE reservation")
				}
				cursor = min(s.right, r.Base+r.ReservedBytes)
				if cursor == r.Base+r.ReservedBytes {
					break
				}
			}
		}
		if cursor != r.Base+r.ReservedBytes {
			return fmt.Errorf("native complete R not present")
		}
	}
	return nil
}

func (k *nativeBirthTraceKernel) receive(ctx context.Context) ([]byte, error) {
	deadline, _ := ctx.Deadline()
	// 仅取消本conn的等待；离开前同步join watcher，不遗留后台deadline写者。
	stop, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-ctx.Done():
			// 原reader会先设置deadline；重复到同步join，封闭取消先发生而
			// reader随后覆盖deadline的竞态。只写本conn，不遗后台操作。
			for {
				_ = k.conn.SetReadDeadline(time.Now())
				select {
				case <-stop:
					return
				case <-time.After(time.Millisecond):
				}
			}
		case <-stop:
		}
	}()
	frame, err := readNativeBirthFrame(k.conn, nativeBirthPeer{PID: int32(k.cfg.Original.PostmasterPID), UID: k.cfg.PeerUID, GID: k.cfg.PeerGID}, deadline)
	close(stop)
	<-joined
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return frame, err
}

type nativeTraceBoundedOutput struct {
	bytes.Buffer
	limit int
}

func (b *nativeTraceBoundedOutput) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, fmt.Errorf("native trusted audit output exceeds bound")
	}
	return b.Buffer.Write(p)
}

func (k *nativeBirthTraceKernel) compile(ctx context.Context, request nativeBirthRequest) (nativeFilterAuditBundle, error) {
	args := []string{"--neon-dsga-audit-filter-v1", strconv.FormatUint(request.Original.AddressSpaceBytes, 10), strconv.FormatUint(uint64(request.Original.MaxBuffers), 10)}
	for _, r := range request.Ranges {
		args = append(args, strconv.FormatUint(r.Base, 10), strconv.FormatUint(r.ReservedBytes, 10))
	}
	cmd := exec.CommandContext(ctx, "/proc/self/fd/3", args...)
	cmd.ExtraFiles = []*os.File{k.binary}
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C"}
	stdout := &nativeTraceBoundedOutput{limit: nativeAuditMaxBytes}
	stderr := &nativeTraceBoundedOutput{limit: 4096}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Run(); err != nil {
		return nativeFilterAuditBundle{}, fmt.Errorf("fixed original native audit failed: %w", err)
	}
	if stderr.Len() != 0 {
		return nativeFilterAuditBundle{}, fmt.Errorf("fixed native audit wrote unexpected stderr")
	}
	return decodeNativeFilterAudit(stdout.Bytes(), request)
}

func nativeTracePtrace(op uint, pid int, data uintptr) error {
	_, _, errno := unix.Syscall6(unix.SYS_PTRACE, uintptr(op), uintptr(pid), 0, data, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
func (k *nativeBirthTraceKernel) seize() error {
	if err := nativeTracePtrace(unix.PTRACE_SEIZE, k.cfg.Original.PostmasterPID, 0); err != nil {
		return err
	}
	k.attached = true
	return nil
}
func (k *nativeBirthTraceKernel) interrupt() error {
	return nativeTracePtrace(unix.PTRACE_INTERRUPT, k.cfg.Original.PostmasterPID, 0)
}
func (k *nativeBirthTraceKernel) exitKill() error {
	return nativeTracePtrace(unix.PTRACE_SETOPTIONS, k.cfg.Original.PostmasterPID, unix.PTRACE_O_EXITKILL)
}
func (k *nativeBirthTraceKernel) cont() error {
	if err := nativeTracePtrace(unix.PTRACE_CONT, k.cfg.Original.PostmasterPID, 0); err != nil {
		return err
	}
	k.stopped = false
	return nil
}
func (k *nativeBirthTraceKernel) detach() error {
	if err := nativeTracePtrace(unix.PTRACE_DETACH, k.cfg.Original.PostmasterPID, 0); err != nil {
		return err
	}
	k.attached, k.stopped = false, false
	return nil
}

func (k *nativeBirthTraceKernel) waitStop(ctx context.Context, final bool) error {
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
		if pid != k.cfg.Original.PostmasterPID {
			return fmt.Errorf("unexpected native trace wait target")
		}
		if status.Exited() || status.Signaled() {
			k.observedExit = true
			k.attached = false
			return fmt.Errorf("original native exited before gate release: %x", uint32(status))
		}
		if !status.Stopped() {
			return fmt.Errorf("native trace wait is not a kernel stop")
		}
		k.stopped = true
		k.rememberStop(status)
		if !final {
			if status.StopSignal() != unix.SIGTRAP || status.TrapCause() != unix.PTRACE_EVENT_STOP {
				return fmt.Errorf("native base requires exact PTRACE_INTERRUPT event stop")
			}
			return nil
		}
		if status.StopSignal() != unix.SIGSTOP || uint32(status)>>16 != 0 {
			return fmt.Errorf("native final requires SIGSTOP signal-delivery stop, not group/event stop")
		}
		// ABI amd64 siginfo_t: 128B，sender联合体从offset16开始。核self tgkill，
		// 不能把另一同UID进程抢发的SIGSTOP误认为原生最终停点。
		raw := [128]byte{}
		_, _, eno := unix.Syscall6(unix.SYS_PTRACE, unix.PTRACE_GETSIGINFO, uintptr(pid), 0, uintptr(unsafe.Pointer(&raw[0])), 0, 0)
		runtime.KeepAlive(raw)
		if eno != 0 {
			return eno
		}
		if int32(binary.LittleEndian.Uint32(raw[0:4])) != int32(unix.SIGSTOP) ||
			int32(binary.LittleEndian.Uint32(raw[8:12])) != -6 ||
			int32(binary.LittleEndian.Uint32(raw[16:20])) != int32(pid) ||
			binary.LittleEndian.Uint32(raw[20:24]) != k.cfg.PeerUID {
			return fmt.Errorf("native final SIGSTOP is not original self tgkill")
		}
		return nil
	}
}
func (k *nativeBirthTraceKernel) filters(b nativeFilterAuditBundle, p nativeFilterPhase) error {
	return readAndCompareNativeKernelFilters(k.cfg.Original.PostmasterPID, k.tid, b, p)
}
func (k *nativeBirthTraceKernel) ack(ctx context.Context, e nativeBirthExpected) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	if err := k.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	ack, err := encodeNativeBirthACK(e)
	if err != nil {
		return err
	}
	n, err := k.conn.Write(ack[:])
	if err != nil {
		return err
	}
	if n != len(ack) {
		return io.ErrShortWrite
	}
	return nil
}
func (k *nativeBirthTraceKernel) closed198() error {
	var st unix.Stat_t
	err := unix.Fstatat(k.procfd, "fd/198", &st, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil {
		return fmt.Errorf("native fixed birth fd198 is still open")
	}
	if !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("native fixed birth fd198 not independently closed: %w", err)
	}
	return nil
}

// 不确认numeric新target就绝不kill/EXITKILL；只尝试停止后detach。期限不确定则
// 退役专用M，让kernel auto-detach。确认原对象后只能原peer pidfd发SIGKILL，
// 严格wait exit；发出kill而未见exit仍是Unknown，账本不得据此归零或重放。
func (k *nativeBirthTraceKernel) cleanup(ctx context.Context, confirmed bool) (bool, string, error) {
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
			if err := nativeTracePtrace(unix.PTRACE_DETACH, k.cfg.Original.PostmasterPID, uintptr(k.undeliveredSignal)); err == nil {
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
		if pid != k.cfg.Original.PostmasterPID {
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
func (k *nativeBirthTraceKernel) waitAnyStop(ctx context.Context) error {
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
func (k *nativeBirthTraceKernel) rememberStop(status unix.WaitStatus) {
	k.undeliveredSignal = 0
	if status.Stopped() && uint32(status)>>16 == 0 {
		k.undeliveredSignal = int(status.StopSignal())
	}
}

// 原SO_PEERPIDFD绑定的wait；绝不以numeric PID重查。其他Go线程可能已reap
// 原child且数字被新child复用，P_PIDFD仍只引用原struct pid；ECHILD/不支持都
// 是Unknown，不fallback Wait4/P_PID。Linux6.12.26 exit.c1740..1747/1783..1808。
// amd64 LP64 siginfo_t=128B，signo/errno/code=0/4/8，pid/uid/status=16/20/24。
type nativeOriginalWaitCall func(int, int, *[128]byte, int) error

func (k *nativeBirthTraceKernel) waitOriginal() (int, unix.WaitStatus, error) {
	return nativePollOriginalWait(k.pidfd, k.cfg.Original.PostmasterPID, k.cfg.PeerUID,
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

// read seam仅供明确模型U；生产构造器只使用上面固定SYS_WAITID，不接caller
// 回调/HTTP reader。P_PIDFD=3和原fd是核心，不因numeric PID相等认新incarnation。
func nativePollOriginalWait(fd, pid int, uid uint32, read nativeOriginalWaitCall) (int, unix.WaitStatus, error) {
	if fd < 0 || pid <= 1 || read == nil {
		return 0, 0, fmt.Errorf("original pidfd wait source missing")
	}
	var info [128]byte
	if err := read(3, fd, &info, unix.WEXITED|unix.WSTOPPED|unix.WNOHANG|unix.WALL); err != nil {
		return 0, 0, err
	}
	return decodeNativeOriginalWait(info, pid, uid)
}

func decodeNativeOriginalWait(info [128]byte, pid int, uid uint32) (int, unix.WaitStatus, error) {
	signo := binary.LittleEndian.Uint32(info[0:4])
	sierrno := binary.LittleEndian.Uint32(info[4:8])
	cause := binary.LittleEndian.Uint32(info[8:12])
	actualPID := binary.LittleEndian.Uint32(info[16:20])
	actualUID := binary.LittleEndian.Uint32(info[20:24])
	status := binary.LittleEndian.Uint32(info[24:28])
	if signo == 0 && sierrno == 0 && cause == 0 && actualPID == 0 && actualUID == 0 && status == 0 {
		return 0, 0, nil
	}
	if signo != uint32(unix.SIGCHLD) || sierrno != 0 || actualPID != uint32(pid) || actualUID != uid || pid <= 1 {
		return 0, 0, fmt.Errorf("original pidfd wait identity or siginfo differs")
	}
	switch cause {
	case 1: // CLD_EXITED，不能把exit status当stop或signal。
		if status > 255 {
			return 0, 0, fmt.Errorf("original pidfd exit status invalid")
		}
		return pid, unix.WaitStatus(status << 8), nil
	case 2, 3: // CLD_KILLED / CLD_DUMPED。
		if status == 0 || status > 64 {
			return 0, 0, fmt.Errorf("original pidfd exit signal invalid")
		}
		if cause == 3 {
			status |= 0x80
		}
		return pid, unix.WaitStatus(status), nil
	case 4: // CLD_TRAPPED：si_status包含完整ptrace event，严禁截成信号byte。
		if status == 0 || status > 0xffff || status&0xff == 0 {
			return 0, 0, fmt.Errorf("original pidfd ptrace status invalid")
		}
		return pid, unix.WaitStatus((status << 8) | 0x7f), nil
	default:
		return 0, 0, fmt.Errorf("original pidfd wait is not an exit or ptrace event")
	}
}
