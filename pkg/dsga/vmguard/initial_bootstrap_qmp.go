//go:build linux

package vmguard

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

// capture只能由唯一RunQEMU在Start成功后、进入唯一Wait之前调用；该窗口原child
// 即使已退出也是未回收zombie，PID不能复用。不要从HTTP PID或并行Wait后补建句柄。
// 后继观察不消费Wait，只持原pidfd/procdir；关闭句柄不杀进程/不释放Host预留。
type initialBootProcess struct {
	pid        int
	pidfd      int
	proc       *os.File
	start      uint64
	uid        uint32
	gid        uint32
	creatorUID uint32
}

func captureInitialBootProcess(cmd *exec.Cmd, source initialBootSource) (*initialBootProcess, error) {
	if source.validate() != nil || cmd == nil || cmd.Process == nil || cmd.ProcessState != nil || cmd.Process.Pid <= 0 {
		return nil, fmt.Errorf("original Start-before-Wait child required")
	}
	raw := initialBootCanonical([]any{cmd.Path, cmd.Args})
	if SourceDigest(raw) != source.CommandSHA256 {
		return nil, fmt.Errorf("original process argv differs")
	}
	fd, e := unix.PidfdOpen(cmd.Process.Pid, 0)
	if e != nil {
		return nil, e
	}
	p := &initialBootProcess{pid: cmd.Process.Pid, pidfd: fd, uid: source.ProcessUID, gid: source.ProcessGID, creatorUID: uint32(os.Geteuid())}
	d, e := unix.Open(fmt.Sprintf("/proc/%d", p.pid), unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		p.close()
		return nil, e
	}
	p.proc = os.NewFile(uintptr(d), "original-qemu-proc")
	p.start, e = p.startTime()
	if e != nil || p.alive() != nil {
		p.close()
		return nil, fmt.Errorf("original QEMU was not captured alive")
	}
	return p, nil
}
func (p *initialBootProcess) close() {
	if p == nil {
		return
	}
	if p.proc != nil {
		p.proc.Close()
		p.proc = nil
	}
	if p.pid > 0 && p.pidfd >= 0 {
		unix.Close(p.pidfd)
		p.pidfd = -1
	}
}
func (p *initialBootProcess) alive() error {
	if p == nil || p.pidfd < 0 || p.proc == nil {
		return fmt.Errorf("original process handles missing")
	}
	poll := []unix.PollFd{{Fd: int32(p.pidfd), Events: unix.POLLIN}}
	n, e := unix.Poll(poll, 0)
	if e != nil || n != 0 {
		return fmt.Errorf("original QEMU exited or pidfd is unknown")
	}
	if e = unix.PidfdSendSignal(p.pidfd, 0, nil, 0); e != nil {
		return fmt.Errorf("original pidfd no longer live: %w", e)
	}
	start, e := p.startTime()
	if e != nil || start != p.start {
		return fmt.Errorf("original fixed proc identity changed")
	}
	return nil
}
func (p *initialBootProcess) startTime() (uint64, error) {
	if p == nil || p.proc == nil {
		return 0, fmt.Errorf("fixed procdir absent")
	}
	fd, e := unix.Openat(int(p.proc.Fd()), "stat", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if e != nil {
		return 0, e
	}
	f := os.NewFile(uintptr(fd), "original-qemu-stat")
	defer f.Close()
	raw, e := io.ReadAll(io.LimitReader(f, 8193))
	if e != nil || len(raw) > 8192 {
		return 0, fmt.Errorf("bounded original stat unavailable")
	}
	s := string(raw)
	end := strings.LastIndexByte(s, ')')
	begin := strings.IndexByte(s, '(')
	if begin < 1 || end < begin {
		return 0, fmt.Errorf("invalid original stat framing")
	}
	pid, e := strconv.Atoi(strings.TrimSpace(s[:begin]))
	fields := strings.Fields(s[end+1:])
	if e != nil || pid != p.pid || len(fields) < 20 {
		return 0, fmt.Errorf("original stat PID differs")
	}
	n, e := strconv.ParseUint(fields[19], 10, 64)
	if e != nil || n == 0 {
		return 0, fmt.Errorf("original start time unavailable")
	}
	return n, nil
}
func (p *initialBootProcess) checkPeer(c *qmpConnection, source initialBootSource) error {
	if e := p.alive(); e != nil {
		return e
	}
	u, ok := c.conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("original private Unix QMP required")
	}
	raw, e := u.SyscallConn()
	if e != nil {
		return e
	}
	var peer *unix.Ucred
	var inner error
	e = raw.Control(func(fd uintptr) { peer, inner = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) })
	if e != nil || inner != nil || peer == nil || int(peer.Pid) != p.pid || (peer.Uid != p.uid && peer.Uid != p.creatorUID) {
		return fmt.Errorf("QMP peer differs from original live Start child")
	}
	// pidfd在peer读前后均存活，不能用已回收PID的新进程伪造同一numeric身份。
	if e = p.alive(); e != nil {
		return e
	}
	if e = p.currentCredentials(); e != nil {
		return e
	}
	fd, e := unix.Openat(int(p.proc.Fd()), "exe", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(fd), "original-qemu-exe")
	defer f.Close()
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || uint64(st.Dev) != source.QEMUDevice || st.Ino != source.QEMUInode || st.Size <= 0 || st.Size > 256<<20 {
		return fmt.Errorf("original QEMU executable changed")
	}
	b, e := io.ReadAll(io.LimitReader(f, 256<<20+1))
	if e != nil || int64(len(b)) != st.Size || initialBootSHA(b) != source.QEMUSHA256 {
		return fmt.Errorf("original QEMU executable bytes changed")
	}
	return p.alive()
}

// immutable构造后不热替换engine.native。本组件Read/Set始终拒绝普通协议；
// inspect仅观察原物理几何，完整真实Guest reader/shared version hook接通前不发qom-set。
type initialBaseQMP struct {
	source  initialBootSource
	q       *nativeQMP
	process *initialBootProcess
}

func (b *initialBaseQMP) Read(context.Context) (Observation, error) {
	return Observation{}, errInitialBootUnavailable
}
func (b *initialBaseQMP) Set(context.Context, control.Uint64) error { return errInitialBootUnavailable }
func (b *initialBaseQMP) inspect(ctx context.Context) (originalMemoryGeometry, error) {
	if b == nil || b.q == nil || b.process == nil || b.source.validate() != nil || b.q.id != b.source.Identity {
		return originalMemoryGeometry{}, fmt.Errorf("fixed original low-base adapter incomplete")
	}
	c, e := b.q.connect(ctx)
	if e != nil {
		return originalMemoryGeometry{}, e
	}
	defer c.close()
	if e = b.process.checkPeer(c, b.source); e != nil {
		return originalMemoryGeometry{}, e
	}
	in, e := initialBootGeometryRead(c)
	if e != nil {
		return originalMemoryGeometry{}, e
	}
	v, e := decodeOriginalMemoryGeometry(b.source.Identity, b.source.OriginalBase, in)
	if e != nil {
		return originalMemoryGeometry{}, e
	}
	if e = b.process.checkPeer(c, b.source); e != nil {
		return originalMemoryGeometry{}, e
	}
	return v, nil
}
func initialBootGeometryRead(c *qmpConnection) (memoryGeometryInputs, error) {
	var in memoryGeometryInputs
	calls := []struct {
		name string
		args any
		dest *json.RawMessage
	}{
		{"query-version", nil, &in.Version},
		{"qom-get", struct {
			Path     string `json:"path"`
			Property string `json:"property"`
		}{"/machine", "memory"}, &in.Machine},
		{"query-memory-devices", nil, &in.Devices},
		{"query-memory-size-summary", nil, &in.Summary},
		{"query-memdev", nil, &in.Backends},
		{"qom-get", struct {
			Path     string `json:"path"`
			Property string `json:"property"`
		}{"/objects/vmem0", "size"}, &in.BackendSize},
	}
	for _, call := range calls {
		raw, e := c.execute(call.name, call.args)
		if e != nil {
			return in, e
		}
		*call.dest = raw
	}
	return in, nil
}
func initialBootTargetOffset(source initialBootSource, target control.Uint64) (control.Uint64, error) {
	if source.validate() != nil || !source.Identity.memoryValid(target) {
		return 0, fmt.Errorf("business target outside original minimum/maximum")
	}
	return target - source.OriginalBase, nil
}

// Unix peer cred可保留listen时的root凭据；仍须同原pidfd/procdir/start，并独立
// 核QEMU -runas后的真实四个uid/gid。跨UID exe读需要原Runner实际ptrace能力；
// 权限不足明确拒绝，不能用旧root-sleep模型外推真实Runner资格。
func (p *initialBootProcess) currentCredentials() error {
	fd, e := unix.Openat(int(p.proc.Fd()), "status", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(fd), "original-qemu-status")
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, 16385))
	if e != nil || len(b) > 16384 {
		return fmt.Errorf("bounded original credentials unavailable")
	}
	seenUID, seenGID := false, false
	for _, line := range strings.Split(string(b), "\n") {
		var expected uint32
		switch {
		case strings.HasPrefix(line, "Uid:"):
			if seenUID {
				return fmt.Errorf("duplicate original UID")
			}
			seenUID = true
			expected = p.uid
		case strings.HasPrefix(line, "Gid:"):
			if seenGID {
				return fmt.Errorf("duplicate original GID")
			}
			seenGID = true
			expected = p.gid
		default:
			continue
		}
		fs := strings.Fields(line)
		if len(fs) != 5 {
			return fmt.Errorf("original credential tuple incomplete")
		}
		for _, v := range fs[1:] {
			n, e := strconv.ParseUint(v, 10, 32)
			if e != nil || uint32(n) != expected {
				return fmt.Errorf("original QEMU runas credentials differ")
			}
		}
	}
	if !seenUID || !seenGID {
		return fmt.Errorf("original QEMU credentials absent")
	}
	return p.alive()
}
