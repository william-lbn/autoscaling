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
	"strconv"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const normalPgStderrSocket = "/run/neon-dsga/pg-stderr.sock"
const normalPgStderrCapacity = 64 << 10
const normalPgStderrSlots = 16

// 此对象只来自同g的原Compute lease和该peer以SCM传来的两个原OFD。
// 不提供PID/路径/nonce/容量/可信布尔构造入口，不从历史WAL重建pin。
type normalPgStderrPin struct {
	registry         *normalPgStderrRegistry
	nonce            string
	lease            *normalSystemOriginalComputeLease
	trusted          *trustedControllerComputePin
	read, write      *os.File
	device, inode    uint64
	pending, binding normalSystemRoleFileWitness
	bound            bool
	action           uint32
	pidfd            int
	finalized        bool
	unknown          bool
}
type normalPgStderrRegistry struct {
	mu       sync.Mutex
	service  *arrayNativeServices
	listener *net.UnixListener
	cancel   context.CancelFunc
	workers  sync.WaitGroup
	pins     map[string]*normalPgStderrPin
	closed   bool
}

var normalPgStderrRegistries = struct {
	sync.Mutex
	byService map[*arrayNativeServices]*normalPgStderrRegistry
}{byService: map[*arrayNativeServices]*normalPgStderrRegistry{}}

func normalPgStderrRegistryFor(s *arrayNativeServices) *normalPgStderrRegistry {
	normalPgStderrRegistries.Lock()
	defer normalPgStderrRegistries.Unlock()
	return normalPgStderrRegistries.byService[s]
}

// 只在准确helper1且原broker-owned实例启动；旧standalone/default0不新增socket。
func normalPgStderrProtocolMode(version uint32, s *arrayNativeServices) (bool, error) {
	if version == 0 {
		return false, nil
	}
	if version != 1 || s == nil || (s.admission == nil && (s.trusted == nil || s.guard == nil || s.guard.config.ExecutionModel != TrustedControllerExecutionModel || s.trusted.guard != s.guard)) {
		return false, ErrUnknown
	}
	return true, nil
}
func startNormalPgStderrSource(parent context.Context, s *arrayNativeServices, sealed SourceConfig) error {
	version, e := arrayAuthorityRuntimeProtocol(sealed)
	if e != nil {
		return e
	}
	enabled, e := normalPgStderrProtocolMode(version, s)
	if e != nil {
		return e
	}
	if !enabled {
		return nil
	}
	a := lookupNormalSystemAuthority(s.guard)
	if s.trusted == nil && (version != 1 || a == nil || a.guard != s.guard || a.admission != s.admission) {
		return ErrUnknown
	}
	listener, e := arrayFixedListener(normalPgStderrSocket)
	if e != nil {
		return e
	}
	// listener在accept前开启PASSCRED，已排队的首包也必须有kernel credentials。
	raw, e := listener.SyscallConn()
	if e != nil {
		listener.Close()
		return e
	}
	var inner error
	e = raw.Control(func(fd uintptr) { inner = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, 1) })
	if e != nil || inner != nil {
		listener.Close()
		return errors.Join(e, inner)
	}
	ctx, cancel := context.WithCancel(parent)
	q := &normalPgStderrRegistry{service: s, listener: listener, cancel: cancel, pins: map[string]*normalPgStderrPin{}}
	normalPgStderrRegistries.Lock()
	if normalPgStderrRegistries.byService[s] != nil || len(normalPgStderrRegistries.byService) >= 64 {
		normalPgStderrRegistries.Unlock()
		cancel()
		listener.Close()
		return ErrUnknown
	}
	normalPgStderrRegistries.byService[s] = q
	normalPgStderrRegistries.Unlock()
	q.workers.Add(1)
	go q.serve(ctx)
	return nil
}
func closeNormalPgStderrSource(s *arrayNativeServices) {
	q := normalPgStderrRegistryFor(s)
	if q == nil {
		return
	}
	q.cancel()
	q.listener.Close()
	q.workers.Wait()
	q.mu.Lock()
	q.closed = true
	for _, p := range q.pins {
		p.close()
	}
	q.pins = nil
	q.mu.Unlock()
	normalPgStderrRegistries.Lock()
	delete(normalPgStderrRegistries.byService, s)
	normalPgStderrRegistries.Unlock()
}
func (p *normalPgStderrPin) close() {
	if p == nil {
		return
	}
	p.unknown = true
	if p.read != nil {
		p.read.Close()
		p.read = nil
	}
	if p.write != nil {
		p.write.Close()
		p.write = nil
	}
	if p.pidfd >= 0 {
		unix.Close(p.pidfd)
		p.pidfd = -1
	}
	if p.trusted != nil {
		p.trusted.close()
		p.trusted = nil
	}
	if p.lease != nil {
		p.lease.close()
		p.lease = nil
	}
}
func (q *normalPgStderrRegistry) serve(ctx context.Context) {
	defer q.workers.Done()
	for ctx.Err() == nil {
		q.listener.SetDeadline(time.Now().Add(250 * time.Millisecond))
		conn, e := q.listener.AcceptUnix()
		if e != nil {
			if n, ok := e.(net.Error); ok && n.Timeout() {
				continue
			}
			if ctx.Err() == nil {
				if q.service.admission != nil {
					q.service.admission.fail()
				}
				if q.service.trusted != nil {
					q.service.trusted.close()
				}
			}
			return
		}
		call, cancel := context.WithTimeout(ctx, 30*time.Second)
		_ = q.register(call, conn)
		cancel()
		conn.Close()
	}
}
func normalPgStderrFrame(raw []byte) error {
	if len(raw) != 16 || string(raw[:8]) != "NDSGPS01" || binary.BigEndian.Uint32(raw[8:12]) != 1 || binary.BigEndian.Uint32(raw[12:16]) != 0 {
		return fmt.Errorf("exact PG stderr registration frame invalid")
	}
	return nil
}

// 同一packet严格恰好两rights和一条kernel credentials。无论解析结果如何，接收的
// 所有FD均先纳入cleanup；截断/多包/额外rights不得留下descriptor或补第二次读取。
func normalPgStderrReceive(conn *net.UnixConn, deadline time.Time) ([2]*os.File, *unix.Ucred, error) {
	var out [2]*os.File
	if e := conn.SetReadDeadline(deadline); e != nil {
		return out, nil, e
	}
	data := make([]byte, 17)
	oob := make([]byte, unix.CmsgSpace(16*4)+unix.CmsgSpace(unix.SizeofUcred))
	raw, e := conn.SyscallConn()
	if e != nil {
		return out, nil, e
	}
	var n, k, flags int
	var inner error
	e = raw.Read(func(fd uintptr) bool {
		for {
			n, k, flags, _, inner = unix.Recvmsg(int(fd), data, oob, unix.MSG_DONTWAIT|unix.MSG_CMSG_CLOEXEC)
			if inner == unix.EINTR {
				continue
			}
			return inner != unix.EAGAIN && inner != unix.EWOULDBLOCK
		}
	})
	if n < 0 || n > len(data) || k < 0 || k > len(oob) {
		return out, nil, errors.Join(ErrUnknown, e, inner)
	}
	var fds []int
	var cred *unix.Ucred
	rights, credentials := 0, 0
	messages, parse := unix.ParseSocketControlMessage(oob[:k])
	for _, m := range messages {
		if m.Header.Level != unix.SOL_SOCKET {
			parse = ErrUnknown
			continue
		}
		switch m.Header.Type {
		case unix.SCM_RIGHTS:
			rights++
			a, x := unix.ParseUnixRights(&m)
			fds = append(fds, a...)
			if x != nil {
				parse = x
			}
		case unix.SCM_CREDENTIALS:
			credentials++
			a, x := unix.ParseUnixCredentials(&m)
			if x != nil {
				parse = x
			}
			cred = a
		default:
			parse = ErrUnknown
		}
	}
	good := false
	defer func() {
		if !good {
			for _, fd := range fds {
				unix.Close(fd)
			}
		}
	}()
	if e != nil || inner != nil || parse != nil || flags&(unix.MSG_CTRUNC|unix.MSG_TRUNC) != 0 || rights != 1 || credentials != 1 || len(fds) != 2 || cred == nil {
		return out, nil, errors.Join(ErrUnknown, e, inner, parse)
	}
	if e = normalPgStderrFrame(data[:n]); e != nil {
		return out, nil, e
	}
	out = [2]*os.File{os.NewFile(uintptr(fds[0]), "original PG stderr read"), os.NewFile(uintptr(fds[1]), "original PG stderr write")}
	good = true
	return out, cred, nil
}
func normalPgStderrEndpoints(files [2]*os.File) (uint64, uint64, error) {
	var objects [2]unix.Stat_t
	for i, f := range files {
		if f == nil {
			return 0, 0, ErrUnknown
		}
		fd := int(f.Fd())
		var fs unix.Statfs_t
		if unix.Fstat(fd, &objects[i]) != nil || unix.Fstatfs(fd, &fs) != nil || fs.Type != unix.PIPEFS_MAGIC || objects[i].Mode&unix.S_IFMT != unix.S_IFIFO {
			return 0, 0, ErrUnknown
		}
		flags, e := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
		clo, x := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		capacity, y := unix.FcntlInt(uintptr(fd), unix.F_GETPIPE_SZ, 0)
		want := unix.O_RDONLY
		if i == 1 {
			want = unix.O_WRONLY
		}
		if e != nil || x != nil || y != nil || clo&unix.FD_CLOEXEC == 0 || flags&unix.O_ACCMODE != want || flags&(unix.O_PATH|unix.O_DIRECT|unix.O_ASYNC|unix.O_NONBLOCK) != 0 || capacity != normalPgStderrCapacity {
			return 0, 0, ErrUnknown
		}
	}
	if objects[0].Dev != objects[1].Dev || objects[0].Ino != objects[1].Ino {
		return 0, 0, ErrUnknown
	}
	return objects[0].Dev, objects[0].Ino, nil
}
func (q *normalPgStderrRegistry) register(ctx context.Context, conn *net.UnixConn) error {
	deadline, _ := ctx.Deadline()
	files, packet, e := normalPgStderrReceive(conn, deadline)
	if e != nil {
		return e
	}
	keep := false
	defer func() {
		if !keep {
			for _, f := range files {
				f.Close()
			}
		}
	}()
	peer, pidfd, e := arraySocketPeer(conn)
	if e != nil {
		return e
	}
	defer unix.Close(pidfd)
	if peer == nil || packet == nil || *peer != *packet || peer.Pid <= 1 {
		return ErrUnknown
	}
	device, inode, e := normalPgStderrEndpoints(files)
	if e != nil {
		return e
	}
	e = q.service.guard.withRecord(ctx, func(r *record, state *lockedState) error {
		var lease *normalSystemOriginalComputeLease
		var trusted *trustedControllerComputePin
		var computeNonce string
		if q.service.trusted != nil {
			trusted, e = q.service.trusted.registerComputeLocked(ctx, state, r, peer, pidfd)
			if e != nil {
				return e
			}
			computeNonce = trusted.nonce
		} else {
			lease, e = q.service.guard.normalSystemOriginalComputeLeaseLocked(ctx, state, r)
			if e != nil {
				return e
			}
			if e = arrayMatchPIDFD(lease.task.pidfd, pidfd); e != nil {
				lease.close()
				return e
			}
			if _, e = lease.authority.source.inspectOriginalTask(ctx, lease.task, "compute", lease.authority.declaration.compute.ComputeArgv, lease.authority.declaration.compute.ComputeEnv); e != nil {
				lease.close()
				return e
			}
			computeNonce = lease.pending.Nonce
		}
		own := false
		defer func() {
			if !own {
				if lease != nil {
					lease.close()
				}
				if trusted != nil {
					trusted.close()
				}
			}
		}()
		q.mu.Lock()
		defer q.mu.Unlock()
		if q.closed {
			return ErrUnknown
		}
		// 仅在原任务已退出、无writer且队列为空时退役；不read，不窃取Rust日志。
		for nonce, p := range q.pins {
			if p.retirable() {
				p.close()
				delete(q.pins, nonce)
			}
		}
		if len(q.pins) >= normalPgStderrSlots {
			return ErrUnknown
		}
		for _, p := range q.pins {
			if p.device == device && p.inode == inode {
				return ErrUnknown
			}
		}
		nonce, e := newNativeBirthNonce()
		if e != nil {
			return e
		}
		p := &normalPgStderrPin{registry: q, nonce: nonce, lease: lease, trusted: trusted, read: files[0], write: files[1], device: device, inode: inode, pidfd: -1}
		raw, e := json.Marshal(struct {
			Version       uint32 `json:"protocol_version"`
			Scope         string `json:"scope"`
			Nonce         string `json:"nonce"`
			ComputeNonce  string `json:"compute_nonce"`
			Device, Inode uint64
		}{1, "original-rust-pg-stderr-v1", nonce, computeNonce, device, inode})
		if e != nil {
			return e
		}
		name := "pg-stderr-" + nonce + "-pending.json"
		if e = state.createPhysicalFile(name, raw); e != nil {
			return errors.Join(ErrUnknown, e)
		}
		p.pending, e = readNormalSystemRoleFileWitness(state, name)
		if e != nil || p.pending.SHA256 != physicalDigest(raw) {
			return errors.Join(ErrUnknown, e)
		}
		q.pins[nonce] = p
		own = true
		keep = true
		return nil
	})
	if e != nil {
		return e
	}
	reply := make([]byte, 16)
	copy(reply, "NDSGPS1A")
	binary.BigEndian.PutUint32(reply[8:12], 1)
	conn.SetWriteDeadline(deadline)
	n, e := conn.Write(reply)
	if e != nil || n != len(reply) {
		return errors.Join(ErrUnknown, e)
	}
	return nil
}
func (p *normalPgStderrPin) retirable() bool {
	if p == nil || p.unknown || !p.bound || p.pidfd < 0 || p.write != nil || p.read == nil {
		return false
	}
	exited := []unix.PollFd{{Fd: int32(p.pidfd), Events: unix.POLLIN}}
	n, e := unix.Poll(exited, 0)
	// 原 task 被 wait 回收后，Linux 会在 POLLIN 之外同时返回 POLLHUP。
	// 复用严格退出解析：仍须 POLLIN，拒绝 HUP-only、ERR/NVAL 及未知位；
	// 该退出事实还必须与下方原 pipe 的 EOF、零排队字节同时成立才能退役。
	exitObserved, exitErr := normalSystemRoleLeaseExitPollResult(n, exited[0].Revents, e)
	if exitErr != nil || !exitObserved {
		return false
	}
	poll := []unix.PollFd{{Fd: int32(p.read.Fd()), Events: unix.POLLHUP}}
	n, e = unix.Poll(poll, 0)
	if e != nil || n != 1 || poll[0].Revents != unix.POLLHUP {
		return false
	}
	queued, e := unix.IoctlGetInt(int(p.read.Fd()), unix.TIOCINQ)
	return e == nil && queued == 0
}

// caller持原resource flock；按同原SUID task的FD2匹配，不接受请求选择registration。
func normalPgStderrBindBeginLocked(ctx context.Context, s *arrayNativeServices, state *lockedState, r *record, launch *arrayLaunchPin, action uint32) error {
	q := normalPgStderrRegistryFor(s)
	if q == nil {
		return nil
	} // 旧协议没有此来源。
	var current *normalSystemOriginalComputeLease
	var trusted *trustedControllerComputePin
	var e error
	var computeNonce string
	if s.trusted != nil {
		trusted, e = s.trusted.currentComputeLocked(ctx, state, r)
		if e != nil {
			return e
		}
		defer trusted.close()
		if e = trusted.verify(ctx, state, r); e != nil {
			return e
		}
		if e = trusted.verifyLaunchParent(launch); e != nil {
			return e
		}
		computeNonce = trusted.nonce
	} else {
		current, e = s.guard.normalSystemOriginalComputeLeaseLocked(ctx, state, r)
		if e != nil {
			return e
		}
		defer current.close()
		computeNonce = current.pending.Nonce
	}
	fd, e := unix.PidfdGetfd(launch.pidfd, 2, 0)
	if e != nil {
		return e
	}
	defer unix.Close(fd)
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrUnknown
	}
	var match *normalPgStderrPin
	for _, p := range q.pins {
		if p.unknown || p.bound || p.write == nil || p.computeNonce() != computeNonce {
			continue
		}
		if e = normalRoleStdioSameOFD(int(p.write.Fd()), fd); e == nil {
			if match != nil {
				return ErrUnknown
			}
			match = p
		}
	}
	if match == nil {
		return ErrUnknown
	}
	got, e := readNormalSystemRoleFileWitness(state, "pg-stderr-"+match.nonce+"-pending.json")
	if e != nil || got != match.pending {
		return errors.Join(ErrUnknown, e)
	}
	if _, _, e = normalPgStderrEndpoints([2]*os.File{match.read, match.write}); e != nil {
		return e
	}
	pf, e := arrayRegistryDup(launch.pidfd)
	if e != nil {
		return e
	}
	match.pidfd = pf
	match.bound = true
	match.action = action
	launch.stderr = match
	raw, e := json.Marshal(struct {
		Version    uint32 `json:"protocol_version"`
		Nonce      string `json:"nonce"`
		Action     uint32 `json:"action"`
		Birth      string `json:"birth_nonce,omitempty"`
		PendingSHA string `json:"previous_sha256"`
	}{1, match.nonce, action, launch.pending.Nonce, match.pending.SHA256})
	if e != nil {
		match.unknown = true
		return e
	}
	if e = state.createPhysicalFile("pg-stderr-"+match.nonce+"-bound.json", raw); e != nil {
		match.unknown = true
		return errors.Join(ErrUnknown, e)
	}
	match.binding, e = readNormalSystemRoleFileWitness(state, "pg-stderr-"+match.nonce+"-bound.json")
	if e != nil || match.binding.SHA256 != physicalDigest(raw) {
		match.unknown = true
		return errors.Join(ErrUnknown, e)
	}
	// aux根本没有NativeFinal，只认证System stdio；以真实Begin action决定，非caller自报。
	if action == 2 {
		match.write.Close()
		match.write = nil
	}
	return nil
}

// caller持原resource和registry锁；字节相同但换inode也不可收养。
func (p *normalPgStderrPin) recheckOriginalStagesLocked(state *lockedState) error {
	if p == nil || state == nil {
		return ErrUnknown
	}
	for phase, want := range map[string]normalSystemRoleFileWitness{"pending": p.pending, "bound": p.binding} {
		if want.SHA256 == "" {
			return ErrUnknown
		}
		got, e := readNormalSystemRoleFileWitness(state, "pg-stderr-"+p.nonce+"-"+phase+".json")
		if e != nil || got != want {
			return errors.Join(ErrUnknown, e)
		}
	}
	return nil
}
func (p *normalPgStderrPin) verifyNativeFinal(ctx context.Context, g *Guard, state *lockedState, pidfd int) error {
	if p == nil {
		normalPgStderrRegistries.Lock()
		defer normalPgStderrRegistries.Unlock()
		for s := range normalPgStderrRegistries.byService {
			if s.guard == g {
				return ErrUnknown
			}
		}
		return nil
	}
	q := p.registry
	if q == nil {
		return ErrUnknown
	}
	if q.service != nil && q.service.trusted != nil {
		return p.verifyTrustedNativeFinal(ctx, g, state, pidfd)
	}
	// 先在q锁下复制并独立dup原Task，锁外执行System复核，最后重新核同一pin。
	// close仅关闭registry自己所有的FD，不得与下面的独立lease竞争。
	q.mu.Lock()
	if q.closed || q.pins[p.nonce] != p || p.unknown || p.lease == nil || p.lease.authority == nil {
		q.mu.Unlock()
		return ErrUnknown
	}
	originalLease := p.lease
	task, e := duplicateNormalSystemOriginalTask(originalLease.task)
	if e != nil {
		q.mu.Unlock()
		return e
	}
	lease := *originalLease
	lease.task = task
	q.mu.Unlock()
	defer lease.close()
	if lease.authority.guard != g || !lease.authority.admission.businessAllowed() {
		return ErrUnknown
	}
	if e := lease.verifyOriginalPrefixesLocked(state); e != nil {
		return e
	}
	if _, e := lease.authority.source.inspectOriginalTask(ctx, lease.task, "compute", lease.authority.declaration.compute.ComputeArgv, lease.authority.declaration.compute.ComputeEnv); e != nil {
		return e
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if p.lease != originalLease {
		return ErrUnknown
	}
	if q.closed || q.pins[p.nonce] != p || p.unknown || !p.bound || p.action != 1 || p.finalized || p.read == nil || p.write == nil {
		return ErrUnknown
	}
	if e := p.recheckOriginalStagesLocked(state); e != nil {
		return e
	}
	if e := arrayMatchPIDFD(p.pidfd, pidfd); e != nil {
		return e
	}
	fd, e := unix.PidfdGetfd(pidfd, 2, 0)
	if e != nil {
		return e
	}
	defer unix.Close(fd)
	if e = normalRoleStdioSameOFD(int(p.write.Fd()), fd); e != nil {
		return e
	}
	_, _, e = normalPgStderrEndpoints([2]*os.File{p.read, p.write})
	return e
}
func (p *normalPgStderrPin) releasedOriginalFinal() error {
	if p == nil {
		return nil
	}
	q := p.registry
	if q == nil {
		return ErrUnknown
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || p.unknown || p.finalized || p.action != 1 || p.write == nil {
		return ErrUnknown
	}
	p.finalized = true
	e := p.write.Close()
	p.write = nil
	if e != nil {
		p.unknown = true
	}
	return e
}

// 仅诊断用的数字，不据此按/proc路径重建来源。
func (p *normalPgStderrPin) String() string {
	return "original-pg-stderr:" + strconv.FormatUint(p.inode, 10)
}

func (p *normalPgStderrPin) computeNonce() string {
	if p == nil {
		return ""
	}
	if p.trusted != nil {
		return p.trusted.nonce
	}
	if p.lease != nil {
		return p.lease.pending.Nonce
	}
	return ""
}
func (p *normalPgStderrPin) verifyTrustedNativeFinal(ctx context.Context, g *Guard, state *lockedState, pidfd int) error {
	q := p.registry
	q.mu.Lock()
	if q.closed || q.service.guard != g || q.service.trusted == nil || q.pins[p.nonce] != p || p.unknown || p.trusted == nil || p.trusted.owner != q.service.trusted {
		q.mu.Unlock()
		return ErrUnknown
	}
	original := p.trusted
	pin, e := original.duplicate()
	q.mu.Unlock()
	if e != nil {
		return e
	}
	defer pin.close()
	r, e := state.load()
	if e != nil || r == nil {
		return ErrUnknown
	}
	if e = pin.verify(ctx, state, r); e != nil {
		return e
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.pins[p.nonce] != p || p.trusted != original || p.unknown || !p.bound || p.action != 1 || p.finalized || p.read == nil || p.write == nil {
		return ErrUnknown
	}
	if e = p.recheckOriginalStagesLocked(state); e != nil {
		return e
	}
	if e = arrayMatchPIDFD(p.pidfd, pidfd); e != nil {
		return e
	}
	fd, e := unix.PidfdGetfd(pidfd, 2, 0)
	if e != nil {
		return e
	}
	defer unix.Close(fd)
	if e = normalRoleStdioSameOFD(int(p.write.Fd()), fd); e != nil {
		return e
	}
	_, _, e = normalPgStderrEndpoints([2]*os.File{p.read, p.write})
	return e
}
