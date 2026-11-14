//go:build linux

package resourceguard

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const normalSystemRoleServiceLockName = "role-broker-service.lock"

type normalSystemRoleService struct {
	authority   *normalSystemResourceAuthority
	source      *normalSystemLifecycleSource
	declaration *normalSystemRoleDeclarationSource
	namespace   *normalSystemRoleNamespaceSource
	broker      *normalSystemRoleBrokerComponent
	parent      *normalSystemOriginalTask
	metadata    normalSystemRoleServiceMetadata
	listener    *net.UnixListener
	lock        *os.File
	enabled     atomic.Bool
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	mu          sync.Mutex
	requests    map[[32]byte]struct{}
	execMu      sync.Mutex
	finished    map[*normalSystemRoleOriginalRun]error
	closeOnce   sync.Once
}

// 本构造器只接已由原spawn/startup链核验的对象；不是HTTP入口，不接受路径。
// listener ready-probe和正式启用分开：Parent完整ready阶段fsync并经原继承通道
// COMMIT前，所有role执行请求都无效果。socket存在/marker存在不等于就绪。
func openNormalSystemRoleService(ctx context.Context, g *Guard, s *normalSystemLifecycleSource, d *normalSystemRoleDeclarationSource, ns *normalSystemRoleNamespaceSource, parent *normalSystemOriginalTask, metadata normalSystemRoleServiceMetadata) (*normalSystemRoleService, error) {
	if ctx == nil || g == nil || s == nil || d == nil || ns == nil || parent == nil || metadata.validatePair() != nil {
		return nil, ErrUnknown
	}
	if e := normalSystemRoleServiceCaller(); e != nil {
		return nil, e
	}
	if e := d.verify(ctx); e != nil {
		return nil, e
	}
	if metadata.Broker != s.binding || metadata.Origin.DeclarationSHA256 != d.rawSHA {
		return nil, fmt.Errorf("original broker declaration/pair differs")
	}
	b, e := newNormalSystemRoleBrokerComponent(g, s, ns)
	if e != nil {
		return nil, e
	}
	serviceCtx, cancel := context.WithCancel(ctx)
	v := &normalSystemRoleService{source: s, declaration: d, namespace: ns, broker: b, parent: parent, metadata: metadata, ctx: serviceCtx, cancel: cancel, requests: map[[32]byte]struct{}{}}
	success := false
	defer func() {
		if !success {
			v.close()
		}
	}()
	fd, e := unix.Open(filepath.Join(g.config.StateDir, normalSystemRoleServiceLockName), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if e != nil {
		return nil, e
	}
	v.lock = os.NewFile(uintptr(fd), "original-System-role-broker-service-lock")
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Uid != 0 || st.Nlink != 1 || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0077 != 0 {
		return nil, fmt.Errorf("original broker service lock differs")
	}
	if e = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); e != nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original broker already active"))
	}
	if e = rootDirectory(filepath.Dir(normalSystemRoleServiceSocketPath), 0755); e != nil {
		return nil, e
	}
	if _, e = os.Lstat(normalSystemRoleServiceSocketPath); !errors.Is(e, os.ErrNotExist) {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("broker socket exists; explicit original recovery required"))
	}
	v.listener, e = net.ListenUnix("unixpacket", &net.UnixAddr{Name: normalSystemRoleServiceSocketPath, Net: "unixpacket"})
	if e != nil {
		return nil, e
	}
	v.listener.SetUnlinkOnClose(false)
	if e = os.Chmod(normalSystemRoleServiceSocketPath, 0600); e != nil {
		return nil, e
	}
	if e = unix.Lstat(normalSystemRoleServiceSocketPath, &st); e != nil || st.Uid != 0 || st.Mode&unix.S_IFMT != unix.S_IFSOCK || st.Mode&0777 != 0600 {
		return nil, fmt.Errorf("fixed broker socket identity differs")
	}
	v.metadata.SocketDevice = st.Dev
	v.metadata.SocketInode = st.Ino
	success = true
	return v, nil
}
func (v *normalSystemRoleService) serve() error {
	semaphore := make(chan struct{}, 16)
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		<-v.ctx.Done()
		if v.listener != nil {
			_ = v.listener.Close()
		}
	}()
	for {
		conn, e := v.listener.AcceptUnix()
		if e != nil {
			if v.ctx.Err() != nil {
				return v.ctx.Err()
			}
			v.cancel()
			return e
		}
		select {
		case semaphore <- struct{}{}:
			v.wg.Add(1)
			go func() { defer func() { <-semaphore; v.wg.Done() }(); defer conn.Close(); _ = v.handle(conn) }()
		default:
			_ = conn.Close()
		}
	}
}
func (v *normalSystemRoleService) close() {
	if v == nil {
		return
	}
	v.closeOnce.Do(func() {
		v.cancel()
		if v.listener != nil {
			v.listener.Close()
		}
		v.wg.Wait()
		if v.lock != nil {
			unix.Flock(int(v.lock.Fd()), unix.LOCK_UN)
			v.lock.Close()
		}
	})
}
func (v *normalSystemRoleService) handle(conn *net.UnixConn) error {
	deadline := time.Now().Add(10 * time.Second)
	raw, e := arrayServicePacket(conn, deadline, normalSystemRoleServiceRequestBytes, false)
	if e != nil {
		return e
	}
	request, e := decodeNormalSystemRoleServiceFrame(raw)
	if e != nil {
		return e
	}
	nonce, e := nativeBirthDigest(v.metadata.Broker.RootConsumer.InstanceNonce)
	if e != nil || nonce != request.BrokerNonce {
		return fmt.Errorf("original broker instance differs")
	}
	peer, e := normalSystemRoleServicePeer(conn, v.source.binding.GuestBootID)
	if e != nil {
		return e
	}
	defer peer.close()
	call, cancel := context.WithDeadline(v.ctx, deadline)
	defer cancel()
	if request.Operation == normalSystemRoleServiceProbeReady {
		if e = arrayMatchPIDFD(peer.pidfd, v.parent.pidfd); e != nil {
			return e
		}
		if peer.source.PID != v.metadata.Origin.Parent.RootConsumer.PID || peer.source.StartTicks != uint64(v.metadata.Origin.Parent.RootConsumer.StartTicks) {
			return fmt.Errorf("original initial Root differs")
		}
		argv, e := normalSystemReadVector(peer.procfd, "cmdline")
		if e != nil {
			return e
		}
		if len(argv) != 2 || argv[0] != normalSystemRoleBrokerExecutable || argv[1] != "initial-boot" && argv[1] != "recover-initial-boot" {
			return fmt.Errorf("ready probe original action differs")
		}
		if e = normalSystemRoleServiceVerifyTask(call, v.declaration, v.namespace, peer, argv); e != nil {
			return e
		}
		return normalSystemRoleServiceWriteReply(conn, request, normalSystemRoleServiceComplete, 0, deadline)
	}
	if !v.enabled.Load() {
		return normalSystemRoleServiceWriteReply(conn, request, normalSystemRoleServiceUnknown, 0, deadline)
	}
	role, e := normalSystemRoleServiceRoleName(request.Role)
	if e != nil {
		return e
	}
	if e = normalSystemRoleServicePID1Client(peer); e != nil {
		return e
	}
	if e = normalSystemRoleServiceVerifyTask(call, v.declaration, v.namespace, peer, []string{normalSystemRoleBrokerExecutable, "exec-system-role", role}); e != nil {
		return e
	}
	if e = normalSystemRoleServiceClientDescriptors(peer.procfd, role); e != nil {
		return e
	}
	peerDiagnostic := normalSystemRoleDiagnosticPeerBaseline(v.ctx, peer)
	if role == "compute" {
		pin, e := normalSystemRoleServiceCaptureComputeLockPeer(peer)
		if e != nil {
			return e
		}
		defer pin.Close()
	}
	v.mu.Lock()
	_, duplicate := v.requests[request.RequestNonce]
	if duplicate || len(v.requests) >= 1024 {
		v.mu.Unlock()
		return normalSystemRoleServiceWriteReply(conn, request, normalSystemRoleServiceUnknown, 0, deadline)
	}
	v.requests[request.RequestNonce] = struct{}{}
	v.mu.Unlock()
	if role == "resource" && v.authority != nil {
		return v.attachResource(conn, peer, request)
	}
	queue, queueCancel := context.WithTimeout(v.ctx, 90*time.Second)
	defer queueCancel()
	if role == "compute" {
		// 原排队预算内锁外等待，尚无pending/stdio/exec效果。
		if e = v.awaitComputeStartup(queue); e != nil {
			return normalSystemRoleServiceWriteReply(conn, request, normalSystemRoleServiceUnknown, 0, time.Now().Add(5*time.Second))
		}
	}
	run, e := v.spawnRole(queue, role, peer)
	result := uint64(0)
	if e != nil {
		result = 1
	}
	normalSystemRoleDiagnosticRecord(v.ctx, normalRoleDiagSpawnReturned, role, 0, result)
	if e != nil {
		fmt.Fprintln(os.Stderr, "fixed System role", role, "spawn denied:", e)
		return normalSystemRoleServiceWriteReply(conn, request, normalSystemRoleServiceUnknown, 0, deadline)
	}
	if e = normalSystemRoleServiceWriteReply(conn, request, normalSystemRoleServiceAccepted, 0, time.Now().Add(10*time.Second)); e != nil {
		decision := normalSystemRoleDiagnosticDecision(v.ctx)
		_ = run.child.cancelAndWait()
		_ = v.finishExitedRole(run)
		normalSystemRoleDiagnosticRecord(v.ctx, normalRoleDiagKillAcceptedWrite, role, decision, 0)
		return e
	}
	if e = conn.SetDeadline(time.Time{}); e != nil {
		decision := normalSystemRoleDiagnosticDecision(v.ctx)
		_ = run.child.cancelAndWait()
		_ = v.finishExitedRole(run)
		normalSystemRoleDiagnosticRecord(v.ctx, normalRoleDiagKillClearDeadline, role, decision, 0)
		return e
	}
	// Accepted之后握手和排队期限不再控制target运行；原client Background长等。
	// 原client消失/断连或broker正常关闭时，只取消该原role pidfd并真正join；
	// 绝不以连接断开为无效果，也不生成新role。唯一Wait仍由child.waitOnce执行。
	monitorDone := make(chan struct{})
	monitorExited := make(chan struct{})
	run.child.startWait()
	go func() {
		defer close(monitorExited)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-monitorDone:
				return
			case <-run.child.done:
				return
			case <-v.ctx.Done():
				decision := normalSystemRoleDiagnosticDecision(v.ctx)
				_ = run.child.cancelAndWait()
				normalSystemRoleDiagnosticRecord(v.ctx, normalRoleDiagKillServiceContext, role, decision, 0)
				return
			case <-ticker.C:
				// 保原OR短路顺序；先确定原杀分支，原kill/Wait完毕后才记诊断。
				peer.mu.Lock()
				observation, aliveErr := arrayRegistryPIDFDAliveBounded(peer.pidfd)
				peer.mu.Unlock()
				if aliveErr != nil {
					decision := normalSystemRoleDiagnosticDecision(v.ctx)
					_ = run.child.cancelAndWait()
					normalSystemRoleDiagnosticRecordPeerPoll(v.ctx, role, decision, peerDiagnostic, observation)
					return
				}
				if observation.Interruptions > 0 {
					normalSystemRoleDiagnosticRecordPeerRestart(v.ctx, role, peerDiagnostic, observation)
				}
				if normalSystemRoleServiceConnectionGone(conn) {
					decision := normalSystemRoleDiagnosticDecision(v.ctx)
					_ = run.child.cancelAndWait()
					normalSystemRoleDiagnosticRecord(v.ctx, normalRoleDiagKillConnectionGone, role, decision, 0)
					return
				}
			}
		}
	}()
	// 长服务仍活着时不得调用旧finish：旧finish先置Unknown，再等待退出。
	// 这里只等待原单一Wait的done，creator租约仍在原M维持；退出后才串行归档。
	<-run.child.done
	finishErr := v.finishExitedRole(run)
	close(monitorDone)
	<-monitorExited
	status, exit := normalSystemRoleServiceOriginalExitResult(run, finishErr)
	return normalSystemRoleServiceWriteReply(conn, request, status, exit, time.Now().Add(5*time.Second))
}

// execMu仅串行原spawn/已退出归档，不跨活服务的生命周期等待。所有候选只来自
// 本broker原runs私有对象；不扫描WAL接管，也不重试已创建的pending或Unknown。
func (v *normalSystemRoleService) spawnRole(ctx context.Context, role string, peer *normalSystemOriginalTask) (*normalSystemRoleOriginalRun, error) {
	v.execMu.Lock()
	defer v.execMu.Unlock()
	if e := ctx.Err(); e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	stage, stageCancel := context.WithTimeout(ctx, 30*time.Second)
	defer stageCancel()
	// 排队不消耗exec-stop期限；取得原序列锁后再核同原peer/Source/FD，不给迟到请求补签。
	if e := normalSystemRoleServicePID1Client(peer); e != nil {
		return nil, e
	}
	if e := normalSystemRoleServiceVerifyTask(stage, v.declaration, v.namespace, peer, []string{normalSystemRoleBrokerExecutable, "exec-system-role", role}); e != nil {
		return nil, e
	}
	if e := normalSystemRoleServiceClientDescriptors(peer.procfd, role); e != nil {
		return nil, e
	}
	if role == "compute" {
		pin, e := normalSystemRoleServiceCaptureComputeLockPeer(peer)
		if e != nil {
			return nil, e
		}
		defer pin.Close()
	}
	for {
		if _, e := v.retireExitedRolesLocked(); e != nil {
			return nil, e
		}
		v.broker.mu.Lock()
		before := len(v.broker.runs)
		v.broker.mu.Unlock()
		run, e := v.broker.spawn(stage, role)
		if e == nil {
			if role == "compute" && v.authority != nil {
				if run.child.installedMain != nil {
					e = v.authority.verifyOriginalComputeV2Published(stage, run)
				} else {
					e = v.authority.bindOriginalCompute(stage, run)
				}
				if e != nil {
					_ = run.child.cancelAndWait()
					_ = v.finishExitedRoleLocked(run)
					return nil, errors.Join(ErrUnknown, e)
				}
			}
			return run, nil
		}
		v.broker.mu.Lock()
		changed := len(v.broker.runs) != before
		v.broker.mu.Unlock()
		if changed {
			return nil, e
		}
		// 子退出到Go唯一Wait发布done之间可能夹着另一角色spawn。只等原pidfd已不活
		// 的已知child；等待不是已退出证明，必须实际done+waited后才调用原finish。
		pending := v.originalExitWaits()
		if len(pending) == 0 {
			return nil, e
		}
		for _, done := range pending {
			select {
			case <-done:
			case <-ctx.Done():
				return nil, errors.Join(ErrUnknown, ctx.Err())
			}
		}
	}
}
func (v *normalSystemRoleService) originalExitWaits() []<-chan struct{} {
	v.broker.mu.Lock()
	defer v.broker.mu.Unlock()
	var waits []<-chan struct{}
	for _, r := range v.broker.runs {
		if r.unknown || r.terminal || r.child == nil {
			continue
		}
		c := r.child
		c.mu.Lock()
		select {
		case <-c.done:
			waits = append(waits, c.done)
		default:
			if arrayRegistryPIDFDAlive(int(c.pidfd.Fd())) != nil {
				waits = append(waits, c.done)
			}
		}
		c.mu.Unlock()
	}
	return waits
}
func (v *normalSystemRoleService) retireExitedRolesLocked() (bool, error) {
	v.broker.mu.Lock()
	var exited []*normalSystemRoleOriginalRun
	for _, r := range v.broker.runs {
		if r.unknown {
			v.broker.mu.Unlock()
			return false, ErrUnknown
		}
		if r.terminal || r.child == nil {
			continue
		}
		select {
		case <-r.child.done:
			exited = append(exited, r)
		default:
		}
	}
	v.broker.mu.Unlock()
	for _, r := range exited {
		_ = v.finishExitedRoleLocked(r)
		v.broker.mu.Lock()
		terminal := r.terminal
		v.broker.mu.Unlock()
		if !terminal {
			return false, ErrUnknown
		}
	}
	return len(exited) > 0, nil
}
func (v *normalSystemRoleService) finishExitedRole(r *normalSystemRoleOriginalRun) error {
	v.execMu.Lock()
	defer v.execMu.Unlock()
	return v.finishExitedRoleLocked(r)
}
func (v *normalSystemRoleService) finishExitedRoleLocked(r *normalSystemRoleOriginalRun) error {
	if v.finished == nil {
		v.finished = map[*normalSystemRoleOriginalRun]error{}
	}
	if e, ok := v.finished[r]; ok {
		return e
	}
	if r == nil || r.child == nil {
		return ErrUnknown
	}
	select {
	case <-r.child.done:
	default:
		return ErrUnknown
	}
	// 原finish不重新核全部前缀；service在它之前只接受原私有三阶段持久见证。
	// 已退出不会把被替换文件/丢失见证洗成可恢复完成；失败持续Unknown。
	r.child.mu.Lock()
	waited := r.child.waited
	r.child.mu.Unlock()
	if !waited {
		v.broker.mu.Lock()
		r.unknown = true
		v.broker.mu.Unlock()
		v.finished[r] = ErrUnknown
		return ErrUnknown
	}
	v.broker.mu.Lock()
	e := v.broker.guard.withRecord(context.Background(), func(rec *record, state *lockedState) error {
		if r.unknown || r.terminal || r.pending.Binding != v.broker.source.binding || rec.Identity != r.pending.ResourceIdentity {
			return ErrUnknown
		}
		var st unix.Stat_t
		if unix.Fstat(int(state.dir.Fd()), &st) != nil || st.Dev != r.pending.StateDevice || st.Ino != r.pending.StateInode {
			return ErrUnknown
		}
		if len(r.seen) != 3 {
			return ErrUnknown
		}
		for _, stage := range []string{"pending", "exec-stopped", "released"} {
			name := normalSystemRoleStageName(r.pending.Nonce, stage)
			wanted, ok := r.seen[name]
			if !ok {
				return ErrUnknown
			}
			got, e := readNormalSystemRoleFileWitness(state, name)
			if e != nil || got != wanted {
				return errors.Join(ErrUnknown, e)
			}
		}
		return nil
	})
	v.broker.mu.Unlock()
	if e != nil {
		v.broker.mu.Lock()
		r.unknown = true
		v.broker.mu.Unlock()
		v.finished[r] = e
		return e
	}
	e = r.finish(context.Background(), false)
	v.finished[r] = e
	return e
}

// 只有同原run的单一Wait、creator租约和durable exited都确定，才映射真实退出。
// 不用errors.As穿透任意Join吞掉Unknown/持久失败；唯一允许的非nil叶子是
// 原cmd.Wait返回的同一个*exec.ExitError，且ProcessState必须同原cmd对象。
func normalSystemRoleServiceOriginalExitResult(r *normalSystemRoleOriginalRun, finishErr error) (uint32, uint32) {
	unknown := uint32(normalSystemRoleServiceUnknown)
	if r == nil || r.broker == nil || r.child == nil {
		return unknown, 0
	}
	r.broker.mu.Lock()
	terminal := r.terminal && !r.unknown
	r.broker.mu.Unlock()
	if !terminal {
		return unknown, 0
	}
	c := r.child
	select {
	case <-c.done:
	default:
		return unknown, 0
	}
	if c.creatorLeaseDone == nil {
		return unknown, 0
	}
	select {
	case <-c.creatorLeaseDone:
	default:
		return unknown, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.waited || c.creatorLeaseErr != nil || c.cmd == nil || c.cmd.ProcessState == nil {
		return unknown, 0
	}
	state := c.cmd.ProcessState
	status, ok := state.Sys().(syscall.WaitStatus)
	if !ok {
		return unknown, 0
	}
	if c.waitErr != nil {
		target, ok := c.waitErr.(*exec.ExitError)
		if !ok || target.ProcessState != state {
			return unknown, 0
		}
	}
	if !normalSystemRoleServiceOnlyOriginalWaitError(finishErr, c.waitErr) {
		return unknown, 0
	}
	if status.Exited() {
		code := status.ExitStatus()
		if code < 0 || code > 255 || code == 0 && c.waitErr != nil || code != 0 && c.waitErr == nil {
			return unknown, 0
		}
		return normalSystemRoleServiceComplete, uint32(code)
	}
	if status.Signaled() && c.waitErr != nil {
		code := 128 + int(status.Signal())
		if code >= 128 && code <= 255 {
			return normalSystemRoleServiceComplete, uint32(code)
		}
	}
	return unknown, 0
}
func normalSystemRoleServiceOnlyOriginalWaitError(combined, original error) bool {
	if combined == nil {
		return original == nil
	}
	if original == nil {
		return false
	}
	count, nodes := 0, 0
	var walk func(error, int) bool
	walk = func(e error, depth int) bool {
		nodes++
		if e == nil || depth > 8 || nodes > 16 {
			return false
		}
		if joined, ok := e.(interface{ Unwrap() []error }); ok {
			children := joined.Unwrap()
			if len(children) == 0 {
				return false
			}
			for _, child := range children {
				if !walk(child, depth+1) {
					return false
				}
			}
			return true
		}
		if e != original {
			return false
		}
		count++
		return count == 1
	}
	return walk(combined, 0) && count == 1
}
func normalSystemRoleServiceWriteReply(conn *net.UnixConn, request normalSystemRoleServiceFrame, status, exit uint32, deadline time.Time) error {
	if e := conn.SetWriteDeadline(deadline); e != nil {
		return e
	}
	raw := normalSystemRoleServiceReply(request, status, exit)
	n, e := conn.Write(raw)
	if e != nil || n != len(raw) {
		return errors.Join(ErrUnknown, e, fmt.Errorf("fixed role reply unconfirmed"))
	}
	return nil
}
func normalSystemRoleServiceConnectionGone(conn *net.UnixConn) bool {
	raw, e := conn.SyscallConn()
	if e != nil {
		return true
	}
	gone := false
	e = raw.Control(func(fd uintptr) {
		var b [1]byte
		n, _, _, _, e := unix.Recvmsg(int(fd), b[:], nil, unix.MSG_PEEK|unix.MSG_DONTWAIT)
		// EINTR只是不确定，不能证明原连接结束；下个受限tick重核。
		gone = e != unix.EAGAIN && e != unix.EWOULDBLOCK && e != unix.EINTR && (e != nil || n >= 0)
	})
	return e != nil || gone
}

func (v *normalSystemRoleService) publishReady(ctx context.Context, readySHA string) error {
	if !normalSystemRoleDeclarationDigest(readySHA) || v.enabled.Load() {
		return ErrUnknown
	}
	v.metadata.ReadySHA256 = readySHA
	raw, e := json.Marshal(v.metadata)
	if e != nil {
		return e
	}
	f, e := os.OpenFile(normalSystemRoleServiceMetadataPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return errors.Join(ErrUnknown, e)
	}
	n, e := f.Write(raw)
	if e == nil && n != len(raw) {
		e = fmt.Errorf("original broker metadata write short")
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return e
	}
	if ce != nil {
		return ce
	}
	dir, e := os.Open(filepath.Dir(normalSystemRoleServiceMetadataPath))
	if e != nil {
		return e
	}
	e = dir.Sync()
	dir.Close()
	if e != nil {
		return e
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	if v.authority != nil {
		if e = v.authority.activate(v.metadata.ResourceAuthority); e != nil {
			return e
		}
	}
	v.enabled.Store(true)
	return nil
}
func normalSystemRoleServiceMetadataDigest(m normalSystemRoleServiceMetadata) string {
	b, _ := json.Marshal(m)
	return physicalDigest(b)
}
func normalSystemRoleServiceNonce() ([32]byte, error) {
	n, e := newNativeBirthNonce()
	if e != nil {
		return [32]byte{}, e
	}
	return nativeBirthDigest(n)
}
func normalSystemRoleServiceNonceHex(n [32]byte) string { return hex.EncodeToString(n[:]) }
