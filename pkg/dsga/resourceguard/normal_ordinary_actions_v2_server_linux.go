//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"sync"
	"time"
)

type normalOrdinaryActionEndpoint struct {
	role, path string
	gid        uint32
}

var normalOrdinaryActionEndpoints = []normalOrdinaryActionEndpoint{
	{"acpi", "/run/neon-dsga/ordinary-acpi.sock", 60006},
	{"daemon", "/run/neon-dsga/ordinary-tls.sock", 60009},
}

// 只提供固定ACPI/TLS动作；udev原kernel-event票据producer另批闭合，不能声称全9可用。
type normalOrdinaryActionsV2 struct {
	g                  *Guard
	filesystem         *normalFilesystemOriginSource
	proof              *normalSystemRoleBrokerExecProof
	authority          *normalSystemResourceAuthority
	originalFS         normalFilesystemOriginReference
	reader             normalSystemOriginalRoleMainLeaseReader
	ctx                context.Context
	cancel             context.CancelFunc
	listeners          []*net.UnixListener
	pid1, tlsDirectory *os.File
	tlsObject          normalFilesystemObject
	powerAttempted     bool // 仅原resource flock下访问；失败后不重发。
	done               chan error
	workers            sync.WaitGroup
	once               sync.Once
}

// 调用者持原resource锁；从真实typedFS/proof/R2同实例构造，不接raw来源/路径/预算。
func (g *Guard) prepareNormalOrdinaryActionsV2Locked(ctx context.Context, state *lockedState, r *record, filesystem *normalFilesystemOriginSource, proof *normalSystemRoleBrokerExecProof, authority *normalSystemResourceAuthority) (out *normalOrdinaryActionsV2, err error) {
	fail := func(e error) (*normalOrdinaryActionsV2, error) { return nil, errors.Join(ErrUnknown, e) }
	reader, ok := any(g).(normalSystemOriginalRoleMainLeaseReader)
	if ctx == nil || ctx.Err() != nil || g == nil || state == nil || r == nil || filesystem == nil || proof == nil || authority == nil || !ok || filesystem.g != g || authority.guard != g || authority.source != filesystem.system || authority.admission == nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return fail(fmt.Errorf("original fixed action producers unavailable"))
	}
	ref, e := filesystem.referenceLocked(ctx, state)
	if e != nil {
		return fail(e)
	}
	family, e := normalFilesystemInitialFamilyFromMemory(filesystem.intent.Memory)
	if e != nil || ref.ProtocolVersion != 2 || family != normalInitialWALFamilyKernelPrepV2 {
		return fail(fmt.Errorf("fixed actions require original metadata filesystem2"))
	}
	task, e := proof.originalTaskForFilesystemLocked(ctx, state, filesystem.system)
	if e != nil {
		return fail(e)
	}
	defer task.close()
	if proof.transfer.Origin.Filesystem != ref {
		return fail(fmt.Errorf("fixed action original proof differs"))
	}
	run, cancel := context.WithCancel(ctx)
	q := &normalOrdinaryActionsV2{g: g, filesystem: filesystem, proof: proof, authority: authority, reader: reader, originalFS: ref, ctx: run, cancel: cancel, done: make(chan error, 1)}
	success := false
	defer func() {
		if !success {
			q.close()
		}
	}()
	filesystem.mu.Lock()
	if filesystem.closed || filesystem.pid1 == nil || filesystem.pid1.pidfd == nil {
		filesystem.mu.Unlock()
		return fail(ErrUnknown)
	}
	fd, e := unix.FcntlInt(filesystem.pid1.pidfd.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	filesystem.mu.Unlock()
	if e != nil {
		return fail(e)
	}
	q.pid1 = os.NewFile(uintptr(fd), "ordinary-original-pid1")
	if e = CheckManagementTLSDirectory(); e != nil {
		return fail(e)
	}
	fd, e = unix.Open(FixedManagementTLSPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return fail(e)
	}
	q.tlsDirectory = os.NewFile(uintptr(fd), "ordinary-fixed-management-directory")
	q.tlsObject, e = normalFilesystemFDObject(fd)
	if e != nil {
		return fail(e)
	}
	if e = rootDirectory("/run/neon-dsga", 0755); e != nil {
		return fail(e)
	}
	for _, endpoint := range normalOrdinaryActionEndpoints {
		// 原broker启动不收养任何旧listener，即便路径是Root socket也不删除/替换。
		var st unix.Stat_t
		if e = unix.Lstat(endpoint.path, &st); e == nil || !errors.Is(e, os.ErrNotExist) {
			return fail(fmt.Errorf("fixed action listener already exists or differs"))
		}
		listener, e := net.ListenUnix("unixpacket", &net.UnixAddr{Name: endpoint.path, Net: "unixpacket"})
		if e != nil {
			return fail(e)
		}
		listener.SetUnlinkOnClose(false)
		q.listeners = append(q.listeners, listener)
		if e = normalOrdinaryEnablePasscred(listener); e != nil {
			return fail(e)
		}
		if e = os.Chown(endpoint.path, 0, int(endpoint.gid)); e != nil {
			return fail(e)
		}
		if e = os.Chmod(endpoint.path, 0660); e != nil {
			return fail(e)
		}
		q.workers.Add(1)
		go q.serve(listener, endpoint)
	}
	success = true
	return q, nil
}

func (q *normalOrdinaryActionsV2) close() {
	if q == nil {
		return
	}
	q.once.Do(func() {
		q.cancel()
		for _, l := range q.listeners {
			_ = l.Close()
		}
		q.workers.Wait()
		if q.pid1 != nil {
			_ = q.pid1.Close()
		}
		if q.tlsDirectory != nil {
			_ = q.tlsDirectory.Close()
		}
	})
}
func (q *normalOrdinaryActionsV2) serve(listener *net.UnixListener, endpoint normalOrdinaryActionEndpoint) {
	defer q.workers.Done()
	for q.ctx.Err() == nil {
		_ = listener.SetDeadline(time.Now().Add(250 * time.Millisecond))
		conn, e := listener.AcceptUnix()
		if e != nil {
			if n, ok := e.(net.Error); ok && n.Timeout() {
				continue
			}
			if q.ctx.Err() == nil {
				select {
				case q.done <- errors.Join(ErrUnknown, fmt.Errorf("fixed ordinary listener stopped")):
				default:
				}
				q.cancel()
			}
			return
		}
		// 每监听端只有一个有界连接/缓冲；串行操作最终统一原resource锁。
		_ = q.handle(conn, endpoint)
		_ = conn.Close()
	}
}
func (q *normalOrdinaryActionsV2) handle(conn *net.UnixConn, endpoint normalOrdinaryActionEndpoint) error {
	call, cancel := context.WithTimeout(q.ctx, 2*time.Second)
	defer cancel()
	deadline, _ := call.Deadline()
	if e := conn.SetDeadline(deadline); e != nil {
		return e
	}
	if e := normalOrdinaryEnablePasscred(conn); e != nil {
		return e
	}
	_, fd, e := arraySocketPeer(conn)
	if e != nil {
		return e
	}
	peer := os.NewFile(uintptr(fd), "ordinary-original-socket-peer")
	defer peer.Close()
	data := make([]byte, 16+normalOrdinaryActionMaxBody+1)
	oob := make([]byte, unix.CmsgSpace(unix.SizeofUcred)+unix.CmsgSpace(16*4))
	n, on, flags, e := normalOrdinaryReadPacket(conn, data, oob)
	if e != nil {
		return e
	}
	cred, e := normalOrdinaryPacketCredentials(oob[:on], flags)
	if e != nil {
		return e
	}
	action, body, e := decodeNormalOrdinaryActionFrame(data[:n], endpoint.role)
	if e != nil {
		return e
	}
	var response []byte
	e = q.g.withRecord(call, func(r *record, state *lockedState) error {
		if !q.authority.admission.businessAllowed() {
			return ErrUnknown
		}
		ref, e := q.filesystem.referenceLocked(call, state)
		if e != nil || ref != q.originalFS {
			return errors.Join(ErrUnknown, e)
		}
		original, e := q.proof.originalTaskForFilesystemLocked(call, state, q.filesystem.system)
		if e != nil {
			return e
		}
		original.close()
		lease, e := q.reader.normalSystemOriginalRoleMainLeaseLocked(call, state, r, endpoint.role)
		if e != nil {
			return e
		}
		defer lease.close()
		if e = lease.matchOriginalMainPacket(call, cred, peer); e != nil {
			return e
		}
		switch action {
		case normalOrdinaryPowerAction:
			return q.poweroffLocked(call, state, r)
		case normalOrdinaryTLSReplaceAction:
			payload, e := decodeNormalOrdinaryTLSFiles(body)
			if e != nil {
				return e
			}
			if e = q.checkTLSDirectory(); e != nil {
				return e
			}
			if e = normalOrdinaryTLSAtomicWrite(FixedManagementTLSPath, payload); e != nil {
				return e
			}
			return q.checkTLSDirectory()
		case normalOrdinaryTLSChecksumAction:
			if e = q.checkTLSDirectory(); e != nil {
				return e
			}
			sum, e := normalOrdinaryTLSChecksum(FixedManagementTLSPath)
			if e != nil {
				return e
			}
			response = []byte(sum)
			return q.checkTLSDirectory()
		default:
			return ErrUnknown // 无Root udev原事件票据绝不执行伪emitter。
		}
	})
	if e != nil {
		return e
	}
	reply := normalOrdinaryActionFrame(action, response, true)
	n, _, e = conn.WriteMsgUnix(reply, nil, nil)
	if e != nil || n != len(reply) {
		return errors.Join(ErrUnknown, e)
	}
	return nil
}
func (q *normalOrdinaryActionsV2) checkTLSDirectory() error {
	if e := CheckManagementTLSDirectory(); e != nil {
		return e
	}
	fd, e := unix.Open(FixedManagementTLSPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	defer unix.Close(fd)
	current, e := normalFilesystemFDObject(fd)
	if e != nil || current != q.tlsObject {
		return errors.Join(ErrUnknown, fmt.Errorf("original TLS directory object changed"))
	}
	held, e := normalFilesystemFDObject(int(q.tlsDirectory.Fd()))
	if e != nil || held != q.tlsObject {
		return errors.Join(ErrUnknown, e)
	}
	return nil
}
func (q *normalOrdinaryActionsV2) poweroffLocked(ctx context.Context, state *lockedState, r *record) error {
	if q.powerAttempted {
		return errors.Join(ErrUnknown, fmt.Errorf("original fixed power action already attempted"))
	}
	q.powerAttempted = true
	value := struct {
		ProtocolVersion uint32              `json:"protocol_version"`
		Scope           string              `json:"scope"`
		Identity        Identity            `json:"identity"`
		Root            nativeBirthConsumer `json:"original_root"`
	}{2, "ordinary-original-pid1-poweroff-v2", r.Identity, q.filesystem.current}
	raw, e := json.Marshal(value)
	if e != nil {
		return e
	}
	if e = state.createPhysicalFile("ordinary-poweroff-v2-intent.json", raw); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	if e = arrayRegistryPIDFDAlive(int(q.pid1.Fd())); e != nil {
		return e
	}
	if e = unix.PidfdSendSignal(int(q.pid1.Fd()), unix.SIGUSR2, nil, 0); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	done, _ := json.Marshal(struct {
		ProtocolVersion uint32 `json:"protocol_version"`
		IntentSHA256    string `json:"intent_sha256"`
	}{2, physicalDigest(raw)})
	if e = state.createPhysicalFile("ordinary-poweroff-v2-complete.json", done); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	return nil
}
