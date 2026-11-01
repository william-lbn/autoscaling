//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"time"

	"golang.org/x/sys/unix"
)

const normalSystemRoleBrokerGatePending = "gate-pending"
const normalSystemRoleBrokerGateExec = "gate-exec-stopped"
const normalSystemRoleBrokerGateReleased = "gate-released"

// 原broker的Gate三链独立于普通role family。新broker不删旧记录、也不把
// initialRoot的consumer冒成自己的runs私有完成见证。固定文件选择需Gate薄hook。
func normalSystemRoleServiceGateName(nonce, stage string) string {
	return normalSystemRoleServiceStageName(nonce, stage)
}

type normalSystemRoleServiceGateJournal struct {
	state    *lockedState
	pending  normalSystemRoleJournalEntry
	previous string
	hashes   [3]string
	channel  *os.File
	stopped  bool
}

func (j *normalSystemRoleServiceGateJournal) originalSystemRoleGateReference() (normalSystemRoleGateReference, error) {
	if j == nil || j.state == nil || j.stopped || !normalSystemRoleDeclarationDigest(j.hashes[0]) || j.pending.Role != normalSystemRoleBrokerKind {
		return normalSystemRoleGateReference{}, ErrUnknown
	}
	return normalSystemRoleGateReference{Nonce: j.pending.Nonce, PendingSHA256: j.hashes[0]}, nil
}
func (j *normalSystemRoleServiceGateJournal) originalSystemRoleBrokerGateChannel() (*os.File, error) {
	if j == nil || j.channel == nil {
		return nil, ErrUnknown
	}
	return j.channel, nil
}
func (j *normalSystemRoleServiceGateJournal) append(ctx context.Context, stage string, task *normalSystemRoleObservation) (string, error) {
	if ctx == nil || ctx.Err() != nil || j == nil || j.state == nil {
		return "", ErrUnknown
	}
	entry := j.pending
	entry.Stage = stage
	entry.PreviousSHA256 = j.previous
	entry.Task = task
	raw, e := json.Marshal(entry)
	if e != nil {
		return "", e
	}
	if e = j.state.createPhysicalFile(normalSystemRoleServiceGateName(entry.Nonce, "gate-"+stage), raw); e != nil {
		return "", errors.Join(ErrUnknown, e)
	}
	sha := physicalDigest(raw)
	w, e := readNormalSystemRoleFileWitness(j.state, normalSystemRoleServiceGateName(entry.Nonce, "gate-"+stage))
	if e != nil || w.SHA256 != sha {
		return "", errors.Join(ErrUnknown, e, fmt.Errorf("original broker Gate durable prefix differs"))
	}
	j.previous = sha
	return sha, nil
}
func (j *normalSystemRoleServiceGateJournal) persistOriginalSystemRoleStopped(ctx context.Context, o normalSystemRoleObservation, ns []normalSystemRoleNamespaceIdentity, plan string) error {
	if j == nil || j.stopped || o.Binding != j.pending.Binding || o.Role != normalSystemRoleBrokerKind || plan != j.pending.PlanSHA256 || !o.OriginalTaskObserved || !o.FixedArgvEnvironmentObserved || !reflect.DeepEqual(ns, j.pending.Namespace) {
		return ErrUnknown
	}
	sha, e := j.append(ctx, "exec-stopped", &o)
	if e != nil {
		return e
	}
	j.hashes[1] = sha
	sha, e = j.append(ctx, "released", &o)
	if e != nil {
		return e
	}
	j.hashes[2] = sha
	j.stopped = true
	return nil
}

type normalSystemRoleServiceTransfer struct {
	ProtocolVersion uint32                        `json:"protocol_version"`
	Origin          normalSystemRoleServiceOrigin `json:"origin"`
	IntentSHA256    string                        `json:"intent_sha256"`
	GateSHA256      [3]string                     `json:"gate_sha256"`
	Task            normalSystemRoleObservation   `json:"task"`
}

func (t normalSystemRoleServiceTransfer) validate() error {
	if t.ProtocolVersion != 1 || t.Origin.validate() != nil || !normalSystemRoleDeclarationDigest(t.IntentSHA256) {
		return fmt.Errorf("original broker transfer origin differs")
	}
	for _, h := range t.GateSHA256 {
		if !normalSystemRoleDeclarationDigest(h) {
			return fmt.Errorf("original broker transfer Gate reference missing")
		}
	}
	if !normalSystemRoleDeclarationDigest(t.Task.ExecutableSHA256) || !normalSystemRoleDeclarationDigest(t.Task.ArgvSHA256) || !normalSystemRoleDeclarationDigest(t.Task.EnvironmentSHA256) || t.Task.PIDFDDevice == 0 || t.Task.PIDFDInode == 0 || t.Task.Role != normalSystemRoleBrokerKind || t.Task.PID <= 1 || t.Task.StartTicks == 0 || t.Task.UID != 0 || t.Task.GID != 0 || t.Task.Binding != t.Origin.Parent || !t.Task.OriginalTaskObserved || !t.Task.FixedArgvEnvironmentObserved || t.Task.ContinuousWritersClosed || t.Task.NormalDecreaseAuthorized {
		return fmt.Errorf("original broker transfer task differs")
	}
	return nil
}

type normalSystemRoleBrokerHandle struct {
	child    *normalSystemRoleOriginalChild
	proof    normalSystemRoleServiceTransfer
	broker   normalSystemLifecycleBinding
	readySHA string
}

// 准备与最终ready各自持同resource flock；跨进程FS rebind/监听等待在二者之间，
// 避免父持锁等child而child必须先取锁的环。partial原marker始终保留且无许可。
func startNormalSystemRoleBroker(ctx context.Context, g *Guard, system *normalSystemLifecycleSource, filesystem *normalFilesystemOriginSource) (*normalSystemRoleBrokerHandle, error) {
	fail := func(e error) (*normalSystemRoleBrokerHandle, error) { return nil, errors.Join(ErrUnknown, e) }
	if g == nil {
		return fail(fmt.Errorf("original Guard missing"))
	}
	var preparation *normalSystemRoleBrokerPreparation
	if e := g.withRecord(ctx, func(r *record, state *lockedState) error {
		var e error
		preparation, e = prepareNormalSystemRoleBrokerLocked(ctx, g, state, r, system, filesystem)
		return e
	}); e != nil {
		return fail(e)
	}
	defer preparation.close()
	completed := false
	defer func() {
		if !completed {
			_ = preparation.child.cancelAndWait()
		}
	}()
	call, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	var metadata normalSystemRoleServiceMetadata
	if e := normalSystemRoleServiceReadObject(preparation.connection, call, "NSYSBR01", &metadata); e != nil {
		return fail(e)
	}
	origin := preparation.transfer.Origin
	if metadata.validate() != nil || !reflect.DeepEqual(metadata.Origin, origin) || metadata.ReleasedSHA256 != preparation.transfer.GateSHA256[2] || metadata.Broker.RootConsumer.PID != preparation.transfer.Task.PID || uint64(metadata.Broker.RootConsumer.StartTicks) != preparation.transfer.Task.StartTicks {
		return fail(fmt.Errorf("original broker ready candidate differs"))
	}
	var readySHA string
	e := g.withRecord(call, func(r *record, state *lockedState) error {
		recordRaw, e := json.Marshal(r)
		if e != nil || physicalDigest(recordRaw) != origin.ResourceStageSHA256 {
			return fmt.Errorf("original broker complete resource record changed")
		}
		current, e := nativeBirthCurrentConsumerIdentity()
		if e != nil || current != origin.Parent.RootConsumer {
			return fmt.Errorf("original broker parent consumer changed")
		}
		if e = preparation.child.task.verify(); e != nil {
			return e
		}
		if r.Identity != origin.ResourceIdentity || r.Floor != origin.ResourceFloor || r.Bootstrap == nil || !r.Bootstrap.Applied || !r.Bootstrap.SystemPlacementObserved || r.Original != nil || r.Birth != nil {
			return fmt.Errorf("original broker resource stage changed before ready")
		}
		ref, e := filesystem.referenceLocked(call, state)
		if e != nil || ref != origin.Filesystem {
			return fmt.Errorf("original broker filesystem origin changed")
		}
		if e = normalSystemRoleServiceValidateTransferLocked(call, state, preparation.transfer); e != nil {
			return e
		}
		if e = probeNormalSystemRoleServiceOriginal(call, system, preparation.declaration, preparation.namespaces, preparation.child, metadata); e != nil {
			return e
		}
		ready := normalSystemRoleServiceStage{ProtocolVersion: 1, Origin: origin, Stage: "ready", PreviousSHA256: preparation.transfer.GateSHA256[2], BrokerTask: &preparation.transfer.Task, Broker: &metadata.Broker, SocketDevice: metadata.SocketDevice, SocketInode: metadata.SocketInode}
		readySHA, e = normalSystemRoleServiceAppendStage(state, ready)
		if e != nil {
			return e
		}
		commit := normalSystemRoleServiceCommit{ProtocolVersion: 1, Nonce: origin.Nonce, ReadySHA256: readySHA}
		if e = normalSystemRoleServiceWriteObject(preparation.connection, "NSYSBC01", commit); e != nil {
			return e
		}
		var ack normalSystemRoleServiceCommit
		if e = normalSystemRoleServiceReadObject(preparation.connection, call, "NSYSBA01", &ack); e != nil || ack != commit {
			return fmt.Errorf("original broker commit acknowledgement unknown")
		}
		return arrayRegistryPIDFDAlive(int(preparation.child.pidfd.Fd()))
	})
	if e != nil {
		return fail(e)
	}
	completed = true
	return &normalSystemRoleBrokerHandle{child: preparation.child, proof: preparation.transfer, broker: metadata.Broker, readySHA: readySHA}, nil
}

type normalSystemRoleBrokerPreparation struct {
	child               *normalSystemRoleOriginalChild
	transfer            normalSystemRoleServiceTransfer
	connection          *net.UnixConn
	declaration         *normalSystemRoleDeclarationSource
	namespaces          *normalSystemRoleNamespaceSource
	namespaceFD, rootFD *os.File
}

func (p *normalSystemRoleBrokerPreparation) close() {
	if p == nil {
		return
	}
	if p.connection != nil {
		p.connection.Close()
	}
	if p.declaration != nil {
		p.declaration.close()
	}
	if p.namespaces != nil {
		p.namespaces.close()
	}
	if p.namespaceFD != nil {
		p.namespaceFD.Close()
	}
	if p.rootFD != nil {
		p.rootFD.Close()
	}
}

// 调用者必须持原resource flock；CheckGuest→原FS capture之后才能调用。
// 不重获同一flock、不按请求构造来源，也不把新record字段或许可写入共享state。
func prepareNormalSystemRoleBrokerLocked(ctx context.Context, g *Guard, state *lockedState, r *record, system *normalSystemLifecycleSource, filesystem *normalFilesystemOriginSource) (*normalSystemRoleBrokerPreparation, error) {
	fail := func(e error) (*normalSystemRoleBrokerPreparation, error) { return nil, errors.Join(ErrUnknown, e) }
	var ownedDeclaration *normalSystemRoleDeclarationSource
	var ownedNamespaces *normalSystemRoleNamespaceSource
	var ownedNS, ownedRoot *os.File
	var ownedConn *net.UnixConn
	var ownedChild *normalSystemRoleOriginalChild
	success := false
	defer func() {
		if !success {
			if ownedChild != nil {
				_ = ownedChild.cancelAndWait()
			}
			if ownedConn != nil {
				_ = ownedConn.Close()
			}
			if ownedNS != nil {
				_ = ownedNS.Close()
			}
			if ownedRoot != nil {
				_ = ownedRoot.Close()
			}
			if ownedNamespaces != nil {
				ownedNamespaces.close()
			}
			if ownedDeclaration != nil {
				ownedDeclaration.close()
			}
		}
	}()
	if ctx == nil || ctx.Err() != nil || g == nil || state == nil || r == nil || system == nil || filesystem == nil {
		return fail(fmt.Errorf("original broker start sources missing"))
	}
	if e := normalSystemRoleServiceCaller(); e != nil {
		return fail(e)
	}
	if os.Getppid() != 1 {
		return fail(fmt.Errorf("original broker initial Root is not a PID1 child"))
	}
	if system.manifest.ProtocolVersion != 3 {
		return fail(fmt.Errorf("production broker requires fixed System source v3"))
	}
	source, e := sealedRuntimeSource()
	if e != nil {
		return fail(e)
	}
	if r.Bootstrap == nil || !r.Bootstrap.Applied || !r.Bootstrap.SystemPlacementObserved || r.Original != nil || r.Birth != nil || sameBootstrapSource(source, r.Bootstrap.Original) != nil {
		return fail(fmt.Errorf("original broker requires initial completed resource stage before PG birth"))
	}
	identity, e := g.kernel.Identity()
	if e != nil || r.validate(identity) != nil {
		return fail(fmt.Errorf("original broker resource identity differs"))
	}
	if e = normalSystemRoleServiceRejectHistory(state); e != nil {
		return fail(e)
	}
	declaration, e := readNormalSystemRoleDeclarationOriginal(ctx, system)
	if e != nil {
		return fail(e)
	}
	ownedDeclaration = declaration
	namespaces, e := captureNormalSystemRoleNamespaces()
	if e != nil {
		return fail(e)
	}
	ownedNamespaces = namespaces
	ns, e := namespaces.snapshot()
	if e != nil {
		return fail(e)
	}
	// 锁序始终resource→FS→System→Task；调用FS时不持System.mu。
	ref, nsfd, rootfd, e := filesystem.pinOriginalNamespaceLocked(ctx, state)
	if e != nil {
		return fail(e)
	}
	ownedNS, ownedRoot = nsfd, rootfd
	if ref.OriginalRoot != system.binding.RootConsumer {
		return fail(fmt.Errorf("original broker FS producer is not this initial Root"))
	}
	var st unix.Stat_t
	if e = unix.Fstat(int(state.dir.Fd()), &st); e != nil {
		return fail(e)
	}
	nonce, e := newNativeBirthNonce()
	if e != nil {
		return fail(e)
	}
	recordRaw, e := json.Marshal(r)
	if e != nil {
		return fail(e)
	}
	origin := normalSystemRoleServiceOrigin{ResourceStageSHA256: physicalDigest(recordRaw), ProtocolVersion: 1, Scope: normalSystemRoleServiceStartScope, Nonce: nonce, Parent: system.binding, Filesystem: ref, DeclarationSHA256: declaration.rawSHA, Namespace: ns, ResourceIdentity: r.Identity, ResourceFloor: r.Floor, StateDevice: st.Dev, StateInode: st.Ino}
	if e = origin.validate(); e != nil {
		return fail(e)
	}
	intent := normalSystemRoleServiceStage{ProtocolVersion: 1, Origin: origin, Stage: "intent"}
	intentSHA, e := normalSystemRoleServiceAppendStage(state, intent)
	if e != nil {
		return fail(e)
	}
	fds, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if e != nil {
		return fail(e)
	}
	parentFile := os.NewFile(uintptr(fds[0]), "original broker initialRoot control")
	targetFile := os.NewFile(uintptr(fds[1]), "original broker inherited startup")
	defer targetFile.Close()
	connection, e := net.FileConn(parentFile)
	parentFile.Close()
	if e != nil {
		return fail(e)
	}
	conn := connection.(*net.UnixConn)
	ownedConn = conn
	plan, e := normalSystemRoleGateLaunchPlan(ctx, system, normalSystemRoleBrokerKind)
	if e != nil {
		return fail(e)
	}
	planSHA, e := plan.referenceSHA256()
	if e != nil {
		return fail(e)
	}
	journal := &normalSystemRoleServiceGateJournal{state: state, pending: normalSystemRoleJournalEntry{ProtocolVersion: 1, Scope: normalSystemRoleJournalScope, Nonce: nonce, Stage: "pending", Role: normalSystemRoleBrokerKind, PlanSHA256: planSHA, Binding: system.binding, Namespace: ns, ResourceIdentity: r.Identity, ResourceFloor: r.Floor, StateDevice: st.Dev, StateInode: st.Ino}, channel: targetFile}
	sha, e := journal.append(ctx, "pending", nil)
	if e != nil {
		return fail(e)
	}
	journal.hashes[0] = sha
	call, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	deadline, _ := call.Deadline()
	if e = conn.SetDeadline(deadline); e != nil {
		return fail(e)
	}
	child, e := spawnNormalSystemRoleOriginal(call, system, namespaces, normalSystemRoleBrokerKind, journal)
	if e != nil {
		return fail(e)
	}
	ownedChild = child
	transfer := normalSystemRoleServiceTransfer{ProtocolVersion: 1, Origin: origin, IntentSHA256: intentSHA, GateSHA256: journal.hashes, Task: child.observed.Task}
	if e = transfer.validate(); e != nil {
		return fail(e)
	}
	// 固定4个原kernel对象，不发送新开self pidfd、不按WAL numeric PID重开。
	child.task.mu.Lock()
	proc, e := unix.FcntlInt(uintptr(child.task.procfd), unix.F_DUPFD_CLOEXEC, 0)
	child.task.mu.Unlock()
	if e != nil {
		return fail(e)
	}
	procfd := os.NewFile(uintptr(proc), "original broker pinned proc handoff")
	defer procfd.Close()
	if e = normalSystemRoleServiceWriteTransfer(conn, transfer, []*os.File{child.pidfd, procfd, nsfd, rootfd}, call); e != nil {
		return fail(e)
	}
	success = true
	return &normalSystemRoleBrokerPreparation{child: child, transfer: transfer, connection: conn, declaration: declaration, namespaces: namespaces, namespaceFD: nsfd, rootFD: rootfd}, nil
}

type normalSystemRoleServiceCommit struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	Nonce           string `json:"nonce"`
	ReadySHA256     string `json:"ready_sha256"`
}

func probeNormalSystemRoleServiceOriginal(ctx context.Context, system *normalSystemLifecycleSource, d *normalSystemRoleDeclarationSource, ns *normalSystemRoleNamespaceSource, child *normalSystemRoleOriginalChild, m normalSystemRoleServiceMetadata) error {
	connected, e := (&net.Dialer{}).DialContext(ctx, "unixpacket", normalSystemRoleServiceSocketPath)
	if e != nil {
		return e
	}
	defer connected.Close()
	conn := connected.(*net.UnixConn)
	peer, e := normalSystemRoleServicePeer(conn, system.binding.GuestBootID)
	if e != nil {
		return e
	}
	defer peer.close()
	if e = arrayMatchPIDFD(peer.pidfd, int(child.pidfd.Fd())); e != nil {
		return e
	}
	if e = normalSystemRoleServiceVerifyTask(ctx, d, ns, peer, normalSystemRoleBrokerArgv()); e != nil {
		return e
	}
	var st unix.Stat_t
	if unix.Lstat(normalSystemRoleServiceSocketPath, &st) != nil || st.Dev != m.SocketDevice || st.Ino != m.SocketInode || st.Mode&0777 != 0600 {
		return fmt.Errorf("original broker ready socket changed")
	}
	nonce, e := nativeBirthDigest(m.Broker.RootConsumer.InstanceNonce)
	if e != nil {
		return e
	}
	requestNonce, e := normalSystemRoleServiceNonce()
	if e != nil {
		return e
	}
	request := normalSystemRoleServiceFrame{Operation: normalSystemRoleServiceProbeReady, BrokerNonce: nonce, RequestNonce: requestNonce}
	deadline, _ := ctx.Deadline()
	if e = conn.SetWriteDeadline(deadline); e != nil {
		return e
	}
	raw := encodeNormalSystemRoleServiceFrame(request)
	if n, e := conn.Write(raw); e != nil || n != len(raw) {
		return ErrUnknown
	}
	raw, e = arrayServicePacket(conn, deadline, normalSystemRoleServiceRequestBytes, false)
	if e != nil {
		return e
	}
	status, exit, e := decodeNormalSystemRoleServiceReply(raw, request)
	if e != nil || status != normalSystemRoleServiceComplete || exit != 0 {
		return ErrUnknown
	}
	return nil
}
