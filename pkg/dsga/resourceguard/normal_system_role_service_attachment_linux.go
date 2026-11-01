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
	"time"
)

var errNormalResourceAlreadyAttached = errors.New("original resource attachment remains active")

type normalSystemResourceAttachment struct {
	nonce                         string
	peer                          *normalSystemOriginalTask
	ended                         bool
	attachedWitness, endedWitness normalSystemRoleFileWitness
}

func (a *normalSystemResourceAttachment) close() {
	if a != nil && a.peer != nil {
		a.peer.close()
	}
}

type normalSystemResourceAttachmentRecord struct {
	ProtocolVersion   uint32                         `json:"protocol_version"`
	Scope             string                         `json:"scope"`
	Phase             string                         `json:"phase"`
	RequestNonce      string                         `json:"request_nonce"`
	Authority         normalSystemAuthorityReference `json:"authority"`
	Broker            normalSystemLifecycleBinding   `json:"broker"`
	ClientPID         int                            `json:"client_pid"`
	ClientStartTicks  uint64                         `json:"client_start_ticks"`
	ClientPIDFDDevice uint64                         `json:"client_pidfd_device"`
	ClientPIDFDInode  uint64                         `json:"client_pidfd_inode"`
}

func (a *normalSystemResourceAuthority) appendAttachmentLocked(state *lockedState, item *normalSystemResourceAttachment, phase string) error {
	name := "role-resource-attach-" + item.nonce + "-" + phase + ".json"
	if phase == "ended" && item.ended {
		got, e := readNormalSystemRoleFileWitness(state, name)
		if e != nil || got != item.endedWitness {
			return errors.Join(ErrUnknown, e)
		}
		return nil
	}
	original := item.peer
	var st unix.Stat_t
	if unix.Fstat(original.pidfd, &st) != nil {
		return ErrUnknown
	}
	value := normalSystemResourceAttachmentRecord{ProtocolVersion: 1, Scope: "original-resource-authority-attachment-v1", Phase: phase, RequestNonce: item.nonce, Authority: a.reference, Broker: a.source.binding, ClientPID: original.source.PID, ClientStartTicks: original.source.StartTicks, ClientPIDFDDevice: st.Dev, ClientPIDFDInode: st.Ino}
	raw, e := json.Marshal(value)
	if e != nil {
		return e
	}
	if e = state.createPhysicalFile(name, raw); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	got, e := readNormalSystemRoleFileWitness(state, name)
	if e != nil || got.SHA256 != physicalDigest(raw) {
		return errors.Join(ErrUnknown, e)
	}
	if phase == "ended" {
		item.ended = true
		item.endedWitness = got
	} else {
		item.attachedWitness = got
	}
	return nil
}
func (a *normalSystemResourceAuthority) appendAttachment(ctx context.Context, peer *normalSystemOriginalTask, nonce, phase string) error {
	return a.guard.withRecord(ctx, func(r *record, state *lockedState) error {
		a.producerMu.Lock()
		defer a.producerMu.Unlock()
		if r.Identity != a.origin.ResourceIdentity || r.Floor < a.origin.ResourceFloor || !normalSystemRoleDeclarationDigest(nonce) {
			return ErrUnknown
		}
		if phase == "attached" {
			if !a.admission.businessAllowed() || a.attachCount >= 128 {
				return ErrUnknown
			}
			if a.attachment != nil && !a.attachment.ended {
				// 新PID1 respawn可能先于旧handler轮询。只按仍持有的原pidfd确认退出，
				// 先持久旧连接结束；不按numeric PID扫描接管、不把它叫target Wait。
				exited, e := normalSystemRoleGateExitObserved(a.attachment.peer.pidfd)
				if e != nil {
					return errors.Join(ErrUnknown, e)
				}
				if !exited {
					return errNormalResourceAlreadyAttached
				}
				if e = a.appendAttachmentLocked(state, a.attachment, "ended"); e != nil {
					return e
				}
			}
			original, e := duplicateNormalSystemOriginalTask(peer)
			if e != nil {
				return e
			}
			item := &normalSystemResourceAttachment{nonce: nonce, peer: original}
			if a.attachments == nil {
				a.attachments = map[string]*normalSystemResourceAttachment{}
			}
			if a.attachments[nonce] != nil {
				original.close()
				return ErrUnknown
			}
			a.attachments[nonce] = item
			a.attachment = item
			a.attachCount++
			return a.appendAttachmentLocked(state, item, "attached")
		}
		item := a.attachments[nonce]
		if phase != "ended" || item == nil {
			return ErrUnknown
		}
		// 同原进程私有完成见证处理迟到旧handler，不让它终结新attachment。
		return a.appendAttachmentLocked(state, item, "ended")
	})
}
func (v *normalSystemRoleService) attachResource(conn *net.UnixConn, peer *normalSystemOriginalTask, request normalSystemRoleServiceFrame) error {
	a := v.authority
	if a == nil || a.guard != v.broker.guard || !v.declaration.declaration.brokerOwnsAuthority() {
		return ErrUnknown
	}
	call, cancel := context.WithTimeout(v.ctx, 30*time.Second)
	defer cancel()
	nonce := normalSystemRoleServiceNonceHex(request.RequestNonce)
	if e := a.appendAttachment(call, peer, nonce, "attached"); e != nil {
		if !errors.Is(e, errNormalResourceAlreadyAttached) {
			a.admission.fail()
		}
		return normalSystemRoleServiceWriteReply(conn, request, normalSystemRoleServiceUnknown, 0, time.Now().Add(5*time.Second))
	}
	if e := normalSystemRoleServiceWriteReply(conn, request, normalSystemRoleServiceAccepted, 0, time.Now().Add(10*time.Second)); e != nil {
		return e
	}
	if e := conn.SetDeadline(time.Time{}); e != nil {
		return e
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-a.admission.done:
			return errors.Join(ErrUnknown, fmt.Errorf("original resource authority stopped"))
		case <-v.ctx.Done():
			return errors.Join(ErrUnknown, v.ctx.Err())
		case <-ticker.C:
			peerErr := arrayRegistryPIDFDAlive(peer.pidfd)
			connectionGone := normalSystemRoleServiceConnectionGone(conn)
			if peerErr != nil || connectionGone {
				fmt.Fprintf(os.Stderr, "resource attachment monitor ended: peer=%d connection_closed=%t peer_error=%v\n", peer.source.PID, connectionGone, peerErr)
				end, done := context.WithTimeout(context.Background(), 30*time.Second)
				e := a.appendAttachment(end, peer, nonce, "ended")
				done()
				if e != nil {
					a.admission.fail()
				}
				return errors.Join(ErrUnknown, e)
			}
		}
	}
}
