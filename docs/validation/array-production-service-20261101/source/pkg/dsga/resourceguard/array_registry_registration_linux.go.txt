//go:build linux

package resourceguard

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

func (o *arrayRegistryOriginal) matchesBirth(q arrayRegistryWireRequest) error {
	if err := q.validate(); err != nil {
		return err
	}
	expected, err := o.pending.Source.expected(o.pending.Nonce, o.pending.Original.AddressSpaceMaxBytes)
	if err != nil {
		return err
	}
	if q.SourceSHA != expected.SourceSHA || q.BirthNonce != expected.Nonce || q.ProfileSHA != expected.ProfileSHA || q.MaximumBuffers != uint64(expected.MaxBuffers) || q.StartupBuffers != uint64(o.pending.Source.Policy.StartupBuffers) {
		return fmt.Errorf("array packet differs from original Birth source/M/N0")
	}
	for i, r := range q.Ranges {
		original := o.reservation.Ranges[i]
		if r.Base != original.Base || r.Mapped != original.ReservedBytes || r.Element != original.ElementBytes || r.Page != uint64(expected.PageBytes) {
			return fmt.Errorf("array packet differs from original reserved geometry")
		}
	}
	return nil
}
func arrayRegistryPeer(o *arrayRegistryOriginal, conn *net.UnixConn) (*unix.Ucred, int, int, error) {
	peer, pidfd, err := arraySocketPeer(conn)
	if err != nil {
		return nil, -1, -1, err
	}
	procfd := -1
	fail := func(err error) (*unix.Ucred, int, int, error) {
		if procfd >= 0 {
			_ = unix.Close(procfd)
		}
		_ = unix.Close(pidfd)
		return nil, -1, -1, err
	}
	if peer == nil || peer.Pid <= 1 || peer.Uid != o.pending.PeerUID || peer.Gid != o.pending.PeerGID {
		return fail(fmt.Errorf("array original kernel peer UID/GID differs"))
	}
	if err = arrayRegistryPIDFDAlive(pidfd); err != nil {
		return fail(err)
	}
	procfd, err = unix.Open("/proc/"+strconv.Itoa(int(peer.Pid)), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fail(err)
	}
	status, err := nativeTraceProcText(procfd, "status")
	if err != nil {
		return fail(err)
	}
	if err = processUID(status, uint64(peer.Uid)); err != nil {
		return fail(err)
	}
	if int(peer.Pid) == o.pending.Original.PostmasterPID {
		if err := arrayMatchPIDFD(pidfd, o.pidfd); err != nil {
			return fail(err)
		}
	} else {
		// 非PM只能是其真实直接backend，不能凭同UID从System调用数组服务。
		parent, err := nativeTraceStatusValues(status, "PPid", 1)
		if err != nil || parent[0] != uint64(o.pending.Original.PostmasterPID) {
			return fail(fmt.Errorf("array requester is not original PM child"))
		}
	}
	group, err := nativeTraceProcText(procfd, "cgroup")
	if err != nil || strings.TrimSpace(group) != "0::"+strings.TrimPrefix(pgCgroup+"/private", "/sys/fs/cgroup") {
		return fail(fmt.Errorf("array peer is outside original private domain"))
	}
	// 原任务可能已退出且numeric PID复用；原SO_PEERPIDFD在每轮末尾仍须存活。
	if err = arrayRegistryPIDFDAlive(pidfd); err != nil {
		return fail(err)
	}
	return peer, pidfd, procfd, nil
}

type arrayRegistryRegistration struct {
	ProtocolVersion   uint32               `json:"protocol_version"`
	Scope             string               `json:"scope"`
	Birth             NativeBirthRef       `json:"birth"`
	ReleasedSHA256    string               `json:"released_sha256"`
	RequestHex        string               `json:"request_hex"`
	RequestSHA256     string               `json:"request_sha256"`
	RegistrationNonce string               `json:"registration_nonce"`
	Files             [4]arrayPopulateFile `json:"files"`
}

func arrayRegistryName(nonce string, generation uint64, phase string) string {
	return "array-registry." + nonce + "." + strconv.FormatUint(generation, 10) + "." + phase + ".json"
}
func (o *arrayRegistryOriginal) registerLocked(s *lockedState, q arrayRegistryWireRequest, raw []byte, peer *unix.Ucred, pidfd, procfd int) error {
	if q.Op != arrayRegistryRegister || int(peer.Pid) != o.pending.Original.PostmasterPID || q.Generation != 1 {
		return fmt.Errorf("array registration must come from original PM startup/reinit")
	}
	if err := o.matchesBirth(q); err != nil {
		return err
	}
	if err := arrayMatchPIDFD(pidfd, o.pidfd); err != nil {
		return err
	}
	if old := o.current; old != nil && (!old.retiring || q.MappingGeneration != old.request.MappingGeneration+1 || q.PGBoot == old.request.PGBoot || q.MappingNonce == old.request.MappingNonce) {
		return errors.Join(ErrUnknown, fmt.Errorf("array new generation lacks original retire/new boot"))
	}
	if o.current == nil && q.MappingGeneration != 1 {
		return errors.Join(ErrUnknown, fmt.Errorf("array first mapping generation differs"))
	}
	files, identities, err := arrayRegistryOpenMappings(procfd, pidfd, q)
	if err != nil {
		return err
	}
	candidate := &arrayRegistryGeneration{request: q, files: files, identities: identities}
	keep := false
	defer func() {
		if !keep {
			candidate.close()
		}
	}()
	if old := o.current; old != nil {
		for _, prior := range old.identities {
			for _, next := range identities {
				if prior.Device == next.Device && prior.Inode == next.Inode {
					return errors.Join(ErrUnknown, fmt.Errorf("array reinit reused original old inode"))
				}
			}
		}
	}
	nonce, err := newNativeBirthNonce()
	if err != nil {
		return err
	}
	decoded, _ := hex.DecodeString(nonce)
	copy(candidate.nonce[:], decoded)
	entry := arrayRegistryRegistration{1, "original-four-array-registration-v1", o.birthRef, o.releasedSHA, hex.EncodeToString(raw), physicalDigest(raw), nonce, identities}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if err = s.createPhysicalFile(arrayRegistryName(o.pending.Nonce, q.MappingGeneration, "registered"), encoded); err != nil {
		// 可见而未确认持久的新FD不能悄悄关闭并归零；保旧代/新代所有保守费用。
		o.retained = append(o.retained, candidate)
		keep = true
		o.failed = true
		return errors.Join(ErrUnknown, err)
	}
	if err = arrayRegistryPIDFDAlive(pidfd); err != nil {
		o.retained = append(o.retained, candidate)
		keep = true
		o.failed = true
		return errors.Join(ErrUnknown, err)
	}
	old := o.current
	o.current = candidate
	keep = true
	// 只在原PM受信reinit门禁之后、完整新4inode已持久注册才退旧Root FD。
	// 仍不声称所有旧folio物理已free；实际charge/counter/I由后继Root资格核。
	if old != nil {
		old.close()
	}
	return nil
}
func (o *arrayRegistryOriginal) retireLocked(s *lockedState, q arrayRegistryWireRequest, raw []byte, peer *unix.Ucred, pidfd int) error {
	if int(peer.Pid) != o.pending.Original.PostmasterPID || o.current == nil || o.current.retiring {
		return errors.Join(ErrUnknown, fmt.Errorf("array retire is not original active PM generation"))
	}
	if err := arrayMatchPIDFD(pidfd, o.pidfd); err != nil {
		return err
	}
	if err := o.matchesCurrent(q); err != nil {
		return err
	}
	entry := struct {
		ProtocolVersion uint32         `json:"protocol_version"`
		RequestSHA256   string         `json:"request_sha256"`
		RequestHex      string         `json:"request_hex"`
		Birth           NativeBirthRef `json:"birth"`
	}{1, physicalDigest(raw), hex.EncodeToString(raw), o.birthRef}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if err = s.createPhysicalFile(arrayRegistryName(o.pending.Nonce, q.MappingGeneration, "retiring"), encoded); err != nil {
		o.failed = true
		return errors.Join(ErrUnknown, err)
	}
	o.current.retiring = true
	return nil
}
func (o *arrayRegistryOriginal) matchesCurrent(q arrayRegistryWireRequest) error {
	if err := o.matchesBirth(q); err != nil {
		return err
	}
	c := o.current
	if c == nil || q.PGBoot != c.request.PGBoot || q.MappingGeneration != c.request.MappingGeneration || q.MappingNonce != c.request.MappingNonce || q.RegistrationNonce != c.nonce || q.Ranges != c.request.Ranges {
		return errors.Join(ErrUnknown, fmt.Errorf("array packet original registration changed"))
	}
	return nil
}

func (s *arrayNativeServices) handleArrays(ctx context.Context, conn *net.UnixConn) error {
	deadline, _ := ctx.Deadline()
	raw, err := arrayServicePacket(conn, deadline, arrayRegistryRequestBytes, false)
	if err != nil {
		return err
	}
	q, err := decodeArrayRegistryWire(raw)
	if err != nil {
		return err
	}
	var reply []byte
	err = s.guard.withRecord(ctx, func(r *record, state *lockedState) error {
		value, exists := arrayGuardBindings.Load(s.guard)
		if !exists {
			return errors.Join(ErrUnknown, fmt.Errorf("array native original is not activated"))
		}
		binding := value.(*arrayGuardBinding)
		o := binding.original
		o.mu.Lock()
		defer o.mu.Unlock()
		if err := o.verifyOriginal(); err != nil {
			return err
		}
		if r.Birth == nil || *r.Birth != o.pending.Original || r.Original != nil && !r.Applied {
			return errors.Join(ErrUnknown, fmt.Errorf("array original Birth/resource source changed"))
		}
		if err := o.matchesBirth(q); err != nil {
			return err
		}
		peer, pidfd, procfd, err := arrayRegistryPeer(o, conn)
		if err != nil {
			return err
		}
		defer unix.Close(pidfd)
		defer unix.Close(procfd)
		switch q.Op {
		case arrayRegistryRegister:
			if err = binding.rejectRPCUnknown(state); err != nil {
				return err
			}
			if err = rejectOutstandingArrayPopulateLocked(state); err != nil {
				return err
			}
			err = o.registerLocked(state, q, raw, peer, pidfd, procfd)
		case arrayRegistryRetireBegin:
			if err = binding.rejectRPCUnknown(state); err != nil {
				return err
			}
			if err = rejectOutstandingArrayPopulateLocked(state); err != nil {
				return err
			}
			err = o.retireLocked(state, q, raw, peer, pidfd)
		case arrayRegistryPopulate:
			err = s.populateLocked(ctx, state, r, binding, q, raw, peer, pidfd, procfd)
		default:
			err = fmt.Errorf("array action unsupported")
		}
		if err != nil {
			return err
		}
		encoded, e := (arrayRegistryWireReply{RequestSHA: arrayRegistryRequestSHA(raw), RegistrationNonce: o.current.nonce, ResourceFloor: uint64(r.Floor), MappingGeneration: q.MappingGeneration, Offset: q.Offset, Bytes: q.Bytes}).encode()
		if e != nil {
			return e
		}
		reply = encoded
		return nil
	})
	if err != nil {
		return err
	}
	if err = conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	n, err := conn.Write(reply)
	if err != nil || n != len(reply) {
		return errors.Join(ErrUnknown, err, fmt.Errorf("array reply unconfirmed"))
	}
	return nil
}
