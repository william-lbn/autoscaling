//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"reflect"
	"strconv"
	"strings"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const arrayPreexecBeginMagic = "NDSGLP02"
const arrayPreexecBeginACK = "NDSGLP2A"
const arrayPreexecBeginReplyBytes = 112

// 固定root元数据只选择同一已验证Doc2，不能把metadata可见性当出生授权。
// 真正Source/约束/nonce仍由daemon原flock内生产；此reader不要求也不改变ruid。
func readArrayBirthConsumerForLaunch(source SourceConfig) (arrayBirthConsumerMetadata, error) {
	var zero arrayBirthConsumerMetadata
	f, e := rootFile(fixedArrayBirthConsumerPath, false)
	if e != nil {
		return zero, e
	}
	defer f.Close()
	raw, e := io.ReadAll(io.LimitReader(f, 16*1024+1))
	if e != nil || len(raw) > 16*1024 {
		return zero, fmt.Errorf("fixed original consumer metadata unreadable")
	}
	var m arrayBirthConsumerMetadata
	if control.UnmarshalObject(raw, &m) != nil || m.validate() != nil || m.Source.Policy.ArrayHelperProtocolVersion != 1 {
		return zero, fmt.Errorf("original consumer metadata invalid")
	}
	current, document, e := runtimeSource()
	if e != nil || !reflect.DeepEqual(current, source) {
		return zero, fmt.Errorf("original launcher source changed")
	}
	var d RuntimeDocument
	if json.Unmarshal(document, &d) != nil || m.Source.Policy.RuntimeDocumentSHA256 != physicalDigest(document) || m.Source.Policy.ResourceSourceSHA256 != d.SourceSHA256 {
		return zero, fmt.Errorf("original consumer runtime binding differs")
	}
	boot, e := kernelText("/proc/sys/kernel/random/boot_id")
	if e != nil || boot != m.GuestBootID {
		return zero, fmt.Errorf("original consumer Guest differs")
	}
	return m, nil
}

// 单次Begin无自选nonce/PID/路径/预算/可信布尔。server/aux只是原受限launcher
// 判定的操作类别；Root按自己的原Source和kernel peer生成/保存Birth。
func arrayPreexecBegin(ctx context.Context, m arrayBirthConsumerMetadata, server bool, pgUID, pgGID int) (NativeBirthRef, error) {
	var zero NativeBirthRef
	if pgUID <= 0 || pgGID <= 0 || os.Geteuid() != 0 || os.Getuid() != 0 && os.Getuid() != pgUID {
		return zero, fmt.Errorf("preexec original caller is not trusted postgres/root")
	}
	if os.Getuid() == pgUID && (os.Getgid() != pgGID || os.Getegid() != pgGID) {
		return zero, fmt.Errorf("preexec original postgres primary group differs")
	}
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	deadline, _ := call.Deadline()
	connected, e := (&net.Dialer{}).DialContext(call, "unixpacket", fixedArrayBirthSocketPath)
	if e != nil {
		return zero, errors.Join(ErrUnknown, e)
	}
	defer connected.Close()
	conn, ok := connected.(*net.UnixConn)
	if !ok {
		return zero, fmt.Errorf("fixed original preexec transport differs")
	}
	peer, rootPIDFD, e := arraySocketPeer(conn)
	if e != nil {
		return zero, e
	}
	defer unix.Close(rootPIDFD)
	if peer == nil || peer.Uid != 0 || int(peer.Pid) != m.Consumer.PID {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("original Root consumer peer differs"))
	}
	if e = arrayRegistryPIDFDAlive(rootPIDFD); e != nil {
		return zero, e
	}
	// 静态root拥有的同一个原程序；不用单独扩大SUID功能或放松Source real-root门。
	selfPath, e := os.Executable()
	if e != nil {
		return zero, e
	}
	self, e := nativeBirthOpenRO(selfPath)
	if e != nil {
		return zero, e
	}
	defer self.Close()
	var st unix.Stat_t
	if unix.Fstat(int(self.Fd()), &st) != nil || st.Uid != 0 || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0022 != 0 || os.Getuid() != 0 && st.Mode&unix.S_ISUID == 0 {
		return zero, fmt.Errorf("original launcher is not root readonly SUID ELF")
	}
	elf, e := nativeBirthParseELF(self)
	if e != nil || elf.Interpreter != "" || len(elf.Needed) != 0 {
		return zero, fmt.Errorf("original launcher is not static")
	}
	rootProc, e := unix.Open("/proc/"+strconv.Itoa(m.Consumer.PID), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return zero, e
	}
	defer unix.Close(rootProc)
	text, e := nativeTraceProcText(rootProc, "stat")
	if e != nil {
		return zero, e
	}
	ticks, e := postmasterTicks(text, m.Consumer.PID)
	if e != nil || ticks != uint64(m.Consumer.StartTicks) {
		return zero, fmt.Errorf("original Root consumer start differs")
	}
	rootExe, e := unix.Openat(rootProc, "exe", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return zero, e
	}
	other := os.NewFile(uintptr(rootExe), "original-root-consumer")
	defer other.Close()
	a, e := nativeBirthFileHash(self)
	if e != nil {
		return zero, e
	}
	b, e := nativeBirthFileHash(other)
	if e != nil || a != b {
		return zero, fmt.Errorf("launcher/Root original static ELF differs")
	}
	if e = arrayRegistryPIDFDAlive(rootPIDFD); e != nil {
		return zero, e
	}
	raw := make([]byte, arrayPreexecBytes)
	copy(raw, arrayPreexecBeginMagic)
	binary.BigEndian.PutUint32(raw[8:12], 2)
	action := uint32(2)
	if server {
		action = 1
	}
	binary.BigEndian.PutUint32(raw[12:16], action)
	consumer, _ := hex.DecodeString(m.Consumer.InstanceNonce)
	copy(raw[16:48], consumer)
	runtimeSHA, _ := hex.DecodeString(m.Source.Policy.RuntimeDocumentSHA256)
	copy(raw[48:80], runtimeSHA)
	if e = conn.SetDeadline(deadline); e != nil {
		return zero, e
	}
	n, e := conn.Write(raw)
	if e != nil || n != len(raw) {
		return zero, errors.Join(ErrUnknown, e, fmt.Errorf("original preexec Begin write unconfirmed"))
	}
	reply, e := arrayServicePacket(conn, deadline, arrayPreexecBeginReplyBytes, false)
	if e != nil {
		return zero, errors.Join(ErrUnknown, e)
	}
	if string(reply[:8]) != arrayPreexecBeginACK || binary.BigEndian.Uint32(reply[8:12]) != 2 || binary.BigEndian.Uint32(reply[12:16]) != 0 || string(reply[48:80]) != string(consumer) {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("original Begin ACK differs"))
	}
	if server {
		ref := NativeBirthRef{ProtocolVersion: 1, Nonce: hex.EncodeToString(reply[16:48]), PendingSHA256: hex.EncodeToString(reply[80:112])}
		if ref.validate() != nil {
			return zero, errors.Join(ErrUnknown, fmt.Errorf("original Root prepared Birth ref invalid"))
		}
		if e = arrayRegistryPIDFDAlive(rootPIDFD); e != nil {
			return zero, e
		}
		return ref, nil
	}
	if string(reply[16:48]) != string(make([]byte, 32)) || string(reply[80:112]) != string(make([]byte, 32)) {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("auxiliary check unexpectedly created Birth"))
	}
	return zero, arrayRegistryPIDFDAlive(rootPIDFD)
}

// 同SO_PEERPIDFD钉住的原Task；允许原SUID的ruid=PG/euid=0，绝不把其ruid
// 改为0。后续Native peer必须仍为同一kernel pidfd对象且已全降为PG uid/gid。
func (s *arrayNativeServices) handlePreexecBegin(ctx context.Context, conn *net.UnixConn) error {
	deadline, _ := ctx.Deadline()
	raw, e := arrayServicePacket(conn, deadline, arrayPreexecBytes, false)
	if e != nil {
		return e
	}
	action := binary.BigEndian.Uint32(raw[12:16])
	if string(raw[:8]) != arrayPreexecBeginMagic || binary.BigEndian.Uint32(raw[8:12]) != 2 || action != 1 && action != 2 {
		return fmt.Errorf("exact preexec Begin frame invalid")
	}
	consumer, e := nativeBirthCurrentConsumerIdentity()
	if e != nil {
		return e
	}
	nonce, _ := hex.DecodeString(consumer.InstanceNonce)
	sourceSHA, _ := hex.DecodeString(s.source.binding.Policy.RuntimeDocumentSHA256)
	if string(raw[16:48]) != string(nonce) || string(raw[48:80]) != string(sourceSHA) {
		return fmt.Errorf("preexec original Root/runtime selection differs")
	}
	peer, pidfd, e := arraySocketPeer(conn)
	if e != nil {
		return e
	}
	pin := &arrayLaunchPin{pidfd: pidfd, procfd: -1}
	keep := false
	defer func() {
		if !keep {
			pin.close()
		}
	}()
	if peer == nil || peer.Uid != 0 || peer.Pid <= 1 {
		return fmt.Errorf("preexec requires original effective-root SUID peer")
	}
	account, e := user.Lookup("postgres")
	if e != nil {
		return e
	}
	uid, e := strconv.ParseUint(account.Uid, 10, 32)
	if e != nil || uid == 0 {
		return fmt.Errorf("original postgres UID unavailable")
	}
	gid, e := strconv.ParseUint(account.Gid, 10, 32)
	if e != nil || gid == 0 {
		return fmt.Errorf("original postgres GID unavailable")
	}
	if e = arrayRegistryPIDFDAlive(pidfd); e != nil {
		return e
	}
	pin.procfd, e = unix.Open("/proc/"+strconv.Itoa(int(peer.Pid)), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	status, e := nativeTraceProcText(pin.procfd, "status")
	if e != nil {
		return e
	}
	uids, e := nativeTraceStatusValues(status, "Uid", 4)
	if e != nil || uids[1] != 0 || uids[2] != 0 || uids[3] != 0 || uids[0] != 0 && uids[0] != uid {
		return fmt.Errorf("original SUID real/effective/saved identity differs")
	}
	gids, e := nativeTraceStatusValues(status, "Gid", 4)
	if e != nil {
		return e
	}
	if uids[0] == uid {
		for _, g := range gids {
			if g != gid {
				return fmt.Errorf("original SUID postgres primary group differs")
			}
		}
	}
	membership, e := nativeTraceProcText(pin.procfd, "cgroup")
	if e != nil || strings.TrimSpace(membership) != "0::"+strings.TrimPrefix(systemCgroup, "/sys/fs/cgroup") {
		return fmt.Errorf("original launcher is outside System")
	}
	text, e := nativeTraceProcText(pin.procfd, "stat")
	if e != nil {
		return e
	}
	ticks, e := postmasterTicks(text, int(peer.Pid))
	if e != nil {
		return e
	}
	exe, e := unix.Openat(pin.procfd, "exe", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	other := os.NewFile(uintptr(exe), "original-suid-peer")
	defer other.Close()
	self, e := os.Open("/proc/self/exe")
	if e != nil {
		return e
	}
	defer self.Close()
	a, e := nativeBirthFileHash(other)
	if e != nil {
		return e
	}
	b, e := nativeBirthFileHash(self)
	if e != nil || a != b {
		return fmt.Errorf("original launcher does not match Root ELF")
	}
	var ref NativeBirthRef
	e = s.guard.withRecord(ctx, func(r *record, state *lockedState) error {
		if r.Bootstrap == nil || !r.Bootstrap.Applied || !r.Bootstrap.SystemPlacementObserved || r.Original != nil && !r.Applied || r.Birth != nil {
			return errors.Join(ErrUnknown, fmt.Errorf("original bootstrap/Birth does not permit a new Begin"))
		}
		source, pg, e := readNativeBirthOriginalSource()
		if e != nil {
			return e
		}
		defer pg.Close()
		if source.binding != s.source.binding || sameBootstrapSource(source.resource, r.Bootstrap.Original) != nil {
			return fmt.Errorf("original daemon source changed")
		}
		limits := r.Bootstrap.Original.Limits
		if r.Held != nil {
			limits = r.Held.limits()
		}
		actual, e := s.guard.kernel.ReadBounds(r.Identity, limits)
		if e != nil {
			return e
		}
		if e = bootstrapMatches(actual, r.Identity, limits); e != nil {
			return e
		}
		if e = actual.systemPlacementMatches(limits.SystemMemoryMaxBytes); e != nil {
			return e
		}
		if e = rejectOutstandingArrayPopulateLocked(state); e != nil {
			return e
		}
		topology, e := s.guard.normalSplitLocked(ctx, s.split, normalSplitObserve, state)
		if e != nil {
			return e
		}
		if topology.Current.Private == nil || topology.Current.Arrays == nil || !topology.Current.Private.Empty || !topology.Current.Arrays.Empty {
			return errors.Join(ErrUnknown, fmt.Errorf("original split leaves are not empty"))
		}
		startup := uint64(0)
		for _, element := range source.binding.Policy.ElementBytes {
			startup += (uint64(source.binding.Policy.StartupBuffers)*element + 4095) &^ uint64(4095)
		}
		reserve := uint64(source.binding.Policy.ArrayHelperReserveBytes)
		cap := uint64(topology.Current.Arrays.MemoryMaxBytes)
		if reserve > cap || startup > cap-reserve {
			return errors.Join(ErrUnknown, fmt.Errorf("original A lacks N0/helper budget"))
		}
		if e = arrayRegistryPIDFDAlive(pidfd); e != nil {
			return e
		}
		latest, e := nativeTraceProcText(pin.procfd, "stat")
		if e != nil {
			return e
		}
		now, e := postmasterTicks(latest, int(peer.Pid))
		if e != nil || now != ticks {
			return fmt.Errorf("original launcher changed before durable Begin")
		}
		if action == 2 {
			return nil
		} // 辅助调用零WAL，不注入root-v1或保存PG Birth。
		birthNonce, e := newNativeBirthNonce()
		if e != nil {
			return e
		}
		var st unix.Stat_t
		if e = unix.Fstat(int(state.dir.Fd()), &st); e != nil {
			return e
		}
		original := ProcessIdentity{ProtocolVersion: 1, Identity: r.Identity, PostmasterPID: int(peer.Pid), PostmasterStartTicks: control.Uint64(ticks), AddressSpaceMaxBytes: r.addressSpaceLimit()}
		pending := nativeBirthPending{ProtocolVersion: 1, Scope: nativeBirthStageScope, Nonce: birthNonce, Source: source.binding, Original: original, Consumer: consumer, PeerUID: uint32(uid), PeerGID: uint32(gid), PrivateDevice: topology.Current.Private.Device, PrivateInode: topology.Current.Private.Inode, StateDevice: control.Uint64(st.Dev), StateInode: control.Uint64(st.Ino), ResourceFloor: r.Floor}
		ref, e = createNativeBirthPendingLocked(state, r, source, pending)
		if e != nil {
			return e
		}
		r.Birth = &original
		if e = r.validate(r.Identity); e != nil {
			return e
		}
		if e = state.save(r); e != nil {
			return errors.Join(ErrUnknown, e)
		}
		pin.pending = pending
		s.pinMu.Lock()
		defer s.pinMu.Unlock()
		if _, yes := s.launchPins[birthNonce]; yes {
			return errors.Join(ErrUnknown, fmt.Errorf("original Begin pin already exists"))
		}
		s.launchPins[birthNonce] = pin
		keep = true
		return arrayRegistryPIDFDAlive(pidfd)
	})
	if e != nil {
		return e
	}
	reply := make([]byte, arrayPreexecBeginReplyBytes)
	copy(reply, arrayPreexecBeginACK)
	binary.BigEndian.PutUint32(reply[8:12], 2)
	copy(reply[48:80], nonce)
	if action == 1 {
		birth, _ := hex.DecodeString(ref.Nonce)
		proof, _ := hex.DecodeString(ref.PendingSHA256)
		copy(reply[16:48], birth)
		copy(reply[80:112], proof)
	}
	if e = conn.SetWriteDeadline(deadline); e != nil {
		return e
	}
	n, e := conn.Write(reply)
	if e != nil || n != len(reply) {
		return errors.Join(ErrUnknown, e, fmt.Errorf("Root prepared Begin reply unconfirmed; original Birth retained"))
	}
	return nil
}
