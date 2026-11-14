//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const arrayPreexecBytes = 80
const arrayPreexecMagic = "NDSGLA01"
const arrayPreexecACK = "NDSGLA1A"

type arrayLaunchPin struct {
	stderr        *normalPgStderrPin
	pidfd, procfd int
	pending       nativeBirthPending
}

func (p *arrayLaunchPin) close() {
	if p == nil {
		return
	}
	if p.pidfd >= 0 {
		_ = unix.Close(p.pidfd)
		p.pidfd = -1
	}
	if p.procfd >= 0 {
		_ = unix.Close(p.procfd)
		p.procfd = -1
	}
}
func arraySocketPeer(conn *net.UnixConn) (*unix.Ucred, int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, -1, err
	}
	var peer *unix.Ucred
	fd := -1
	var inner error
	err = raw.Control(func(socket uintptr) {
		var kind int
		kind, inner = unix.GetsockoptInt(int(socket), unix.SOL_SOCKET, unix.SO_TYPE)
		if inner != nil {
			return
		}
		if kind != unix.SOCK_SEQPACKET {
			inner = fmt.Errorf("native peer is not seqpacket")
			return
		}
		peer, inner = unix.GetsockoptUcred(int(socket), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if inner == nil {
			fd, inner = unix.GetsockoptInt(int(socket), unix.SOL_SOCKET, nativeTracePeerPIDFD)
		}
	})
	if err != nil || inner != nil {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
		return nil, -1, errors.Join(err, inner)
	}
	unix.CloseOnExec(fd)
	return peer, fd, nil
}
func arrayServicePeekMagic(conn *net.UnixConn, deadline time.Time) (string, error) {
	if err := conn.SetReadDeadline(deadline); err != nil {
		return "", err
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return "", err
	}
	data := make([]byte, 8)
	oob := make([]byte, unix.CmsgSpace(16*4))
	var n, oobn, flags int
	var inner error
	err = raw.Read(func(fd uintptr) bool {
		for {
			n, oobn, flags, _, inner = unix.Recvmsg(int(fd), data, oob, unix.MSG_PEEK|unix.MSG_DONTWAIT|unix.MSG_CMSG_CLOEXEC)
			if inner == unix.EINTR {
				continue
			}
			return inner != unix.EAGAIN && inner != unix.EWOULDBLOCK
		}
	})
	if oobn > 0 {
		messages, e := unix.ParseSocketControlMessage(oob[:oobn])
		if e == nil {
			for _, m := range messages {
				if m.Header.Level == unix.SOL_SOCKET && m.Header.Type == unix.SCM_RIGHTS {
					fds, e := unix.ParseUnixRights(&m)
					if e == nil {
						for _, fd := range fds {
							_ = unix.Close(fd)
						}
					}
				}
			}
		}
		return "", fmt.Errorf("native peek ancillary forbidden")
	}
	if err != nil || inner != nil {
		return "", errors.Join(err, inner)
	}
	if n != 8 || flags&unix.MSG_CTRUNC != 0 {
		return "", fmt.Errorf("native magic missing")
	}
	return string(data), nil
}
func arrayMatchPIDFD(a, b int) error {
	if _, err := arrayRegistryPIDFDAliveBounded(a); err != nil {
		return err
	}
	if _, err := arrayRegistryPIDFDAliveBounded(b); err != nil {
		return err
	}
	var x, y unix.Stat_t
	if unix.Fstat(a, &x) != nil || unix.Fstat(b, &y) != nil || x.Dev != y.Dev || x.Ino != y.Ino {
		return fmt.Errorf("original pre-exec and native peer pidfd objects differ")
	}
	return nil
}

// caller仍为SUID helper且持原flock，Pending/Birth已fsync。先取得当前Root
// 服务的kernel peer pidfd再单次发送；不导入FD，不因未知回复重发或重造nonce。
func arrayPreexecPin(ctx context.Context, p nativeBirthPending) error {
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	deadline, _ := call.Deadline()
	dialer := net.Dialer{}
	connected, err := dialer.DialContext(call, "unixpacket", fixedArrayBirthSocketPath)
	var conn *net.UnixConn
	if err == nil {
		var ok bool
		conn, ok = connected.(*net.UnixConn)
		if !ok {
			connected.Close()
			return errors.Join(ErrUnknown, fmt.Errorf("fixed native transport is not Unix"))
		}
	}
	if err != nil {
		return errors.Join(ErrUnknown, err)
	}
	defer conn.Close()
	peer, pidfd, err := arraySocketPeer(conn)
	if err != nil {
		return err
	}
	defer unix.Close(pidfd)
	if peer == nil || peer.Uid != 0 || int(peer.Pid) != p.Consumer.PID {
		return errors.Join(ErrUnknown, fmt.Errorf("pre-exec Root consumer peer changed"))
	}
	if err = arrayRegistryPIDFDAlive(pidfd); err != nil {
		return err
	}
	if err = conn.SetDeadline(deadline); err != nil {
		return err
	}
	nonce, _ := hex.DecodeString(p.Nonce)
	consumer, _ := hex.DecodeString(p.Consumer.InstanceNonce)
	raw := make([]byte, arrayPreexecBytes)
	copy(raw, arrayPreexecMagic)
	binary.BigEndian.PutUint32(raw[8:12], 1)
	copy(raw[16:48], nonce)
	copy(raw[48:80], consumer)
	n, err := conn.Write(raw)
	if err != nil || n != len(raw) {
		return errors.Join(ErrUnknown, err, fmt.Errorf("pre-exec pin request write unconfirmed"))
	}
	reply, err := arrayServicePacket(conn, deadline, arrayPreexecBytes, false)
	if err != nil {
		return errors.Join(ErrUnknown, err)
	}
	if string(reply[:8]) != arrayPreexecACK || binary.BigEndian.Uint32(reply[8:12]) != 1 || binary.BigEndian.Uint32(reply[12:16]) != 0 || string(reply[16:]) != string(raw[16:]) {
		return errors.Join(ErrUnknown, fmt.Errorf("pre-exec original pin ACK differs"))
	}
	return arrayRegistryPIDFDAlive(pidfd)
}

// 此handler故意不取resource flock：真正SUID producer正持该锁等待ACK。
// 只读Root已fsync的immutable Pending，先用kernel peer钉原Go同一Task，
// Native回来后仍在原flock内全量复核；这里不创建WAL/迁组/发normal资格。
func (s *arrayNativeServices) handlePreexec(ctx context.Context, conn *net.UnixConn) error {
	deadline, _ := ctx.Deadline()
	raw, err := arrayServicePacket(conn, deadline, arrayPreexecBytes, false)
	if err != nil {
		return err
	}
	if string(raw[:8]) != arrayPreexecMagic || binary.BigEndian.Uint32(raw[8:12]) != 1 || binary.BigEndian.Uint32(raw[12:16]) != 0 {
		return fmt.Errorf("pre-exec exact frame invalid")
	}
	nonce := hex.EncodeToString(raw[16:48])
	if _, err = nativeBirthDigest(nonce); err != nil {
		return err
	}
	file, err := rootFile(filepath.Join(s.guard.config.StateDir, nativeBirthStageName(nonce, "pending")), false)
	if err != nil {
		return err
	}
	pendingRaw, err := nativeBirthReadBounded(file, 16*1024)
	_ = file.Close()
	if err != nil {
		return err
	}
	var pending nativeBirthPending
	if control.UnmarshalObject(pendingRaw, &pending) != nil || pending.validate() != nil || pending.Nonce != nonce || pending.Source != s.source.binding || pending.Consumer.InstanceNonce != hex.EncodeToString(raw[48:80]) {
		return fmt.Errorf("pre-exec original Pending differs")
	}
	if err = nativeBirthCurrentConsumer(pending); err != nil {
		return err
	}
	peer, pidfd, err := arraySocketPeer(conn)
	if err != nil {
		return err
	}
	pin := &arrayLaunchPin{pidfd: pidfd, procfd: -1, pending: pending}
	keep := false
	defer func() {
		if !keep {
			pin.close()
		}
	}()
	if peer == nil || peer.Uid != 0 || int(peer.Pid) != pending.Original.PostmasterPID {
		return fmt.Errorf("pre-exec requires original effective-root launcher peer")
	}
	pin.procfd, err = unix.Open("/proc/"+strconv.Itoa(int(peer.Pid)), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	stat, err := nativeTraceProcText(pin.procfd, "stat")
	if err != nil {
		return err
	}
	ticks, err := postmasterTicks(stat, int(peer.Pid))
	if err != nil || ticks != uint64(pending.Original.PostmasterStartTicks) {
		return fmt.Errorf("pre-exec original Task start differs")
	}
	// 同一个Root程序的SUID入口，不能由任意PG程序自称launcher。文件是原
	// readonly image；以当前实际daemon ELF摘要核对，复制安装inode可不同。
	exe, err := unix.Openat(pin.procfd, "exe", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	other := os.NewFile(uintptr(exe), "original-root-launcher")
	defer other.Close()
	self, err := os.Open("/proc/self/exe")
	if err != nil {
		return err
	}
	defer self.Close()
	a, err := nativeBirthFileHash(other)
	if err != nil {
		return err
	}
	b, err := nativeBirthFileHash(self)
	if err != nil || a != b {
		return fmt.Errorf("pre-exec caller is not original Root launcher binary")
	}
	if err = arrayRegistryPIDFDAlive(pin.pidfd); err != nil {
		return err
	}
	s.pinMu.Lock()
	if _, exists := s.launchPins[nonce]; exists {
		s.pinMu.Unlock()
		return errors.Join(ErrUnknown, fmt.Errorf("original pre-exec pin already exists; no implicit replay"))
	}
	s.launchPins[nonce] = pin
	keep = true
	s.pinMu.Unlock()
	reply := append([]byte(nil), raw...)
	copy(reply, arrayPreexecACK)
	if err = conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	n, err := conn.Write(reply)
	if err != nil || n != len(reply) {
		return errors.Join(ErrUnknown, err, fmt.Errorf("pre-exec pin reply unknown"))
	}
	return nil
}
