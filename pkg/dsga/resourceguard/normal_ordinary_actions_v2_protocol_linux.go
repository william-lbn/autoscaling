//go:build linux

package resourceguard

import (
	"encoding/binary"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"net"
	"syscall"
)

const normalOrdinaryActionHeaderBytes = 16
const normalOrdinaryActionMaxBody = 64 << 10
const normalOrdinaryPowerAction = 1
const normalOrdinaryTLSReplaceAction = 2
const normalOrdinaryUdevEmitAction = 3
const normalOrdinaryTLSChecksumAction = 4

func normalOrdinaryActionFrame(action uint32, payload []byte, reply bool) []byte {
	b := make([]byte, normalOrdinaryActionHeaderBytes+len(payload))
	magic := "NDSGOA02"
	if reply {
		magic = "NDSGOAOK"
	}
	copy(b[:8], magic)
	binary.BigEndian.PutUint32(b[8:12], 2)
	binary.BigEndian.PutUint32(b[12:16], action)
	copy(b[16:], payload)
	return b
}

// 角色由监听端固定；正文不携role、PID、FD、来源摘要或路径。
func decodeNormalOrdinaryActionFrame(raw []byte, role string) (uint32, []byte, error) {
	if len(raw) < 16 || len(raw) > 16+normalOrdinaryActionMaxBody || string(raw[:8]) != "NDSGOA02" || binary.BigEndian.Uint32(raw[8:12]) != 2 {
		return 0, nil, fmt.Errorf("fixed ordinary action frame differs")
	}
	action := binary.BigEndian.Uint32(raw[12:16])
	body := raw[16:]
	valid := role == "acpi" && action == normalOrdinaryPowerAction && len(body) == 0 ||
		role == "daemon" && action == normalOrdinaryTLSReplaceAction && len(body) > 0 ||
		role == "daemon" && action == normalOrdinaryTLSChecksumAction && len(body) == 0 ||
		role == "udev" && action == normalOrdinaryUdevEmitAction && len(body) > 0 && len(body) <= 8192
	if !valid {
		return 0, nil, fmt.Errorf("ordinary action does not belong to fixed endpoint")
	}
	return action, body, nil
}

// 所有已交付rights先关闭再拒，不能因校验早退泄漏FD；只信每packet内核凭据。
func normalOrdinaryPacketCredentials(oob []byte, flags int) (*unix.Ucred, error) {
	msgs, err := unix.ParseSocketControlMessage(oob)
	var cred *unix.Ucred
	bad := err != nil || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0
	for _, m := range msgs {
		if m.Header.Level != unix.SOL_SOCKET {
			bad = true
			continue
		}
		switch m.Header.Type {
		case unix.SCM_RIGHTS:
			fds, e := unix.ParseUnixRights(&m)
			if e != nil {
				bad = true
			}
			for _, fd := range fds {
				_ = unix.Close(fd)
			}
			bad = true
		case unix.SCM_CREDENTIALS:
			c, e := unix.ParseUnixCredentials(&m)
			if e != nil || cred != nil {
				bad = true
			} else {
				cred = c
			}
		default:
			bad = true
		}
	}
	if bad || cred == nil || cred.Pid <= 1 {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("single kernel packet credentials required"))
	}
	return cred, nil
}

func normalOrdinaryEnablePasscred(c syscall.Conn) error {
	raw, e := c.SyscallConn()
	if e != nil {
		return e
	}
	var inside error
	if e = raw.Control(func(fd uintptr) { inside = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, 1) }); e != nil {
		return e
	}
	return inside
}

// net.UnixListener与net.UnixConn都提供同一真实内核socket操作，不接受任意FD。

// 接收即由内核设置CLOEXEC，随后再关闭并拒rights；避免与其它Go线程fork/exec竞态。
func normalOrdinaryReadPacket(conn *net.UnixConn, data, oob []byte) (n, on, flags int, err error) {
	raw, e := conn.SyscallConn()
	if e != nil {
		return 0, 0, 0, e
	}
	var inner error
	e = raw.Read(func(fd uintptr) bool {
		for {
			n, on, flags, _, inner = unix.Recvmsg(int(fd), data, oob, unix.MSG_DONTWAIT|unix.MSG_CMSG_CLOEXEC)
			if inner == unix.EINTR {
				continue
			}
			return inner != unix.EAGAIN && inner != unix.EWOULDBLOCK
		}
	})
	return n, on, flags, errors.Join(e, inner)
}
