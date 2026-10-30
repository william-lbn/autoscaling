//go:build linux

package resourceguard

import (
	"fmt"
	"net"
	"time"

	"golang.org/x/sys/unix"
)

type nativeBirthPeer struct {
	PID int32
	UID uint32
	GID uint32
}

// 收包前后都核真实内核 peer，不收对端自报 PID/UID。此事实仍不能替代原
// Pending、start ticks、exe/namespace、实际 AS/maps/filter 与入组的独立核验。
// 不创建 listener，不接受传入路径，不接现 HTTP 服务；仅供后续原出生链使用。
func readNativeBirthFrame(conn *net.UnixConn, expectedPeer nativeBirthPeer, deadline time.Time) ([]byte, error) {
	if conn == nil || expectedPeer.PID <= 1 || expectedPeer.UID == 0 || expectedPeer.GID == 0 || deadline.IsZero() ||
		!deadline.After(time.Now()) || deadline.After(time.Now().Add(30*time.Second)) {
		return nil, fmt.Errorf("bounded native birth peer and deadline required")
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	peer := func() error {
		var credential *unix.Ucred
		var inner error
		var socketType int
		err := raw.Control(func(fd uintptr) {
			credential, inner = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
			if inner == nil {
				socketType, inner = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
			}
		})
		if err != nil {
			return err
		}
		if inner != nil {
			return inner
		}
		if socketType != unix.SOCK_SEQPACKET || credential == nil || credential.Pid != expectedPeer.PID || credential.Uid != expectedPeer.UID || credential.Gid != expectedPeer.GID {
			return fmt.Errorf("native birth kernel peer differs from original process")
		}
		return nil
	}
	if err := peer(); err != nil {
		return nil, err
	}
	if err := conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	defer conn.SetReadDeadline(time.Time{})
	// 多一个字节封闭非 MSG_TRUNC 的超长帧；收到任何 ancillary 均拒绝。
	// MSG_CMSG_CLOEXEC 原子设置 fd，随后无论正常/截断都关闭可见 SCM_RIGHTS；
	// 超过控制缓冲区的 fd 由 Linux 在 MSG_CTRUNC 路径关闭，不能遗留到 exec。
	buffer := make([]byte, nativeBirthRequestBytes+1)
	control := make([]byte, unix.CmsgSpace(16*4))
	var n, oobn, flags int
	var receiveErr error
	err = raw.Read(func(fd uintptr) bool {
		for {
			n, oobn, flags, _, receiveErr = unix.Recvmsg(int(fd), buffer, control, unix.MSG_DONTWAIT|unix.MSG_CMSG_CLOEXEC)
			if receiveErr == unix.EINTR {
				continue
			}
			return receiveErr != unix.EAGAIN && receiveErr != unix.EWOULDBLOCK
		}
	})
	// 即使伴随截断/错误也先清理已安装的 fd；不允许通过返回早退泄漏。
	if oobn > 0 {
		messages, parseErr := unix.ParseSocketControlMessage(control[:oobn])
		if parseErr == nil {
			for _, message := range messages {
				if message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SCM_RIGHTS {
					fds, rightsErr := unix.ParseUnixRights(&message)
					if rightsErr == nil {
						for _, fd := range fds {
							_ = unix.Close(fd)
						}
					}
				}
			}
		}
		return nil, fmt.Errorf("native birth ancillary messages forbidden")
	}
	if err != nil {
		return nil, err
	}
	if receiveErr != nil {
		return nil, receiveErr
	}
	if n != nativeBirthRequestBytes || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return nil, fmt.Errorf("native birth packet missing or truncated")
	}
	if err := peer(); err != nil {
		return nil, err
	}
	return buffer[:n], nil
}
