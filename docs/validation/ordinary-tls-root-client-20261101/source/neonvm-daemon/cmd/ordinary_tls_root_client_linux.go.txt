//go:build linux

package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"time"
)

const ordinaryTLSRootSocket = "/run/neon-dsga/ordinary-tls.sock"

type ordinaryTLSRootClient struct{}

func ordinaryTLSNeedsRootDelegate() bool { return os.Getuid() != 0 || os.Geteuid() != 0 }
func newOrdinaryTLSRootDelegate() (ordinaryTLSRootDelegate, error) {
	plan, e := resourceguard.NormalSystemOrdinaryV2RolePlan("daemon")
	if e != nil || uint32(os.Getuid()) != plan.UID || uint32(os.Geteuid()) != plan.UID || uint32(os.Getgid()) != plan.GID || uint32(os.Getegid()) != plan.GID {
		return nil, errOrdinaryTLSRootDelegateUnavailable
	}
	// 客户端UID不是授权；Root仍逐packet核原main clone/creator/SCM/peerPIDFD。
	return &ordinaryTLSRootClient{}, nil
}
func (c *ordinaryTLSRootClient) replaceManagementTLS(ctx context.Context, v ordinaryTLSReplacement) error {
	if v.ProtocolVersion != 2 || v.Operation != "replace-management-tls" {
		return fmt.Errorf("fixed TLS replacement required")
	}
	raw, e := json.Marshal(v.Files)
	if e != nil {
		return e
	}
	if _, e = decodeOrdinaryTLSReplacement(raw); e != nil {
		return e
	}
	_, e = ordinaryTLSRootExchange(ctx, 2, raw)
	return e
}
func (c *ordinaryTLSRootClient) managementTLSChecksum(ctx context.Context) (string, error) {
	raw, e := ordinaryTLSRootExchange(ctx, 4, nil)
	if e != nil {
		return "", e
	}
	return ordinaryTLSRootChecksumReply(raw)
}

func ordinaryTLSRootChecksumReply(raw []byte) (string, error) {
	sum := string(raw)
	decoded, e := base64.RawStdEncoding.DecodeString(sum)
	if e != nil || len(decoded) != 32 || len(raw) != 43 || base64.RawStdEncoding.EncodeToString(decoded) != sum {
		return "", fmt.Errorf("fixed TLS checksum reply differs")
	}
	return sum, nil
}
func ordinaryTLSRootReply(raw []byte, action uint32) ([]byte, error) {
	want := 16
	if action == 4 {
		want += 43
	} else if action != 2 {
		return nil, fmt.Errorf("fixed TLS action required")
	}
	if len(raw) != want || string(raw[:8]) != "NDSGOAOK" || binary.BigEndian.Uint32(raw[8:12]) != 2 || binary.BigEndian.Uint32(raw[12:16]) != action {
		return nil, fmt.Errorf("fixed TLS Root reply differs")
	}
	return raw[16:], nil
}
func ordinaryTLSRootExchange(ctx context.Context, action uint32, payload []byte) ([]byte, error) {
	if ctx == nil || ctx.Err() != nil || len(payload) > 64<<10 || action != 2 && action != 4 || action == 2 && len(payload) == 0 || action == 4 && len(payload) != 0 {
		return nil, errOrdinaryTLSRootDelegateUnavailable
	}
	// 固定Root目录/组/socket身份，不接受HTTP路径或环境提供的目标。
	for _, path := range []string{"/run", "/run/neon-dsga"} {
		var st unix.Stat_t
		if unix.Lstat(path, &st) != nil || st.Uid != 0 || st.Gid != 0 || st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Mode&0022 != 0 {
			return nil, errOrdinaryTLSRootDelegateUnavailable
		}
	}
	var st unix.Stat_t
	if unix.Lstat(ordinaryTLSRootSocket, &st) != nil || st.Uid != 0 || st.Gid != 60009 || st.Mode&unix.S_IFMT != unix.S_IFSOCK || st.Mode&0777 != 0660 {
		return nil, errOrdinaryTLSRootDelegateUnavailable
	}
	call, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	deadline, _ := call.Deadline()
	rawConn, e := (&net.Dialer{Deadline: deadline}).DialContext(call, "unixpacket", ordinaryTLSRootSocket)
	if e != nil {
		return nil, errOrdinaryTLSRootDelegateUnavailable
	}
	defer rawConn.Close()
	conn, ok := rawConn.(*net.UnixConn)
	if !ok {
		return nil, errOrdinaryTLSRootDelegateUnavailable
	}
	if e = conn.SetDeadline(deadline); e != nil {
		return nil, e
	}
	socket, e := conn.SyscallConn()
	if e != nil {
		return nil, e
	}
	var root *unix.Ucred
	peerFD := -1
	var inner error
	if e = socket.Control(func(fd uintptr) {
		root, inner = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if inner != nil {
			return
		}
		peerFD, inner = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, 77 /* Linux6.12 SO_PEERPIDFD */)
		if inner != nil {
			return
		}
		inner = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, 1)
	}); e != nil || inner != nil {
		if peerFD >= 0 {
			_ = unix.Close(peerFD)
		}
		return nil, errOrdinaryTLSRootDelegateUnavailable
	}
	unix.CloseOnExec(peerFD)
	defer unix.Close(peerFD)
	if root == nil || root.Pid <= 1 || root.Uid != 0 || root.Gid != 0 || ordinaryTLSRootPeerAlive(peerFD) != nil {
		return nil, errOrdinaryTLSRootDelegateUnavailable
	}
	frame := make([]byte, 16+len(payload))
	copy(frame, "NDSGOA02")
	binary.BigEndian.PutUint32(frame[8:12], 2)
	binary.BigEndian.PutUint32(frame[12:16], action)
	copy(frame[16:], payload)
	// 只发送一次；丢ACK/中断返回Unknown，不建立客户端自动重连循环。
	n, _, e := conn.WriteMsgUnix(frame, nil, nil)
	if e != nil || n != len(frame) {
		return nil, errOrdinaryTLSRootDelegateUnavailable
	}
	data := make([]byte, 60)
	oob := make([]byte, unix.CmsgSpace(unix.SizeofUcred)+unix.CmsgSpace(16*4))
	n, on, flags, e := ordinaryTLSRootReadPacket(conn, data, oob)
	if e != nil {
		return nil, errOrdinaryTLSRootDelegateUnavailable
	}
	messages, parseErr := unix.ParseSocketControlMessage(oob[:on])
	var cred *unix.Ucred
	bad := parseErr != nil || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0
	for _, m := range messages {
		if m.Header.Level != unix.SOL_SOCKET {
			bad = true
			continue
		}
		if m.Header.Type == unix.SCM_RIGHTS {
			fds, e := unix.ParseUnixRights(&m)
			if e != nil {
				bad = true
			}
			for _, fd := range fds {
				_ = unix.Close(fd)
			}
			bad = true
		} else if m.Header.Type == unix.SCM_CREDENTIALS {
			c, e := unix.ParseUnixCredentials(&m)
			if e != nil || cred != nil {
				bad = true
			} else {
				cred = c
			}
		} else {
			bad = true
		}
	}
	if bad || cred == nil || *cred != *root || ordinaryTLSRootPeerAlive(peerFD) != nil {
		return nil, errOrdinaryTLSRootDelegateUnavailable
	}
	return ordinaryTLSRootReply(data[:n], action)
}
func ordinaryTLSRootPeerAlive(fd int) error {
	poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	for {
		n, e := unix.Poll(poll, 0)
		if errors.Is(e, unix.EINTR) {
			continue
		}
		if e != nil || n != 0 {
			return errOrdinaryTLSRootDelegateUnavailable
		}
		return nil
	}
}

func ordinaryTLSRootReadPacket(conn *net.UnixConn, data, oob []byte) (n, on, flags int, err error) {
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
