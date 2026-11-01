//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const normalOrdinaryDeviceCheckSocket = "/run/neon-dsga/ordinary-udev-init.sock"
const normalOrdinaryDeviceCheckDeadline = 65 * time.Second

// 只允许显式ordinary2的原60005目标发固定请求。UID检查不是Root授权；
// 服务端还必须逐packet核原clone/main租约、五cap集合、NNP及完整控制来源。
func RunNormalSystemOrdinaryV2DeviceCheck(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil {
		return ErrUnknown
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := normalOrdinaryDeviceCheckCaller(); err != nil {
		return errors.Join(ErrUnknown, err)
	}
	call, cancel := context.WithTimeout(ctx, normalOrdinaryDeviceCheckDeadline)
	defer cancel()
	for _, name := range []string{"/run", "/run/neon-dsga"} {
		var st unix.Stat_t
		if unix.Lstat(name, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != 0 || st.Gid != 0 || st.Mode&07777 != 0755 {
			return ErrUnknown
		}
	}
	var st unix.Stat_t
	if unix.Lstat(normalOrdinaryDeviceCheckSocket, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFSOCK || st.Uid != 0 || st.Gid != 60005 || st.Mode&07777 != 0660 {
		return ErrUnknown
	}
	deadline, _ := call.Deadline()
	raw, err := (&net.Dialer{Deadline: deadline}).DialContext(call, "unixpacket", normalOrdinaryDeviceCheckSocket)
	if err != nil {
		return errors.Join(ErrUnknown, err)
	}
	defer raw.Close()
	conn, ok := raw.(*net.UnixConn)
	if !ok {
		return ErrUnknown
	}
	return normalOrdinaryDeviceCheckConnected(call, conn)
}

func normalOrdinaryDeviceCheckCaller() error {
	f, err := os.Open("/proc/thread-self/status")
	if err != nil {
		return err
	}
	defer f.Close()
	var fs unix.Statfs_t
	if unix.Fstatfs(int(f.Fd()), &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC {
		return ErrUnknown
	}
	raw, err := nativeBirthReadBounded(f, 64<<10)
	if err != nil {
		return err
	}
	return normalOrdinaryDeviceCheckStatus(raw)
}

// 当前M的真实/proc字段；纯解析结果不能构造角色租约或源对象。
func normalOrdinaryDeviceCheckStatus(raw []byte) error {
	if len(raw) == 0 || len(raw) > 64<<10 || strings.ContainsRune(string(raw), 0) {
		return ErrUnknown
	}
	want := map[string]string{"Uid": "60005 60005 60005 60005", "Gid": "60005 60005 60005 60005", "Groups": "", "CapInh": "0000000000000000", "CapPrm": "0000000000000000", "CapEff": "0000000000000000", "CapBnd": "0000000000000000", "CapAmb": "0000000000000000", "NoNewPrivs": "1"}
	seen := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		expected, check := want[key]
		if !check {
			continue
		}
		if seen[key] || strings.Join(strings.Fields(value), " ") != expected {
			return fmt.Errorf("ordinary device-check target identity/capabilities differ")
		}
		seen[key] = true
	}
	if len(seen) != len(want) {
		return ErrUnknown
	}
	return nil
}

func normalOrdinaryDeviceCheckPeer(ctx context.Context, fd int) error {
	var st unix.Stat_t
	var fs unix.Statfs_t
	if fd < 0 || unix.Fstat(fd, &st) != nil || unix.Fstatfs(fd, &fs) != nil || uint64(fs.Type) != 0x50494446 || st.Ino == 0 {
		return ErrUnknown
	}
	for {
		if ctx == nil || ctx.Err() != nil {
			return ErrUnknown
		}
		poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, e := unix.Poll(poll, 0)
		if errors.Is(e, unix.EINTR) {
			continue
		}
		if e != nil || n != 0 || poll[0].Revents != 0 {
			return ErrUnknown
		}
		return nil
	}
}

func normalOrdinaryDeviceCheckReply(raw []byte) error {
	if len(raw) != 16 || string(raw[:8]) != "NDSGOAOK" || binary.BigEndian.Uint32(raw[8:12]) != 2 || binary.BigEndian.Uint32(raw[12:16]) != normalOrdinaryUdevInitAction {
		return ErrUnknown
	}
	return nil
}

// 只收一个完整ACK。收到rights由内核先置CLOEXEC、统一关闭并拒；不重连/重发。
func normalOrdinaryDeviceCheckConnected(ctx context.Context, conn *net.UnixConn) error {
	if ctx == nil || conn == nil || ctx.Err() != nil {
		return ErrUnknown
	}
	deadline, ok := ctx.Deadline()
	if !ok || !deadline.After(time.Now()) || deadline.After(time.Now().Add(normalOrdinaryDeviceCheckDeadline)) {
		return ErrUnknown
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if err := normalOrdinaryEnablePasscred(conn); err != nil {
		return err
	}
	root, fd, err := arraySocketPeer(conn)
	if err != nil {
		return errors.Join(ErrUnknown, err)
	}
	defer unix.Close(fd)
	if root == nil || root.Pid <= 1 || root.Uid != 0 || root.Gid != 0 || normalOrdinaryDeviceCheckPeer(ctx, fd) != nil {
		return ErrUnknown
	}
	if ctx.Err() != nil {
		return ErrUnknown
	}
	request := normalOrdinaryActionFrame(normalOrdinaryUdevInitAction, nil, false)
	n, _, err := conn.WriteMsgUnix(request, nil, nil)
	if err != nil || n != 16 {
		return errors.Join(ErrUnknown, err)
	}
	data := make([]byte, 17)
	oob := make([]byte, unix.CmsgSpace(unix.SizeofUcred)+unix.CmsgSpace(16*4))
	n, on, flags, err := normalOrdinaryDeviceCheckRead(ctx, conn, data, oob)
	// 即使payload/IO错误，已交付的rights也要关闭，不能先返回。
	credential, credErr := normalOrdinaryPacketCredentials(oob[:on], flags)
	if err != nil || credErr != nil || credential == nil || *credential != *root || normalOrdinaryDeviceCheckPeer(ctx, fd) != nil {
		return errors.Join(ErrUnknown, err, credErr)
	}
	return normalOrdinaryDeviceCheckReply(data[:n])
}

func normalOrdinaryDeviceCheckRead(ctx context.Context, conn *net.UnixConn, data, oob []byte) (n, on, flags int, err error) {
	raw, e := conn.SyscallConn()
	if e != nil {
		return 0, 0, 0, e
	}
	var inner error
	e = raw.Read(func(fd uintptr) bool {
		for {
			if ctx.Err() != nil {
				inner = ctx.Err()
				return true
			}
			n, on, flags, _, inner = unix.Recvmsg(int(fd), data, oob, unix.MSG_DONTWAIT|unix.MSG_CMSG_CLOEXEC)
			if errors.Is(inner, unix.EINTR) {
				continue
			}
			return inner != unix.EAGAIN && inner != unix.EWOULDBLOCK
		}
	})
	return n, on, flags, errors.Join(e, inner)
}
