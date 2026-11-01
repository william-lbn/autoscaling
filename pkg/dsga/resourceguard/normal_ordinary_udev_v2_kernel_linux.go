//go:build linux

package resourceguard

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"strconv"
	"strings"
	"sync"
)

type normalOrdinaryUdevV2KernelPacket struct {
	raw        []byte
	properties map[string]string
	sequence   uint64
}
type normalOrdinaryUdevV2KernelMonitor struct {
	fd       int
	object   normalFilesystemObject
	cookie   uint64
	port     uint32
	stop     chan struct{}
	finished chan struct{}
	once     sync.Once
	started  bool
}

// 仅内核group1 parser允许pid0；原Unix role包的Pid>1门不在这里复用。
func normalOrdinaryUdevV2DecodeKernelPacket(raw, oob []byte, flags int, from unix.Sockaddr) (normalOrdinaryUdevV2KernelPacket, error) {
	var out normalOrdinaryUdevV2KernelPacket
	address, ok := from.(*unix.SockaddrNetlink)
	if !ok || address.Pid != 0 || address.Groups != 1 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || len(raw) == 0 || len(raw) > 8192 {
		return out, errors.Join(ErrUnknown, fmt.Errorf("udev original kernel address/framing differs"))
	}
	messages, e := unix.ParseSocketControlMessage(oob)
	if e != nil {
		return out, errors.Join(ErrUnknown, e)
	}
	credential := false
	bad := false
	for _, m := range messages {
		if m.Header.Level != unix.SOL_SOCKET {
			bad = true
			continue
		}
		switch m.Header.Type {
		case unix.SCM_CREDENTIALS:
			c, e := unix.ParseUnixCredentials(&m)
			if e != nil || credential || c == nil || c.Pid != 0 || c.Uid != 0 || c.Gid != 0 {
				bad = true
			}
			credential = true
		case unix.SCM_RIGHTS:
			fds, e := unix.ParseUnixRights(&m)
			if e != nil {
				bad = true
			}
			for _, fd := range fds {
				unix.Close(fd)
			}
			bad = true
		case unix.SO_RXQ_OVFL:
			if len(m.Data) != 4 || binary.NativeEndian.Uint32(m.Data) != 0 {
				bad = true
			}
		default:
			bad = true
		}
	}
	if bad || !credential {
		return out, errors.Join(ErrUnknown, fmt.Errorf("udev exact kernel credentials/loss required"))
	}
	split := bytes.IndexByte(raw, 0)
	if split <= 0 {
		return out, ErrUnknown
	}
	head := string(raw[:split])
	action, path, ok := strings.Cut(head, "@")
	if !ok || action == "" || path == "" || !strings.HasPrefix(path, "/") {
		return out, ErrUnknown
	}
	properties, e := normalOrdinaryUdevV2KernelProperties(raw[split+1:])
	if e != nil {
		return out, errors.Join(ErrUnknown, e)
	}
	seq, e := strconv.ParseUint(properties["SEQNUM"], 10, 64)
	if e != nil || seq == 0 || properties["SEQNUM"] != strconv.FormatUint(seq, 10) || properties["ACTION"] != action || properties["DEVPATH"] != path || properties["SUBSYSTEM"] == "" {
		return out, errors.Join(ErrUnknown, fmt.Errorf("udev original kernel fields differ"))
	}
	known := false
	for op := uint16(1); op <= 8; op++ {
		if normalOrdinaryUdevV2Action(op) == action {
			known = true
		}
	}
	if !known {
		return out, errors.Join(ErrUnknown, fmt.Errorf("udev kernel action not explicitly supported"))
	}
	return normalOrdinaryUdevV2KernelPacket{bytes.Clone(raw), properties, seq}, nil
}
func openNormalOrdinaryUdevV2KernelMonitor() (m *normalOrdinaryUdevV2KernelMonitor, err error) {
	fd, e := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, unix.NETLINK_KOBJECT_UEVENT)
	if e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	m = &normalOrdinaryUdevV2KernelMonitor{fd: fd, stop: make(chan struct{}), finished: make(chan struct{})}
	success := false
	defer func() {
		if !success {
			unix.Close(fd)
		}
	}()
	for _, option := range []int{unix.SO_PASSCRED, unix.SO_RXQ_OVFL} {
		if e = unix.SetsockoptInt(fd, unix.SOL_SOCKET, option, 1); e != nil {
			return nil, errors.Join(ErrUnknown, e)
		}
	}
	if e = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, 2<<20); e != nil {
		return nil, e
	}
	if e = unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Groups: 1}); e != nil {
		return nil, e
	}
	address, e := unix.Getsockname(fd)
	if e != nil {
		return nil, e
	}
	bound, ok := address.(*unix.SockaddrNetlink)
	if !ok || bound.Pid == 0 || bound.Groups != 1 {
		return nil, ErrUnknown
	}
	m.port = bound.Pid
	m.cookie, e = unix.GetsockoptUint64(fd, unix.SOL_SOCKET, unix.SO_COOKIE)
	if e != nil || m.cookie == 0 {
		return nil, errors.Join(ErrUnknown, e)
	}
	m.object, e = normalFilesystemFDObject(fd)
	if e != nil {
		return nil, e
	}
	success = true
	return m, nil
}
func (m *normalOrdinaryUdevV2KernelMonitor) verify() error {
	if m == nil || m.fd < 0 {
		return ErrUnknown
	}
	o, e := normalFilesystemFDObject(m.fd)
	if e != nil || o != m.object {
		return errors.Join(ErrUnknown, e)
	}
	c, e := unix.GetsockoptUint64(m.fd, unix.SOL_SOCKET, unix.SO_COOKIE)
	if e != nil || c != m.cookie {
		return errors.Join(ErrUnknown, e)
	}
	a, e := unix.Getsockname(m.fd)
	v, ok := a.(*unix.SockaddrNetlink)
	if e != nil || !ok || v.Pid != m.port || v.Groups != 1 {
		return errors.Join(ErrUnknown, e)
	}
	return nil
}
func (m *normalOrdinaryUdevV2KernelMonitor) start(consume func(normalOrdinaryUdevV2KernelPacket) error, fail func(error), idle func() error) {
	m.started = true
	go func() {
		defer close(m.finished)
		var raw [8192]byte
		var oob [256]byte
		for {
			select {
			case <-m.stop:
				return
			default:
			}
			poll := []unix.PollFd{{Fd: int32(m.fd), Events: unix.POLLIN}}
			n, e := unix.Poll(poll, 100)
			if e == unix.EINTR {
				continue
			}
			if e != nil {
				fail(e)
				return
			}
			if n == 0 {
				if idle != nil {
					if e = idle(); e != nil {
						fail(e)
						return
					}
				}
				continue
			}
			if poll[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
				fail(errors.Join(ErrUnknown, fmt.Errorf("udev original monitor interrupted/lost events")))
				return
			}
			n, on, flags, from, e := unix.Recvmsg(m.fd, raw[:], oob[:], unix.MSG_DONTWAIT|unix.MSG_CMSG_CLOEXEC)
			if e == unix.EINTR || e == unix.EAGAIN || e == unix.EWOULDBLOCK {
				continue
			}
			if e != nil {
				fail(e)
				return
			}
			packet, e := normalOrdinaryUdevV2DecodeKernelPacket(raw[:n], oob[:on], flags, from)
			if e != nil {
				fail(e)
				return
			}
			if e = consume(packet); e != nil {
				fail(e)
				return
			}
		}
	}()
}
func (m *normalOrdinaryUdevV2KernelMonitor) close() {
	if m == nil {
		return
	}
	m.once.Do(func() {
		close(m.stop)
		if m.started {
			<-m.finished
		}
		if m.fd >= 0 {
			unix.Close(m.fd)
			m.fd = -1
		}
	})
}

func normalOrdinaryUdevV2Broadcast(fd int, body []byte) error {
	if len(body) < 40 {
		return ErrUnknown
	}
	if e := unix.Sendto(fd, body, unix.MSG_NOSIGNAL, &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Groups: 2}); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	return nil
}
