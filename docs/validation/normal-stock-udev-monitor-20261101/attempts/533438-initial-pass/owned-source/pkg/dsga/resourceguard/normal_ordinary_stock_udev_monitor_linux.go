//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// 纯捕获组件：target必须由同Root原/sys捕获。此map、UID0和本receipt均不产Source。
// Root调用者另核唯一pre-ordinary窗口、原stock执行闭包及所有writer真实Wait。
type normalOrdinaryStockUdevMonitorTarget struct {
	Devpath, Subsystem string
	KernelProperties   map[string]string
}
type normalOrdinaryStockUdevMonitorEvent struct {
	Sequence   uint64
	Properties map[string]string
	Raw        []byte
	SenderPID  int32
	SenderPort uint32
}
type normalOrdinaryStockUdevMonitorReceipt struct {
	Object              normalFilesystemObject
	Cookie              uint64
	Port                uint32
	KernelEvents        []normalOrdinaryStockUdevMonitorEvent
	ProcessedBySequence map[uint64]normalOrdinaryStockUdevMonitorEvent
}
type normalOrdinaryStockUdevMonitor struct {
	mu                sync.Mutex
	fd                int
	object            normalFilesystemObject
	cookie            uint64
	port              uint32
	stop, done        chan struct{}
	stopOnce          sync.Once
	finished, closed  bool
	err               error
	kernel, processed map[uint64]normalOrdinaryStockUdevMonitorEvent
	rawBytes          int
}

func normalOrdinaryStockUdevMonitorCopy(v normalOrdinaryStockUdevMonitorEvent) normalOrdinaryStockUdevMonitorEvent {
	v.Raw = bytes.Clone(v.Raw)
	p := make(map[string]string, len(v.Properties))
	for k, s := range v.Properties {
		p[k] = s
	}
	v.Properties = p
	return v
}

func normalOrdinaryStockUdevMonitorProperties(raw []byte) (map[string]string, error) {
	if len(raw) == 0 || len(raw) > 4096 || raw[len(raw)-1] != 0 {
		return nil, ErrUnknown
	}
	out := map[string]string{}
	for _, b := range bytes.Split(raw[:len(raw)-1], []byte{0}) {
		k, v, ok := strings.Cut(string(b), "=")
		if !ok || len(k) == 0 || len(k) > 64 || len(v) > 4095 || !normalOrdinaryStockUdevScalar(v) || len(out) >= 126 {
			return nil, ErrUnknown
		}
		for _, c := range k {
			if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
				return nil, ErrUnknown
			}
		}
		if _, yes := out[k]; yes {
			return nil, ErrUnknown
		}
		out[k] = v
	}
	return out, nil
}

func decodeNormalOrdinaryStockUdevMonitorEvent(raw, oob []byte, flags int, from unix.Sockaddr) (uint32, normalOrdinaryStockUdevMonitorEvent, error) {
	var zero normalOrdinaryStockUdevMonitorEvent
	a, ok := from.(*unix.SockaddrNetlink)
	if !ok || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return 0, zero, ErrUnknown
	}
	if a.Groups == 1 {
		v, e := normalOrdinaryUdevV2DecodeKernelPacket(raw, oob, flags, from)
		if e != nil {
			return 0, zero, e
		}
		return 1, normalOrdinaryStockUdevMonitorEvent{Sequence: v.sequence, Properties: v.properties, Raw: v.raw}, nil
	}
	if a.Groups != 2 || a.Pid == 0 || len(raw) < 40 || len(raw) > 40+4096 {
		return 0, zero, ErrUnknown
	}
	msgs, e := unix.ParseSocketControlMessage(oob)
	if e != nil {
		return 0, zero, errors.Join(ErrUnknown, e)
	}
	var cred *unix.Ucred
	bad := false
	for _, c := range msgs {
		if c.Header.Level != unix.SOL_SOCKET {
			bad = true
			continue
		}
		switch c.Header.Type {
		case unix.SCM_CREDENTIALS:
			v, e := unix.ParseUnixCredentials(&c)
			if e != nil || cred != nil || v == nil || v.Pid <= 1 || v.Uid != 0 || v.Gid != 0 {
				bad = true
			}
			cred = v
		case unix.SO_RXQ_OVFL:
			if len(c.Data) != 4 || binary.NativeEndian.Uint32(c.Data) != 0 {
				bad = true
			}
		case unix.SCM_RIGHTS:
			fds, _ := unix.ParseUnixRights(&c)
			for _, fd := range fds {
				_ = unix.Close(fd)
			}
			bad = true
		default:
			bad = true
		}
	}
	if bad || cred == nil {
		return 0, zero, ErrUnknown
	}
	if !bytes.Equal(raw[:8], []byte{'l', 'i', 'b', 'u', 'd', 'e', 'v', 0}) || binary.BigEndian.Uint32(raw[8:12]) != 0xfeedcafe || binary.NativeEndian.Uint32(raw[12:16]) != 40 || binary.NativeEndian.Uint32(raw[16:20]) != 40 || uint64(binary.NativeEndian.Uint32(raw[20:24])) != uint64(len(raw)-40) {
		return 0, zero, ErrUnknown
	}
	p, e := normalOrdinaryStockUdevMonitorProperties(raw[40:])
	if e != nil {
		return 0, zero, e
	}
	seq, e := strconv.ParseUint(p["SEQNUM"], 10, 64)
	if e != nil || seq == 0 || strconv.FormatUint(seq, 10) != p["SEQNUM"] || !normalOrdinaryStockUdevAbsolute(p["DEVPATH"], "") || !normalOrdinaryStockUdevWord(p["SUBSYSTEM"]) {
		return 0, zero, ErrUnknown
	}
	known := false
	for op := uint16(1); op <= 8; op++ {
		if normalOrdinaryUdevV2Action(op) == p["ACTION"] {
			known = true
		}
	}
	if !known || binary.BigEndian.Uint32(raw[24:28]) != normalOrdinaryUdevV2Hash(p["SUBSYSTEM"]) || binary.BigEndian.Uint32(raw[28:32]) != normalOrdinaryUdevV2Hash(p["DEVTYPE"]) {
		return 0, zero, ErrUnknown
	}
	var bloom uint64
	if tags := p["TAGS"]; tags != "" {
		if !strings.HasPrefix(tags, ":") || !strings.HasSuffix(tags, ":") {
			return 0, zero, ErrUnknown
		}
		seen := map[string]bool{}
		for _, tag := range strings.Split(tags[1:len(tags)-1], ":") {
			if !normalOrdinaryStockUdevWord(tag) || seen[tag] {
				return 0, zero, ErrUnknown
			}
			seen[tag] = true
			h := normalOrdinaryUdevV2Hash(tag)
			for _, shift := range []uint{0, 6, 12, 18} {
				bloom |= uint64(1) << ((h >> shift) & 63)
			}
		}
	}
	if binary.BigEndian.Uint32(raw[32:36]) != uint32(bloom>>32) || binary.BigEndian.Uint32(raw[36:40]) != uint32(bloom) {
		return 0, zero, ErrUnknown
	}
	return 2, normalOrdinaryStockUdevMonitorEvent{seq, p, bytes.Clone(raw), cred.Pid, a.Pid}, nil
}

func (m *normalOrdinaryStockUdevMonitor) consume(raw, oob []byte, flags int, from unix.Sockaddr) error {
	group, v, e := decodeNormalOrdinaryStockUdevMonitorEvent(raw, oob, flags, from)
	if e != nil {
		return e
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.kernel)+len(m.processed) >= 8192 || m.rawBytes+len(raw) > 32<<20 {
		return ErrUnknown
	}
	to := m.kernel
	if group == 2 {
		to = m.processed
	}
	if _, yes := to[v.Sequence]; yes {
		return errors.Join(ErrUnknown, fmt.Errorf("duplicate original udev sequence"))
	}
	to[v.Sequence] = v
	m.rawBytes += len(raw)
	return nil
}

func openNormalOrdinaryStockUdevMonitor() (_ *normalOrdinaryStockUdevMonitor, err error) {
	fd, e := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.NETLINK_KOBJECT_UEVENT)
	if e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	m := &normalOrdinaryStockUdevMonitor{fd: fd, stop: make(chan struct{}), done: make(chan struct{}), kernel: map[uint64]normalOrdinaryStockUdevMonitorEvent{}, processed: map[uint64]normalOrdinaryStockUdevMonitorEvent{}}
	defer func() {
		if err != nil {
			_ = unix.Close(fd)
		}
	}()
	for _, opt := range []int{unix.SO_PASSCRED, unix.SO_RXQ_OVFL} {
		if e = unix.SetsockoptInt(fd, unix.SOL_SOCKET, opt, 1); e != nil {
			return nil, e
		}
	}
	if e = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, 4<<20); e != nil {
		return nil, e
	}
	if e = unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Groups: 3}); e != nil {
		return nil, e
	}
	a, e := unix.Getsockname(fd)
	if e != nil {
		return nil, e
	}
	bound, ok := a.(*unix.SockaddrNetlink)
	if !ok || bound.Groups != 3 || bound.Pid == 0 {
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
	go m.readOriginalOFD()
	return m, nil
}
func (m *normalOrdinaryStockUdevMonitor) verify() error {
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
	if e != nil || !ok || v.Groups != 3 || v.Pid != m.port {
		return errors.Join(ErrUnknown, e)
	}
	return nil
}
func (m *normalOrdinaryStockUdevMonitor) readOriginalOFD() {
	defer close(m.done)
	fail := func(e error) { m.mu.Lock(); m.err = errors.Join(ErrUnknown, m.err, e); m.mu.Unlock() }
	raw := make([]byte, 8193)
	oob := make([]byte, 256)
	for {
		finishing := false
		select {
		case <-m.stop:
			finishing = true
		default:
		}
		n, c, flags, from, e := unix.Recvmsg(m.fd, raw, oob, unix.MSG_DONTWAIT|unix.MSG_CMSG_CLOEXEC)
		if errors.Is(e, unix.EINTR) {
			continue
		}
		if errors.Is(e, unix.EAGAIN) || errors.Is(e, unix.EWOULDBLOCK) {
			// 仅调用者已实际Wait完所有writer时，原OFD的EAGAIN才用于终态。
			if finishing {
				return
			}
			poll := []unix.PollFd{{Fd: int32(m.fd), Events: unix.POLLIN}}
			_, e = unix.Poll(poll, 50)
			if errors.Is(e, unix.EINTR) {
				continue
			}
			if e != nil || poll[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
				fail(errors.Join(e, ErrUnknown))
				return
			}
			continue
		}
		if e != nil {
			fail(e)
			return
		}
		if e = m.consume(raw[:n], oob[:c], flags, from); e != nil {
			fail(e)
			return
		}
	}
}

func normalOrdinaryStockUdevMonitorMatch(kernel, processed map[uint64]normalOrdinaryStockUdevMonitorEvent, targets []normalOrdinaryStockUdevMonitorTarget) (normalOrdinaryStockUdevMonitorReceipt, error) {
	var out normalOrdinaryStockUdevMonitorReceipt
	if len(targets) == 0 || len(targets) > 4096 || len(kernel) == 0 || len(kernel) > 4096 || len(kernel) != len(processed) {
		return out, ErrUnknown
	}
	by := map[string]normalOrdinaryStockUdevMonitorTarget{}
	for _, v := range targets {
		if !normalOrdinaryStockUdevAbsolute(v.Devpath, "") || !normalOrdinaryStockUdevWord(v.Subsystem) {
			return out, ErrUnknown
		}
		if _, yes := by[v.Devpath]; yes {
			return out, ErrUnknown
		}
		by[v.Devpath] = v
		for k := range v.KernelProperties {
			switch k {
			case "ACTION", "DEVPATH", "SUBSYSTEM", "SEQNUM", "SYNTH_UUID":
				return out, ErrUnknown
			}
		}
	}
	var seqs []uint64
	for seq := range kernel {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	out.ProcessedBySequence = map[uint64]normalOrdinaryStockUdevMonitorEvent{}
	for _, seq := range seqs {
		k := kernel[seq]
		p, ok := processed[seq]
		if !ok || k.Sequence != seq || p.Sequence != seq {
			return normalOrdinaryStockUdevMonitorReceipt{}, ErrUnknown
		}
		t, ok := by[k.Properties["DEVPATH"]]
		if !ok || t.Subsystem != k.Properties["SUBSYSTEM"] {
			return normalOrdinaryStockUdevMonitorReceipt{}, ErrUnknown
		}
		for name, value := range t.KernelProperties {
			if got, yes := k.Properties[name]; !yes || got != value {
				return normalOrdinaryStockUdevMonitorReceipt{}, ErrUnknown
			}
		}
		for name, value := range k.Properties {
			switch name {
			case "ACTION", "DEVPATH", "SUBSYSTEM", "SEQNUM", "SYNTH_UUID":
			default:
				if got, yes := t.KernelProperties[name]; !yes || got != value {
					return normalOrdinaryStockUdevMonitorReceipt{}, ErrUnknown
				}
			}
			if name == "DEVNAME" && !strings.HasPrefix(value, "/") {
				value = "/dev/" + value
			}
			if got, yes := p.Properties[name]; !yes || got != value {
				return normalOrdinaryStockUdevMonitorReceipt{}, ErrUnknown
			}
		}
		out.KernelEvents = append(out.KernelEvents, normalOrdinaryStockUdevMonitorCopy(k))
		out.ProcessedBySequence[seq] = normalOrdinaryStockUdevMonitorCopy(p)
	}
	return out, nil
}

// 必须在同原supervisor所有writer真实Wait并复核三actor后调用；API不验证/冒充该来源。
func (m *normalOrdinaryStockUdevMonitor) finishAfterOriginalWritersWait(ctx context.Context, targets []normalOrdinaryStockUdevMonitorTarget) (normalOrdinaryStockUdevMonitorReceipt, error) {
	var zero normalOrdinaryStockUdevMonitorReceipt
	if m == nil || ctx == nil {
		return zero, ErrUnknown
	}
	m.mu.Lock()
	if m.finished || m.closed {
		m.mu.Unlock()
		return zero, ErrUnknown
	}
	m.finished = true
	m.mu.Unlock()
	m.stopOnce.Do(func() { close(m.stop) })
	select {
	case <-ctx.Done():
		return zero, errors.Join(ErrUnknown, ctx.Err())
	case <-m.done:
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.err != nil {
		return zero, errors.Join(ErrUnknown, m.err)
	}
	if e := m.verify(); e != nil {
		return zero, e
	}
	out, e := normalOrdinaryStockUdevMonitorMatch(m.kernel, m.processed, targets)
	if e != nil {
		return zero, e
	}
	out.Object, out.Cookie, out.Port = m.object, m.cookie, m.port
	return out, nil
}
func (m *normalOrdinaryStockUdevMonitor) close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.err = errors.Join(ErrUnknown, m.err)
	m.mu.Unlock()
	m.stopOnce.Do(func() { close(m.stop) })
	<-m.done
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		_ = unix.Close(m.fd)
		m.fd = -1
		m.closed = true
	}
}
