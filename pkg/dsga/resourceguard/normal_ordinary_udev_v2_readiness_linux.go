//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"strconv"
	"sync"
)

const normalUdevOriginalControlPath = "/run/udev/control"
const normalUdevReadinessMaximumFDs = 1024

// 只持原active main的真实dup；每次使用还核同候选/原FD/OFD及当前socket状态。
type normalOrdinaryUdevReadiness struct {
	mu                           sync.Mutex
	closed                       bool
	candidate                    *normalSystemRoleLeaseCandidate
	task                         *normalSystemOriginalTask
	control, netlink             *os.File
	controlNumber, netlinkNumber int
}

func (p *normalOrdinaryUdevReadiness) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	if p.control != nil {
		p.control.Close()
	}
	if p.netlink != nil {
		p.netlink.Close()
	}
	if p.task != nil {
		p.task.close()
	}
}

// kind=1: 原control，kind=2: 原kernel netlink，0: 非候选。ready只取内核实际状态。
func normalUdevReadinessSocket(fd int) (kind int, ready bool, err error) {
	var st unix.Stat_t
	if e := unix.Fstat(fd, &st); e != nil {
		return 0, false, e
	}
	if st.Mode&unix.S_IFMT != unix.S_IFSOCK {
		return 0, false, nil
	}
	domain, e := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_DOMAIN)
	if e != nil {
		return 0, false, e
	}
	typ, e := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TYPE)
	if e != nil {
		return 0, false, e
	}
	address, e := unix.Getsockname(fd)
	if e != nil {
		return 0, false, e
	}
	if domain == unix.AF_UNIX && typ == unix.SOCK_SEQPACKET {
		a, ok := address.(*unix.SockaddrUnix)
		if !ok {
			return 0, false, ErrUnknown
		}
		listening, e := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_ACCEPTCONN)
		if e != nil {
			return 0, false, e
		}
		if a.Name != normalUdevOriginalControlPath {
			if listening != 0 {
				return 0, false, ErrUnknown
			}
			return 0, false, nil
		}
		if listening != 0 && listening != 1 {
			return 0, false, ErrUnknown
		}
		return 1, listening == 1, nil
	}
	if domain == unix.AF_NETLINK {
		protocol, e := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_PROTOCOL)
		if e != nil {
			return 0, false, e
		}
		if protocol != unix.NETLINK_KOBJECT_UEVENT {
			return 0, false, nil
		}
		if typ != unix.SOCK_RAW && typ != unix.SOCK_DGRAM {
			return 0, false, ErrUnknown
		}
		a, ok := address.(*unix.SockaddrNetlink)
		if !ok || a.Groups&^uint32(1) != 0 {
			return 0, false, ErrUnknown
		}
		return 2, a.Pid != 0 && a.Groups == 1, nil
	}
	return 0, false, nil
}
func (p *normalOrdinaryUdevReadiness) recheckFile(task *normalSystemOriginalTask, held *os.File, number, kind int) (bool, error) {
	fd, e := unix.PidfdGetfd(task.pidfd, number, 0)
	if e != nil {
		return false, e
	}
	defer unix.Close(fd)
	if e = normalRoleStdioSameOFD(int(held.Fd()), fd); e != nil {
		return false, e
	}
	k, ready, e := normalUdevReadinessSocket(fd)
	if e != nil || k != kind {
		return false, errors.Join(ErrUnknown, e)
	}
	return ready, nil
}
func (p *normalOrdinaryUdevReadiness) refresh(task *normalSystemOriginalTask) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.task == nil || task == nil {
		return ErrUnknown
	}
	task.mu.Lock()
	defer task.mu.Unlock()
	if e := task.verify(); e != nil {
		return e
	}
	if e := arrayMatchPIDFD(task.pidfd, p.task.pidfd); e != nil {
		return e
	}
	if p.control == nil || p.netlink == nil {
		fd, e := unix.Openat(task.procfd, "fd", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if e != nil {
			return e
		}
		directory := os.NewFile(uintptr(fd), "original-udev-main-fds")
		names, e := directory.Readdirnames(normalUdevReadinessMaximumFDs + 1)
		directory.Close()
		if e != nil && !errors.Is(e, io.EOF) {
			return e
		}
		if len(names) > normalUdevReadinessMaximumFDs {
			return ErrUnknown
		}
		for _, name := range names {
			n, e := strconv.Atoi(name)
			if e != nil || n < 0 || strconv.Itoa(n) != name {
				return ErrUnknown
			}
			if p.control != nil && n == p.controlNumber || p.netlink != nil && n == p.netlinkNumber {
				continue
			}
			dup, e := unix.PidfdGetfd(task.pidfd, n, 0)
			if errors.Is(e, unix.EBADF) {
				// 尚未捕获的FD可由原应用正常关闭；原Task前后真活才忽略该次扫描项。
				// 已持有control/netlink从recheckFile走严格同OFD校验，不进此分支。
				if e = task.verify(); e != nil {
					return e
				}
				continue
			}
			if e != nil {
				return e
			}
			kind, _, e := normalUdevReadinessSocket(dup)
			if e != nil {
				unix.Close(dup)
				return e
			}
			if kind == 1 {
				if p.control != nil {
					unix.Close(dup)
					return ErrUnknown
				}
				p.control, p.controlNumber = os.NewFile(uintptr(dup), "original-udev-control"), n
			} else if kind == 2 {
				if p.netlink != nil {
					unix.Close(dup)
					return ErrUnknown
				}
				p.netlink, p.netlinkNumber = os.NewFile(uintptr(dup), "original-udev-kernel-monitor"), n
			} else {
				unix.Close(dup)
			}
		}
	}
	controlReady, netlinkReady := false, false
	var e error
	if p.control != nil {
		controlReady, e = p.recheckFile(task, p.control, p.controlNumber, 1)
		if e != nil {
			return e
		}
	}
	if p.netlink != nil {
		netlinkReady, e = p.recheckFile(task, p.netlink, p.netlinkNumber, 2)
		if e != nil {
			return e
		}
	}
	if e = task.verify(); e != nil {
		return e
	}
	if !controlReady || !netlinkReady {
		return errNormalStartupUnpublished
	}
	return nil
}
func (q *normalOrdinaryUdevV2) ensureOriginalUdevReadyLocked(ctx context.Context, state *lockedState, r *record) error {
	if q == nil || ctx == nil || ctx.Err() != nil {
		return ErrUnknown
	}
	lease, e := normalStartupActiveLeaseLocked(ctx, q, state, r, "udev")
	if e != nil {
		return e
	}
	defer lease.close()
	q.mu.Lock()
	ready := q.readiness
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return ErrUnknown
	}
	if ready == nil {
		task, e := duplicateNormalSystemOriginalTask(lease.task)
		if e != nil {
			return e
		}
		ready = &normalOrdinaryUdevReadiness{candidate: lease.candidate, task: task, controlNumber: -1, netlinkNumber: -1}
		q.mu.Lock()
		if q.closed || q.readiness != nil {
			q.mu.Unlock()
			ready.close()
			return ErrUnknown
		}
		q.readiness = ready
		q.mu.Unlock()
	}
	if ready.candidate != lease.candidate {
		return fmt.Errorf("%w: original udev generation changed", ErrUnknown)
	}
	return ready.refresh(lease.task)
}
