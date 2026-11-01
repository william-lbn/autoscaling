//go:build linux

package resourceguard

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"testing"
)

// 真实内核socket状态定向U，不构造RoleMainLease或Source正例。
func TestNormalOriginalUdevSocketReadiness(t *testing.T) {
	t.Run("nonSocket", func(t *testing.T) {
		r, w, e := os.Pipe()
		if e != nil {
			t.Fatal(e)
		}
		defer r.Close()
		defer w.Close()
		k, ready, e := normalUdevReadinessSocket(int(r.Fd()))
		if e != nil || k != 0 || ready {
			t.Fatalf("pipe: %d %t %v", k, ready, e)
		}
	})
	t.Run("anonymousControlNotListening", func(t *testing.T) {
		fds, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
		if e != nil {
			t.Fatal(e)
		}
		defer unix.Close(fds[0])
		defer unix.Close(fds[1])
		k, ready, e := normalUdevReadinessSocket(fds[0])
		if e != nil || k != 0 || ready {
			t.Fatalf("anonymous pair: %d %t %v", k, ready, e)
		}
	})
	t.Run("differentListenerRejected", func(t *testing.T) {
		fd, e := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
		if e != nil {
			t.Fatal(e)
		}
		defer unix.Close(fd)
		if e = unix.Bind(fd, &unix.SockaddrUnix{Name: filepath.Join(t.TempDir(), "control")}); e != nil {
			t.Fatal(e)
		}
		if e = unix.Listen(fd, 1); e != nil {
			t.Fatal(e)
		}
		if _, _, e = normalUdevReadinessSocket(fd); !errors.Is(e, ErrUnknown) {
			t.Fatalf("foreign listener: %v", e)
		}
	})
	t.Run("unboundKernelMonitorPending", func(t *testing.T) {
		fd, e := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_KOBJECT_UEVENT)
		if e != nil {
			t.Fatal(e)
		}
		defer unix.Close(fd)
		k, ready, e := normalUdevReadinessSocket(fd)
		if e != nil || k != 2 || ready {
			t.Fatalf("unsubscribed: %d %t %v", k, ready, e)
		}
	})
	t.Run("closedDescriptorRejected", func(t *testing.T) {
		if _, _, e := normalUdevReadinessSocket(-1); !errors.Is(e, unix.EBADF) {
			t.Fatalf("closed: %v", e)
		}
	})
}

func TestNormalOriginalUdevReadinessClosedAndUnowned(t *testing.T) {
	for _, closed := range []bool{false, true} {
		p := &normalOrdinaryUdevReadiness{closed: closed}
		if e := p.refresh(nil); !errors.Is(e, ErrUnknown) {
			t.Fatalf("no owned task closed=%t: %v", closed, e)
		}
		p.close()
		p.close()
	}
}
