//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestInitialGuestChannelRejectsDeviceAliases(t *testing.T) {
	for _, name := range []string{"vport0p0", "vport1p2", "vport4294967295p4"} {
		require.True(t, initialGuestDeviceName(name), name)
	}
	for _, name := range []string{"vport01p0", "vport0p01", "vport-1p0", "vport1p0/../x", "/dev/vport0p1", "vport1p2p3", "vport4294967296p0", "vport0p", "vportp1"} {
		require.False(t, initialGuestDeviceName(name), name)
	}
	n, e := initialGuestDeviceNumber("240:12")
	require.NoError(t, e)
	require.Equal(t, unix.Mkdev(240, 12), n)
	for _, v := range []string{"0:0", "0240:0", "240:01", "240:+1", "240:0:1", "240:0\n", "4294967296:0"} {
		_, e = initialGuestDeviceNumber(v)
		require.Error(t, e, v)
	}
}
func TestInitialGuestChannelStreamPartialAndEOF(t *testing.T) {
	var fds [2]int
	require.NoError(t, unix.Pipe2(fds[:], unix.O_NONBLOCK|unix.O_CLOEXEC))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s := &initialGuestStream{fd: fds[0], ctx: ctx}
	defer s.close()
	_, e := unix.Write(fds[1], []byte("DS"))
	require.NoError(t, e)
	require.NoError(t, unix.Close(fds[1]))
	_, e = bootprotocol.ReadInitialPrebootChallenge(s)
	require.ErrorIs(t, e, io.ErrUnexpectedEOF)
	var b [1]byte
	_, e = s.Read(b[:])
	require.ErrorIs(t, e, io.EOF)
}
func TestInitialGuestChannelUnconnectedAndBlockedCancel(t *testing.T) {
	for _, closeWriter := range []bool{true, false} {
		t.Run(map[bool]string{true: "before-host-connect", false: "blocked-read"}[closeWriter], func(t *testing.T) {
			var fds [2]int
			require.NoError(t, unix.Pipe2(fds[:], unix.O_NONBLOCK|unix.O_CLOEXEC))
			if closeWriter {
				require.NoError(t, unix.Close(fds[1]))
			} else {
				defer unix.Close(fds[1])
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			s := &initialGuestStream{fd: fds[0], ctx: ctx}
			defer s.close()
			var b [1]byte
			n, e := s.Read(b[:])
			require.Zero(t, n)
			require.True(t, errors.Is(e, context.DeadlineExceeded) || errors.Is(e, context.Canceled), e)
		})
	}
	var missing *initialGuestStream
	_, e := missing.Read(make([]byte, 1))
	require.Error(t, e)
	_, e = missing.Write(make([]byte, 1))
	require.Error(t, e)
	missing.close()
}

func TestInitialGuestChannelOnlyRetriesMissingMovablePages(t *testing.T) {
	c, _, _, _ := initialPrebootFixture(t)
	before := initialPrebootPhysicalFixture(t, c)
	expected := uint64(c.BusinessMinimum-c.OriginalBase) / 4096
	for _, mode := range []string{"pending", "over-grow", "fixed-capacity"} {
		t.Run(mode, func(t *testing.T) {
			current := before
			current.Zones = append([]PhysicalKernelZone(nil), before.Zones...)
			for i := range current.Zones {
				if current.Zones[i].Name == "Movable" {
					current.Zones[i].PresentPages = control.Uint64(expected)
					if mode == "pending" {
						current.Zones[i].PresentPages--
					}
					if mode == "over-grow" {
						current.Zones[i].PresentPages++
					}
				}
			}
			if mode == "fixed-capacity" {
				current.NonMovableUpperBytes++
			}
			_, err := initialPrebootMovablePages(c, before, current)
			require.Error(t, err)
			require.Equal(t, mode == "pending", errors.Is(err, errInitialPrebootMovablePending))
		})
	}
}

// 使用真实openat/FD验证枚举边界；普通临时文件绝不能冒充sysfs目标。
func TestInitialGuestChannelNameSelectionClosesUnselectedAndKeepsStrictLeaf(t *testing.T) {
	for _, mode := range []string{"unnamed-console", "target-on-ordinary-fs", "unrelated-on-ordinary-fs", "name-symlink", "closed-dir-fd"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			switch mode {
			case "target-on-ordinary-fs":
				require.NoError(t, os.WriteFile(filepath.Join(dir, "name"), []byte(initialGuestPortName+"\n"), 0644))
			case "unrelated-on-ordinary-fs":
				require.NoError(t, os.WriteFile(filepath.Join(dir, "name"), []byte("tech.neon.log.0\n"), 0644))
			case "name-symlink":
				require.NoError(t, os.WriteFile(filepath.Join(dir, "other"), []byte(initialGuestPortName+"\n"), 0644))
				require.NoError(t, os.Symlink("other", filepath.Join(dir, "name")))
			}
			fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
			require.NoError(t, err)
			if mode == "closed-dir-fd" {
				require.NoError(t, unix.Close(fd))
			}
			selected, err := initialGuestSelectPortName(fd)
			require.False(t, selected)
			if mode == "unnamed-console" {
				require.NoError(t, err, "只有首枚举无name是合法未选口")
			} else {
				require.Error(t, err, "其它读取失败和非sysfs目标仍须拒绝")
				if mode == "name-symlink" {
					require.ErrorIs(t, err, unix.ELOOP)
				}
				if mode == "closed-dir-fd" {
					require.ErrorIs(t, err, unix.EBADF)
				}
			}
			var st unix.Stat_t
			require.ErrorIs(t, unix.Fstat(fd, &st), unix.EBADF, "跳过或拒绝前必须关闭该port FD")
		})
	}
}
