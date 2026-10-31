//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"io"
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
