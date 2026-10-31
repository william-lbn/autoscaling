//go:build linux

package vmguard

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestInitialBootstrapV2FrozenBaseAndBusinessBounds(t *testing.T) {
	// PG17-U-INITIAL-H06：原物理base不冒充businessMin，普通min边界不降低。
	source := initialBootModelSource(t)
	offset, e := initialBootTargetOffset(source, source.Identity.MinimumBytes)
	require.NoError(t, e)
	require.Equal(t, source.Identity.MinimumBytes-source.OriginalBase, offset)
	require.NotEqual(t, control.Uint64(0), offset)
	for _, n := range []control.Uint64{0, source.OriginalBase, source.Identity.MinimumBytes - 1, source.Identity.MaximumBytes + source.Identity.BlockBytes} {
		_, e = initialBootTargetOffset(source, n)
		require.Error(t, e)
	}
	f, _, cmd := initialBootSourceFixture(t)
	_, e = initialBootArgv(cmd, f.runner.birth.Identity, 256<<20)
	require.NoError(t, e)
	for _, name := range []string{"slots", "backend", "node", "requested", "duplicate", "missing", "empty", "overflow", "unaligned"} {
		t.Run(name, func(t *testing.T) {
			c := &exec.Cmd{Path: cmd.Path, Args: append([]string(nil), cmd.Args...)}
			base := control.Uint64(256 << 20)
			switch name {
			case "slots":
				c.Args[2] = strings.ReplaceAll(c.Args[2], "slots=1", "slots=2")
			case "backend":
				c.Args[4] = strings.ReplaceAll(c.Args[4], "3840M", "4096M")
			case "node":
				c.Args[6] = strings.ReplaceAll(c.Args[6], "node=0", "node=1")
			case "requested":
				c.Args[6] = strings.ReplaceAll(c.Args[6], "requested-size=0", "requested-size=8388608")
			case "duplicate":
				c.Args = append(c.Args, "-m", "size=256M,slots=1,maxmem=4G")
			case "missing":
				c.Args = append(c.Args[:3], c.Args[5:]...)
			case "empty":
				c.Args = nil
			case "overflow":
				c.Args[2] = "size=18446744073709551615G,slots=1,maxmem=4G"
			case "unaligned":
				base++
			}
			_, e := initialBootArgv(c, f.runner.birth.Identity, base)
			require.Error(t, e)
		})
	}
	// 原v1总量decoder仍拒绝base-only小于Min，不为新mode放宽它。
	id := geometryIdentityFixture()
	g, e := decodeOriginalMemoryGeometry(id, 256<<20, geometryInputFixture())
	require.NoError(t, e)
	require.Error(t, (Observation{g.RequestedBytes, g.ActualBytes}).validate(id))
}
func TestInitialBootstrapV2OriginalChildPidfdLifecycleAndWrongPeer(t *testing.T) {
	// PG17-U-INITIAL-H07：真实当前Linux子进程/pidfd/procdir；非QEMU/Guest认证。
	flags, stdinErr := unix.FcntlInt(0, unix.F_GETFD, 0)
	(&initialBootProcess{}).close()
	afterFlags, afterErr := unix.FcntlInt(0, unix.F_GETFD, 0)
	require.Equal(t, flags, afterFlags)
	require.Equal(t, stdinErr, afterErr, "zero process不得关闭stdin")
	source := initialBootModelSource(t)
	path, e := exec.LookPath("sleep")
	require.NoError(t, e)
	path, e = filepath.Abs(path)
	require.NoError(t, e)
	dev, ino, sha, e := initialBootExecutable(path)
	require.NoError(t, e)
	source.QEMUPath, source.QEMUDevice, source.QEMUInode, source.QEMUSHA256 = path, dev, ino, sha
	cmd := exec.Command(path, "30")
	source.CommandSHA256 = SourceDigest(initialBootCanonical([]any{cmd.Path, cmd.Args}))
	_, e = captureInitialBootProcess(cmd, source)
	require.Error(t, e)
	require.NoError(t, cmd.Start())
	p, e := captureInitialBootProcess(cmd, source)
	require.NoError(t, e)
	defer p.close()
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	require.NoError(t, p.alive())
	dir, e := os.MkdirTemp("/tmp", "initial-peer-")
	require.NoError(t, e)
	defer os.RemoveAll(dir)
	address := filepath.Join(dir, "qmp.sock")
	l, e := net.ListenUnix("unix", &net.UnixAddr{Name: address, Net: "unix"})
	require.NoError(t, e)
	defer l.Close()
	c, e := net.DialUnix("unix", nil, &net.UnixAddr{Name: address, Net: "unix"})
	require.NoError(t, e)
	defer c.Close()
	server, e := l.AcceptUnix()
	require.NoError(t, e)
	defer server.Close()
	require.Error(t, p.checkPeer(&qmpConnection{conn: c}, source), "same UID/UUID不能替代原child")
	bad := source
	bad.CommandSHA256 = strings.Repeat("ab", 32)
	_, e = captureInitialBootProcess(cmd, bad)
	require.Error(t, e)
	require.NoError(t, cmd.Process.Kill())
	require.Error(t, cmd.Wait())
	require.Error(t, p.alive())
	fd := p.pidfd
	p.close()
	_, e = unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	require.ErrorIs(t, e, unix.EBADF)
	require.ErrorIs(t, (&initialBaseQMP{}).Set(context.Background(), source.Identity.MinimumBytes), errInitialBootUnavailable)
}
