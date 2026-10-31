//go:build linux

package vmguard

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func initialTransportModelReceipt(c bootprotocol.InitialPrebootChallenge) bootprotocol.InitialPrebootReceipt {
	h := strings.Repeat("ab", 32)
	return bootprotocol.InitialPrebootReceipt{Challenge: c, GuestBootID: "12345678-abcd-1234-abcd-123456789abc", RootInstanceNonce: h, KernelPreparationSHA256: h, PhysicalOriginSHA256: h, PreSystemSHA256: h, CurrentPhysicalSHA256: h}
}
func initialTransportChild(t *testing.T, mode string) {
	// 只在_test的真实子进程中提供模型回执；生产没有环境变量seam或verified输入。
	l, e := net.ListenUnix("unix", &net.UnixAddr{Net: "unix", Name: filepath.Join(os.Getenv("DSGA_BOOT_TRANSPORT_TEST_DIR"), initialBootSocketName)})
	require.NoError(t, e)
	defer l.Close()
	require.NoError(t, l.SetDeadline(time.Now().Add(10*time.Second)))
	c, e := l.AcceptUnix()
	require.NoError(t, e)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(10*time.Second)))
	want, e := bootprotocol.ReadInitialPrebootChallenge(c)
	require.NoError(t, e)
	if mode == "stall" {
		ready := os.NewFile(3, "private-test-challenge-ready")
		_, err := ready.Write([]byte{1})
		require.NoError(t, err)
		require.NoError(t, ready.Close())
		var b [1]byte
		_, _ = c.Read(b[:])
		return
	}
	prepared := initialTransportModelReceipt(want)
	if mode == "foreign-receipt" {
		prepared.Challenge.Nonce = strings.Repeat("cd", 32)
		require.NoError(t, bootprotocol.WriteInitialPrebootReceipt(c, prepared))
		return
	}
	require.NoError(t, bootprotocol.WriteInitialPrebootReceipt(c, prepared))
	next, e := bootprotocol.ReadInitialMovableChallenge(c)
	require.NoError(t, e)
	require.Equal(t, prepared, next.Preboot)
	h, e := prepared.SHA256()
	require.NoError(t, e)
	delta := next.TargetBytes - prepared.Challenge.OriginalBase
	observed := bootprotocol.InitialMovableReceipt{Challenge: next, GuestBootID: prepared.GuestBootID, RootInstanceNonce: strings.Repeat("ef", 32), PreparedReceiptSHA256: h, PhysicalOriginSHA256: prepared.PhysicalOriginSHA256, PreSystemSHA256: prepared.PreSystemSHA256, CurrentPhysicalSHA256: strings.Repeat("12", 32), ExpectedIncrementBytes: delta, MovablePresentPages: delta / 4096}
	require.NoError(t, bootprotocol.WriteInitialMovableReceipt(c, observed))
	done, e := bootprotocol.ReadInitialCompletion(c)
	require.NoError(t, e)
	require.NoError(t, done.MatchReceipt(observed))
	// QEMU模型进程须活到Host最终peer复核；Host主动close之后才退出。
	var b [1]byte
	_, e = c.Read(b[:])
	require.ErrorIs(t, e, io.EOF)
}
func initialTransportProcessFixture(t *testing.T, mode string) (string, initialBootSource, *initialBootProcess, *exec.Cmd, *os.File) {
	t.Helper()
	dir, e := os.MkdirTemp("/tmp", "boot-transport-")
	require.NoError(t, e)
	t.Cleanup(func() { os.RemoveAll(dir) })
	source := initialBootModelSource(t)
	path, e := os.Executable()
	require.NoError(t, e)
	path, e = filepath.EvalSymlinks(path)
	require.NoError(t, e)
	dev, ino, h, e := initialBootExecutable(path)
	require.NoError(t, e)
	source.QEMUPath, source.QEMUDevice, source.QEMUInode, source.QEMUSHA256 = path, dev, ino, h
	readReady, writeReady, e := os.Pipe()
	require.NoError(t, e)
	t.Cleanup(func() { readReady.Close(); writeReady.Close() })
	cmd := exec.Command(path, "-test.run=^TestInitialBootTransportOriginalPeerFlow$", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "DSGA_BOOT_TRANSPORT_TEST_MODE="+mode, "DSGA_BOOT_TRANSPORT_TEST_DIR="+dir)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	cmd.ExtraFiles = []*os.File{writeReady}
	source.CommandSHA256 = SourceDigest(initialBootCanonical([]any{cmd.Path, cmd.Args}))
	require.NoError(t, cmd.Start())
	require.NoError(t, writeReady.Close())
	p, e := captureInitialBootProcess(cmd, source)
	require.NoError(t, e)
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = unix.PidfdSendSignal(p.pidfd, unix.SIGKILL, nil, 0)
			_ = cmd.Wait()
		}
		p.close()
	})
	return dir, source, p, cmd, readReady
}
func initialTransportChallenge(t *testing.T, s initialBootSource) bootprotocol.InitialPrebootChallenge {
	t.Helper()
	ref := initialBootRef{ProtocolVersion: 2, Scope: initialBootScope, OriginalSHA256: initialBootSHA(initialBootCanonical(s)), Nonce: strings.Repeat("34", 32), Stage: "base_observed", TailSHA256: strings.Repeat("56", 32)}
	c, e := initialBootChallenge(s, ref)
	require.NoError(t, e)
	return c
}

func TestInitialBootTransportOriginalPeerFlow(t *testing.T) {
	if mode := os.Getenv("DSGA_BOOT_TRANSPORT_TEST_MODE"); mode != "" {
		initialTransportChild(t, mode)
		return
	}
	for _, mode := range []string{"valid", "foreign-receipt", "stall"} {
		t.Run(mode, func(t *testing.T) {
			dir, source, p, cmd, ready := initialTransportProcessFixture(t, mode)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			channel, e := openInitialBootChannel(ctx, dir, p, source)
			require.NoError(t, e)
			defer channel.close()
			want := initialTransportChallenge(t, source)
			confirmed := make(chan error, 1)
			call := ctx
			stop := func() {}
			if mode == "stall" {
				call, stop = context.WithCancel(ctx)
				go func() { var b [1]byte; _, err := io.ReadFull(ready, b[:]); confirmed <- err; stop() }()
			}
			defer stop()
			prepared, e := channel.prepare(call, want)
			if mode == "stall" {
				require.NoError(t, <-confirmed)
			}
			if mode != "valid" {
				require.Error(t, e)
				require.True(t, channel.closed)
				_, e = channel.prepare(ctx, want)
				require.Error(t, e)
				require.NoError(t, cmd.Wait())
				return
			}
			require.NoError(t, e)
			require.NoError(t, prepared.MatchChallenge(want))
			next := bootprotocol.InitialMovableChallenge{ProtocolVersion: 1, Scope: bootprotocol.InitialMovableScope, Preboot: prepared, HostStageSHA256: strings.Repeat("78", 32), TargetBytes: want.BusinessMinimum}
			observed, e := channel.movable(ctx, next)
			require.NoError(t, e)
			require.NoError(t, observed.MatchChallenge(next))
			h, e := observed.SHA256()
			require.NoError(t, e)
			require.NoError(t, channel.complete(ctx, bootprotocol.InitialCompletion{ProtocolVersion: 1, Scope: bootprotocol.InitialCompletionScope, Challenge: next, MovableReceiptSHA256: h, HostStageSHA256: strings.Repeat("90", 32)}))
			require.True(t, channel.closed)
			require.Equal(t, uint8(3), channel.phase)
			require.NoError(t, cmd.Wait())
		})
	}
}
func TestInitialBootTransportRejectsWrongPeerAndCancellation(t *testing.T) {
	dir, source, p, _, _ := initialTransportProcessFixture(t, "stall")
	// 原子进程尚在自己的原socket等待，另一个同UID listener不能替代它。
	wrong, e := os.MkdirTemp("/tmp", "boot-wrong-peer-")
	require.NoError(t, e)
	defer os.RemoveAll(wrong)
	l, e := net.ListenUnix("unix", &net.UnixAddr{Net: "unix", Name: filepath.Join(wrong, initialBootSocketName)})
	require.NoError(t, e)
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, e = openInitialBootChannel(ctx, wrong, p, source)
	require.Error(t, e)
	empty, e := os.MkdirTemp("/tmp", "boot-no-socket-")
	require.NoError(t, e)
	defer os.RemoveAll(empty)
	c, stop := context.WithCancel(context.Background())
	stop()
	_, e = openInitialBootChannel(c, empty, p, source)
	require.Error(t, e)
	// 不删除或覆盖任何既有socket。
	_, e = os.Lstat(filepath.Join(wrong, initialBootSocketName))
	require.NoError(t, e)
	require.NotEmpty(t, dir)
}
func TestInitialBootTransportRejectsSourceAndPhase(t *testing.T) {
	dir, source, p, _, _ := initialTransportProcessFixture(t, "stall")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, e := openInitialBootChannel(ctx, dir, p, source)
	require.NoError(t, e)
	defer c.close()
	want := initialTransportChallenge(t, source)
	want.RuntimeSHA256 = strings.Repeat("ff", 32)
	_, e = c.prepare(ctx, want)
	require.Error(t, e)
	require.True(t, c.closed)
	var missing *initialBootChannel
	_, e = missing.prepare(ctx, want)
	require.Error(t, e)
	_, e = missing.movable(ctx, bootprotocol.InitialMovableChallenge{})
	require.Error(t, e)
	require.Error(t, missing.complete(ctx, bootprotocol.InitialCompletion{}))
	missing.close()
	require.Contains(t, fmt.Sprint(e), "missing")
}
