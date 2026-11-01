//go:build linux

package resourceguard

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestNormalSystemRoleStdioBoundedDrain(t *testing.T) {
	o, e := newNormalRoleStdioObjects()
	require.NoError(t, e)
	defer o.close()
	spawn, e := o.spawnFiles()
	require.NoError(t, e)
	defer closeNormalRoleStdioFiles(spawn)
	for i := 1; i < 3; i++ {
		flags, e := unix.FcntlInt(spawn[i].Fd(), unix.F_GETFL, 0)
		require.NoError(t, e)
		require.Zero(t, flags&unix.O_NONBLOCK)
	}
	raw := bytes.Repeat([]byte("bounded-original-role-stream-"), 4096)
	for i := 0; i < 2; i++ {
		_, e = spawn[i+1].Write(raw)
		require.NoError(t, e)
		spawn[i+1].Close()
		spawn[i+1] = nil
		o.originals[i+1].Close()
		o.originals[i+1] = nil
		select {
		case <-o.drains[i].done:
		case <-time.After(3 * time.Second):
			t.Fatal("bounded drain did not observe actual final writer close")
		}
		n, kept, h := o.drains[i].summary()
		require.Equal(t, uint64(len(raw)), n)
		require.Equal(t, normalRoleStdioRingBytes, kept)
		require.Equal(t, sha256.Sum256(raw[len(raw)-normalRoleStdioRingBytes:]), h)
		require.ErrorIs(t, o.drains[i].err, io.EOF)
	}
}
func TestNormalSystemRoleStdioMissingOriginalAuthorityHasNoEffects(t *testing.T) {
	_, e := newNormalSystemRoleStdioFactory(context.Background(), nil, nil, nil)
	require.ErrorIs(t, e, ErrUnknown)
	for _, role := range []string{"resource", "system-role-broker", "shutdown", "/arbitrary", ""} {
		require.False(t, normalRoleStdioFixedRole(role))
	}
	for _, role := range []string{"acpi", "daemon", "compute"} {
		require.True(t, normalRoleStdioFixedRole(role))
	}
	var p *normalSystemRoleStdio
	_, e = p.inspectOriginalStoppedTargetLocked(context.Background(), nil, nil, nil, nil)
	require.ErrorIs(t, e, ErrUnknown)
}

// 只验真正exec完成后的fd目录边界；无原role/权限或Source资格。
func TestNormalSystemRoleStdioExecDescriptorBoundary(t *testing.T) {
	for _, extra := range []bool{false, true} {
		t.Run(map[bool]string{false: "only-stdio", true: "foreign-inherited-fd"}[extra], func(t *testing.T) {
			cmd := exec.Command("/bin/sh", "-c", "printf 'ready\n'; read ignored")
			cmd.Env = []string{}
			pidfd := -1
			cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &pidfd}
			input, e := cmd.StdinPipe()
			require.NoError(t, e)
			output, e := cmd.StdoutPipe()
			require.NoError(t, e)
			if extra {
				f, e := os.CreateTemp(t.TempDir(), "foreign")
				require.NoError(t, e)
				defer f.Close()
				cmd.ExtraFiles = []*os.File{f}
			}
			require.NoError(t, cmd.Start())
			require.GreaterOrEqual(t, pidfd, 0)
			defer unix.Close(pidfd)
			defer func() { input.Close(); _ = unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0); _ = cmd.Wait() }()
			line, e := bufio.NewReader(output).ReadString('\n')
			require.NoError(t, e)
			require.Equal(t, "ready\n", line)
			fd, e := unix.Open("/proc/"+strconv.Itoa(cmd.Process.Pid), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
			require.NoError(t, e)
			defer unix.Close(fd)
			e = normalRoleStdioOnlyThreeFDs(fd)
			if extra {
				require.Error(t, e)
			} else {
				require.NoError(t, e)
			}
		})
	}
}
