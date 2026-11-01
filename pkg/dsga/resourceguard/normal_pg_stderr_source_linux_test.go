//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func devPgStderrPipe(t *testing.T) [2]*os.File {
	t.Helper()
	var pair [2]int
	require.NoError(t, unix.Pipe2(pair[:], unix.O_CLOEXEC))
	files := [2]*os.File{os.NewFile(uintptr(pair[0]), "DEV pipe read"), os.NewFile(uintptr(pair[1]), "DEV pipe write")}
	t.Cleanup(func() { files[0].Close(); files[1].Close() })
	_, e := unix.FcntlInt(files[1].Fd(), unix.F_SETPIPE_SZ, normalPgStderrCapacity)
	require.NoError(t, e)
	return files
}
func TestNormalPgStderrNewProtocolCannotLoseAuthority(t *testing.T) {
	enabled, e := normalPgStderrProtocolMode(0, nil)
	require.NoError(t, e)
	require.False(t, enabled)
	for _, s := range []*arrayNativeServices{nil, {}} {
		enabled, e = normalPgStderrProtocolMode(1, s)
		require.Error(t, e)
		require.False(t, enabled)
	}
	enabled, e = normalPgStderrProtocolMode(2, &arrayNativeServices{admission: &normalAuthorityAdmission{}})
	require.Error(t, e)
	require.False(t, enabled)
}
func TestNormalPgStderrFinalCloseRaceFailClosed(t *testing.T) {
	q := &normalPgStderrRegistry{pins: map[string]*normalPgStderrPin{}}
	p := &normalPgStderrPin{registry: q, nonce: "DEV_NOT_SOURCE", pidfd: -1}
	q.pins[p.nonce] = p
	done := make(chan struct{})
	go func() {
		defer close(done)
		for n := 0; n < 1000; n++ {
			q.mu.Lock()
			p.close()
			q.closed = true
			q.mu.Unlock()
		}
	}()
	for n := 0; n < 1000; n++ {
		require.Error(t, p.verifyNativeFinal(context.Background(), nil, nil, -1))
		require.Error(t, p.releasedOriginalFinal())
	}
	<-done
}
func TestNormalPgStderrExactFrame(t *testing.T) {
	raw := make([]byte, 16)
	copy(raw, "NDSGPS01")
	binary.BigEndian.PutUint32(raw[8:12], 1)
	require.NoError(t, normalPgStderrFrame(raw))
	for _, name := range []string{"short", "long", "magic", "version", "reserved"} {
		t.Run(name, func(t *testing.T) {
			q := append([]byte{}, raw...)
			switch name {
			case "short":
				q = q[:15]
			case "long":
				q = append(q, 0)
			case "magic":
				q[0] ^= 1
			case "version":
				q[11] = 2
			case "reserved":
				q[15] = 1
			}
			require.Error(t, normalPgStderrFrame(q))
		})
	}
}
func TestNormalPgStderrEndpointsOriginalPipeOnly(t *testing.T) {
	original := devPgStderrPipe(t)
	device, inode, e := normalPgStderrEndpoints(original)
	require.NoError(t, e)
	require.NotZero(t, device)
	require.NotZero(t, inode)
	t.Run("reversed", func(t *testing.T) {
		_, _, e := normalPgStderrEndpoints([2]*os.File{original[1], original[0]})
		require.Error(t, e)
	})
	t.Run("foreign-pipe", func(t *testing.T) {
		other := devPgStderrPipe(t)
		_, _, e := normalPgStderrEndpoints([2]*os.File{original[0], other[1]})
		require.Error(t, e)
	})
	t.Run("same-read-twice", func(t *testing.T) {
		_, _, e := normalPgStderrEndpoints([2]*os.File{original[0], original[0]})
		require.Error(t, e)
	})
	t.Run("nil", func(t *testing.T) { _, _, e := normalPgStderrEndpoints([2]*os.File{}); require.Error(t, e) })
	for _, n := range []int{0, 1} {
		t.Run([]string{"nonblock-read", "nonblock-write"}[n], func(t *testing.T) {
			f := original[n]
			flags, e := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
			require.NoError(t, e)
			_, e = unix.FcntlInt(f.Fd(), unix.F_SETFL, flags|unix.O_NONBLOCK)
			require.NoError(t, e)
			_, _, e = normalPgStderrEndpoints(original)
			require.Error(t, e)
			_, e = unix.FcntlInt(f.Fd(), unix.F_SETFL, flags)
			require.NoError(t, e)
		})
	}
	t.Run("resized", func(t *testing.T) {
		_, e := unix.FcntlInt(original[1].Fd(), unix.F_SETPIPE_SZ, 4096)
		require.NoError(t, e)
		_, _, e = normalPgStderrEndpoints(original)
		require.Error(t, e)
	})
}
func TestNormalPgStderrReceiveExactAncillary(t *testing.T) {
	t.Run("peer-close", func(t *testing.T) {
		pair, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
		require.NoError(t, e)
		f := os.NewFile(uintptr(pair[1]), "DEV closed peer receiver")
		defer f.Close()
		conn, e := net.FileConn(f)
		require.NoError(t, e)
		defer conn.Close()
		require.NoError(t, unix.Close(pair[0]))
		files, _, e := normalPgStderrReceive(conn.(*net.UnixConn), time.Now().Add(time.Second))
		require.Error(t, e)
		require.Nil(t, files[0])
		require.Nil(t, files[1])
	})
	for _, n := range []int{0, 1, 2, 3} {
		t.Run(string(rune('0'+n))+"-rights", func(t *testing.T) {
			pair, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, e)
			defer unix.Close(pair[0])
			f := os.NewFile(uintptr(pair[1]), "DEV receiver")
			defer f.Close()
			require.NoError(t, unix.SetsockoptInt(pair[1], unix.SOL_SOCKET, unix.SO_PASSCRED, 1))
			conn, e := net.FileConn(f)
			require.NoError(t, e)
			defer conn.Close()
			pipe := devPgStderrPipe(t)
			raw := make([]byte, 16)
			copy(raw, "NDSGPS01")
			binary.BigEndian.PutUint32(raw[8:12], 1)
			var rights []int
			for i := 0; i < n; i++ {
				rights = append(rights, int(pipe[i%2].Fd()))
			}
			var oob []byte
			if n > 0 {
				oob = unix.UnixRights(rights...)
			}
			require.NoError(t, unix.Sendmsg(pair[0], raw, oob, nil, 0))
			files, cred, e := normalPgStderrReceive(conn.(*net.UnixConn), time.Now().Add(time.Second))
			if n != 2 {
				require.Error(t, e)
				require.Nil(t, files[0])
				require.Nil(t, files[1])
				return
			}
			require.NoError(t, e)
			defer files[0].Close()
			defer files[1].Close()
			require.NotNil(t, cred)
			require.Equal(t, int32(os.Getpid()), cred.Pid)
			_, _, e = normalPgStderrEndpoints(files)
			require.NoError(t, e)
		})
	}
}
func TestNormalPgStderrRetireNeedsRealExitAndEmptyEOF(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "queued"}[queued], func(t *testing.T) {
			pipe := devPgStderrPipe(t)
			child := exec.Command("/bin/sh", "-c", "sleep 30")
			require.NoError(t, child.Start())
			defer func() { child.Process.Kill(); child.Wait() }()
			pf, e := unix.PidfdOpen(child.Process.Pid, 0)
			require.NoError(t, e)
			defer unix.Close(pf)
			pin := &normalPgStderrPin{bound: true, pidfd: pf, read: pipe[0], write: pipe[1]}
			require.False(t, pin.retirable())
			if queued {
				_, e = pipe[1].Write([]byte("DEV unread original log"))
				require.NoError(t, e)
			}
			require.NoError(t, pipe[1].Close())
			pin.write = nil
			require.False(t, pin.retirable())
			require.NoError(t, child.Process.Kill())
			require.Error(t, child.Wait())
			require.Equal(t, !queued, pin.retirable())
			pin.unknown = true
			require.False(t, pin.retirable())
		})
	}
}
func TestNormalPgStderrNoSyntheticFinalSource(t *testing.T) {
	var pin *normalPgStderrPin
	require.NoError(t, pin.verifyNativeFinal(context.Background(), nil, nil, -1)) // legacy has no new registry
	for _, p := range []*normalPgStderrPin{{}, {unknown: true}, {bound: true, finalized: true}} {
		require.Error(t, p.verifyNativeFinal(context.Background(), nil, nil, -1))
		require.Error(t, p.releasedOriginalFinal())
	}
}

func TestNormalPgStderrStageWitnessRejectsMutationOrReplacement(t *testing.T) {
	for _, phase := range []string{"pending", "bound"} {
		for _, mutation := range []string{"delete", "bytes", "replace"} {
			t.Run(phase+"/"+mutation, func(t *testing.T) {
				directory := t.TempDir()
				require.NoError(t, os.Chmod(directory, 0700))
				dir, e := os.Open(directory)
				require.NoError(t, e)
				defer dir.Close()
				state := &lockedState{dir: dir}
				pin := &normalPgStderrPin{nonce: "DEV_COMPONENT_NOT_SOURCE"}
				for _, name := range []string{"pending", "bound"} {
					path := "pg-stderr-" + pin.nonce + "-" + name + ".json"
					require.NoError(t, os.WriteFile(filepath.Join(directory, path), []byte(`{"version":1}`), 0600))
					w, e := readNormalSystemRoleFileWitness(state, path)
					require.NoError(t, e)
					if name == "pending" {
						pin.pending = w
					} else {
						pin.binding = w
					}
				}
				require.NoError(t, pin.recheckOriginalStagesLocked(state))
				path := filepath.Join(directory, "pg-stderr-"+pin.nonce+"-"+phase+".json")
				switch mutation {
				case "delete":
					require.NoError(t, os.Remove(path))
				case "bytes":
					require.NoError(t, os.WriteFile(path, []byte(`{"version":2}`), 0600))
				case "replace":
					tmp := path + ".new"
					require.NoError(t, os.WriteFile(tmp, []byte(`{"version":1}`), 0600))
					require.NoError(t, os.Rename(tmp, path))
				}
				require.Error(t, pin.recheckOriginalStagesLocked(state))
			})
		}
	}
}
