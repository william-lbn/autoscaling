//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestKernelPrepV2HandoffReadAtSharesOFDWithoutOffsetRace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ordinary-temp")
	require.NoError(t, os.WriteFile(path, []byte("0\n"), 0600))
	f, e := os.Open(path)
	require.NoError(t, e)
	defer f.Close()
	fd, e := unix.FcntlInt(f.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	require.NoError(t, e)
	alias := os.NewFile(uintptr(fd), "own shared OFD")
	defer alias.Close()
	_, e = f.Seek(1, 0)
	require.NoError(t, e)
	v, e := normalKernelPrepV2ReadAtControl(alias)
	require.NoError(t, e)
	require.Zero(t, v)
	pos, e := f.Seek(0, 1)
	require.NoError(t, e)
	require.EqualValues(t, 1, pos)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var b [1]byte
		for i := 0; i < 200; i++ {
			f.Seek(0, 0)
			f.Read(b[:])
		}
	}()
	for i := 0; i < 200; i++ {
		_, e := normalKernelPrepV2ReadAtControl(alias)
		require.NoError(t, e)
	}
	wg.Wait()
	require.Error(t, normalKernelPrepV2TransferredControl(alias, normalKernelPrepV2Object{}), "ordinary temp is never accepted as original procfs")
}
func TestKernelPrepV2HandoffPreadRejectsNoncanonicalAndUnsupportedFD(t *testing.T) {
	for _, raw := range []string{"", "0", "00\n", "1\n", "0\nx"} {
		t.Run(raw, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "own")
			require.NoError(t, os.WriteFile(p, []byte(raw), 0600))
			f, e := os.Open(p)
			require.NoError(t, e)
			defer f.Close()
			_, e = normalKernelPrepV2ReadAtControl(f)
			require.ErrorIs(t, e, ErrUnknown)
		})
	}
	read, write, e := os.Pipe()
	require.NoError(t, e)
	defer read.Close()
	defer write.Close()
	_, e = normalKernelPrepV2ReadAtControl(read)
	require.ErrorIs(t, e, ErrUnknown, "ESPIPE cannot fall back to reopen")
}
func TestKernelPrepV2HandoffR2CannotAdoptJSONOrMissingProof(t *testing.T) {
	var source normalKernelPrepV2BrokerConsumerSource
	require.NoError(t, json.Unmarshal([]byte(`{}`), &source))
	_, e := source.referenceLocked(context.Background(), nil)
	require.ErrorIs(t, e, ErrUnknown)
	require.ErrorIs(t, source.requireContinuousQualification(), ErrUnknown)
	source.close()
	source.close()
	g := &Guard{}
	actual, e := g.bindNormalKernelPrepV2BrokerConsumerLocked(context.Background(), nil, nil, nil)
	require.Nil(t, actual)
	require.ErrorIs(t, e, ErrUnknown)
}

func TestKernelPrepV2HandoffR2PrivilegesDoNotRequireParentPID1(t *testing.T) {
	mask := uint64(1<<unix.CAP_KILL | 1<<unix.CAP_SYS_ADMIN | 1<<unix.CAP_SYS_PTRACE | 1<<unix.CAP_SYS_RESOURCE)
	status := func(parent int) string {
		return fmt.Sprintf("PPid:\t%d\nUid:\t0\t0\t0\t0\nGid:\t0\t0\t0\t0\nNoNewPrivs:\t0\nSeccomp:\t0\nCapEff:\t%016x\n", parent, mask)
	}
	var caps [2]unix.CapUserData
	caps[0].Effective = uint32(mask)
	for _, parent := range []int{87, 1} {
		t.Run(fmt.Sprint(parent), func(t *testing.T) { require.NoError(t, normalKernelPrepV2R2PrivilegeStatus(status(parent), caps)) })
	}
	for _, field := range []string{"Uid", "Gid"} {
		for i := 0; i < 4; i++ {
			t.Run(fmt.Sprintf("%s%d", field, i), func(t *testing.T) {
				values := []string{"0", "0", "0", "0"}
				values[i] = "1000"
				bad := strings.Replace(status(87), field+":\t0\t0\t0\t0", field+":\t"+strings.Join(values, "\t"), 1)
				require.Error(t, normalKernelPrepV2R2PrivilegeStatus(bad, caps))
			})
		}
	}
	for _, field := range []string{"NoNewPrivs", "Seccomp"} {
		t.Run(field, func(t *testing.T) {
			bad := strings.Replace(status(87), field+":\t0", field+":\t1", 1)
			require.Error(t, normalKernelPrepV2R2PrivilegeStatus(bad, caps))
		})
	}
	for _, cap := range []uint{unix.CAP_KILL, unix.CAP_SYS_ADMIN, unix.CAP_SYS_PTRACE} {
		t.Run(fmt.Sprint("missing-cap", cap), func(t *testing.T) {
			bad := strings.Replace(status(87), fmt.Sprintf("%016x", mask), fmt.Sprintf("%016x", mask&^(1<<cap)), 1)
			require.Error(t, normalKernelPrepV2R2PrivilegeStatus(bad, caps))
		})
	}
	caps[0].Effective &^= 1 << unix.CAP_SYS_RESOURCE
	require.Error(t, normalKernelPrepV2R2PrivilegeStatus(status(87), caps))
}
