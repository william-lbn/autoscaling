//go:build linux

package resourceguard

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/util"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestNormalOrdinaryActionsV2FixedEndpointFrames(t *testing.T) {
	for _, tc := range []struct {
		role   string
		action uint32
		body   []byte
	}{
		{"acpi", 1, nil}, {"daemon", 2, []byte(`{"tls.crt":{"data":"YQ=="}}`)}, {"daemon", 4, nil}, {"udev", 3, []byte("bounded-device-body")},
	} {
		raw := normalOrdinaryActionFrame(tc.action, tc.body, false)
		action, body, e := decodeNormalOrdinaryActionFrame(raw, tc.role)
		require.NoError(t, e)
		require.Equal(t, tc.action, action)
		require.Equal(t, string(tc.body), string(body))
		for _, other := range []string{"acpi", "daemon", "udev", "compute", "resource", ""} {
			if other != tc.role {
				_, _, e := decodeNormalOrdinaryActionFrame(raw, other)
				require.Error(t, e)
			}
		}
		for _, bad := range [][]byte{raw[:15], append([]byte("NDSGOAOK"), raw[8:]...), normalOrdinaryActionFrame(tc.action, tc.body, true)} {
			_, _, e := decodeNormalOrdinaryActionFrame(bad, tc.role)
			require.Error(t, e)
		}
		bad := append([]byte(nil), raw...)
		binary.BigEndian.PutUint32(bad[8:12], 1)
		_, _, e = decodeNormalOrdinaryActionFrame(bad, tc.role)
		require.Error(t, e)
	}
	for _, tc := range []struct {
		role   string
		action uint32
		body   []byte
	}{
		{"acpi", 1, []byte("caller-pid")}, {"daemon", 2, nil}, {"daemon", 4, []byte("caller-path")}, {"daemon", 99, nil}, {"udev", 3, make([]byte, 8193)}, {"daemon", 2, make([]byte, (64<<10)+1)},
	} {
		_, _, e := decodeNormalOrdinaryActionFrame(normalOrdinaryActionFrame(tc.action, tc.body, false), tc.role)
		require.Error(t, e)
	}
}

func TestNormalOrdinaryActionsV2KernelCredentialsCloseReceivedRights(t *testing.T) {
	fds, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, e)
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])
	require.NoError(t, unix.SetsockoptInt(fds[1], unix.SOL_SOCKET, unix.SO_PASSCRED, 1))
	receive := func(oob []byte) ([]byte, int) {
		t.Helper()
		require.NoError(t, unix.Sendmsg(fds[0], []byte("x"), oob, nil, 0))
		data := make([]byte, 2)
		control := make([]byte, 256)
		n, on, flags, _, e := unix.Recvmsg(fds[1], data, control, unix.MSG_CMSG_CLOEXEC)
		require.NoError(t, e)
		require.Equal(t, 1, n)
		return control[:on], flags
	}
	oob, flags := receive(nil)
	cred, e := normalOrdinaryPacketCredentials(oob, flags)
	require.NoError(t, e)
	require.Equal(t, int32(os.Getpid()), cred.Pid)
	for _, bad := range []int{unix.MSG_CTRUNC, unix.MSG_TRUNC} {
		_, e := normalOrdinaryPacketCredentials(oob, bad)
		require.Error(t, e)
	}
	_, e = normalOrdinaryPacketCredentials(nil, 0)
	require.Error(t, e)
	_, e = normalOrdinaryPacketCredentials(append(append([]byte(nil), oob...), oob...), 0)
	require.Error(t, e)
	pipe := make([]int, 2)
	require.NoError(t, unix.Pipe2(pipe, unix.O_CLOEXEC))
	defer unix.Close(pipe[0])
	defer unix.Close(pipe[1])
	oob, flags = receive(unix.UnixRights(pipe[0]))
	msgs, e := unix.ParseSocketControlMessage(oob)
	require.NoError(t, e)
	var received []int
	for _, m := range msgs {
		if m.Header.Level == unix.SOL_SOCKET && m.Header.Type == unix.SCM_RIGHTS {
			got, e := unix.ParseUnixRights(&m)
			require.NoError(t, e)
			received = append(received, got...)
		}
	}
	require.Len(t, received, 1)
	require.NotEqual(t, pipe[0], received[0])
	_, e = normalOrdinaryPacketCredentials(oob, flags)
	require.Error(t, e)
	_, e = unix.FcntlInt(uintptr(received[0]), unix.F_GETFD, 0)
	require.ErrorIs(t, e, unix.EBADF)
	_, e = unix.FcntlInt(uintptr(pipe[0]), unix.F_GETFD, 0)
	require.NoError(t, e, "only delivered rights are closed")
}

func TestNormalOrdinaryActionsV2TLSAtomicRotationUsesOriginalChecksum(t *testing.T) {
	dir := t.TempDir()
	makePayload := func(entries map[string]string) []byte {
		t.Helper()
		files := map[string]normalOrdinaryTLSFile{}
		for k, v := range entries {
			files[k] = normalOrdinaryTLSFile{Data: base64.StdEncoding.EncodeToString([]byte(v))}
		}
		raw, e := json.Marshal(files)
		require.NoError(t, e)
		return raw
	}
	first, e := decodeNormalOrdinaryTLSFiles(makePayload(map[string]string{"tls.crt": "TEST_ONLY cert1", "tls.key": "TEST_ONLY key1", "ca.crt": "TEST_ONLY ca1"}))
	require.NoError(t, e)
	require.NoError(t, normalOrdinaryTLSAtomicWrite(dir, first))
	firstGeneration, e := os.Readlink(filepath.Join(dir, "..data"))
	require.NoError(t, e)
	firstSum, e := normalOrdinaryTLSChecksum(dir)
	require.NoError(t, e)
	legacy, e := util.ChecksumFlatDir(filepath.Join(dir, "..data"))
	require.NoError(t, e)
	require.Equal(t, legacy, firstSum)
	second, e := decodeNormalOrdinaryTLSFiles(makePayload(map[string]string{"tls.crt": "TEST_ONLY cert2"}))
	require.NoError(t, e)
	require.NoError(t, normalOrdinaryTLSAtomicWrite(dir, second))
	secondGeneration, e := os.Readlink(filepath.Join(dir, "..data"))
	require.NoError(t, e)
	require.NotEqual(t, firstGeneration, secondGeneration)
	current, e := os.ReadFile(filepath.Join(dir, "tls.crt"))
	require.NoError(t, e)
	require.Equal(t, "TEST_ONLY cert2", string(current))
	_, e = os.Lstat(filepath.Join(dir, "tls.key"))
	require.True(t, os.IsNotExist(e))
	_, e = os.Lstat(filepath.Join(dir, firstGeneration))
	require.True(t, os.IsNotExist(e))
	sum, e := normalOrdinaryTLSChecksum(dir)
	require.NoError(t, e)
	require.NotEqual(t, firstSum, sum)
	legacy, e = util.ChecksumFlatDir(filepath.Join(dir, "..data"))
	require.NoError(t, e)
	require.Equal(t, legacy, sum)
	require.NoError(t, normalOrdinaryTLSAtomicWrite(dir, second))
	sum2, e := normalOrdinaryTLSChecksum(dir)
	require.NoError(t, e)
	require.Equal(t, sum, sum2)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "..data", "foreign"), []byte("TEST_ONLY"), 0600))
	_, e = normalOrdinaryTLSChecksum(dir)
	require.Error(t, e)
}

func TestNormalOrdinaryActionsV2TLSRejectsAliasesAndCallerPaths(t *testing.T) {
	for name, raw := range map[string][]byte{
		"empty": []byte(`{}`), "null": []byte(`null`), "path": []byte(`{"../../config":{"data":"YQ=="}}`),
		"field": []byte(`{"tls.crt":{"Data":"YQ=="}}`), "unknown": []byte(`{"tls.crt":{"data":"YQ==","mode":511}}`),
		"duplicate": []byte(`{"tls.crt":{"data":"YQ=="},"tls.crt":{"data":"Yg=="}}`),
		"encoding":  []byte(`{"tls.key":{"data":"!!"}}`), "size": []byte(strings.Repeat("x", (64<<10)+1)),
	} {
		t.Run(name, func(t *testing.T) { _, e := decodeNormalOrdinaryTLSFiles(raw); require.Error(t, e) })
	}
}

func TestNormalOrdinaryActionsV2ReceiveCreatesCloexecRights(t *testing.T) {
	fds, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, e)
	sender := os.NewFile(uintptr(fds[0]), "TEST_ONLY original sender")
	defer sender.Close()
	receiver := os.NewFile(uintptr(fds[1]), "TEST_ONLY original receiver")
	raw, e := net.FileConn(receiver)
	receiver.Close()
	require.NoError(t, e)
	defer raw.Close()
	conn, ok := raw.(*net.UnixConn)
	require.True(t, ok)
	require.NoError(t, normalOrdinaryEnablePasscred(conn))
	pipe := make([]int, 2)
	require.NoError(t, unix.Pipe2(pipe, unix.O_CLOEXEC))
	defer unix.Close(pipe[0])
	defer unix.Close(pipe[1])
	require.NoError(t, unix.Sendmsg(int(sender.Fd()), []byte("x"), unix.UnixRights(pipe[0]), nil, 0))
	data := make([]byte, 2)
	oob := make([]byte, 256)
	n, on, flags, e := normalOrdinaryReadPacket(conn, data, oob)
	require.NoError(t, e)
	require.Equal(t, 1, n)
	messages, e := unix.ParseSocketControlMessage(oob[:on])
	require.NoError(t, e)
	var received []int
	for _, m := range messages {
		if m.Header.Level == unix.SOL_SOCKET && m.Header.Type == unix.SCM_RIGHTS {
			fds, e := unix.ParseUnixRights(&m)
			require.NoError(t, e)
			received = append(received, fds...)
		}
	}
	require.Len(t, received, 1)
	fdflags, e := unix.FcntlInt(uintptr(received[0]), unix.F_GETFD, 0)
	require.NoError(t, e)
	require.NotZero(t, fdflags&unix.FD_CLOEXEC)
	_, e = normalOrdinaryPacketCredentials(oob[:on], flags)
	require.Error(t, e)
	_, e = unix.FcntlInt(uintptr(received[0]), unix.F_GETFD, 0)
	require.ErrorIs(t, e, unix.EBADF)
}
