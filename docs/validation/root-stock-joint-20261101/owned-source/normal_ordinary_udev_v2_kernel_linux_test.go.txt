//go:build linux

package resourceguard

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"testing"
)

func TestOrdinaryUdevV2KernelOnlyPIDZeroCredAndGroupOne(t *testing.T) {
	raw := []byte("add@/devices/test0\x00ACTION=add\x00DEVPATH=/devices/test0\x00SEQNUM=8\x00SUBSYSTEM=test\x00SYNTH_UUID=0\x00")
	cred := unix.UnixCredentials(&unix.Ucred{})
	from := &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Groups: 1}
	p, e := normalOrdinaryUdevV2DecodeKernelPacket(raw, cred, 0, from)
	require.NoError(t, e)
	require.EqualValues(t, 8, p.sequence)
	for _, bad := range []struct {
		name  string
		cred  []byte
		flags int
		from  unix.Sockaddr
	}{{"missing", nil, 0, from}, {"user", unix.UnixCredentials(&unix.Ucred{Pid: 87, Uid: 1000}), 0, from}, {"duplicate", append(append([]byte(nil), cred...), cred...), 0, from}, {"trunc", cred, unix.MSG_TRUNC, from}, {"ctrunc", cred, unix.MSG_CTRUNC, from}, {"unicast", cred, 0, &unix.SockaddrNetlink{Groups: 0}}, {"sender", cred, 0, &unix.SockaddrNetlink{Pid: 87, Groups: 1}}} {
		t.Run(bad.name, func(t *testing.T) {
			_, e := normalOrdinaryUdevV2DecodeKernelPacket(raw, bad.cred, bad.flags, bad.from)
			require.ErrorIs(t, e, ErrUnknown)
		})
	}
}
