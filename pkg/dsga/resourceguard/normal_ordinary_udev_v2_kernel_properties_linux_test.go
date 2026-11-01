//go:build linux

package resourceguard

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestOrdinaryUdevV2KernelCPUOriginalSingleLFIsNarrow(t *testing.T) {
	const raw = "ACTION=add\x00DEVPATH=/devices/system/cpu/cpu0\x00SUBSYSTEM=cpu\x00MODALIAS=cpu:type:x86,ven0000fam0006mod0006:feature:,0001\n\x00SEQNUM=22\x00"
	original := []byte(raw)
	saved := bytes.Clone(original)
	props, e := normalOrdinaryUdevV2KernelProperties(original)
	require.NoError(t, e)
	require.Equal(t, "cpu:type:x86,ven0000fam0006mod0006:feature:,0001", props["MODALIAS"])
	require.Equal(t, saved, original)
	_, e = normalOrdinaryUdevV2Properties(original, false)
	require.Error(t, e, "general wire remains strict")
	for _, tc := range []struct{ name, raw string }{
		{"other-subsystem", strings.Replace(raw, "SUBSYSTEM=cpu", "SUBSYSTEM=pci", 1)},
		{"other-path", strings.Replace(raw, "/devices/system/cpu/cpu0", "/devices/forged/cpu0", 1)},
		{"cpu00", strings.Replace(raw, "/cpu0", "/cpu00", 1)},
		{"cpu-child", strings.Replace(raw, "/cpu0", "/cpu0/child", 1)},
		{"cpu-overflow", strings.Replace(raw, "/cpu0", "/cpu4294967296", 1)},
		{"double-lf", strings.Replace(raw, "0001\n", "0001\n\n", 1)},
		{"interior-lf", strings.Replace(raw, "cpu:type:x86", "cpu:type:x\n86", 1)},
		{"unknown-lf", raw + "UNKNOWN=bad\n\x00"},
		{"unknown-cr", raw + "UNKNOWN=bad\r\x00"},
		{"duplicate-alias", raw + "MODALIAS=cpu:type:forged\n\x00"},
		{"duplicate-path", raw + "DEVPATH=/devices/system/cpu/cpu0\x00"},
		{"duplicate-subsystem", raw + "SUBSYSTEM=cpu\x00"},
		{"duplicate-other", raw + "SEQNUM=22\x00"},
		{"empty-field", raw + "\x00"},
		{"truncated", raw[:len(raw)-1]},
		{"overlong-value", strings.Replace(raw, "cpu:type:x86", "cpu:type:"+strings.Repeat("x", 1024), 1)},
		{"overlong-body", raw + "X=" + strings.Repeat("x", normalOrdinaryUdevV2MaximumBody) + "\x00"},
	} {
		t.Run(tc.name, func(t *testing.T) { _, e := normalOrdinaryUdevV2KernelProperties([]byte(tc.raw)); require.Error(t, e) })
	}
	// 原body界限在删除一个LF前执行，不能靠归一挤过原最大输入长度。
	_, e = normalOrdinaryUdevV2CPUPropertyBytes(original, len(original)-1)
	require.ErrorIs(t, e, ErrUnknown)
	packet := append([]byte("add@/devices/system/cpu/cpu0\x00"), original...)
	out, e := normalOrdinaryUdevV2DecodeKernelPacket(packet, unix.UnixCredentials(&unix.Ucred{}), 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Groups: 1})
	require.NoError(t, e)
	require.Equal(t, packet, out.raw)
	_, e = normalOrdinaryUdevV2DecodeKernelPacket(packet, unix.UnixCredentials(&unix.Ucred{Pid: 88}), 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Groups: 1})
	require.ErrorIs(t, e, ErrUnknown)
	_, e = normalOrdinaryUdevV2DecodeKernelPacket(packet, unix.UnixCredentials(&unix.Ucred{}), 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Groups: 2})
	require.ErrorIs(t, e, ErrUnknown)
}
