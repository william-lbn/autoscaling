//go:build linux

package resourceguard

import (
	"encoding/binary"
	"github.com/stretchr/testify/require"
	"testing"
)

func ordinaryUdevV2TestRequest(t *testing.T, kind uint16, index uint32, properties map[string]string) []byte {
	t.Helper()
	raw, e := normalOrdinaryUdevV2CanonicalProperties(properties)
	require.NoError(t, e)
	b := make([]byte, 40+len(raw))
	copy(b, "NDSGUE01")
	binary.BigEndian.PutUint32(b[8:12], 1)
	binary.BigEndian.PutUint16(b[12:14], kind)
	binary.BigEndian.PutUint16(b[14:16], 1)
	binary.BigEndian.PutUint32(b[16:20], index)
	binary.BigEndian.PutUint64(b[20:28], 8)
	binary.BigEndian.PutUint32(b[28:32], uint32(len(properties)))
	binary.BigEndian.PutUint32(b[32:36], uint32(len(raw)))
	copy(b[40:], raw)
	return b
}
func ordinaryUdevV2TestProperties() map[string]string {
	return map[string]string{"ACTION": "add", "DEVPATH": "/devices/virtual/test0", "SUBSYSTEM": "test", "SEQNUM": "8"}
}
func TestOrdinaryUdevV2WireAndStandardHeader(t *testing.T) {
	p := ordinaryUdevV2TestProperties()
	b := ordinaryUdevV2TestRequest(t, 3, 0, p)
	q, e := decodeNormalOrdinaryUdevV2Request(b)
	require.NoError(t, e)
	require.Equal(t, p, q.Properties)
	require.EqualValues(t, 0xa8f0c311, normalOrdinaryUdevV2Hash("virtio-ports"))
	p["TAGS"] = ":systemd:uaccess:"
	packet, e := normalOrdinaryUdevV2PublicPacket(p)
	require.NoError(t, e)
	require.Equal(t, []byte("libudev\x00"), packet[:8])
	require.EqualValues(t, 0xfeedcafe, binary.BigEndian.Uint32(packet[8:12]))
	require.EqualValues(t, 40, binary.LittleEndian.Uint32(packet[16:20]))
	require.NotZero(t, binary.BigEndian.Uint64(packet[32:40]))
	require.EqualValues(t, len(packet)-40, binary.LittleEndian.Uint32(packet[20:24]))
}
func TestOrdinaryUdevV2WireRejectsAuthorityAndBoundaryMutations(t *testing.T) {
	base := ordinaryUdevV2TestRequest(t, 3, 0, ordinaryUdevV2TestProperties())
	for _, offset := range []int{0, 8, 12, 14, 16, 20, 28, 32, 36} {
		t.Run(string(rune('a'+offset)), func(t *testing.T) {
			b := append([]byte(nil), base...)
			b[offset] ^= 0x80
			_, e := decodeNormalOrdinaryUdevV2Request(b)
			require.Error(t, e)
		})
	}
	for _, raw := range []string{"A=1\x00A=2\x00", "B=1\x00A=2\x00", "A=x\n\x00", "A=1", "a=1\x00", "SOURCE=1\x00VERIFIED=true\x00"} {
		t.Run(raw, func(t *testing.T) {
			p, e := normalOrdinaryUdevV2Properties([]byte(raw), true)
			if raw == "SOURCE=1\x00VERIFIED=true\x00" {
				require.NoError(t, e)
				require.Contains(t, p, "SOURCE")
				return
			}
			require.Error(t, e)
		})
	}
}

func TestOrdinaryUdevV2TriggerUUIDUsesExactlyKernelUUIDWidth(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 64; i++ {
		uuid, e := normalOrdinaryUdevV2NewTriggerUUID()
		require.NoError(t, e)
		require.Len(t, uuid, 36)
		require.Equal(t, byte('-'), uuid[8])
		require.Equal(t, byte('-'), uuid[13])
		require.Equal(t, byte('-'), uuid[18])
		require.Equal(t, byte('-'), uuid[23])
		require.Equal(t, byte('4'), uuid[14])
		require.Equal(t, byte('a'), uuid[19])
		require.False(t, seen[uuid])
		seen[uuid] = true
	}
}
