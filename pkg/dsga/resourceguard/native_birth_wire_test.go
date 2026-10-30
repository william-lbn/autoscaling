package resourceguard

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func nativeBirthWireFixture() (nativeBirthExpected, []byte) {
	e := nativeBirthExpected{AddressSpaceBytes: 128 << 20, MaxBuffers: 64, PageBytes: 4096, ElementBytes: [4]uint64{64, 8192, 16, 24}}
	e.Nonce[0] = 1
	e.SourceSHA[0] = 2
	e.ProfileSHA[0] = 3
	raw := make([]byte, nativeBirthRequestBytes)
	ack, _ := encodeNativeBirthACK(e)
	copy(raw, ack[:])
	binary.BigEndian.PutUint32(raw[12:16], 1)
	lengths, total, _ := e.geometry()
	binary.BigEndian.PutUint64(raw[112:120], e.AddressSpaceBytes)
	binary.BigEndian.PutUint32(raw[120:124], e.MaxBuffers)
	binary.BigEndian.PutUint32(raw[124:128], e.PageBytes)
	binary.BigEndian.PutUint64(raw[128:136], total)
	base := uint64(1 << 30)
	for i := range lengths {
		offset := 136 + i*24
		binary.BigEndian.PutUint64(raw[offset:offset+8], base)
		binary.BigEndian.PutUint64(raw[offset+8:offset+16], lengths[i])
		binary.BigEndian.PutUint64(raw[offset+16:offset+24], e.ElementBytes[i])
		base += lengths[i]
	}
	return e, raw
}

func TestNativeBirthWireExactOriginalContextAndRanges(t *testing.T) {
	e, raw := nativeBirthWireFixture()
	request, err := decodeNativeBirthRequest(raw, e)
	require.NoError(t, err)
	require.Equal(t, e, request.Original)
	_, total, err := e.geometry()
	require.NoError(t, err)
	require.Equal(t, total, request.TotalReservedBytes)
	require.Equal(t, uint64(1<<30), request.Ranges[0].Base)
	ack, err := encodeNativeBirthACK(e)
	require.NoError(t, err)
	require.NoError(t, nativeBirthHeader(ack[:], 2, e))
	require.Equal(t, raw[16:112], ack[16:112])
	_, err = decodeNativeBirthRequest(ack[:], e)
	require.Error(t, err)
}

func TestNativeBirthWireRejectsForgedOrAmbiguousFrames(t *testing.T) {
	e, valid := nativeBirthWireFixture()
	for _, tc := range []struct {
		name   string
		change func([]byte) []byte
	}{
		{"short", func(b []byte) []byte { return b[:len(b)-1] }},
		{"tail", func(b []byte) []byte { return append(b, 0) }},
		{"magic", func(b []byte) []byte { b[0] ^= 1; return b }},
		{"version", func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:12], 2); return b }},
		{"ack-as-request", func(b []byte) []byte { binary.BigEndian.PutUint32(b[12:16], 2); return b }},
		{"nonce", func(b []byte) []byte { b[16] ^= 1; return b }},
		{"source", func(b []byte) []byte { b[48] ^= 1; return b }},
		{"profile", func(b []byte) []byte { b[80] ^= 1; return b }},
		{"AS", func(b []byte) []byte { binary.BigEndian.PutUint64(b[112:120], e.AddressSpaceBytes+65536); return b }},
		{"maximum", func(b []byte) []byte { binary.BigEndian.PutUint32(b[120:124], 65); return b }},
		{"page", func(b []byte) []byte { binary.BigEndian.PutUint32(b[124:128], 8192); return b }},
		{"total", func(b []byte) []byte { binary.BigEndian.PutUint64(b[128:136], 0); return b }},
		{"null-base", func(b []byte) []byte { binary.BigEndian.PutUint64(b[136:144], 0); return b }},
		{"unaligned-base", func(b []byte) []byte { binary.BigEndian.PutUint64(b[136:144], 4097); return b }},
		{"high-base", func(b []byte) []byte { binary.BigEndian.PutUint64(b[136:144], 1<<47); return b }},
		{"overflow-base", func(b []byte) []byte { binary.BigEndian.PutUint64(b[136:144], math.MaxUint64-4095); return b }},
		{"length", func(b []byte) []byte { binary.BigEndian.PutUint64(b[144:152], 8192); return b }},
		{"element", func(b []byte) []byte { binary.BigEndian.PutUint64(b[152:160], 128); return b }},
		{"overlap", func(b []byte) []byte { copy(b[160:168], b[136:144]); return b }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeNativeBirthRequest(tc.change(append([]byte(nil), valid...)), e)
			require.Error(t, err)
		})
	}
}

func TestNativeBirthWireRequiresIndependentOriginalGeometry(t *testing.T) {
	valid, raw := nativeBirthWireFixture()
	for _, tc := range []struct {
		name   string
		change func(*nativeBirthExpected)
	}{
		{"nonce", func(e *nativeBirthExpected) { e.Nonce = [32]byte{} }},
		{"source", func(e *nativeBirthExpected) { e.SourceSHA = [32]byte{} }},
		{"profile", func(e *nativeBirthExpected) { e.ProfileSHA = [32]byte{} }},
		{"AS-zero", func(e *nativeBirthExpected) { e.AddressSpaceBytes = 0 }},
		{"AS-alignment", func(e *nativeBirthExpected) { e.AddressSpaceBytes++ }},
		{"AS-overflow", func(e *nativeBirthExpected) { e.AddressSpaceBytes = math.MaxUint64 }},
		{"R-equals-AS", func(e *nativeBirthExpected) {
			e.MaxBuffers = 64
			e.ElementBytes = [4]uint64{524288, 524288, 524288, 524288}
		}},
		{"buffers-zero", func(e *nativeBirthExpected) { e.MaxBuffers = 0 }},
		{"buffers-overflow", func(e *nativeBirthExpected) { e.MaxBuffers = math.MaxUint32 }},
		{"page", func(e *nativeBirthExpected) { e.PageBytes = 8192 }},
		{"ABI-zero", func(e *nativeBirthExpected) { e.ElementBytes[0] = 0 }},
		{"ABI-overflow", func(e *nativeBirthExpected) { e.ElementBytes[0] = math.MaxUint64 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := valid
			tc.change(&e)
			_, err := decodeNativeBirthRequest(raw, e)
			require.Error(t, err)
			_, err = encodeNativeBirthACK(e)
			require.Error(t, err)
		})
	}
}
