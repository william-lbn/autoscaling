package resourceguard

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"
)

func nativeAuditFixture(t *testing.T) (nativeBirthRequest, []byte) {
	t.Helper()
	e := nativeBirthExpected{AddressSpaceBytes: 64 << 20, MaxBuffers: 8, PageBytes: 4096, ElementBytes: [4]uint64{64, 8192, 16, 20}}
	e.Nonce[0] = 1
	e.SourceSHA[0] = 2
	e.ProfileSHA[0] = 3
	lengths, total, err := e.geometry()
	if err != nil {
		t.Fatal(err)
	}
	r := nativeBirthRequest{Original: e, TotalReservedBytes: total}
	for i := range r.Ranges {
		r.Ranges[i] = nativeBirthRange{uint64(i+1) << 20, lengths[i], e.ElementBytes[i]}
	}
	b := make([]byte, nativeAuditHeaderBytes)
	copy(b, "NDSGFP01")
	binary.BigEndian.PutUint32(b[8:12], 1)
	binary.BigEndian.PutUint32(b[12:16], 0xc000003e)
	binary.BigEndian.PutUint32(b[16:20], 3)
	binary.BigEndian.PutUint64(b[24:32], e.AddressSpaceBytes)
	binary.BigEndian.PutUint32(b[32:36], e.MaxBuffers)
	binary.BigEndian.PutUint32(b[36:40], e.PageBytes)
	binary.BigEndian.PutUint64(b[40:48], total)
	for i, x := range r.Ranges {
		o := 48 + i*24
		binary.BigEndian.PutUint64(b[o:o+8], x.Base)
		binary.BigEndian.PutUint64(b[o+8:o+16], x.ReservedBytes)
		binary.BigEndian.PutUint64(b[o+16:o+24], x.ElementBytes)
	}
	// 明确模型expected，不声称这些是生产native已安装的合法BPF；长度/指令均不同，
	// 可以捕获把新→旧顺序错误接受，或只比较数量/末段/散列的错误。
	for kind := uint32(1); kind <= 3; kind++ {
		prefix := make([]byte, 8)
		binary.BigEndian.PutUint32(prefix[:4], kind)
		binary.BigEndian.PutUint32(prefix[4:], kind)
		b = append(b, prefix...)
		for j := uint32(0); j < kind; j++ {
			ins := make([]byte, 8)
			binary.BigEndian.PutUint16(ins[:2], uint16(6+kind))
			ins[2] = uint8(j)
			ins[3] = uint8(kind)
			binary.BigEndian.PutUint32(ins[4:], 0x7fff0000+kind+j)
			b = append(b, ins...)
		}
	}
	return r, b
}

func TestNativeFilterAuditDecodesOriginalGeometryAndOwnsBytes(t *testing.T) {
	r, raw := nativeAuditFixture(t)
	wantSHA := sha256.Sum256(raw)
	b, err := decodeNativeFilterAudit(raw, r)
	if err != nil {
		t.Fatal(err)
	}
	if b.Request != r || b.EncodedSHA256 != wantSHA {
		t.Fatal("original request/hash lost")
	}
	for i, program := range b.Programs {
		if len(program) != i+1 {
			t.Fatal("program count changed")
		}
		for j, x := range program {
			if x.Code != uint16(7+i) || x.JT != uint8(j) || x.JF != uint8(i+1) || x.K != 0x7fff0000+uint32(i+1+j) {
				t.Fatal("instruction field/endian changed")
			}
		}
	}
	before := b.Programs[2][2]
	for i := range raw {
		raw[i] = 0
	}
	if b.Programs[2][2] != before || b.EncodedSHA256 != wantSHA {
		t.Fatal("borrowed mutable caller bytes")
	}
}

func TestNativeFilterAuditRejectsMalformedABIAndOriginalDrift(t *testing.T) {
	r, raw := nativeAuditFixture(t)
	cases := []struct {
		name   string
		change func([]byte) []byte
	}{
		{"short-header", func(b []byte) []byte { return b[:143] }},
		{"magic", func(b []byte) []byte { b[0] ^= 1; return b }},
		{"version", func(b []byte) []byte { b[11] = 2; return b }},
		{"arch", func(b []byte) []byte { b[15] ^= 1; return b }},
		{"program-number", func(b []byte) []byte { b[19] = 2; return b }},
		{"reserved-byte", func(b []byte) []byte { b[23] = 1; return b }},
		{"address-space", func(b []byte) []byte { b[31] ^= 1; return b }},
		{"maximum", func(b []byte) []byte { b[35] ^= 1; return b }},
		{"page", func(b []byte) []byte { b[39] ^= 1; return b }},
		{"total-R", func(b []byte) []byte { b[47] ^= 1; return b }},
		{"base", func(b []byte) []byte { b[55] ^= 1; return b }},
		{"span", func(b []byte) []byte { b[63] ^= 1; return b }},
		{"compiled-sizeof", func(b []byte) []byte { b[71] ^= 1; return b }},
		{"kind-order", func(b []byte) []byte { b[147] = 3; return b }},
		{"zero-count", func(b []byte) []byte { binary.BigEndian.PutUint32(b[148:152], 0); return b }},
		{"oversized-count", func(b []byte) []byte { binary.BigEndian.PutUint32(b[148:152], 1025); return b }},
		{"wrapping-count", func(b []byte) []byte { binary.BigEndian.PutUint32(b[148:152], 0xffffffff); return b }},
		{"truncated-program", func(b []byte) []byte { return b[:len(b)-1] }},
		{"extra-byte", func(b []byte) []byte { return append(b, 0) }},
		{"maximum-buffer", func(b []byte) []byte { return append(b, make([]byte, nativeAuditMaxBytes)...) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := c.change(append([]byte(nil), raw...))
			if _, err := decodeNativeFilterAudit(b, r); err == nil {
				t.Fatal("accepted malformed audit")
			}
		})
	}
	drift := []struct {
		name   string
		change func(*nativeBirthRequest)
	}{
		{"missing-original-nonce", func(r *nativeBirthRequest) { r.Original.Nonce = [32]byte{} }},
		{"missing-original-source", func(r *nativeBirthRequest) { r.Original.SourceSHA = [32]byte{} }},
		{"missing-original-profile", func(r *nativeBirthRequest) { r.Original.ProfileSHA = [32]byte{} }},
		{"typed-range-overlap", func(r *nativeBirthRequest) { r.Ranges[1].Base = r.Ranges[0].Base }},
		{"typed-reservation", func(r *nativeBirthRequest) { r.TotalReservedBytes++ }},
		{"typed-element", func(r *nativeBirthRequest) { r.Ranges[3].ElementBytes++ }},
		{"typed-high-address", func(r *nativeBirthRequest) { r.Ranges[0].Base = 1 << 47 }},
	}
	for _, c := range drift {
		t.Run(c.name, func(t *testing.T) {
			v := r
			c.change(&v)
			if _, err := decodeNativeFilterAudit(raw, v); err == nil {
				t.Fatal("accepted drifted typed source")
			}
		})
	}
}

func TestNativeFilterAuditComparesCompleteOldestFirstKernelChain(t *testing.T) {
	r, raw := nativeAuditFixture(t)
	b, err := decodeNativeFilterAudit(raw, r)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []nativeFilterPhase{nativeFilterPMBase, nativeFilterPMFinal, nativeFilterChild} {
		t.Run(fmt.Sprint(phase), func(t *testing.T) {
			var calls []uint32
			reader := func(offset uint32, dst []nativeFilterInstruction) (int, error) {
				calls = append(calls, offset)
				if offset >= uint32(phase) {
					return 0, io.EOF
				}
				p := b.Programs[offset]
				if dst != nil {
					copy(dst, p)
				}
				return len(p), nil
			}
			if err := compareNativeInstalledFilters(b, phase, reader, func(err error) bool { return errors.Is(err, io.EOF) }); err != nil {
				t.Fatal(err)
			}
			if len(calls) != int(phase)*2+1 {
				t.Fatal("missing length/data or final terminal query")
			}
			for i := 0; i < int(phase); i++ {
				if calls[2*i] != uint32(i) || calls[2*i+1] != uint32(i) {
					t.Fatal("kernel index not oldest first")
				}
			}
			if calls[len(calls)-1] != uint32(phase) {
				t.Fatal("extra inherited segment was not checked")
			}
		})
	}
}

func TestNativeFilterAuditRejectsUnknownKernelOrPartialComparison(t *testing.T) {
	r, raw := nativeAuditFixture(t)
	b, err := decodeNativeFilterAudit(raw, r)
	if err != nil {
		t.Fatal(err)
	}
	unknown := errors.New("unknown kernel capability or non-cBPF")
	names := []string{"missing-base", "changed-count", "changed-data-count", "code", "jt", "jf", "k", "reversed", "extra-inherited", "unknown-terminal", "empty-terminal-success"}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			reader := func(offset uint32, dst []nativeFilterInstruction) (int, error) {
				if offset == 3 {
					if name == "extra-inherited" {
						return 1, nil
					}
					if name == "unknown-terminal" {
						return 0, unknown
					}
					if name == "empty-terminal-success" {
						return 0, nil
					}
					return 0, io.EOF
				}
				if offset == 0 && name == "missing-base" {
					return 0, io.EOF
				}
				program := b.Programs[offset]
				if name == "reversed" {
					program = b.Programs[2-offset]
				}
				if dst == nil {
					if offset == 1 && name == "changed-count" {
						return len(program) + 1, nil
					}
					return len(program), nil
				}
				copy(dst, program)
				if offset == 1 {
					switch name {
					case "changed-data-count":
						return len(dst) - 1, nil
					case "code":
						dst[0].Code ^= 1
					case "jt":
						dst[0].JT ^= 1
					case "jf":
						dst[0].JF ^= 1
					case "k":
						dst[0].K ^= 1
					}
				}
				return len(program), nil
			}
			if err := compareNativeInstalledFilters(b, nativeFilterChild, reader, func(err error) bool { return errors.Is(err, io.EOF) }); err == nil {
				t.Fatal("accepted incomplete or altered actual kernel chain")
			}
		})
	}
	for _, phase := range []nativeFilterPhase{0, 4, 255} {
		if err := compareNativeInstalledFilters(b, phase, func(uint32, []nativeFilterInstruction) (int, error) {
			t.Fatal("invalid phase reached kernel")
			return 0, nil
		}, func(error) bool { return true }); err == nil {
			t.Fatal("accepted unsupported phase")
		}
	}
	if err := compareNativeInstalledFilters(b, nativeFilterPMBase, nil, func(error) bool { return false }); err == nil {
		t.Fatal("accepted missing reader")
	}
	broken := b
	broken.Programs[1] = nil
	if err := compareNativeInstalledFilters(broken, nativeFilterPMBase, func(uint32, []nativeFilterInstruction) (int, error) {
		t.Fatal("unbound expected bundle reached kernel")
		return 0, nil
	}, func(error) bool { return true }); err == nil {
		t.Fatal("accepted incomplete expected bundle")
	}
}
