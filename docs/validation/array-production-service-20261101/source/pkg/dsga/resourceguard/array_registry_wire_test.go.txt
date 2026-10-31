package resourceguard

import (
	"encoding/binary"
	"math"
	"testing"
)

func arrayRegistryWireFixture() arrayRegistryWireRequest {
	r := arrayRegistryWireRequest{Op: arrayRegistryRegister, PGBoot: 123, Generation: 1, MaximumBuffers: 8192, StartupBuffers: 1024, OriginalHigh: 1024, MappingGeneration: 1}
	r.SourceSHA[0] = 1
	r.BirthNonce[0] = 2
	r.ProfileSHA[0] = 3
	r.MappingNonce[0] = 4
	for i, e := range []uint64{64, 8192, 16, 20} {
		n := r.MaximumBuffers * e
		r.Ranges[i] = arrayRegistryWireRange{Base: 0x7f1000000000 + uint64(i)*(1<<30), Maximum: n, Mapped: (n + 4095) &^ uint64(4095), Element: e, Page: 4096}
	}
	return r
}

// PG17-U-ARRAY-REGISTRY-001：独立512B codec，各阶段/缩到N0以下后regrow保留原布局。
func TestArrayRegistryWireRoundTrip(t *testing.T) {
	for _, mode := range []string{"register", "startup-populate", "grow-after-shrink-below-N0", "retire-begin"} {
		t.Run(mode, func(t *testing.T) {
			r := arrayRegistryWireFixture()
			if mode != "register" {
				r.RegistrationNonce[0] = 9
			}
			switch mode {
			case "startup-populate":
				r.Op = arrayRegistryPopulate
				r.Array = 1
				r.Bytes = r.StartupBuffers * r.Ranges[1].Element
			case "grow-after-shrink-below-N0":
				r.Op = arrayRegistryPopulate
				r.Array = 1
				r.OriginalHigh = 512
				r.TargetBuffers = 4096
				r.Offset = 512 * 8192
				r.Bytes = 2048 * 8192
				r.IssuerEpoch = 3
				r.Revision = 7
				r.OperationLow = 5
				r.ContextSHA[0] = 8
			case "retire-begin":
				r.Op = arrayRegistryRetireBegin
				r.OriginalHigh = 512
			}
			raw, e := encodeArrayRegistryWire(r)
			if e != nil {
				t.Fatal(e)
			}
			if len(raw) != 512 || string(raw[:8]) != "NDSGARR1" || binary.BigEndian.Uint64(raw[16:24]) != 123 {
				t.Fatal("native ABI scalar/endian differs")
			}
			got, e := decodeArrayRegistryWire(raw)
			if e != nil || got != r {
				t.Fatal("exact roundtrip differs", e)
			}
		})
	}
}

// PG17-U-ARRAY-REGISTRY-002：旧nonce/半Key/几何溢出/错阶段等不能被截断成合法packet。
func TestArrayRegistryWireRejectsMalformed(t *testing.T) {
	for name, mutate := range map[string]func(*arrayRegistryWireRequest){
		"version-op": func(r *arrayRegistryWireRequest) { r.Op = 99 }, "zero-boot": func(r *arrayRegistryWireRequest) { r.PGBoot = 0 }, "zero-gen": func(r *arrayRegistryWireRequest) { r.Generation = 0 }, "zero-mapgen": func(r *arrayRegistryWireRequest) { r.MappingGeneration = 0 }, "M-over-int": func(r *arrayRegistryWireRequest) { r.MaximumBuffers = math.MaxInt32 + 1 }, "source-missing": func(r *arrayRegistryWireRequest) { r.SourceSHA = [32]byte{} }, "profile-missing": func(r *arrayRegistryWireRequest) { r.ProfileSHA = [32]byte{} }, "birth-missing": func(r *arrayRegistryWireRequest) { r.BirthNonce = [32]byte{} }, "mapping-missing": func(r *arrayRegistryWireRequest) { r.MappingNonce = [32]byte{} }, "startup-over-M": func(r *arrayRegistryWireRequest) { r.StartupBuffers = r.MaximumBuffers + 1 }, "base-wrap": func(r *arrayRegistryWireRequest) { r.Ranges[0].Base = math.MaxUint64 - 4095 }, "length-wrap": func(r *arrayRegistryWireRequest) { r.Ranges[0].Element = math.MaxUint64 }, "wrong-page": func(r *arrayRegistryWireRequest) { r.Ranges[0].Page = 8192 }, "wrong-max": func(r *arrayRegistryWireRequest) { r.Ranges[0].Maximum++ }, "wrong-mapped": func(r *arrayRegistryWireRequest) { r.Ranges[0].Mapped++ }, "overlap": func(r *arrayRegistryWireRequest) { r.Ranges[1].Base = r.Ranges[0].Base }, "register-has-ticket": func(r *arrayRegistryWireRequest) { r.RegistrationNonce[0] = 1 }, "register-has-key": func(r *arrayRegistryWireRequest) { r.Revision = 1 }, "startup-shifted": func(r *arrayRegistryWireRequest) {
			r.Op = arrayRegistryPopulate
			r.RegistrationNonce[0] = 1
			r.Array = 1
			r.Offset = 4096
			r.Bytes = 4096
		}, "runtime-half-key": func(r *arrayRegistryWireRequest) {
			r.Op = arrayRegistryPopulate
			r.RegistrationNonce[0] = 1
			r.IssuerEpoch = 1
			r.Array = 1
			r.Bytes = 4096
			r.TargetBuffers = 2048
		}, "retire-has-cost": func(r *arrayRegistryWireRequest) {
			r.Op = arrayRegistryRetireBegin
			r.RegistrationNonce[0] = 1
			r.Bytes = 4096
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := arrayRegistryWireFixture()
			mutate(&r)
			if _, e := encodeArrayRegistryWire(r); e == nil {
				t.Fatal("malformed wire accepted")
			}
		})
	}
	r := arrayRegistryWireFixture()
	raw, e := encodeArrayRegistryWire(r)
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"short", "long", "magic", "version", "reserved"} {
		t.Run(mode, func(t *testing.T) {
			b := append([]byte(nil), raw...)
			switch mode {
			case "short":
				b = b[:511]
			case "long":
				b = append(b, 0)
			case "magic":
				b[0] = 'X'
			case "version":
				b[11] = 2
			case "reserved":
				b[511] = 1
			}
			if _, e := decodeArrayRegistryWire(b); e == nil {
				t.Fatal("wire framing borrowed")
			}
		})
	}
}

// PG17-U-ARRAY-REGISTRY-003：成功编码必须绑定原request、Root票据/资源floor，不是授权生产者。
func TestArrayRegistryWireReplyBindings(t *testing.T) {
	raw, _ := encodeArrayRegistryWire(arrayRegistryWireFixture())
	r := arrayRegistryWireReply{RequestSHA: arrayRegistryRequestSHA(raw), ResourceFloor: 1, MappingGeneration: 1}
	r.RegistrationNonce[0] = 1
	b, e := r.encode()
	if e != nil || len(b) != 160 || string(b[:8]) != "NDSGARA1" || binary.BigEndian.Uint64(b[104:112]) != 1 {
		t.Fatal(e)
	}
	for _, mode := range []string{"unknown-status", "no-request-sha", "no-ticket", "no-floor", "no-generation", "success-errno", "denial-no-errno", "denial-overflow"} {
		t.Run(mode, func(t *testing.T) {
			x := r
			switch mode {
			case "unknown-status":
				x.Status = 3
			case "no-request-sha":
				x.RequestSHA = [32]byte{}
			case "no-ticket":
				x.RegistrationNonce = [32]byte{}
			case "no-floor":
				x.ResourceFloor = 0
			case "no-generation":
				x.MappingGeneration = 0
			case "success-errno":
				x.Errno = 1
			case "denial-no-errno":
				x.Status = 1
			case "denial-overflow":
				x.Status = 2
				x.Errno = 4096
			}
			if _, e := x.encode(); e == nil {
				t.Fatal("unbound success or unknown accepted")
			}
		})
	}
}
