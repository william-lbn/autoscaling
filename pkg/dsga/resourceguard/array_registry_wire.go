package resourceguard

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
)

// 四数组私有中立ABI，PG主版本只由原闭源码layout决定；packet不是权限来源。
const (
	arrayRegistryRequestBytes    = 512
	arrayRegistryReplyBytes      = 160
	arrayRegistryProtocolVersion = 1
)

type arrayRegistryOpcode uint32

const (
	arrayRegistryRegister arrayRegistryOpcode = iota + 1
	arrayRegistryPopulate
	arrayRegistryRetireBegin
)

type arrayRegistryWireRange struct{ Base, Maximum, Mapped, Element, Page uint64 }
type arrayRegistryWireRequest struct {
	Op                                                          arrayRegistryOpcode
	PGBoot, Generation, MaximumBuffers, StartupBuffers          uint64
	IssuerEpoch, Revision, OperationHigh, OperationLow          uint64
	Array, Offset, Bytes, MappingGeneration                     uint64
	SourceSHA, BirthNonce, ProfileSHA, MappingNonce, ContextSHA [32]byte
	Ranges                                                      [4]arrayRegistryWireRange
	RegistrationNonce                                           [32]byte
	TargetBuffers, OriginalHigh                                 uint64
}

func (r arrayRegistryWireRequest) keyZero() bool {
	return r.IssuerEpoch == 0 && r.Revision == 0 && r.OperationHigh == 0 && r.OperationLow == 0 && r.ContextSHA == [32]byte{}
}
func (r arrayRegistryWireRequest) validate() error {
	if r.Op < arrayRegistryRegister || r.Op > arrayRegistryRetireBegin || r.PGBoot == 0 || r.Generation == 0 || r.MaximumBuffers == 0 || r.MaximumBuffers > math.MaxInt32 || r.StartupBuffers == 0 || r.StartupBuffers > r.MaximumBuffers || r.MappingGeneration == 0 || r.OriginalHigh == 0 || r.OriginalHigh > r.MaximumBuffers {
		return fmt.Errorf("array registry wire phase/geometry invalid")
	}
	for _, h := range [][32]byte{r.SourceSHA, r.BirthNonce, r.ProfileSHA, r.MappingNonce} {
		if h == [32]byte{} {
			return fmt.Errorf("array registry wire original binding missing")
		}
	}
	for i, a := range r.Ranges {
		if a.Base == 0 || a.Page != 4096 || a.Base%a.Page != 0 || a.Element == 0 || a.Element > math.MaxUint64/r.MaximumBuffers || a.Maximum != r.MaximumBuffers*a.Element || a.Maximum > math.MaxUint64-(a.Page-1) || a.Mapped != (a.Maximum+a.Page-1)&^(a.Page-1) || a.Mapped > math.MaxUint64-a.Base {
			return fmt.Errorf("array registry wire range invalid")
		}
		for j := 0; j < i; j++ {
			b := r.Ranges[j]
			if a.Base < b.Base+b.Mapped && b.Base < a.Base+a.Mapped {
				return fmt.Errorf("array registry wire ranges overlap")
			}
		}
	}
	if r.Op == arrayRegistryRegister {
		if r.RegistrationNonce != [32]byte{} || r.Array != 0 || r.Offset != 0 || r.Bytes != 0 || !r.keyZero() || r.TargetBuffers != 0 || r.OriginalHigh != r.StartupBuffers {
			return fmt.Errorf("array register wire has runtime authority fields")
		}
	} else {
		if r.RegistrationNonce == [32]byte{} {
			return fmt.Errorf("original Root registration reference missing")
		}
		if r.Op == arrayRegistryRetireBegin {
			if r.Array != 0 || r.Offset != 0 || r.Bytes != 0 || !r.keyZero() || r.TargetBuffers != 0 {
				return fmt.Errorf("array retire wire differs")
			}
		} else {
			if r.Array >= 4 {
				return fmt.Errorf("array identifier differs")
			}
			a := r.Ranges[r.Array]
			if r.Bytes == 0 || r.Offset%a.Page != 0 || r.Bytes%a.Page != 0 || r.Offset > a.Mapped || r.Bytes > a.Mapped-r.Offset {
				return fmt.Errorf("array populate wire exceeds original mapping")
			}
			if r.keyZero() {
				if r.TargetBuffers != 0 || r.OriginalHigh != r.StartupBuffers {
					return fmt.Errorf("startup populate wire has runtime target")
				}
				n := (r.StartupBuffers*a.Element + a.Page - 1) &^ (a.Page - 1)
				if r.Offset != 0 || r.Bytes != n {
					return fmt.Errorf("startup populate wire differs from original N0")
				}
			} else {
				if r.IssuerEpoch == 0 || r.Revision == 0 || r.OperationHigh == 0 && r.OperationLow == 0 || r.ContextSHA == [32]byte{} || r.TargetBuffers <= r.OriginalHigh || r.TargetBuffers > r.MaximumBuffers {
					return fmt.Errorf("runtime populate wire lacks complete original Key/target")
				}
				start := (r.OriginalHigh * a.Element) &^ (a.Page - 1)
				end := (r.TargetBuffers*a.Element + a.Page - 1) &^ (a.Page - 1)
				if r.Offset < start || r.Offset+r.Bytes > end {
					return fmt.Errorf("runtime populate wire outside planned new prefix")
				}
			}
		}
	}
	return nil
}
func decodeArrayRegistryWire(raw []byte) (arrayRegistryWireRequest, error) {
	var r arrayRegistryWireRequest
	if len(raw) != arrayRegistryRequestBytes || string(raw[:8]) != "NDSGARR1" || binary.BigEndian.Uint32(raw[8:12]) != arrayRegistryProtocolVersion {
		return r, fmt.Errorf("array registry wire header differs")
	}
	for _, v := range raw[480:] {
		if v != 0 {
			return r, fmt.Errorf("array registry wire reserved byte set")
		}
	}
	get := func(o int) uint64 { return binary.BigEndian.Uint64(raw[o : o+8]) }
	r.Op = arrayRegistryOpcode(binary.BigEndian.Uint32(raw[12:16]))
	r.PGBoot = get(16)
	r.Generation = get(24)
	r.MaximumBuffers = get(32)
	r.StartupBuffers = get(40)
	r.IssuerEpoch = get(48)
	r.Revision = get(56)
	r.OperationHigh = get(64)
	r.OperationLow = get(72)
	r.Array = get(80)
	r.Offset = get(88)
	r.Bytes = get(96)
	r.MappingGeneration = get(104)
	copy(r.SourceSHA[:], raw[112:144])
	copy(r.BirthNonce[:], raw[144:176])
	copy(r.ProfileSHA[:], raw[176:208])
	copy(r.MappingNonce[:], raw[208:240])
	copy(r.ContextSHA[:], raw[240:272])
	for i := range r.Ranges {
		o := 272 + i*40
		r.Ranges[i] = arrayRegistryWireRange{get(o), get(o + 8), get(o + 16), get(o + 24), get(o + 32)}
	}
	copy(r.RegistrationNonce[:], raw[432:464])
	r.TargetBuffers = get(464)
	r.OriginalHigh = get(472)
	if e := r.validate(); e != nil {
		return arrayRegistryWireRequest{}, e
	}
	return r, nil
}
func encodeArrayRegistryWire(r arrayRegistryWireRequest) ([]byte, error) {
	if e := r.validate(); e != nil {
		return nil, e
	}
	b := make([]byte, arrayRegistryRequestBytes)
	copy(b, "NDSGARR1")
	binary.BigEndian.PutUint32(b[8:12], 1)
	binary.BigEndian.PutUint32(b[12:16], uint32(r.Op))
	put := func(o int, n uint64) { binary.BigEndian.PutUint64(b[o:o+8], n) }
	for o, n := range map[int]uint64{16: r.PGBoot, 24: r.Generation, 32: r.MaximumBuffers, 40: r.StartupBuffers, 48: r.IssuerEpoch, 56: r.Revision, 64: r.OperationHigh, 72: r.OperationLow, 80: r.Array, 88: r.Offset, 96: r.Bytes, 104: r.MappingGeneration, 464: r.TargetBuffers, 472: r.OriginalHigh} {
		put(o, n)
	}
	copy(b[112:144], r.SourceSHA[:])
	copy(b[144:176], r.BirthNonce[:])
	copy(b[176:208], r.ProfileSHA[:])
	copy(b[208:240], r.MappingNonce[:])
	copy(b[240:272], r.ContextSHA[:])
	for i, a := range r.Ranges {
		o := 272 + i*40
		put(o, a.Base)
		put(o+8, a.Maximum)
		put(o+16, a.Mapped)
		put(o+24, a.Element)
		put(o+32, a.Page)
	}
	copy(b[432:464], r.RegistrationNonce[:])
	return b, nil
}

// 仅固定Root实际执行结果可构造成功回复；编码函数本身不证明Source、cap或helper终态。
type arrayRegistryWireReply struct {
	Status, Errno                    uint32
	Offset, Bytes                    uint64
	RequestSHA, RegistrationNonce    [32]byte
	ResourceFloor, MappingGeneration uint64
}

func (r arrayRegistryWireReply) encode() ([]byte, error) {
	if r.Status > 2 || r.RequestSHA == [32]byte{} || r.MappingGeneration == 0 || r.Status == 0 && (r.Errno != 0 || r.RegistrationNonce == [32]byte{} || r.ResourceFloor == 0) || r.Status != 0 && (r.Errno == 0 || r.Errno >= 4096) {
		return nil, fmt.Errorf("array reply binding/status differs")
	}
	b := make([]byte, arrayRegistryReplyBytes)
	copy(b, "NDSGARA1")
	binary.BigEndian.PutUint32(b[8:12], 1)
	binary.BigEndian.PutUint32(b[12:16], r.Status)
	binary.BigEndian.PutUint32(b[16:20], r.Errno)
	binary.BigEndian.PutUint64(b[24:32], r.Offset)
	binary.BigEndian.PutUint64(b[32:40], r.Bytes)
	copy(b[40:72], r.RequestSHA[:])
	copy(b[72:104], r.RegistrationNonce[:])
	binary.BigEndian.PutUint64(b[104:112], r.ResourceFloor)
	binary.BigEndian.PutUint64(b[112:120], r.MappingGeneration)
	return b, nil
}
func arrayRegistryRequestSHA(raw []byte) [32]byte { return sha256.Sum256(raw) }
