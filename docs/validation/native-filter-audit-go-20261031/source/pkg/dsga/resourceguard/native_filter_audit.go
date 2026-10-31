package resourceguard

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

const nativeAuditHeaderBytes = 144
const nativeAuditMaxInstructions = 1024
const nativeAuditMaxBytes = nativeAuditHeaderBytes + 3*(8+8*nativeAuditMaxInstructions)

// audit输出是固定可信二进制对原几何的纯编译结果，不是PG安装状态自报。
// Root还须在原出生停点独立读取内核全部程序；本组件本身不生成出生或normal许可。
type nativeFilterInstruction struct {
	Code   uint16
	JT, JF uint8
	K      uint32
}
type nativeFilterAuditBundle struct {
	Request       nativeBirthRequest
	Programs      [3][]nativeFilterInstruction // ABI kind1 base、kind2 final、kind3 child。
	EncodedSHA256 [32]byte
}
type nativeFilterPhase uint8

const (
	nativeFilterPMBase nativeFilterPhase = iota + 1
	nativeFilterPMFinal
	nativeFilterChild
)

// 复用唯一wire几何验证器检查内部typed对象，不能让内部调用者通过自造range绕过。
func validateNativeBirthRequestValue(request nativeBirthRequest) error {
	header, err := encodeNativeBirthACK(request.Original)
	if err != nil {
		return err
	}
	raw := make([]byte, nativeBirthRequestBytes)
	copy(raw, header[:])
	binary.BigEndian.PutUint32(raw[12:16], 1)
	binary.BigEndian.PutUint64(raw[112:120], request.Original.AddressSpaceBytes)
	binary.BigEndian.PutUint32(raw[120:124], request.Original.MaxBuffers)
	binary.BigEndian.PutUint32(raw[124:128], request.Original.PageBytes)
	binary.BigEndian.PutUint64(raw[128:136], request.TotalReservedBytes)
	for i, r := range request.Ranges {
		offset := 136 + 24*i
		binary.BigEndian.PutUint64(raw[offset:offset+8], r.Base)
		binary.BigEndian.PutUint64(raw[offset+8:offset+16], r.ReservedBytes)
		binary.BigEndian.PutUint64(raw[offset+16:offset+24], r.ElementBytes)
	}
	decoded, err := decodeNativeBirthRequest(raw, request.Original)
	if err != nil {
		return err
	}
	if decoded != request {
		return fmt.Errorf("typed native request differs from canonical wire")
	}
	return nil
}

// NDSGFP01：header144，三段kind/count+完整8字节指令，全部网络字节序。
// 不接受截断、额外段/尾部、未知arch、源几何漂移或零程序；绑定的是原request。
func decodeNativeFilterAudit(raw []byte, request nativeBirthRequest) (nativeFilterAuditBundle, error) {
	var zero nativeFilterAuditBundle
	if err := validateNativeBirthRequestValue(request); err != nil {
		return zero, err
	}
	if len(raw) < nativeAuditHeaderBytes || len(raw) > nativeAuditMaxBytes || string(raw[:8]) != "NDSGFP01" ||
		binary.BigEndian.Uint32(raw[8:12]) != 1 || binary.BigEndian.Uint32(raw[12:16]) != 0xc000003e ||
		binary.BigEndian.Uint32(raw[16:20]) != 3 || binary.BigEndian.Uint32(raw[20:24]) != 0 ||
		binary.BigEndian.Uint64(raw[24:32]) != request.Original.AddressSpaceBytes ||
		binary.BigEndian.Uint32(raw[32:36]) != request.Original.MaxBuffers ||
		binary.BigEndian.Uint32(raw[36:40]) != request.Original.PageBytes ||
		binary.BigEndian.Uint64(raw[40:48]) != request.TotalReservedBytes {
		return zero, fmt.Errorf("native filter audit header or original geometry differs")
	}
	for i, r := range request.Ranges {
		offset := 48 + 24*i
		if binary.BigEndian.Uint64(raw[offset:offset+8]) != r.Base ||
			binary.BigEndian.Uint64(raw[offset+8:offset+16]) != r.ReservedBytes ||
			binary.BigEndian.Uint64(raw[offset+16:offset+24]) != r.ElementBytes {
			return zero, fmt.Errorf("native filter audit range differs from original")
		}
	}
	result := nativeFilterAuditBundle{Request: request, EncodedSHA256: sha256.Sum256(raw)}
	offset := nativeAuditHeaderBytes
	for i := range result.Programs {
		if len(raw)-offset < 8 {
			return zero, fmt.Errorf("native filter audit program prefix truncated")
		}
		kind := binary.BigEndian.Uint32(raw[offset : offset+4])
		count := binary.BigEndian.Uint32(raw[offset+4 : offset+8])
		offset += 8
		if kind != uint32(i+1) || count == 0 || count > nativeAuditMaxInstructions || int(count) > (len(raw)-offset)/8 {
			return zero, fmt.Errorf("native filter audit kind/count invalid")
		}
		program := make([]nativeFilterInstruction, int(count))
		for j := range program {
			b := raw[offset : offset+8]
			offset += 8
			program[j] = nativeFilterInstruction{binary.BigEndian.Uint16(b[:2]), b[2], b[3], binary.BigEndian.Uint32(b[4:])}
		}
		result.Programs[i] = program
	}
	if offset != len(raw) {
		return zero, fmt.Errorf("native filter audit has trailing bytes")
	}
	return result, nil
}

// read(nil)先问长度，read(buffer)再取指令。真实backend只能在同一OS tracer线程的
// 原内核ptrace-stop调用；测试reader是明确模型。Linux off0是最老程序，必须严格
// base→final→child顺序并在下一offset得到ENOENT，不能只对len/hash或忽略额外继承段。
type nativeFilterReader func(offset uint32, dst []nativeFilterInstruction) (int, error)

func compareNativeInstalledFilters(bundle nativeFilterAuditBundle, phase nativeFilterPhase, read nativeFilterReader, isEnd func(error) bool) error {
	if read == nil || isEnd == nil || phase < nativeFilterPMBase || phase > nativeFilterChild {
		return fmt.Errorf("native filter audit phase/reader missing")
	}
	if err := validateNativeBirthRequestValue(bundle.Request); err != nil {
		return err
	}
	for i, expected := range bundle.Programs {
		if len(expected) == 0 || len(expected) > nativeAuditMaxInstructions {
			return fmt.Errorf("native expected program %d missing", i)
		}
	}
	for i := 0; i < int(phase); i++ {
		n, err := read(uint32(i), nil)
		if err != nil {
			return fmt.Errorf("actual native filter %d length read: %w", i, err)
		}
		if n != len(bundle.Programs[i]) || n == 0 || n > nativeAuditMaxInstructions {
			return fmt.Errorf("actual native filter %d length differs: got %d want %d", i, n, len(bundle.Programs[i]))
		}
		actual := make([]nativeFilterInstruction, n)
		got, err := read(uint32(i), actual)
		if err != nil {
			return fmt.Errorf("actual native filter %d instruction read: %w", i, err)
		}
		if got != n {
			return fmt.Errorf("actual native filter %d changed length during read", i)
		}
		for j := range actual {
			if actual[j] != bundle.Programs[i][j] {
				return fmt.Errorf("actual native filter %d instruction %d differs", i, j)
			}
		}
	}
	_, err := read(uint32(phase), nil)
	if err == nil {
		return fmt.Errorf("native filter chain has an extra segment")
	}
	if !isEnd(err) {
		return fmt.Errorf("native filter chain unknown terminal: %w", err)
	}
	return nil
}
