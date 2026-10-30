package resourceguard

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

const (
	nativeBirthWireMagic    = "NDSGBR01"
	nativeBirthHeaderBytes  = 112
	nativeBirthRequestBytes = 232
)

// 这是尚未接入生产出生链的内部传输组件。Expected 必须来自原 root Pending
// 及可信构建 ABI；收到一帧或 SO_PEERCRED 匹配都不证明 AS/maps/filter/入组。
// 本组件不迁移 cgroup、不落 Enrolled、不发送 ACK，也不授予 normal/VM 降限。
type nativeBirthExpected struct {
	Nonce             [32]byte
	SourceSHA         [32]byte
	ProfileSHA        [32]byte
	AddressSpaceBytes uint64
	MaxBuffers        uint32
	PageBytes         uint32
	ElementBytes      [4]uint64
}

type nativeBirthRange struct {
	Base          uint64
	ReservedBytes uint64
	ElementBytes  uint64
}

type nativeBirthRequest struct {
	Original           nativeBirthExpected
	TotalReservedBytes uint64
	Ranges             [4]nativeBirthRange
}

func (e nativeBirthExpected) geometry() ([4]uint64, uint64, error) {
	var lengths [4]uint64
	if e.Nonce == [32]byte{} || e.SourceSHA == [32]byte{} || e.ProfileSHA == [32]byte{} ||
		e.AddressSpaceBytes == 0 || e.AddressSpaceBytes > math.MaxInt64 || e.AddressSpaceBytes%(64<<10) != 0 ||
		e.MaxBuffers == 0 || e.MaxBuffers > math.MaxInt32 || e.PageBytes != 4096 {
		return lengths, 0, fmt.Errorf("original native birth source or geometry missing")
	}
	var total uint64
	for i, element := range e.ElementBytes {
		// 只支持当前普通页 PG17 ABI，不能用对端自报 element/页尺寸补齐来源。
		if element == 0 || element > 1<<20 || uint64(e.MaxBuffers) > (math.MaxUint64-4095)/element {
			return lengths, 0, fmt.Errorf("invalid original native array ABI or multiplication")
		}
		length := (uint64(e.MaxBuffers)*element + 4095) &^ uint64(4095)
		if length > math.MaxUint64-total {
			return lengths, 0, fmt.Errorf("native array reservation overflow")
		}
		lengths[i] = length
		total += length
	}
	if total >= e.AddressSpaceBytes {
		return lengths, 0, fmt.Errorf("native array reservation leaves no non-array address space")
	}
	return lengths, total, nil
}

func nativeBirthHeader(raw []byte, kind uint32, expected nativeBirthExpected) error {
	if len(raw) < nativeBirthHeaderBytes || !bytes.Equal(raw[:8], []byte(nativeBirthWireMagic)) ||
		binary.BigEndian.Uint32(raw[8:12]) != 1 || binary.BigEndian.Uint32(raw[12:16]) != kind ||
		!bytes.Equal(raw[16:48], expected.Nonce[:]) || !bytes.Equal(raw[48:80], expected.SourceSHA[:]) ||
		!bytes.Equal(raw[80:112], expected.ProfileSHA[:]) {
		return fmt.Errorf("native birth frame differs from original pending source")
	}
	return nil
}

// 精确长度/网络字节序，不接受扩展尾部、请求兼 ACK、另 nonce 或自报 ABI。
// 地址只接受本 profile 的低 47-bit、4KiB 对齐用户区。真正映射/完整 seccomp
// 核验须由原锁内独立内核读回完成，不能因为帧通过就从该数据扣除 R。
func decodeNativeBirthRequest(raw []byte, expected nativeBirthExpected) (nativeBirthRequest, error) {
	var result nativeBirthRequest
	lengths, total, err := expected.geometry()
	if err != nil {
		return result, err
	}
	if len(raw) != nativeBirthRequestBytes {
		return result, fmt.Errorf("invalid native birth request length")
	}
	if err := nativeBirthHeader(raw, 1, expected); err != nil {
		return result, err
	}
	if binary.BigEndian.Uint64(raw[112:120]) != expected.AddressSpaceBytes ||
		binary.BigEndian.Uint32(raw[120:124]) != expected.MaxBuffers ||
		binary.BigEndian.Uint32(raw[124:128]) != expected.PageBytes ||
		binary.BigEndian.Uint64(raw[128:136]) != total {
		return result, fmt.Errorf("native birth frame changed original limits or reservation")
	}
	result.Original = expected
	result.TotalReservedBytes = total
	for i := range result.Ranges {
		offset := 136 + i*24
		r := nativeBirthRange{binary.BigEndian.Uint64(raw[offset : offset+8]), binary.BigEndian.Uint64(raw[offset+8 : offset+16]), binary.BigEndian.Uint64(raw[offset+16 : offset+24])}
		if r.Base < 4096 || r.Base%4096 != 0 || r.ReservedBytes != lengths[i] || r.ElementBytes != expected.ElementBytes[i] ||
			r.Base >= 1<<47 || r.ReservedBytes > (1<<47)-r.Base {
			return nativeBirthRequest{}, fmt.Errorf("invalid native birth array mapping geometry")
		}
		for j := 0; j < i; j++ {
			old := result.Ranges[j]
			if r.Base < old.Base+old.ReservedBytes && old.Base < r.Base+r.ReservedBytes {
				return nativeBirthRequest{}, fmt.Errorf("overlapping native birth array reservations")
			}
		}
		result.Ranges[i] = r
	}
	return result, nil
}

// 只构造回显字节；调用者未来必须在原映射/filter/迁组核验和 Enrolled fsync
// 全部成功后才发送，不能将此纯格式函数当 Root 授权。当前没有生产发送者。
func encodeNativeBirthACK(expected nativeBirthExpected) ([nativeBirthHeaderBytes]byte, error) {
	var raw [nativeBirthHeaderBytes]byte
	if _, _, err := expected.geometry(); err != nil {
		return raw, err
	}
	copy(raw[:8], nativeBirthWireMagic)
	binary.BigEndian.PutUint32(raw[8:12], 1)
	binary.BigEndian.PutUint32(raw[12:16], 2)
	copy(raw[16:48], expected.Nonce[:])
	copy(raw[48:80], expected.SourceSHA[:])
	copy(raw[80:112], expected.ProfileSHA[:])
	return raw, nil
}
