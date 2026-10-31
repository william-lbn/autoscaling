//go:build linux

package resourceguard

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const arrayPopulateHelperPath = "/neonvm/bin/neon-dsga-array-populate"
const arrayPopulateScope = "original-array-populate-component-only"
const arrayPopulatePlanBytes = 368
const arrayPopulateStatusBytes = 256
const arrayPopulateControlFD = 9
const arrayPopulateParentFD = 10

// 四FD必须由原Root真实map_files/原PG boot验证者提供。本私有组件不接受HTTP、
// 租户路径或“verified”bool；仍须后继原Source/完整生命周期消费者接线。
type arrayPopulateFile struct {
	Device    control.Uint64 `json:"device"`
	Inode     control.Uint64 `json:"inode"`
	FileBytes control.Uint64 `json:"file_bytes"`
	Offset    control.Uint64 `json:"offset"`
	Length    control.Uint64 `json:"length"`
}
type arrayPopulateContext struct {
	Original              ProcessIdentity      `json:"original"`
	PGBootID              control.Uint64       `json:"pg_boot_id"`
	ResourceFloor         control.Uint64       `json:"resource_floor"`
	OriginalContextSHA256 string               `json:"original_context_sha256"`
	HelperSHA256          string               `json:"helper_sha256"`
	MemlockUpperBytes     control.Uint64       `json:"helper_memlock_upper_bytes"`
	AddressSpaceMaxBytes  control.Uint64       `json:"helper_address_space_max_bytes"`
	ArraysDevice          control.Uint64       `json:"arrays_device"`
	HelperReserveBytes    control.Uint64       `json:"helper_reserve_bytes"`
	ArraysMaxBytes        control.Uint64       `json:"arrays_max_bytes"`
	SystemMaxBytes        control.Uint64       `json:"system_max_bytes"`
	ArraysInode           control.Uint64       `json:"arrays_inode"`
	SystemDevice          control.Uint64       `json:"system_device"`
	SystemInode           control.Uint64       `json:"system_inode"`
	Files                 [4]arrayPopulateFile `json:"files"`
}

func (p arrayPopulateContext) validate() error {
	if p.Original.Validate() != nil || p.PGBootID == 0 || p.ResourceFloor == 0 ||
		p.MemlockUpperBytes < 4096 || p.MemlockUpperBytes > 1<<20 || p.MemlockUpperBytes%4096 != 0 ||
		p.AddressSpaceMaxBytes < 1<<20 || p.AddressSpaceMaxBytes >= control.Uint64(math.MaxInt64) ||
		p.ArraysDevice != p.Original.Identity.CgroupDevice || p.ArraysInode == 0 || p.ArraysInode == p.Original.Identity.CgroupInode ||
		p.ArraysMaxBytes == 0 || p.ArraysMaxBytes%4096 != 0 || p.ArraysMaxBytes >= control.Uint64(math.MaxInt64) || p.SystemMaxBytes < 4096 || p.SystemMaxBytes%4096 != 0 || p.SystemMaxBytes >= control.Uint64(math.MaxInt64) ||
		p.SystemDevice != p.ArraysDevice || p.SystemInode == 0 || p.SystemInode == p.ArraysInode || p.SystemInode == p.Original.Identity.CgroupInode {
		return fmt.Errorf("array populate original context incomplete")
	}
	if _, e := nativeBirthDigest(p.OriginalContextSHA256); e != nil {
		return e
	}
	if _, e := nativeBirthDigest(p.HelperSHA256); e != nil {
		return e
	}
	seen := map[[2]control.Uint64]bool{}
	total := uint64(0)
	for _, f := range p.Files {
		key := [2]control.Uint64{f.Device, f.Inode}
		if f.Device == 0 || f.Inode == 0 || seen[key] || f.FileBytes == 0 || uint64(f.FileBytes) >= math.MaxInt64 ||
			f.FileBytes%4096 != 0 || f.Offset%4096 != 0 || f.Length%4096 != 0 || f.Offset > f.FileBytes || f.Length > f.FileBytes-f.Offset || total > math.MaxUint64-uint64(f.Length) {
			return fmt.Errorf("array populate FD geometry invalid")
		}
		total += uint64(f.Length)
		seen[key] = true
	}
	if total == 0 || total > uint64(p.AddressSpaceMaxBytes)-uint64(p.MemlockUpperBytes) || p.HelperReserveBytes < p.AddressSpaceMaxBytes || p.HelperReserveBytes > p.ArraysMaxBytes || total > uint64(p.ArraysMaxBytes-p.HelperReserveBytes) {
		return fmt.Errorf("array populate mapped spans exceed original helper AS")
	}
	return nil
}

type arrayPopulatePending struct {
	ProtocolVersion   uint32               `json:"protocol_version"`
	Scope             string               `json:"scope"`
	Nonce             string               `json:"nonce"`
	Parent            nativeBirthConsumer  `json:"parent"`
	ParentGuestBootID string               `json:"parent_guest_boot_id"`
	Context           arrayPopulateContext `json:"context"`
	StateDevice       control.Uint64       `json:"state_device"`
	StateInode        control.Uint64       `json:"state_inode"`
	LockDevice        control.Uint64       `json:"lock_device"`
	LockInode         control.Uint64       `json:"lock_inode"`
}

func (p arrayPopulatePending) validate() error {
	if p.ProtocolVersion != 1 || p.Scope != arrayPopulateScope || p.Context.validate() != nil || p.Parent.PID <= 1 || p.Parent.StartTicks == 0 || p.ParentGuestBootID != p.Context.Original.Identity.GuestBootID || !validGuestBoot(p.ParentGuestBootID) || p.StateDevice == 0 || p.StateInode == 0 || p.LockDevice != p.StateDevice || p.LockInode == 0 {
		return fmt.Errorf("array populate original pending invalid")
	}
	if _, e := nativeBirthDigest(p.Nonce); e != nil {
		return e
	}
	_, e := nativeBirthDigest(p.Parent.InstanceNonce)
	return e
}
func (p arrayPopulatePending) wire() ([]byte, error) {
	if e := p.validate(); e != nil {
		return nil, e
	}
	b := make([]byte, arrayPopulatePlanBytes)
	copy(b, "NAPOP001")
	binary.LittleEndian.PutUint32(b[8:12], 1)
	w := func(off int, v uint64) { binary.LittleEndian.PutUint64(b[off:off+8], v) }
	w(16, uint64(p.Parent.PID))
	binary.LittleEndian.PutUint32(b[24:28], 65534)
	binary.LittleEndian.PutUint32(b[28:32], 65534)
	w(32, uint64(p.Context.MemlockUpperBytes))
	w(40, uint64(p.Context.AddressSpaceMaxBytes))
	w(48, uint64(p.LockDevice))
	w(56, uint64(p.LockInode))
	w(64, uint64(p.Context.ArraysDevice))
	w(72, uint64(p.Context.ArraysInode))
	w(80, uint64(p.Context.SystemDevice))
	w(88, uint64(p.Context.SystemInode))
	w(112, uint64(p.StateDevice))
	w(120, uint64(p.StateInode))
	w(128, uint64(p.Context.PGBootID))
	w(136, uint64(p.Context.ResourceFloor))
	nonce, _ := hex.DecodeString(p.Nonce)
	copy(b[144:176], nonce)
	digest, _ := hex.DecodeString(p.Context.OriginalContextSHA256)
	copy(b[176:208], digest)
	for i, f := range p.Context.Files {
		for j, v := range []control.Uint64{f.Device, f.Inode, f.FileBytes, f.Offset, f.Length} {
			w(208+40*i+8*j, uint64(v))
		}
	}
	return b, nil
}

type arrayPopulateStatus struct {
	Phase       uint32
	Errno       uint32
	LockedBytes uint64
	PID         uint64
	Ranges      [4][2]uint64
	FilterCount uint32
	Index       uint32
}

func decodeArrayPopulateStatus(raw []byte, p arrayPopulatePending, prior *arrayPopulateStatus) (arrayPopulateStatus, error) {
	var o arrayPopulateStatus
	if p.validate() != nil {
		return o, fmt.Errorf("array populate original pending invalid")
	}
	if len(raw) != arrayPopulateStatusBytes || string(raw[:8]) != "NAPS0001" {
		return o, fmt.Errorf("array populate response size/magic invalid")
	}
	nonce, _ := hex.DecodeString(p.Nonce)
	if len(nonce) != 32 || string(raw[96:128]) != string(nonce) {
		return o, fmt.Errorf("array populate response original nonce differs")
	}
	for _, v := range raw[136:] {
		if v != 0 {
			return o, fmt.Errorf("array populate response reserved bytes nonzero")
		}
	}
	o.Phase = binary.LittleEndian.Uint32(raw[8:12])
	o.Errno = binary.LittleEndian.Uint32(raw[12:16])
	o.LockedBytes = binary.LittleEndian.Uint64(raw[16:24])
	o.PID = binary.LittleEndian.Uint64(raw[24:32])
	o.FilterCount = binary.LittleEndian.Uint32(raw[128:132])
	o.Index = binary.LittleEndian.Uint32(raw[132:136])
	if o.Phase < 1 || o.Phase > 3 || o.PID <= 1 || o.PID == uint64(p.Parent.PID) || o.PID == uint64(p.Context.Original.PostmasterPID) || o.Phase == 1 && (o.Index != 0 || o.Errno != 0) || o.Phase == 2 && (o.Index > 3 || o.Errno != 0) || o.Phase == 3 && (o.Errno == 0 || o.Errno > 4095) || o.LockedBytes == 0 || o.LockedBytes > uint64(p.Context.MemlockUpperBytes) || o.LockedBytes%4096 != 0 || o.FilterCount == 0 || o.FilterCount > 128 {
		return arrayPopulateStatus{}, fmt.Errorf("array populate response facts invalid")
	}
	for i := range o.Ranges {
		base := binary.LittleEndian.Uint64(raw[32+16*i : 40+16*i])
		size := binary.LittleEndian.Uint64(raw[40+16*i : 48+16*i])
		o.Ranges[i] = [2]uint64{base, size}
		if size != uint64(p.Context.Files[i].Length) || size == 0 && base != 0 || size > 0 && (base == 0 || base%4096 != 0 || base > math.MaxUint64-size) {
			return arrayPopulateStatus{}, fmt.Errorf("array populate mapped span differs")
		}
		for j := 0; j < i; j++ {
			if size != 0 && o.Ranges[j][1] != 0 && base < o.Ranges[j][0]+o.Ranges[j][1] && o.Ranges[j][0] < base+size {
				return arrayPopulateStatus{}, fmt.Errorf("array populate mapped spans overlap")
			}
		}
	}
	if o.FilterCount != uint32(len(arrayPopulateFilter(o.Ranges))) {
		return arrayPopulateStatus{}, fmt.Errorf("helper response filter count differs from exact program")
	}
	if prior != nil && (o.PID != prior.PID || o.LockedBytes != prior.LockedBytes || o.Ranges != prior.Ranges || o.FilterCount != prior.FilterCount) {
		return arrayPopulateStatus{}, fmt.Errorf("array populate immutable prepared facts drifted")
	}
	return o, nil
}

// 与C按同一Linux ABI独立生成完整cBPF，实际Root必须ptrace读取整个程序并核
// 下一offset ENOENT；不能凭Seccomp:2或helper自报hash发GO。
func arrayPopulateFilter(ranges [4][2]uint64) []unix.SockFilter {
	var out []unix.SockFilter
	emit := func(c uint16, t, f uint8, k uint32) { out = append(out, unix.SockFilter{Code: c, Jt: t, Jf: f, K: k}) }
	emit(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, 0, 0, 4)
	emit(unix.BPF_JMP|unix.BPF_JEQ|unix.BPF_K, 1, 0, 0xc000003e)
	emit(unix.BPF_RET|unix.BPF_K, 0, 0, 0x80000000)
	emit(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, 0, 0, 0)
	emit(unix.BPF_JMP|unix.BPF_JSET|unix.BPF_K, 0, 1, 0x40000000)
	emit(unix.BPF_RET|unix.BPF_K, 0, 0, 0x80000000)
	rule := func(nr uint32, offset, value []uint32) {
		emit(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, 0, 0, 0)
		emit(unix.BPF_JMP|unix.BPF_JEQ|unix.BPF_K, 0, uint8(2*len(offset)+1), nr)
		for i, o := range offset {
			emit(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, 0, 0, o)
			emit(unix.BPF_JMP|unix.BPF_JEQ|unix.BPF_K, 0, uint8(2*(len(offset)-i-1)+1), value[i])
		}
		emit(unix.BPF_RET|unix.BPF_K, 0, 0, 0x7fff0000)
	}
	rule(unix.SYS_RECVFROM, []uint32{16, 20, 32, 36, 40, 44, 48, 52, 56, 60}, []uint32{9, 0, 1, 0, 32, 0, 0, 0, 0, 0})
	rule(unix.SYS_WRITE, []uint32{16, 20, 32, 36}, []uint32{9, 0, 256, 0})
	rule(unix.SYS_POLL, []uint32{24, 28, 32, 36}, []uint32{1, 0, 0, 0})
	rule(unix.SYS_EXIT_GROUP, nil, nil)
	rule(unix.SYS_EXIT, nil, nil)
	for _, r := range ranges {
		if r[1] != 0 {
			rule(unix.SYS_MADVISE, []uint32{16, 20, 24, 28, 32, 36}, []uint32{uint32(r[0]), uint32(r[0] >> 32), uint32(r[1]), uint32(r[1] >> 32), 23, 0})
		}
	}
	emit(unix.BPF_RET|unix.BPF_K, 0, 0, 0x50001)
	return out
}
