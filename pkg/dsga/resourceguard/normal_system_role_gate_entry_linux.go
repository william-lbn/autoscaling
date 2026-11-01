//go:build linux

package resourceguard

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type normalSystemRoleEntryGeometry struct {
	kind          uint16
	entry, offset uint64
	device, inode uint64
}

func normalSystemRoleReadEntry(f *os.File) (normalSystemRoleEntryGeometry, error) {
	var g normalSystemRoleEntryGeometry
	if _, e := nativeBirthParseELF(f); e != nil {
		return g, e
	}
	var h [64]byte
	if _, e := f.ReadAt(h[:], 0); e != nil {
		return g, e
	}
	if string(h[:4]) != "\x7fELF" || h[4] != 2 || h[5] != 1 {
		return g, fmt.Errorf("role entry requires exact ELF64LE")
	}
	g.kind = binary.LittleEndian.Uint16(h[16:18])
	if g.kind != 2 && g.kind != 3 {
		return g, fmt.Errorf("role entry ELF type unsupported")
	}
	g.entry = binary.LittleEndian.Uint64(h[24:32])
	phoff := binary.LittleEndian.Uint64(h[32:40])
	size := binary.LittleEndian.Uint16(h[54:56])
	count := binary.LittleEndian.Uint16(h[56:58])
	if size != 56 || count == 0 || count > 4096 {
		return g, fmt.Errorf("role entry program table differs")
	}
	matches := 0
	for i := uint64(0); i < uint64(count); i++ {
		if phoff > uint64(^uint64(0)>>1)-i*56 {
			return g, fmt.Errorf("role entry program table overflow")
		}
		var p [56]byte
		if _, e := f.ReadAt(p[:], int64(phoff+i*56)); e != nil {
			return g, e
		}
		if binary.LittleEndian.Uint32(p[:4]) != 1 || binary.LittleEndian.Uint32(p[4:8])&1 == 0 {
			continue
		}
		off := binary.LittleEndian.Uint64(p[8:16])
		va := binary.LittleEndian.Uint64(p[16:24])
		file := binary.LittleEndian.Uint64(p[32:40])
		if g.entry < va || g.entry-va >= file {
			continue
		}
		delta := g.entry - va
		if off > ^uint64(0)-delta {
			return g, fmt.Errorf("role entry file offset overflow")
		}
		g.offset = off + delta
		matches++
	}
	if matches != 1 {
		return g, fmt.Errorf("role entry not in unique executable file segment")
	}
	var st unix.Stat_t
	if e := unix.Fstat(int(f.Fd()), &st); e != nil {
		return g, e
	}
	g.device = st.Dev
	g.inode = st.Ino
	return g, nil
}
func normalSystemRoleMappedEntry(raw string, g normalSystemRoleEntryGeometry) (uint64, uint64, error) {
	if len(raw) == 0 || len(raw) > 64<<10 {
		return 0, 0, fmt.Errorf("role exec maps exceed bound")
	}
	matches := 0
	var entry, bias uint64
	for _, line := range strings.Split(strings.TrimSuffix(raw, "\n"), "\n") {
		f := strings.Fields(line)
		if len(f) < 5 {
			return 0, 0, fmt.Errorf("role exec mapping malformed")
		}
		span := strings.Split(f[0], "-")
		dev := strings.Split(f[3], ":")
		if len(span) != 2 || len(dev) != 2 || len(f[1]) != 4 {
			return 0, 0, fmt.Errorf("role exec mapping header differs")
		}
		start, e := strconv.ParseUint(span[0], 16, 64)
		if e != nil {
			return 0, 0, e
		}
		end, e := strconv.ParseUint(span[1], 16, 64)
		if e != nil || start >= end {
			return 0, 0, fmt.Errorf("role exec mapping range differs")
		}
		off, e := strconv.ParseUint(f[2], 16, 64)
		if e != nil {
			return 0, 0, e
		}
		major, e := strconv.ParseUint(dev[0], 16, 32)
		if e != nil {
			return 0, 0, e
		}
		minor, e := strconv.ParseUint(dev[1], 16, 32)
		if e != nil {
			return 0, 0, e
		}
		inode, e := strconv.ParseUint(f[4], 10, 64)
		if e != nil {
			return 0, 0, e
		}
		if inode != g.inode || uint64(unix.Major(g.device)) != major || uint64(unix.Minor(g.device)) != minor || f[1][2] != 'x' || g.offset < off || g.offset-off >= end-start {
			continue
		}
		if start > ^uint64(0)-(g.offset-off) {
			return 0, 0, fmt.Errorf("role mapped entry overflow")
		}
		mapped := start + g.offset - off
		if mapped < g.entry {
			return 0, 0, fmt.Errorf("role load bias underflow")
		}
		candidate := mapped - g.entry
		if candidate&4095 != 0 || g.kind == 2 && candidate != 0 {
			return 0, 0, fmt.Errorf("role load bias differs")
		}
		entry = mapped
		bias = candidate
		matches++
	}
	if matches != 1 {
		return 0, 0, fmt.Errorf("role entry mapping missing or ambiguous")
	}
	return entry, bias, nil
}

// 第二exec-stop发生在真正PT_INTERP首条用户指令前：动态目标核loader entry+实际
// load bias，静态目标才核主ELF entry。条件只读闭包仍另由原Source核验，不能把
// static C的e_entry断言冒成chronyd/vector/Compute真实动态loader证据。
func verifyNormalSystemRoleExecEntry(s *normalSystemLifecycleSource, task *normalSystemOriginalTask, plan normalSystemRoleLaunchSpec) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	main := s.files[s.logical[plan.argv[0]]]
	if main == nil {
		return 0, fmt.Errorf("original main ELF absent")
	}
	mainGeometry, e := normalSystemRoleReadEntry(main)
	if e != nil {
		return 0, e
	}
	raw, e := nativeTraceProcText(task.procfd, "maps")
	if e != nil {
		return 0, e
	}
	mainEntry, _, e := normalSystemRoleMappedEntry(raw, mainGeometry)
	if e != nil {
		return 0, e
	}
	expected := mainEntry
	metadata := s.metadata[s.logical[plan.argv[0]]]
	if metadata.Interpreter != "" {
		canonical, e := nativeBirthCanonical(metadata.Interpreter)
		if e != nil {
			return 0, e
		}
		loader := s.files[canonical]
		if loader == nil {
			return 0, fmt.Errorf("actual PT_INTERP absent from original closure")
		}
		geometry, e := normalSystemRoleReadEntry(loader)
		if e != nil {
			return 0, e
		}
		expected, _, e = normalSystemRoleMappedEntry(raw, geometry)
		if e != nil {
			return 0, e
		}
	}
	var regs unix.PtraceRegs
	if e = unix.PtraceGetRegs(task.source.PID, &regs); e != nil {
		return 0, e
	}
	if regs.Rip != expected {
		return 0, fmt.Errorf("original exec-stop instruction pointer differs from actual loader entry")
	}
	return expected, nil
}
