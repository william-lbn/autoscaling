//go:build linux

package resourceguard

import (
	"crypto/sha256"
	"debug/elf"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

const fixedNativeBirthPolicyPath = "/neonvm/runtime/dsga-native-birth.json"
const fixedNativeBirthBuildPath = "/usr/share/neon-dsga/native-build.json"

// 只支持明确的无cache/preload/RPATH默认loader闭包。遇到其他发行构建拒绝，
// 不能把“列了一些.so”当完整pre-main来源；post-GUC/plugins/FS/I仍由外层证明。
var nativeBirthLoaderDirs = []string{"/lib/x86_64-linux-gnu", "/usr/lib/x86_64-linux-gnu", "/lib", "/usr/lib"}

func nativeBirthReadBounded(f *os.File, n int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(f, n+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > n {
		return nil, fmt.Errorf("native original source exceeds bound")
	}
	return b, nil
}
func nativeBirthReadISO(path string, n int64) ([]byte, error) {
	f, err := rootFile(path, false)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var fs unix.Statfs_t
	if err = unix.Fstatfs(int(f.Fd()), &fs); err != nil {
		return nil, err
	}
	if fs.Type != unix.ISOFS_SUPER_MAGIC || fs.Flags&unix.ST_RDONLY == 0 {
		return nil, fmt.Errorf("native policy requires original read-only ISO")
	}
	return nativeBirthReadBounded(f, n)
}

// 每个父目录及软链所在目录必须root只读。纯路径解析不授予身份；后续固定FD
// 再核regular/root/readonly/hash，原inode由持锁消费者每阶段重新读取并配对。
func nativeBirthReadonlyPath(path string) error {
	if !nativeBirthCleanPath(path) {
		return fmt.Errorf("invalid native fixed path")
	}
	for cur := filepath.Dir(path); ; cur = filepath.Dir(cur) {
		var st unix.Stat_t
		if err := unix.Lstat(cur, &st); err != nil {
			return err
		}
		kind := st.Mode & unix.S_IFMT
		// 软链的0777不代表可改内容；其父目录和真实目标都须原root只读。
		if st.Uid != 0 || (kind != unix.S_IFDIR && kind != unix.S_IFLNK) || (kind != unix.S_IFLNK && st.Mode&0022 != 0) {
			return fmt.Errorf("native source parent is writable or foreign")
		}
		var fs unix.Statfs_t
		if err := unix.Statfs(cur, &fs); err != nil {
			return err
		}
		if fs.Flags&unix.ST_RDONLY == 0 {
			return fmt.Errorf("native source parent is not read-only")
		}
		if cur == "/" {
			break
		}
	}
	return nil
}
func nativeBirthOpenRO(path string) (*os.File, error) {
	if err := nativeBirthReadonlyPath(path); err != nil {
		return nil, err
	}
	f, err := rootFile(path, false)
	if err != nil {
		return nil, err
	}
	var st unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(int(f.Fd()), &st) != nil || unix.Fstatfs(int(f.Fd()), &fs) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Size <= 0 || st.Size > 256<<20 || fs.Flags&unix.ST_RDONLY == 0 {
		f.Close()
		return nil, fmt.Errorf("native source is not bounded read-only regular file")
	}
	return f, nil
}
func nativeBirthFileHash(f *os.File) (string, error) {
	h := sha256.New()
	n, err := io.Copy(h, io.NewSectionReader(f, 0, (256<<20)+1))
	if err != nil {
		return "", err
	}
	if n > 256<<20 {
		return "", fmt.Errorf("native pre-main file too large")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func nativeBirthCanonical(path string) (string, error) {
	if err := nativeBirthReadonlyPath(path); err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	if !nativeBirthCleanPath(resolved) {
		return "", fmt.Errorf("native canonical path invalid")
	}
	return resolved, nil
}

type nativeBirthELFMetadata struct {
	Needed      []string
	Interpreter string
}
type nativeBirthELFLoad struct{ offset, address, fileBytes, pageStart, pageEnd uint64 }
type nativeBirthELFSection struct {
	name, kind                   uint32
	flags, address, offset, size uint64
	link                         uint32
	entry                        uint64
}

// 只读raw ELF64元数据，完全不调用debug/elf的解压或DynString路径。
// loader按PT_DYNAMIC/DT_STRTAB取值；section名称不是信任来源。所有读取/字符串
// 有固定上限，所用表拒压缩，真实地址须唯一PT_LOAD映射且和section逐字段一致。
func nativeBirthParseELF(f *os.File) (nativeBirthELFMetadata, error) {
	var out nativeBirthELFMetadata
	fail := func() (nativeBirthELFMetadata, error) {
		return nativeBirthELFMetadata{}, fmt.Errorf("native ELF loader metadata missing, ambiguous or unsupported")
	}
	st, err := f.Stat()
	if err != nil {
		return out, err
	}
	if st.Size() < 64 || st.Size() > 256<<20 {
		return fail()
	}
	size := uint64(st.Size())
	read := func(off, count uint64) ([]byte, error) {
		if off > size || count > size-off || count > 1<<20 {
			return nil, fmt.Errorf("native ELF bounded read invalid")
		}
		raw := make([]byte, int(count))
		_, err := f.ReadAt(raw, int64(off))
		return raw, err
	}
	h, err := read(0, 64)
	if err != nil {
		return out, err
	}
	order := binary.LittleEndian
	if string(h[:4]) != "\x7fELF" || h[4] != 2 || h[5] != 1 || h[6] != 1 || order.Uint16(h[18:20]) != uint16(elf.EM_X86_64) || order.Uint32(h[20:24]) != 1 || order.Uint16(h[52:54]) != 64 {
		return fail()
	}
	kind := order.Uint16(h[16:18])
	if kind != uint16(elf.ET_EXEC) && kind != uint16(elf.ET_DYN) {
		return fail()
	}
	phoff, shoff := order.Uint64(h[32:40]), order.Uint64(h[40:48])
	phnum, shnum := uint64(order.Uint16(h[56:58])), uint64(order.Uint16(h[60:62]))
	phsize, shsize := uint64(order.Uint16(h[54:56])), uint64(order.Uint16(h[58:60]))
	within := func(off, n uint64) bool { return off <= size && n <= size-off }
	if phnum > 4096 || shnum > 4096 || phnum != 0 && phsize != 56 || shnum != 0 && shsize != 64 || shnum == 0 && shoff != 0 || !within(phoff, phnum*phsize) || !within(shoff, shnum*shsize) {
		return fail()
	}
	var loads []nativeBirthELFLoad
	var dynOff, dynAddr, dynSize uint64
	hasDynamic := false
	for n := uint64(0); n < phnum; n++ {
		raw, err := read(phoff+n*56, 56)
		if err != nil {
			return out, err
		}
		typ := order.Uint32(raw[:4])
		off, addr, count, mem := order.Uint64(raw[8:16]), order.Uint64(raw[16:24]), order.Uint64(raw[32:40]), order.Uint64(raw[40:48])
		if !within(off, count) || addr > ^uint64(0)-mem {
			return fail()
		}
		switch typ {
		case uint32(elf.PT_LOAD):
			if mem < count || mem == 0 || addr > ^uint64(0)-count || addr%4096 != off%4096 {
				return fail()
			}
			// Linux elf_load使用page-rounded MAP_FIXED，后一个仅覆盖target一字节
			// 或BSS零填充也可能覆写整页。该profile严格拒所有LOAD页虚址交叠，
			// 不只统计完整覆盖字符串range的segment；部分合法布局明确unsupported。
			pageStart, pageEnd := addr&^uint64(4095), addr+mem
			if rem := pageEnd % 4096; rem != 0 {
				if pageEnd > ^uint64(0)-(4096-rem) {
					return fail()
				}
				pageEnd += 4096 - rem
			}
			for _, prior := range loads {
				if pageStart < prior.pageEnd && prior.pageStart < pageEnd {
					return fail()
				}
			}
			loads = append(loads, nativeBirthELFLoad{off, addr, count, pageStart, pageEnd})
		case uint32(elf.PT_DYNAMIC):
			if hasDynamic || count == 0 || count > 1<<20 || count%16 != 0 {
				return fail()
			}
			hasDynamic = true
			dynOff, dynAddr, dynSize = off, addr, count
		case uint32(elf.PT_INTERP):
			if out.Interpreter != "" || count < 2 || count > 4096 {
				return fail()
			}
			b, err := read(off, count)
			if err != nil {
				return out, err
			}
			if b[len(b)-1] != 0 || strings.ContainsRune(string(b[:len(b)-1]), 0) || !nativeBirthCleanPath(string(b[:len(b)-1])) {
				return fail()
			}
			out.Interpreter = string(b[:len(b)-1])
		}
	}
	mapAddress := func(addr, count uint64) (uint64, error) {
		found := false
		offset := uint64(0)
		for _, l := range loads {
			if addr < l.address {
				continue
			}
			delta := addr - l.address
			if delta > l.fileBytes || count > l.fileBytes-delta {
				continue
			}
			if found {
				return 0, fmt.Errorf("native ELF loader range has multiple PT_LOAD mappings")
			}
			found = true
			offset = l.offset + delta
		}
		if !found || !within(offset, count) {
			return 0, fmt.Errorf("native ELF loader range has no file-backed PT_LOAD mapping")
		}
		return offset, nil
	}
	sections := make([]nativeBirthELFSection, int(shnum))
	for n := uint64(0); n < shnum; n++ {
		raw, err := read(shoff+n*64, 64)
		if err != nil {
			return out, err
		}
		sh := nativeBirthELFSection{order.Uint32(raw[:4]), order.Uint32(raw[4:8]), order.Uint64(raw[8:16]), order.Uint64(raw[16:24]), order.Uint64(raw[24:32]), order.Uint64(raw[32:40]), order.Uint32(raw[40:44]), order.Uint64(raw[56:64])}
		if sh.kind != uint32(elf.SHT_NOBITS) && !within(sh.offset, sh.size) {
			return fail()
		}
		sections[n] = sh
	}
	var names []byte
	if shnum != 0 {
		index := uint64(order.Uint16(h[62:64]))
		if index >= shnum {
			return fail()
		}
		sh := sections[index]
		if sh.kind != uint32(elf.SHT_STRTAB) || sh.flags&uint64(elf.SHF_COMPRESSED) != 0 || sh.size == 0 || sh.size > 1<<20 {
			return fail()
		}
		names, err = read(sh.offset, sh.size)
		if err != nil {
			return out, err
		}
		if names[0] != 0 || names[len(names)-1] != 0 {
			return fail()
		}
	}
	sectionName := func(sh nativeBirthELFSection) (string, error) {
		if uint64(sh.name) >= uint64(len(names)) {
			return "", fmt.Errorf("native ELF section name outside original table")
		}
		b := names[sh.name:]
		if len(b) > 4096 {
			b = b[:4096]
		}
		end := strings.IndexByte(string(b), 0)
		if end < 0 {
			return "", fmt.Errorf("native ELF section name exceeds bound")
		}
		return string(b[:end]), nil
	}
	usedTable := func(sh nativeBirthELFSection) error {
		name, err := sectionName(sh)
		if err != nil {
			return err
		}
		if sh.flags&uint64(elf.SHF_COMPRESSED) != 0 || strings.HasPrefix(name, ".zdebug") || strings.HasPrefix(name, ".z") {
			return fmt.Errorf("native ELF used metadata compression unsupported")
		}
		return nil
	}
	if shnum != 0 {
		if err := usedTable(sections[order.Uint16(h[62:64])]); err != nil {
			return out, err
		}
	}
	var dynamic *nativeBirthELFSection
	for i := range sections {
		if sections[i].kind != uint32(elf.SHT_DYNAMIC) {
			continue
		}
		if dynamic != nil || !hasDynamic {
			return fail()
		}
		dynamic = &sections[i]
	}
	if !hasDynamic {
		if dynamic != nil || out.Interpreter != "" {
			return fail()
		}
		return out, nil
	}
	if dynamic == nil || dynamic.offset != dynOff || dynamic.address != dynAddr || dynamic.size != dynSize || dynamic.entry != 16 || uint64(dynamic.link) >= shnum {
		return fail()
	}
	if err := usedTable(*dynamic); err != nil {
		return out, err
	}
	mapped, err := mapAddress(dynAddr, dynSize)
	if err != nil || mapped != dynOff {
		return fail()
	}
	str := sections[dynamic.link]
	if str.kind != uint32(elf.SHT_STRTAB) || str.size == 0 || str.size > 1<<20 || str.flags&uint64(elf.SHF_ALLOC) == 0 {
		return fail()
	}
	if err := usedTable(str); err != nil {
		return out, err
	}
	raw, err := read(dynOff, dynSize)
	if err != nil {
		return out, err
	}
	var stringAddr, stringSize uint64
	seenAddr, seenSize, terminated := false, false, false
	var offsets []uint64
	for i := 0; i < len(raw); i += 16 {
		tag, value := order.Uint64(raw[i:i+8]), order.Uint64(raw[i+8:i+16])
		if terminated {
			if tag != 0 || value != 0 {
				return fail()
			}
			continue
		}
		switch tag {
		case uint64(elf.DT_NULL):
			terminated = true
		case uint64(elf.DT_STRTAB):
			if seenAddr {
				return fail()
			}
			seenAddr = true
			stringAddr = value
		case uint64(elf.DT_STRSZ):
			if seenSize {
				return fail()
			}
			seenSize = true
			stringSize = value
		case uint64(elf.DT_NEEDED):
			if len(offsets) >= 64 {
				return fail()
			}
			offsets = append(offsets, value)
		case uint64(elf.DT_RPATH), uint64(elf.DT_RUNPATH), uint64(elf.DT_AUDIT), uint64(elf.DT_DEPAUDIT), uint64(elf.DT_FILTER), uint64(elf.DT_AUXILIARY):
			return nativeBirthELFMetadata{}, fmt.Errorf("native default-loader profile rejects raw dynamic search/audit/filter tag")
		}
	}
	if !terminated || !seenAddr || !seenSize || stringSize != str.size || stringAddr != str.address {
		return fail()
	}
	mapped, err = mapAddress(stringAddr, stringSize)
	if err != nil || mapped != str.offset {
		return fail()
	}
	stringsRaw, err := read(mapped, stringSize)
	if err != nil {
		return out, err
	}
	if stringsRaw[0] != 0 || stringsRaw[len(stringsRaw)-1] != 0 {
		return fail()
	}
	seenNeeded := map[string]bool{}
	for _, off := range offsets {
		if off >= stringSize {
			return fail()
		}
		b := stringsRaw[off:]
		if len(b) > 4096 {
			b = b[:4096]
		}
		end := strings.IndexByte(string(b), 0)
		if end <= 0 {
			return fail()
		}
		name := string(b[:end])
		if strings.ContainsAny(name, "/$\x00\r\n\t ") || seenNeeded[name] {
			return fail()
		}
		seenNeeded[name] = true
		out.Needed = append(out.Needed, name)
	}
	return out, nil
}
func nativeBirthBoundedELF(f *os.File) error { _, err := nativeBirthParseELF(f); return err }

func nativeBirthVerifyClosure(m nativeBirthBuildManifest) (*os.File, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	// glibc默认目录不是任意请求路径；cache、preload或hwcaps存在时本profile不支持。
	for _, path := range []string{"/etc/ld.so.cache", "/etc/ld.so.preload"} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			return nil, fmt.Errorf("native default-loader profile rejects cache/preload or unknown")
		}
		if err := nativeBirthReadonlyPath(path); err != nil {
			return nil, err
		}
	}
	for _, dir := range nativeBirthLoaderDirs {
		if _, err := os.Lstat(dir); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return nil, err
		}
		if err := nativeBirthReadonlyPath(filepath.Join(dir, "placeholder")); err != nil {
			return nil, err
		}
		if _, err := os.Lstat(filepath.Join(dir, "glibc-hwcaps")); !os.IsNotExist(err) {
			return nil, fmt.Errorf("native default-loader profile rejects hwcaps")
		}
	}
	resolved, err := nativeBirthCanonical(m.LaunchPath)
	if err != nil || resolved != m.PostgresPath {
		return nil, fmt.Errorf("native fixed PG alias differs")
	}
	files := map[string]*os.File{}
	elfs := map[string]nativeBirthELFMetadata{}
	success := false
	defer func() {
		for path, f := range files {
			if !success || path != m.PostgresPath {
				f.Close()
			}
		}
	}()
	for _, item := range m.Files {
		real, err := nativeBirthCanonical(item.Path)
		if err != nil || real != item.Path {
			return nil, fmt.Errorf("manifest file is not canonical")
		}
		f, err := nativeBirthOpenRO(item.Path)
		if err != nil {
			return nil, err
		}
		files[item.Path] = f
		h, err := nativeBirthFileHash(f)
		if err != nil || h != item.SHA256 {
			return nil, fmt.Errorf("native pre-main file digest differs")
		}
		e, err := nativeBirthParseELF(f)
		if err != nil {
			return nil, err
		}
		elfs[item.Path] = e
	}
	seen := map[string]bool{}
	var visit func(string) error
	visit = func(path string) error {
		if seen[path] {
			return nil
		}
		seen[path] = true
		e, ok := elfs[path]
		if !ok {
			return fmt.Errorf("native pre-main closure missing resolved ELF")
		}
		for _, name := range e.Needed {
			if name == "" || strings.ContainsAny(name, "/$\x00") {
				return fmt.Errorf("native needed name unsupported")
			}
			found := ""
			for _, dir := range nativeBirthLoaderDirs {
				candidate := filepath.Join(dir, name)
				if _, err := os.Lstat(candidate); os.IsNotExist(err) {
					continue
				} else if err != nil {
					return err
				}
				resolved, err := nativeBirthCanonical(candidate)
				if err != nil {
					return err
				}
				found = resolved
				break
			}
			if found == "" {
				return fmt.Errorf("native dependency not found in fixed default loader paths")
			}
			if err := visit(found); err != nil {
				return err
			}
		}
		return nil
	}
	entry := elfs[m.PostgresPath]
	actualInterpreter := ""
	if entry.Interpreter != "" {
		actualInterpreter, err = nativeBirthCanonical(entry.Interpreter)
		if err != nil {
			return nil, err
		}
	}
	if actualInterpreter != m.InterpreterPath {
		return nil, fmt.Errorf("native interpreter differs from manifest")
	}
	if err := visit(m.PostgresPath); err != nil {
		return nil, err
	}
	if actualInterpreter != "" {
		if err := visit(actualInterpreter); err != nil {
			return nil, err
		}
	}
	if len(seen) != len(files) {
		return nil, fmt.Errorf("native manifest contains unrelated or unreached ELF")
	}
	success = true
	return files[m.PostgresPath], nil
}

// 只读原ISO+封存runtime+原RO构建；返回的PG FD必须由持resource flock消费者
// 每阶段重新核来源并关闭。此入口本身不保存Birth，不启动PG，不发任何normal许可。
func readNativeBirthOriginalSource() (nativeBirthSource, *os.File, error) {
	var zero nativeBirthSource
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return zero, nil, fmt.Errorf("native source requires real root")
	}
	runtimeRaw, err := nativeBirthReadISO(FixedRuntimePath, 64<<10)
	if err != nil {
		return zero, nil, err
	}
	if err := matchSealedFile(FixedSealedRuntimePath, runtimeRaw); err != nil {
		return zero, nil, err
	}
	policyRaw, err := nativeBirthReadISO(fixedNativeBirthPolicyPath, 16<<10)
	if err != nil {
		return zero, nil, err
	}
	f, err := nativeBirthOpenRO(fixedNativeBirthBuildPath)
	if err != nil {
		return zero, nil, err
	}
	manifestRaw, err := nativeBirthReadBounded(f, 32<<10)
	f.Close()
	if err != nil {
		return zero, nil, err
	}
	source, err := decodeNativeBirthSource(runtimeRaw, policyRaw, manifestRaw)
	if err != nil {
		return zero, nil, err
	}
	sealed, err := sealedRuntimeSource()
	if err != nil || !reflect.DeepEqual(source.resource, sealed) {
		return zero, nil, fmt.Errorf("native sealed resource source differs")
	}
	pg, err := nativeBirthVerifyClosure(source.manifest)
	if err != nil {
		return zero, nil, err
	}
	return source, pg, nil
}
