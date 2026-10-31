//go:build linux

package resourceguard

import (
	"crypto/sha256"
	"debug/elf"
	"encoding/binary"
	"encoding/hex"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

// 只读真实/bin/true ELF作元数据正控，从不执行它或任意变异ELF；恶意metadata
// 只交给本私有raw parser，绝不交Go解压器。此U不证明真实发行RO闭包/FS/I。
func TestNativeBirthSourceELFDynamicMetadata(t *testing.T) {
	original, err := os.ReadFile("/bin/true")
	require.NoError(t, err)
	h := sha256.Sum256(original)
	t.Logf("actual genuine dynamic ELF=/bin/true sha256=%s", hex.EncodeToString(h[:]))
	order := binary.LittleEndian
	phoff := int(order.Uint64(original[32:40]))
	phnum := int(order.Uint16(original[56:58]))
	shoff := int(order.Uint64(original[40:48]))
	shnum := int(order.Uint16(original[60:62]))
	namesIndex := int(order.Uint16(original[62:64]))
	section := func(raw []byte, index int) []byte { return raw[shoff+index*64 : shoff+(index+1)*64] }
	dynamicIndex := -1
	dynamicProgram := -1
	spareProgram := -1
	loadProgram := -1
	for i := 0; i < shnum; i++ {
		if order.Uint32(section(original, i)[4:8]) == uint32(elf.SHT_DYNAMIC) {
			dynamicIndex = i
		}
	}
	require.GreaterOrEqual(t, dynamicIndex, 0)
	dynamic := section(original, dynamicIndex)
	stringIndex := int(order.Uint32(dynamic[40:44]))
	dynOff := int(order.Uint64(dynamic[24:32]))
	dynSize := int(order.Uint64(dynamic[32:40]))
	for i := 0; i < phnum; i++ {
		raw := original[phoff+i*56 : phoff+(i+1)*56]
		kind := order.Uint32(raw[:4])
		if kind == uint32(elf.PT_DYNAMIC) {
			dynamicProgram = i
		}
		if kind == uint32(elf.PT_LOAD) && loadProgram < 0 {
			loadProgram = i
		}
		if kind == uint32(elf.PT_NOTE) {
			spareProgram = i
		}
	}
	require.GreaterOrEqual(t, dynamicProgram, 0)
	require.GreaterOrEqual(t, spareProgram, 0)
	require.GreaterOrEqual(t, loadProgram, 0)
	tagIndex := func(raw []byte, tag elf.DynTag) int {
		for i := dynOff; i < dynOff+dynSize; i += 16 {
			if order.Uint64(raw[i:i+8]) == uint64(tag) {
				return i
			}
		}
		t.Fatalf("genuine fixture lacks required tag %v", tag)
		return -1
	}
	parse := func(t *testing.T, raw []byte) (nativeBirthELFMetadata, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "metadata-only.elf")
		require.NoError(t, os.WriteFile(path, raw, 0600))
		f, err := os.Open(path)
		require.NoError(t, err)
		defer f.Close()
		return nativeBirthParseELF(f)
	}
	genuine, err := parse(t, original)
	require.NoError(t, err)
	require.Contains(t, genuine.Needed, "libc.so.6")
	// 独立标准库仅接原始真实ELF作正控，绝不接下方变异输入。
	f, err := os.Open("/bin/true")
	require.NoError(t, err)
	defer f.Close()
	standard, err := elf.NewFile(f)
	require.NoError(t, err)
	expected, err := standard.ImportedLibraries()
	require.NoError(t, err)
	require.Equal(t, expected, genuine.Needed)
	renamed := func(raw []byte) {
		names := section(raw, namesIndex)
		off := int(order.Uint64(names[24:32])) + int(order.Uint32(section(raw, dynamicIndex)[:4]))
		raw[off] = 'X'
	}
	legacyName := func(raw []byte, index int, name string) []byte {
		names := section(raw, namesIndex)
		off, size := int(order.Uint64(names[24:32])), int(order.Uint64(names[32:40]))
		blob := append([]byte(nil), raw[off:off+size]...)
		blob = append(blob, []byte(name)...)
		blob = append(blob, 0)
		newOff := len(raw)
		raw = append(raw, blob...)
		order.PutUint64(section(raw, namesIndex)[24:32], uint64(newOff))
		order.PutUint64(section(raw, namesIndex)[32:40], uint64(len(blob)))
		order.PutUint32(section(raw, index)[:4], uint32(size))
		return raw
	}
	t.Run("normal-dynamic-renamed-section", func(t *testing.T) {
		raw := append([]byte(nil), original...)
		renamed(raw)
		got, err := parse(t, raw)
		require.NoError(t, err)
		require.Equal(t, genuine, got)
	})
	mutations := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"wrong-dynamic-link", func(raw []byte) []byte {
			order.PutUint32(section(raw, dynamicIndex)[40:44], uint32(namesIndex))
			return raw
		}},
		{"renamed-corrupt-dynamic", func(raw []byte) []byte {
			renamed(raw)
			i := tagIndex(raw, elf.DT_STRSZ)
			order.PutUint64(raw[i+8:i+16], 0)
			return raw
		}},
		{"wrong-real-strtab", func(raw []byte) []byte {
			i := tagIndex(raw, elf.DT_STRTAB)
			order.PutUint64(raw[i+8:i+16], order.Uint64(raw[i+8:i+16])+1)
			return raw
		}},
		{"wrong-real-strsz", func(raw []byte) []byte {
			i := tagIndex(raw, elf.DT_STRSZ)
			order.PutUint64(raw[i+8:i+16], order.Uint64(raw[i+8:i+16])+1)
			return raw
		}},
		{"compressed-shstr-huge-expanded-size", func(raw []byte) []byte {
			sh := section(raw, namesIndex)
			order.PutUint64(sh[8:16], order.Uint64(sh[8:16])|uint64(elf.SHF_COMPRESSED))
			off := int(order.Uint64(sh[24:32]))
			order.PutUint32(raw[off:off+4], 1)
			order.PutUint64(raw[off+8:off+16], 1<<40)
			return raw
		}},
		{"compressed-dynstr", func(raw []byte) []byte {
			sh := section(raw, stringIndex)
			order.PutUint64(sh[8:16], order.Uint64(sh[8:16])|uint64(elf.SHF_COMPRESSED))
			return raw
		}},
		{"legacy-compressed-shstr", func(raw []byte) []byte { return legacyName(raw, namesIndex, ".zdebug_shstr") }},
		{"legacy-compressed-dynstr", func(raw []byte) []byte { return legacyName(raw, stringIndex, ".zdebug_dynstr") }},
		{"ambiguous-load-map", func(raw []byte) []byte {
			copy(raw[phoff+spareProgram*56:phoff+(spareProgram+1)*56], raw[phoff+loadProgram*56:phoff+(loadProgram+1)*56])
			return raw
		}},
		{"dynamic-address-map-mismatch", func(raw []byte) []byte {
			ph := raw[phoff+dynamicProgram*56 : phoff+(dynamicProgram+1)*56]
			order.PutUint64(ph[16:24], order.Uint64(ph[16:24])+1)
			sh := section(raw, dynamicIndex)
			order.PutUint64(sh[16:24], order.Uint64(sh[16:24])+1)
			return raw
		}},
	}

	// 后一个LOAD只覆盖一字节，旧“完整覆盖target才计数”会漏它；实际MAP_FIXED
	// 会从不同文件页覆盖整个字符串虚页。zero-fill同页也不能被忽略。
	for _, zeroFill := range []bool{false, true} {
		zeroFill := zeroFill
		name := "partial-one-byte-other-file-page"
		if zeroFill {
			name = "partial-zero-fill-same-virtual-page"
		}
		mutations = append(mutations, struct {
			name   string
			mutate func([]byte) []byte
		}{name: name, mutate: func(raw []byte) []byte {
			target := order.Uint64(raw[tagIndex(raw, elf.DT_STRTAB)+8 : tagIndex(raw, elf.DT_STRTAB)+16])
			load := raw[phoff+spareProgram*56 : phoff+(spareProgram+1)*56]
			for i := range load {
				load[i] = 0
			}
			order.PutUint32(load[:4], uint32(elf.PT_LOAD))
			order.PutUint32(load[4:8], 4)
			order.PutUint64(load[8:16], uint64(0x2000)+target%4096)
			order.PutUint64(load[16:24], target)
			order.PutUint64(load[24:32], target)
			count := uint64(1)
			if zeroFill {
				count = 0
			}
			order.PutUint64(load[32:40], count)
			order.PutUint64(load[40:48], 1)
			order.PutUint64(load[48:56], 4096)
			return raw
		}})
	}
	for _, tag := range []elf.DynTag{elf.DT_RPATH, elf.DT_RUNPATH, elf.DT_AUDIT, elf.DT_DEPAUDIT, elf.DT_FILTER, elf.DT_AUXILIARY} {
		tag := tag
		mutations = append(mutations, struct {
			name   string
			mutate func([]byte) []byte
		}{name: "forbidden-raw-" + tag.String(), mutate: func(raw []byte) []byte {
			i := tagIndex(raw, elf.DT_DEBUG)
			order.PutUint64(raw[i:i+8], uint64(tag))
			order.PutUint64(raw[i+8:i+16], 0)
			return raw
		}})
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.mutate(append([]byte(nil), original...))
			_, err := parse(t, raw)
			require.Error(t, err, "malformed or forbidden loader metadata must not appear dependency-free")
		})
	}
}
