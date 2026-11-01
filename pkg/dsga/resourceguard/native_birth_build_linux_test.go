//go:build linux && amd64

package resourceguard

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// 最小静态ELF仅测collector流程，不执行、不声明真实发行来源。真实GNU16闭包另核。
func nativeBuildCollectorFixture(t *testing.T) ([]byte, []byte, string, func(string) (string, error), func(string) (*os.File, error), func(string) (os.FileInfo, error)) {
	t.Helper()
	root := t.TempDir()
	b := make([]byte, 64)
	copy(b, []byte{0x7f, 'E', 'L', 'F', 2, 1, 1})
	binary.LittleEndian.PutUint16(b[16:18], 2)
	binary.LittleEndian.PutUint16(b[18:20], 62)
	binary.LittleEndian.PutUint32(b[20:24], 1)
	binary.LittleEndian.PutUint16(b[52:54], 64)
	file := filepath.Join(root, "postgres")
	require.NoError(t, os.WriteFile(file, b, 0555))
	seed, e := json.Marshal(nativeBirthBuildManifest{ProtocolVersion: 1, Scope: nativeBirthBuildScope, PGmajor: 17, LaunchPath: FixedPostgresPath, PostgresPath: FixedPostgresPath, Files: []nativeBirthBuildFile{{Path: FixedPostgresPath, SHA256: strings.Repeat("12", 32)}}})
	require.NoError(t, e)
	controls, e := NormalSystemOrdinaryV2ManifestJSON()
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(root, "controls"), controls, 0444))
	systemManifest := normalSystemV3ModelManifest(t)
	for i := range systemManifest.Files {
		if systemManifest.Files[i].Path == NormalSystemOrdinaryV2ManifestPath {
			systemManifest.Files[i].Bytes = uint64(len(controls))
			systemManifest.Files[i].SHA256 = physicalDigest(controls)
		}
	}
	system, e := json.Marshal(systemManifest)
	require.NoError(t, e)
	system = append(system, '\n')
	canonical := func(path string) (string, error) {
		if path == "/etc" || path == NormalSystemOrdinaryV2ManifestPath {
			return path, nil
		}
		if path != FixedPostgresPath {
			return "", &os.PathError{Op: "fixture", Path: path, Err: os.ErrNotExist}
		}
		return path, nil
	}
	open := func(path string) (*os.File, error) {
		if path == NormalSystemOrdinaryV2ManifestPath {
			return os.Open(filepath.Join(root, "controls"))
		}
		if path != FixedPostgresPath {
			return nil, os.ErrNotExist
		}
		return os.Open(file)
	}
	lstat := func(path string) (os.FileInfo, error) {
		return nil, &os.PathError{Op: "fixture", Path: path, Err: os.ErrNotExist}
	}
	return seed, system, file, canonical, open, lstat
}

func TestNativeBirthBuildCollectorExactBytes(t *testing.T) {
	seed, system, file, canonical, open, lstat := nativeBuildCollectorFixture(t)
	canonicalWithEtc := func(path string) (string, error) {
		if path == "/etc" {
			return path, nil
		}
		return canonical(path)
	}
	raw, e := BuildNativeBirthManifestJSON(seed, system, canonicalWithEtc, open, lstat)
	require.NoError(t, e)
	var got nativeBirthBuildManifest
	require.NoError(t, json.Unmarshal(raw, &got))
	original, e := os.ReadFile(file)
	require.NoError(t, e)
	require.Equal(t, []nativeBirthBuildFile{{Path: FixedPostgresPath, SHA256: physicalDigest(original)}}, got.Files)
	require.Empty(t, got.InterpreterPath)
	require.Equal(t, physicalDigest(system), got.RuntimeClosure.SystemManifestSHA256)
	require.NotEqual(t, physicalDigest(system[:len(system)-1]), got.RuntimeClosure.SystemManifestSHA256)
	again, e := BuildNativeBirthManifestJSON(seed, system, canonicalWithEtc, open, lstat)
	require.NoError(t, e)
	require.Equal(t, raw, again)
}

func TestNativeBirthBuildCollectorRejectsOriginalInputChanges(t *testing.T) {
	for _, name := range []string{"PG18", "nonregular", "tooLarge", "changedOFD", "foreignPath", "cache", "preload", "system2", "controls1", "helperMismatch"} {
		t.Run(name, func(t *testing.T) {
			seed, system, file, canonical, open, lstat := nativeBuildCollectorFixture(t)
			baseCanonical := canonical
			canonical = func(path string) (string, error) {
				if path == "/etc" {
					return path, nil
				}
				return baseCanonical(path)
			}
			switch name {
			case "PG18":
				seed = []byte(strings.Replace(string(seed), `"pg_major":17`, `"pg_major":18`, 1))
			case "nonregular":
				open = func(string) (*os.File, error) { return os.Open(filepath.Dir(file)) }
			case "tooLarge":
				require.NoError(t, os.Truncate(file, 257<<20))
			case "changedOFD":
				old := open
				count := 0
				open = func(path string) (*os.File, error) {
					if path == FixedPostgresPath {
						count++
					}
					if path == FixedPostgresPath && count == 2 {
						raw, e := os.ReadFile(file)
						require.NoError(t, e)
						require.NoError(t, os.WriteFile(file+".new", raw, 0555))
						require.NoError(t, os.Rename(file+".new", file))
					}
					return old(path)
				}
			case "foreignPath":
				canonical = func(string) (string, error) { return "/foreign", nil }
			case "cache", "preload":
				bad := "/etc/ld.so.cache"
				if name == "preload" {
					bad = "/etc/ld.so.preload"
				}
				old := lstat
				lstat = func(path string) (os.FileInfo, error) {
					if path == bad {
						return os.Stat(file)
					}
					return old(path)
				}
			case "system2":
				system = []byte(strings.Replace(string(system), `"protocol_version":3`, `"protocol_version":2`, 1))
			case "controls1":
				controls, e := NormalKernelControlSourceManifestJSON()
				require.NoError(t, e)
				require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(file), "controls"), controls, 0444))
				var m NormalSystemSourceManifest
				require.NoError(t, json.Unmarshal(system, &m))
				for i := range m.Files {
					if m.Files[i].Path == NormalSystemOrdinaryV2ManifestPath {
						m.Files[i].Bytes = uint64(len(controls))
						m.Files[i].SHA256 = physicalDigest(controls)
					}
				}
				system, _ = json.Marshal(m)
			case "helperMismatch":
				var m nativeBirthBuildManifest
				require.NoError(t, json.Unmarshal(seed, &m))
				m.ArrayHelper = &nativeArrayHelperBuild{ProtocolVersion: 1, Path: fixedArrayHelperPath, SHA256: strings.Repeat("34", 32)}
				seed, _ = json.Marshal(m)
			}
			raw, e := BuildNativeBirthManifestJSON(seed, system, canonical, open, lstat)
			require.Error(t, e)
			require.Nil(t, raw)
		})
	}
}

func TestNativeBirthBuildCollectorReaderErrorsNeverDisappear(t *testing.T) {
	seed, system, _, canonical, open, lstat := nativeBuildCollectorFixture(t)
	for _, operation := range []string{"controlLstat", "canonical", "open"} {
		t.Run(operation, func(t *testing.T) {
			c, o, l := canonical, open, lstat
			switch operation {
			case "controlLstat":
				l = func(string) (os.FileInfo, error) { return nil, os.ErrPermission }
			case "canonical":
				c = func(string) (string, error) { return "", os.ErrPermission }
			case "open":
				c = func(path string) (string, error) { return path, nil }
				o = func(string) (*os.File, error) { return nil, fmt.Errorf("original reader denied") }
			}
			raw, e := BuildNativeBirthManifestJSON(seed, system, c, o, l)
			require.Error(t, e)
			require.Nil(t, raw)
		})
	}
}

// 原真实动态ELF只作有界图输入，不执行。64包含PG、解释器与所有递归库，
// 超界不是只把seed中的预先列举数量改大；实际collector必须遍历至第65对象才拒。
func TestNativeBirthBuildCollectorRecursive64ObjectBoundary(t *testing.T) {
	original, e := os.ReadFile("/bin/true")
	require.NoError(t, e)
	needle := []byte("libc.so.6\x00")
	offset := bytes.Index(original, needle)
	require.Greater(t, offset, 0)
	order := binary.LittleEndian
	phoff := int(order.Uint64(original[32:40]))
	phnum := int(order.Uint16(original[56:58]))
	neededTag := -1
	for i := 0; i < phnum; i++ {
		ph := original[phoff+i*56 : phoff+(i+1)*56]
		if order.Uint32(ph[:4]) != 2 {
			continue
		}
		start := int(order.Uint64(ph[8:16]))
		size := int(order.Uint64(ph[32:40]))
		for j := start; j < start+size; j += 16 {
			if order.Uint64(original[j:j+8]) == 1 {
				neededTag = j
				break
			}
		}
	}
	require.Greater(t, neededTag, 0)
	for _, total := range []int{64, 65} {
		t.Run(fmt.Sprint(total), func(t *testing.T) {
			seed, system, pg, _, originalOpen, _ := nativeBuildCollectorFixture(t)
			dir := filepath.Dir(pg)
			files := map[string]string{}
			libraries := total - 2
			for i := 0; i <= libraries; i++ {
				b := append([]byte(nil), original...)
				if i == libraries {
					order.PutUint64(b[neededTag:neededTag+8], 21)
				} else {
					copy(b[offset:offset+9], []byte(fmt.Sprintf("f%03d.so.x", i+1)))
				}
				path := FixedPostgresPath
				if i > 0 {
					path = fmt.Sprintf("/lib/x86_64-linux-gnu/f%03d.so.x", i)
				}
				actual := filepath.Join(dir, fmt.Sprintf("obj-%03d", i))
				require.NoError(t, os.WriteFile(actual, b, 0555))
				files[path] = actual
			}
			loader := "/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2"
			files[loader] = "/lib64/ld-linux-x86-64.so.2"
			canonical := func(path string) (string, error) {
				if path == "/etc" || path == "/lib/x86_64-linux-gnu" || path == NormalSystemOrdinaryV2ManifestPath {
					return path, nil
				}
				if path == "/lib64/ld-linux-x86-64.so.2" {
					return loader, nil
				}
				if _, ok := files[path]; ok {
					return path, nil
				}
				return "", os.ErrNotExist
			}
			open := func(path string) (*os.File, error) {
				if path == NormalSystemOrdinaryV2ManifestPath {
					return originalOpen(path)
				}
				f, ok := files[path]
				if !ok {
					return nil, os.ErrNotExist
				}
				return os.Open(f)
			}
			lstat := func(path string) (os.FileInfo, error) {
				if path == "/lib/x86_64-linux-gnu" {
					return os.Stat(dir)
				}
				if f, ok := files[path]; ok {
					return os.Stat(f)
				}
				return nil, os.ErrNotExist
			}
			raw, e := BuildNativeBirthManifestJSON(seed, system, canonical, open, lstat)
			if total == 64 {
				require.NoError(t, e)
				var m nativeBirthBuildManifest
				require.NoError(t, json.Unmarshal(raw, &m))
				require.Len(t, m.Files, 64)
			} else {
				require.Error(t, e)
				require.Nil(t, raw)
			}
		})
	}
}
