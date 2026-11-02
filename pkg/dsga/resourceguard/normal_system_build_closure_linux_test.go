//go:build linux && amd64

package resourceguard

import (
	"bytes"
	"debug/elf"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// 最小静态文件只测构建器，绝不执行或冒Source；真实三profile由原105库存另核。
func systemBuildStaticELF() []byte {
	b := make([]byte, 64)
	copy(b, []byte{0x7f, 'E', 'L', 'F', 2, 1, 1})
	binary.LittleEndian.PutUint16(b[16:18], 2)
	binary.LittleEndian.PutUint16(b[18:20], 62)
	binary.LittleEndian.PutUint32(b[20:24], 1)
	binary.LittleEndian.PutUint16(b[52:54], 64)
	return b
}
func systemBuildFixture(t *testing.T) (string, NormalSystemSourceManifest, func(string) (string, error), func(string) (*os.File, error), func(string) (os.FileInfo, error)) {
	t.Helper()
	root := t.TempDir()
	m := normalSystemV3ModelManifest(t)
	for i := range m.Files {
		entry := &m.Files[i]
		raw := []byte("BUILD_ONLY_STATIC_MODEL\n")
		if entry.Kind == "exec" {
			raw = systemBuildStaticELF()
		}
		if entry.Kind == "script" {
			raw = []byte("#!/neonvm/bin/sh\nexit 0\n")
		}
		if entry.Path == NormalSystemOrdinaryV2ManifestPath {
			var e error
			raw, e = NormalSystemOrdinaryV2ManifestJSON()
			require.NoError(t, e)
		}
		path := filepath.Join(root, entry.CanonicalPath)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, raw, 0555))
		entry.Bytes = uint64(len(raw))
		entry.SHA256 = physicalDigest(raw)
	}
	canonical := func(path string) (string, error) {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return "", os.ErrInvalid
		}
		actual, e := filepath.EvalSymlinks(filepath.Join(root, path))
		if e != nil {
			return "", e
		}
		relative, e := filepath.Rel(root, actual)
		if e != nil || relative == ".." || strings.HasPrefix(relative, "../") {
			return "", os.ErrPermission
		}
		return "/" + filepath.ToSlash(relative), nil
	}
	open := func(path string) (*os.File, error) {
		return os.OpenFile(filepath.Join(root, path), os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	}
	lstat := func(path string) (os.FileInfo, error) { return os.Lstat(filepath.Join(root, path)) }
	return root, m, canonical, open, lstat
}
func systemBuildMarshal(t *testing.T, m NormalSystemSourceManifest) []byte {
	t.Helper()
	raw, e := json.Marshal(m)
	require.NoError(t, e)
	return raw
}
func systemBuildChangeFile(t *testing.T, root string, m *NormalSystemSourceManifest, path string, raw []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(root, path), raw, 0555))
	for i := range m.Files {
		if m.Files[i].Path == path {
			m.Files[i].Bytes = uint64(len(raw))
			m.Files[i].SHA256 = physicalDigest(raw)
			return
		}
	}
	t.Fatal("fixture file missing", path)
}

func TestNormalSystemBuildSharedThreeLoaderProfiles(t *testing.T) {
	for _, c := range []struct{ interp, profile string }{{"", "static"}, {"/lib64/ld-linux-x86-64.so.2", "glibc"}, {"/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2", "glibc"}, {"/lib/ld-musl-x86_64.so.1", "musl"}} {
		t.Run(c.profile+c.interp, func(t *testing.T) {
			p, e := normalSystemLoaderProfile(nativeBirthELFMetadata{Interpreter: c.interp})
			require.NoError(t, e)
			require.Equal(t, c.profile, p)
			dirs, e := normalSystemLoaderDirectories(nativeBirthELFMetadata{}, p)
			require.NoError(t, e)
			if p == "glibc" {
				require.Equal(t, nativeBirthLoaderDirs, dirs)
				dirs[0] = "MUTATED_COPY"
				require.NotEqual(t, "MUTATED_COPY", nativeBirthLoaderDirs[0])
			}
			if p == "musl" {
				require.Equal(t, []string{"/lib", "/usr/local/lib", "/usr/lib"}, dirs)
			}
		})
	}
	_, e := normalSystemLoaderProfile(nativeBirthELFMetadata{Interpreter: "/tmp/ld.so"})
	require.Error(t, e)
	_, e = normalSystemLoaderDirectories(nativeBirthELFMetadata{Needed: []string{"libc.so.6"}}, "static")
	require.Error(t, e)
	_, e = normalSystemLoaderDirectories(nativeBirthELFMetadata{}, "other")
	require.Error(t, e)
}
func TestNormalSystemBuildStaticInventoryAndExactMetadata18c(t *testing.T) {
	_, m, c, o, l := systemBuildFixture(t)
	before := systemBuildMarshal(t, m)
	raw, e := BuildNormalSystemClosureManifestJSON(before, c, o, l)
	require.NoError(t, e)
	got, e := DecodeNormalSystemSourceManifest(raw)
	require.NoError(t, e)
	expected, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	require.NoError(t, e)
	require.Equal(t, expected, got.Kernel)
	require.NotEqual(t, m.Kernel, got.Kernel)
	require.Equal(t, m.Files, got.Files)
	require.Equal(t, before, systemBuildMarshal(t, m))
	again, e := BuildNormalSystemClosureManifestJSON(before, c, o, l)
	require.NoError(t, e)
	require.Equal(t, raw, again)
}
func TestNormalSystemBuildRejectsDeclaredDataAndUnsupportedTags(t *testing.T) {
	for _, name := range []string{"wrongDigest", "wrongAlias", "controls1", "wrongScript", "unusedLoader", "RPATH"} {
		t.Run(name, func(t *testing.T) {
			root, m, c, o, l := systemBuildFixture(t)
			switch name {
			case "wrongDigest":
				m.Files[0].SHA256 = physicalDigest([]byte("different"))
			case "wrongAlias":
				old := c
				c = func(path string) (string, error) {
					if path == "/neonvm/bin/neonvmd" {
						return "/foreign", nil
					}
					return old(path)
				}
			case "controls1":
				raw, e := NormalKernelControlSourceManifestJSON()
				require.NoError(t, e)
				systemBuildChangeFile(t, root, &m, NormalSystemOrdinaryV2ManifestPath, raw)
			case "wrongScript":
				systemBuildChangeFile(t, root, &m, "/neonvm/bin/vmstart", []byte("#!/bin/sh\nexit 0\n"))
			case "unusedLoader":
				path := "/lib/unrelated.so"
				raw := systemBuildStaticELF()
				require.NoError(t, os.MkdirAll(filepath.Join(root, "/lib"), 0755))
				require.NoError(t, os.WriteFile(filepath.Join(root, path), raw, 0555))
				m.Files = append(m.Files, NormalSystemSourceFile{Path: path, CanonicalPath: path, Kind: "loader", Bytes: uint64(len(raw)), SHA256: physicalDigest(raw)})
				sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
			case "RPATH":
				raw, e := os.ReadFile("/bin/true")
				require.NoError(t, e)
				order := binary.LittleEndian
				offset := int(order.Uint64(raw[32:40]))
				count := int(order.Uint16(raw[56:58]))
				found := false
				for i := 0; i < count; i++ {
					ph := raw[offset+i*56 : offset+(i+1)*56]
					if order.Uint32(ph[:4]) != uint32(elf.PT_DYNAMIC) {
						continue
					}
					start := int(order.Uint64(ph[8:16]))
					size := int(order.Uint64(ph[32:40]))
					for j := start; j < start+size; j += 16 {
						if order.Uint64(raw[j:j+8]) == uint64(elf.DT_DEBUG) {
							order.PutUint64(raw[j:j+8], uint64(elf.DT_RPATH))
							found = true
							break
						}
					}
				}
				require.True(t, found)
				systemBuildChangeFile(t, root, &m, "/usr/local/bin/compute_ctl", raw)
			}
			raw, e := BuildNormalSystemClosureManifestJSON(systemBuildMarshal(t, m), c, o, l)
			require.Error(t, e)
			require.Nil(t, raw)
		})
	}
}
func TestNormalSystemBuildRejectsNonregularAndChangedOriginalFiles(t *testing.T) {
	for _, name := range []string{"FIFO", "changedFinalPath", "readDenied"} {
		t.Run(name, func(t *testing.T) {
			root, m, c, o, l := systemBuildFixture(t)
			const path = "/neonvm/bin/neonvmd"
			switch name {
			case "FIFO":
				require.NoError(t, os.Remove(filepath.Join(root, path)))
				require.NoError(t, syscall.Mkfifo(filepath.Join(root, path), 0600))
			case "changedFinalPath":
				old := o
				count := 0
				o = func(p string) (*os.File, error) {
					if p == path {
						count++
						if count == 2 {
							raw, e := os.ReadFile(filepath.Join(root, path))
							require.NoError(t, e)
							require.NoError(t, os.WriteFile(filepath.Join(root, path)+".replacement", raw, 0555))
							require.NoError(t, os.Rename(filepath.Join(root, path)+".replacement", filepath.Join(root, path)))
						}
					}
					return old(p)
				}
			case "readDenied":
				old := o
				o = func(p string) (*os.File, error) {
					if p == path {
						return nil, os.ErrPermission
					}
					return old(p)
				}
			}
			raw, e := BuildNormalSystemClosureManifestJSON(systemBuildMarshal(t, m), c, o, l)
			require.Error(t, e)
			require.Nil(t, raw)
		})
	}
}
func TestNormalSystemBuildRejectsUnknownInsteadOfSkipping(t *testing.T) {
	_, m, c, o, l := systemBuildFixture(t)
	for _, name := range []string{"lstatDenied", "canonicalDenied", "version2"} {
		t.Run(name, func(t *testing.T) {
			cc, ll := c, l
			input := systemBuildMarshal(t, m)
			switch name {
			case "lstatDenied":
				ll = func(string) (os.FileInfo, error) { return nil, os.ErrPermission }
			case "canonicalDenied":
				cc = func(string) (string, error) { return "", os.ErrPermission }
			case "version2":
				input = bytes.Replace(input, []byte(`"protocol_version":3`), []byte(`"protocol_version":2`), 1)
			}
			raw, e := BuildNormalSystemClosureManifestJSON(input, cc, o, ll)
			require.Error(t, e)
			require.Nil(t, raw)
		})
	}
}
