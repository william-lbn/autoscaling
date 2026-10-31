//go:build linux && dsga_native_normal_system_source

package resourceguard

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// 只导出显式独立夹具，所有服务仍是测试ELF，绝不称成真实System服务/PG认证。
func TestNativeNormalSystemV2Fixture(t *testing.T) {
	TestNativeNormalSystemSourceFixture(t)
	root := "/out/system-fixture"
	p := filepath.Join(root, NormalSystemSourceManifestPath)
	raw, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	m, e := DecodeNormalSystemSourceManifest(raw)
	if e != nil {
		t.Fatal(e)
	}
	m.ProtocolVersion = 2
	m.ProfileID = NormalSystemSourceV2ProfileID
	rows, e := NormalSystemSourceInitEntriesForVersion(2)
	if e != nil {
		t.Fatal(e)
	}
	b := []byte(strings.Join(rows, "\n") + "\n")
	if e := os.WriteFile(filepath.Join(root, "/etc/inittab"), b, 0644); e != nil {
		t.Fatal(e)
	}
	for i := range m.Files {
		if m.Files[i].Path == "/etc/inittab" {
			m.Files[i].Bytes = uint64(len(b))
			m.Files[i].SHA256 = physicalDigest(b)
		}
	}
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	elf, e := os.ReadFile(exe)
	if e != nil {
		t.Fatal(e)
	}
	const path = "/neonvm/bin/su-exec"
	if e := os.WriteFile(filepath.Join(root, path), elf, 0755); e != nil {
		t.Fatal(e)
	}
	m.Files = append(m.Files, NormalSystemSourceFile{Path: path, CanonicalPath: path, Kind: "exec", Bytes: uint64(len(elf)), SHA256: physicalDigest(elf)})
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	if e := m.Validate(); e != nil {
		t.Fatal(e)
	}
	raw, e = json.Marshal(m)
	if e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(p, raw, 0644); e != nil {
		t.Fatal(e)
	}
}
func TestNativeNormalSystemV2Component(t *testing.T) {
	raw, e := os.ReadFile(NormalSystemSourceManifestPath)
	if e != nil {
		t.Fatal(e)
	}
	m, e := DecodeNormalSystemSourceManifest(raw)
	if e != nil {
		t.Fatal(e)
	}
	if m.ProtocolVersion != 2 || m.ProfileID != NormalSystemSourceV2ProfileID {
		t.Fatal("actual v2 fixture required")
	}
	// 同真实生产reader与原8个物理负例，不把v1旧binary转签v2。
	TestNativeNormalSystemSourceComponent(t)
	if _, e := CheckNormalSystemSource(context.Background(), raw); e != nil {
		t.Fatal(e)
	}
}
