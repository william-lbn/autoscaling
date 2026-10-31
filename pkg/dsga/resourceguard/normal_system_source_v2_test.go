package resourceguard

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

func TestNormalSystemSourceV2FixedGatesAndV1Compatibility(t *testing.T) {
	old := NormalSystemSourceInitEntries()
	v1, e := NormalSystemSourceInitEntriesForVersion(1)
	if e != nil || strings.Join(old, "\n") != strings.Join(v1, "\n") {
		t.Fatal("old table changed")
	}
	v2, e := NormalSystemSourceInitEntriesForVersion(2)
	if e != nil || len(v2) != 13 {
		t.Fatal("new table")
	}
	for i := 1; i < 12; i++ {
		if !strings.Contains(v2[i], "/neonvm/bin/sh /neonvm/bin/dsga-resource-service ") {
			t.Fatal("service bypasses fixed dispatcher")
		}
	}
	if e := normalSystemCheckVersionInittab(2, []byte(strings.Join(v2, "\n")+"\n")); e != nil {
		t.Fatal(e)
	}
	if normalSystemCheckInittab([]byte(strings.Join(v2, "\n"))) == nil || normalSystemCheckVersionInittab(2, []byte(strings.Join(old, "\n"))) == nil {
		t.Fatal("version fallback")
	}
	if _, e := NormalSystemSourceInitEntriesForVersion(3); e == nil {
		t.Fatal("future version accepted")
	}
	m := normalSystemModelManifest(t)
	m.ProtocolVersion = 2
	m.ProfileID = NormalSystemSourceV2ProfileID
	required, _ := NormalSystemSourceRequiredFilesForVersion(2)
	if required["/neonvm/bin/su-exec"] != "exec" {
		t.Fatal("new exact dependency missing")
	}
	for _, f := range m.Files {
		if f.Path == "/neonvm/bin/su-exec" {
			t.Fatal("old fixture silently grew")
		}
	}
	// 仅改版本不能把旧manifest升级；新工具必须真实入表。
	if e := m.Validate(); e == nil {
		t.Fatal("missing v2 exec accepted")
	}
	b, _ := json.Marshal(m)
	if _, e := DecodeNormalSystemSourceManifest(b); e == nil {
		t.Fatal("partial v2 accepted")
	}
}

func TestNormalSystemSourceV2ExactJSON(t *testing.T) {
	m := normalSystemModelManifest(t)
	m.ProtocolVersion = 2
	m.ProfileID = NormalSystemSourceV2ProfileID
	m.Files = append(m.Files, NormalSystemSourceFile{Path: "/neonvm/bin/su-exec", CanonicalPath: "/neonvm/bin/su-exec", Kind: "exec", Bytes: 4, SHA256: physicalDigest([]byte("model"))})
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	b, _ := json.Marshal(m)
	if _, e := DecodeNormalSystemSourceManifest(b); e != nil {
		t.Fatal(e)
	}
	for _, field := range []string{"profile_id", "kernel_release", "path"} {
		t.Run(field, func(t *testing.T) {
			from := []byte(`"` + field + `":`)
			to := []byte(`"` + strings.ToUpper(field) + `":`)
			bad := bytes.Replace(b, from, to, 1)
			if bytes.Equal(b, bad) {
				t.Fatal("fixture key absent")
			}
			if _, e := DecodeNormalSystemSourceManifest(bad); e == nil {
				t.Fatal("v2 semantic alias accepted")
			}
		})
	}
}
