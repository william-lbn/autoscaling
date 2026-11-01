//go:build linux

package resourceguard

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
)

func stockUdevManifestFixture(t *testing.T) normalOrdinaryStockUdevManifest {
	t.Helper()
	m := normalOrdinaryStockUdevManifest{ProtocolVersion: 2, Scope: normalOrdinaryStockUdevManifestScope, PackageLockSHA256: strings.Repeat("1", 64)}
	for _, name := range normalOrdinaryStockUdevSortedRequiredPaths() {
		kind, _, ok := normalOrdinaryStockUdevFileClass(name)
		if !ok {
			t.Fatal(name)
		}
		m.Files = append(m.Files, normalOrdinaryStockUdevFile{Path: name, Kind: kind, Code: kind == "program" || kind == "helper", Bytes: 64, SHA256: strings.Repeat("2", 64)})
	}
	m.Files = append(m.Files, normalOrdinaryStockUdevFile{Path: "/lib/ld-musl-x86_64.so.1", Kind: "dso", Code: true, Bytes: 4096, SHA256: strings.Repeat("3", 64)})
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	return m
}

// PG17-U-STOCK-UDEV-001 纯静态inventory，不模拟只读FD/ELF/source已完成。
func TestNormalOrdinaryStockUdevManifestExactInventory(t *testing.T) {
	m := stockUdevManifestFixture(t)
	raw, e := json.Marshal(m)
	if e != nil {
		t.Fatal(e)
	}
	got, e := decodeNormalOrdinaryStockUdevManifest(raw)
	if e != nil {
		t.Fatal(e)
	}
	if len(got.Files) != 71 {
		t.Fatal(len(got.Files))
	}
	counts := map[string]int{}
	for _, f := range got.Files {
		counts[f.Kind]++
	}
	for k, n := range map[string]int{"program": 2, "helper": 8, "rule": 28, "hwdb-text": 28, "config": 3, "acl-override": 1, "dso": 1} {
		if counts[k] != n {
			t.Fatalf("%s=%d", k, counts[k])
		}
	}
	t.Run("copy-no-global-authority", func(t *testing.T) {
		a := normalOrdinaryStockUdevRequiredInventory()
		delete(a, "/neonvm/bin/udevd")
		if normalOrdinaryStockUdevRequiredInventory()["/neonvm/bin/udevd"] != "program" {
			t.Fatal("global alias")
		}
	})
	for _, name := range normalOrdinaryStockUdevOptionalHWDBPaths() {
		t.Run("optional-"+name, func(t *testing.T) {
			next := m
			next.Files = append(append([]normalOrdinaryStockUdevFile(nil), m.Files...), normalOrdinaryStockUdevFile{Path: name, Kind: "hwdb-bin", Bytes: 4096, SHA256: strings.Repeat("4", 64)})
			sort.Slice(next.Files, func(i, j int) bool { return next.Files[i].Path < next.Files[j].Path })
			raw, _ := json.Marshal(next)
			if _, e := decodeNormalOrdinaryStockUdevManifest(raw); e != nil {
				t.Fatal(e)
			}
		})
	}
}

// PG17-U-STOCK-UDEV-002 运行参数/新Root路径不能进入静态库存；范围/类别逐项拒。
func TestNormalOrdinaryStockUdevManifestRejectsUnsupported(t *testing.T) {
	for _, name := range []string{"version", "scope", "zero-lock", "zero-file", "missing-rule", "missing-hwdb", "missing-config", "missing-passwd", "missing-group", "missing-acl", "arbitrary-command", "wrong-kind", "code-rule", "size", "reorder", "duplicate", "unknown-dso", "no-loader"} {
		t.Run(name, func(t *testing.T) {
			m := stockUdevManifestFixture(t)
			switch name {
			case "version":
				m.ProtocolVersion = 1
			case "scope":
				m.Scope = "arbitrary"
			case "zero-lock":
				m.PackageLockSHA256 = strings.Repeat("0", 64)
			case "zero-file":
				m.Files[0].SHA256 = strings.Repeat("0", 64)
			case "missing-rule", "missing-hwdb", "missing-config", "missing-acl", "no-loader":
				kind := map[string]string{"missing-rule": "rule", "missing-hwdb": "hwdb-text", "missing-config": "config", "missing-acl": "acl-override", "no-loader": "dso"}[name]
				for n, f := range m.Files {
					if f.Kind == kind {
						m.Files = append(m.Files[:n], m.Files[n+1:]...)
						break
					}
				}
			case "missing-passwd", "missing-group":
				missing := "/etc/passwd"
				if name == "missing-group" {
					missing = "/etc/group"
				}
				for n, f := range m.Files {
					if f.Path == missing {
						m.Files = append(m.Files[:n], m.Files[n+1:]...)
						break
					}
				}
			case "arbitrary-command":
				m.Files[0].Path = "/bin/sh"
			case "wrong-kind":
				m.Files[0].Kind = "program"
			case "code-rule":
				m.Files[0].Code = true
			case "size":
				m.Files[0].Bytes = 1 << 63
			case "reorder":
				m.Files[0], m.Files[1] = m.Files[1], m.Files[0]
			case "duplicate":
				m.Files[1] = m.Files[0]
			case "unknown-dso":
				for n, f := range m.Files {
					if f.Kind == "dso" {
						m.Files[n].Path = "/lib/arbitrary.so.1"
					}
				}
			}
			raw, _ := json.Marshal(m)
			if _, e := decodeNormalOrdinaryStockUdevManifest(raw); !errors.Is(e, ErrUnknown) {
				t.Fatalf("accepted %s: %v", name, e)
			}
		})
	}
}

// PG17-U-STOCK-UDEV-003 严格schema，不让omitempty/null/大小写别名降级协议。
func TestNormalOrdinaryStockUdevManifestStrictJSON(t *testing.T) {
	m := stockUdevManifestFixture(t)
	raw, _ := json.Marshal(m)
	cases := map[string][]byte{"top-alias": bytes.Replace(raw, []byte(`"protocol_version"`), []byte(`"Protocol_Version"`), 1), "top-duplicate": bytes.Replace(raw, []byte(`"protocol_version":2`), []byte(`"protocol_version":2,"protocol_version":2`), 1), "source-cycle": append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"runtime_document_sha256":"x"}`)...), "file-alias": bytes.Replace(raw, []byte(`"path"`), []byte(`"Path"`), 1), "null-code": bytes.Replace(raw, []byte(`"code":false`), []byte(`"code":null`), 1), "missing-code": bytes.Replace(raw, []byte(`"code":false,`), nil, 1), "null-files": []byte(`{"protocol_version":2,"scope":"normal-ordinary-stock-udev-manifest-v2","package_lock_sha256":"` + strings.Repeat("1", 64) + `","files":null}`)}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, e := decodeNormalOrdinaryStockUdevManifest(b); !errors.Is(e, ErrUnknown) {
				t.Fatalf("accepted: %v", e)
			}
		})
	}
}
