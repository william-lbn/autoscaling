//go:build linux

package main

import (
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func normalStockBuilderFixture(t *testing.T) string {
	t.Helper()
	root := normalV3BuilderFixture(t)
	if e := prepareNormalSystemV3Assets(root, normalTestJSON(normalTestPlan())); e != nil {
		t.Fatal(e)
	}
	var entries []map[string]any
	required := resourceguard.NormalSystemSourceStockUdevRequiredFiles()
	delete(required, resourceguard.NormalSystemStockUdevManifestPath)
	required["/lib/libudev.so.1.6.3"] = "loader"
	var names []string
	for name := range required {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		kind := required[name]
		path := filepath.Join(root, name)
		raw, e := os.ReadFile(path)
		if os.IsNotExist(e) {
			if e = os.MkdirAll(filepath.Dir(path), 0755); e != nil {
				t.Fatal(e)
			}
			raw = []byte("static model not source\n")
			if kind == "exec" || kind == "loader" {
				raw = []byte("\x7fELF-MODEL")
			}
			if e = os.WriteFile(path, raw, 0755); e != nil {
				t.Fatal(e)
			}
		} else if e != nil {
			t.Fatal(e)
		}
		class := "config"
		switch {
		case kind == "loader":
			class = "dso"
		case strings.HasPrefix(name, "/neonvm/bin/"):
			class = "program"
		case kind == "exec":
			class = "helper"
		case strings.HasPrefix(name, "/lib/udev/rules.d/"):
			class = "rule"
		case strings.HasPrefix(name, "/etc/udev/hwdb.d/"):
			class = "hwdb-text"
		case strings.HasPrefix(name, "/etc/udev/rules.d/"):
			class = "acl-override"
		}
		entries = append(entries, map[string]any{"path": name, "kind": class, "code": kind == "exec" || kind == "loader", "bytes": control.Uint64(len(raw)), "sha256": normalDigest(raw)})
	}
	// 模型包锁只用来核最终原始字节绑定，不称真实provider来源。
	lockRaw := []byte("TEST_ONLY original package lock, not Source\n")
	if e := os.WriteFile(filepath.Join(root, resourceguard.NormalSystemOrdinaryV2PackageLockPath), lockRaw, 0444); e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(map[string]any{"protocol_version": 2, "scope": "normal-ordinary-stock-udev-manifest-v2", "package_lock_sha256": normalDigest(lockRaw), "files": entries})
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(root, resourceguard.NormalSystemStockUdevManifestPath)
	if e = os.MkdirAll(filepath.Dir(path), 0755); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, raw, 0444); e != nil {
		t.Fatal(e)
	}
	return root
}
func TestNormalSplitV3StockFinalBytes(t *testing.T) {
	root := normalStockBuilderFixture(t)
	raw, e := generateNormalSystemManifestForVersion(root, normalTestPlan(), strings.Repeat("ab", 32), 3)
	if e != nil {
		t.Fatal(e)
	}
	m, e := resourceguard.DecodeNormalSystemSourceManifest(raw)
	if e != nil {
		t.Fatal(e)
	}
	found := map[string]bool{}
	for _, f := range m.Files {
		found[f.Path] = true
	}
	for name := range resourceguard.NormalSystemSourceStockUdevRequiredFiles() {
		if !found[name] {
			t.Fatal("actual final inventory omitted", name)
		}
	}
}
func TestNormalSplitV3StockRejectsFinalDrift(t *testing.T) {
	for _, mode := range []string{"bytes", "extra-rule", "extra-hwdb", "unlisted-bin", "malformed-manifest", "manifest-alias", "missing-helper", "usr-rule", "usr-local-rule"} {
		t.Run(mode, func(t *testing.T) {
			root := normalStockBuilderFixture(t)
			path := resourceguard.NormalSystemStockUdevManifestPath
			switch mode {
			case "bytes":
				path = "/etc/group"
				if e := os.WriteFile(filepath.Join(root, path), []byte("different bytes"), 0644); e != nil {
					t.Fatal(e)
				}
			case "extra-rule":
				path = "/etc/udev/rules.d/10-other.rules"
				if e := os.WriteFile(filepath.Join(root, path), []byte("unlisted"), 0644); e != nil {
					t.Fatal(e)
				}
			case "extra-hwdb":
				path = "/etc/udev/hwdb.d/99-other.hwdb"
				if e := os.WriteFile(filepath.Join(root, path), []byte("unlisted"), 0644); e != nil {
					t.Fatal(e)
				}
			case "unlisted-bin":
				path = "/etc/udev/hwdb.bin"
				if e := os.WriteFile(filepath.Join(root, path), []byte("unlisted"), 0644); e != nil {
					t.Fatal(e)
				}
			case "malformed-manifest":
				if e := os.WriteFile(filepath.Join(root, path), []byte(`{"protocol_version":2}`), 0644); e != nil {
					t.Fatal(e)
				}
			case "manifest-alias":
				actual := filepath.Join(root, path)
				if e := os.Rename(actual, actual+".other"); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink(path+".other", actual); e != nil {
					t.Fatal(e)
				}
			case "usr-rule", "usr-local-rule":
				logical := "/usr/lib/udev/rules.d"
				if mode == "usr-local-rule" {
					logical = "/usr/local/lib/udev/rules.d"
				}
				dir := filepath.Join(root, logical)
				if e := os.MkdirAll(dir, 0755); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(filepath.Join(dir, "10-shadow.rules"), []byte("unlisted effective rule"), 0644); e != nil {
					t.Fatal(e)
				}
			case "missing-helper":
				if e := os.Remove(filepath.Join(root, "/lib/udev/ata_id")); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := generateNormalSystemManifestForVersion(root, normalTestPlan(), strings.Repeat("ab", 32), 3); e == nil {
				t.Fatal("drift accepted", mode)
			}
		})
	}
}

func TestNormalSplitV3StockAlternativeRulesEmptyOrSameObject(t *testing.T) {
	for _, mode := range []string{"absent", "empty", "alias"} {
		t.Run(mode, func(t *testing.T) {
			root := normalStockBuilderFixture(t)
			for _, logical := range resourceguard.NormalSystemStockUdevStaticRuleDirectories()[2:] {
				if mode == "absent" {
					continue
				}
				if e := os.MkdirAll(filepath.Dir(filepath.Join(root, logical)), 0755); e != nil {
					t.Fatal(e)
				}
				if mode == "empty" {
					if e := os.Mkdir(filepath.Join(root, logical), 0755); e != nil {
						t.Fatal(e)
					}
				} else {
					// 绝对alias仅由原rootdisk canonical resolver解析，不能跳到构建Host。
					if e := os.Symlink("/lib/udev/rules.d", filepath.Join(root, logical)); e != nil {
						t.Fatal(e)
					}
				}
			}
			if _, e := generateNormalSystemManifestForVersion(root, normalTestPlan(), strings.Repeat("ab", 32), 3); e != nil {
				t.Fatal(e)
			}
		})
	}
}
