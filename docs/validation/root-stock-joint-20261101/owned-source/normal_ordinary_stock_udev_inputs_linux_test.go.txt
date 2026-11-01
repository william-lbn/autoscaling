//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func stockUdevInputAccountsFixture() ([]byte, []byte) {
	p := []byte("root:x:0:0:root:/root:/bin/sh\nneon-udev:x:60004:60004:udev:/nonexistent:/sbin/nologin\n")
	g := "root:x:0:\n"
	for n, name := range []string{"audio", "cdrom", "dialout", "disk", "input", "kmem", "kvm", "lp", "tape", "tty", "video"} {
		g += name + ":x:" + []string{"11", "19", "20", "6", "25", "3", "34", "7", "26", "5", "27"}[n] + ":\n"
	}
	return p, []byte(g)
}

// PG17-U-STOCK-INPUT-001 纯musl文件关联模型，不冒认当前镜像账户已被钉住。
func TestNormalOrdinaryStockUdevInputsAccounts(t *testing.T) {
	p, g := stockUdevInputAccountsFixture()
	t.Run("all-original-symbolic-groups", func(t *testing.T) {
		if e := normalOrdinaryStockUdevAccounts(p, g); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("comments-and-unused-users", func(t *testing.T) {
		if e := normalOrdinaryStockUdevAccounts(append([]byte("# fixture\n\n"), p...), append(g, []byte("unused:x:12345:a,b\n")...)); e != nil {
			t.Fatal(e)
		}
	})
	for _, name := range []string{"missing-group", "same-id-duplicate-name", "different-id-duplicate-name", "uid-overflow", "gid-overflow", "numeric-alias", "root-uid", "root-gid", "empty", "nul", "crlf", "unterminated", "invalid-fields", "nss-compat-name"} {
		t.Run(name, func(t *testing.T) {
			pp := append([]byte(nil), p...)
			gg := append([]byte(nil), g...)
			switch name {
			case "missing-group":
				gg = bytes.ReplaceAll(gg, []byte("video:x:27:\n"), nil)
			case "same-id-duplicate-name":
				gg = append(gg, []byte("video:x:27:\n")...)
			case "different-id-duplicate-name":
				gg = append(gg, []byte("video:x:123:\n")...)
			case "uid-overflow":
				pp = bytes.Replace(pp, []byte(":60004:"), []byte(":4294967296:"), 1)
			case "gid-overflow":
				gg = bytes.Replace(gg, []byte("video:x:27:"), []byte("video:x:4294967296:"), 1)
			case "numeric-alias":
				gg = bytes.Replace(gg, []byte("video:x:27:"), []byte("video:x:027:"), 1)
			case "root-uid":
				pp = bytes.Replace(pp, []byte("root:x:0:0:"), []byte("root:x:1:0:"), 1)
			case "root-gid":
				gg = bytes.Replace(gg, []byte("root:x:0:"), []byte("root:x:1:"), 1)
			case "empty":
				gg = nil
			case "nul":
				gg = append(gg, 0)
			case "crlf":
				gg = bytes.ReplaceAll(gg, []byte("\n"), []byte("\r\n"))
			case "unterminated":
				gg = gg[:len(gg)-1]
			case "invalid-fields":
				gg = append(gg, []byte("extra:x:1:2:3\n")...)
			case "nss-compat-name":
				gg = append(gg, []byte("+nis:x:12:\n")...)
			}
			if normalOrdinaryStockUdevAccounts(pp, gg) == nil {
				t.Fatal("accepted ambiguous account file")
			}
		})
	}
}

func stockUdevInputInventoryFixture(t *testing.T) (NormalSystemSourceManifest, normalOrdinaryStockUdevManifest) {
	t.Helper()
	m := stockUdevManifestFixture(t)
	s := NormalSystemSourceManifest{ProtocolVersion: 3}
	s.Files = append(s.Files, NormalSystemSourceFile{Path: normalOrdinaryStockUdevManifestPath, CanonicalPath: normalOrdinaryStockUdevManifestPath, Kind: "config", Bytes: 4096, SHA256: strings.Repeat("a", 64)})
	for _, f := range m.Files {
		kind, _ := normalSystemStockUdevFileKind(f.Path)
		s.Files = append(s.Files, NormalSystemSourceFile{Path: f.Path, CanonicalPath: f.Path, Kind: kind, Bytes: uint64(f.Bytes), SHA256: f.SHA256})
	}
	sort.Slice(s.Files, func(a, b int) bool { return s.Files[a].Path < s.Files[b].Path })
	return s, m
}

// PG17-U-STOCK-INPUT-002 原System3与prep MF交叉匹配模型；不会构造System source。
func TestNormalOrdinaryStockUdevInputsInventory(t *testing.T) {
	t.Run("complete-correlated-static-model", func(t *testing.T) {
		s, m := stockUdevInputInventoryFixture(t)
		if e := normalOrdinaryStockUdevMatchInventory(s, m); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("other-system-role-dso-is-not-stock-input", func(t *testing.T) {
		s, m := stockUdevInputInventoryFixture(t)
		s.Files = append(s.Files, NormalSystemSourceFile{Path: "/lib/libcrypto.so.3", CanonicalPath: "/lib/libcrypto.so.3", Kind: "loader", Bytes: 64, SHA256: strings.Repeat("c", 64)})
		if e := normalOrdinaryStockUdevMatchInventory(s, m); e != nil {
			t.Fatal(e)
		}
	})
	for _, name := range []string{"old-version", "missing-manifest", "missing-original", "changed-original-hash", "changed-original-size", "changed-original-kind", "changed-code", "duplicate-prep", "original-extra-hwdb-bin"} {
		t.Run(name, func(t *testing.T) {
			s, m := stockUdevInputInventoryFixture(t)
			switch name {
			case "old-version":
				s.ProtocolVersion = 2
			case "missing-manifest":
				for n, f := range s.Files {
					if f.Path == normalOrdinaryStockUdevManifestPath {
						s.Files = append(s.Files[:n], s.Files[n+1:]...)
						break
					}
				}
			case "missing-original":
				s.Files = s.Files[1:]
			case "changed-original-hash":
				s.Files[0].SHA256 = strings.Repeat("f", 64)
			case "changed-original-size":
				s.Files[0].Bytes++
			case "changed-original-kind":
				s.Files[0].Kind = "exec"
			case "changed-code":
				m.Files[0].Code = !m.Files[0].Code
			case "duplicate-prep":
				m.Files = append(m.Files, m.Files[0])
			case "original-extra-hwdb-bin":
				s.Files = append(s.Files, NormalSystemSourceFile{Path: "/lib/udev/hwdb.bin", CanonicalPath: "/lib/udev/hwdb.bin", Kind: "config", Bytes: 1, SHA256: strings.Repeat("b", 64)})
			}
			if normalOrdinaryStockUdevMatchInventory(s, m) == nil {
				t.Fatal("accepted original/prep drift")
			}
		})
	}
}

// PG17-U-STOCK-INPUT-003 重复读取同一原FD使用pread语义，不被共享offset污染。
func TestNormalOrdinaryStockUdevInputsReadAt(t *testing.T) {
	p, g := stockUdevInputAccountsFixture()
	path := filepath.Join(t.TempDir(), "group")
	if e := os.WriteFile(path, g, 0600); e != nil {
		t.Fatal(e)
	}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if _, e = f.Seek(7, 0); e != nil {
		t.Fatal(e)
	}
	i := &normalOrdinaryStockUdevInputs{pins: map[string]*os.File{"/etc/group": f}}
	pf, e := os.CreateTemp(t.TempDir(), "passwd")
	if e != nil {
		t.Fatal(e)
	}
	defer pf.Close()
	if _, e = pf.Write(p); e != nil {
		t.Fatal(e)
	}
	i.pins["/etc/passwd"] = pf
	for n := 0; n < 2; n++ {
		if e = i.checkAccountsLocked(); e != nil {
			t.Fatal(e)
		}
	}
	at, e := f.Seek(0, 1)
	if e != nil || at != 7 {
		t.Fatalf("shared offset changed: %d %v", at, e)
	}
	if _, e = normalOrdinaryStockUdevReadAt(f, 4); !errors.Is(e, ErrUnknown) {
		t.Fatal("oversize accepted")
	}
	if _, e = normalOrdinaryStockUdevReadAt(nil, 4); !errors.Is(e, ErrUnknown) {
		t.Fatal("nil FD accepted")
	}
}

// PG17-U-STOCK-INPUT-004 没有原source/有限ctx/readonly对象时保持Unknown，零Source正例。
func TestNormalOrdinaryStockUdevInputsFailClosed(t *testing.T) {
	for _, name := range []string{"nil-system", "nil-context", "cancelled", "legacy-system", "missing-manifest", "closed-system"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := &normalSystemLifecycleSource{manifest: NormalSystemSourceManifest{ProtocolVersion: 3}}
			switch name {
			case "nil-system":
				s = nil
			case "nil-context":
				ctx = nil
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "legacy-system":
				s.manifest.ProtocolVersion = 2
			case "closed-system":
				s.closed = true
			}
			if i, e := pinNormalOrdinaryStockUdevInputs(ctx, s); !errors.Is(e, ErrUnknown) || i != nil {
				t.Fatalf("missing source accepted: %v", e)
			}
		})
	}
	t.Run("rw-regular-is-not-original-ro", func(t *testing.T) {
		f, e := os.CreateTemp(t.TempDir(), "rw")
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		if _, e = f.Write([]byte("x")); e != nil {
			t.Fatal(e)
		}
		if _, e = normalOrdinaryStockUdevFileFact(f, "/etc/group", "/etc/group", "config", false); e == nil {
			t.Fatal("ordinary RW temp inode accepted as original RO source")
		}
	})
	t.Run("nil-and-closed-recheck", func(t *testing.T) {
		var i *normalOrdinaryStockUdevInputs
		if !errors.Is(i.recheck(context.Background()), ErrUnknown) {
			t.Fatal("nil accepted")
		}
		i = &normalOrdinaryStockUdevInputs{closed: true}
		if !errors.Is(i.recheck(context.Background()), ErrUnknown) {
			t.Fatal("closed accepted")
		}
		i.close()
		i.close()
	})
}

// PG17-U-STOCK-INPUT-005 仅模型缓存副本，关闭后不可获取；深副本不授资格。
func TestNormalOrdinaryStockUdevInputsSnapshot(t *testing.T) {
	s, m := stockUdevInputInventoryFixture(t)
	_ = s
	i := &normalOrdinaryStockUdevInputs{system: &normalSystemLifecycleSource{}, manifest: m, pins: map[string]*os.File{"model-only": nil}, reference: normalOrdinaryStockUdevInputReference{MissingSources: []string{"not-qualified"}, Files: []normalOrdinaryStockUdevInputFile{{Path: "model-only"}}}}
	a, b, e := i.snapshot()
	if e != nil {
		t.Fatal(e)
	}
	a.MissingSources[0] = "changed"
	a.Files[0].Path = "changed"
	b.Files[0].Path = "changed"
	c, d, e := i.snapshot()
	if e != nil || c.MissingSources[0] != "not-qualified" || c.Files[0].Path != "model-only" || d.Files[0].Path == "changed" {
		t.Fatal("snapshot aliases original cache")
	}
	i.close()
	if _, _, e = i.snapshot(); !errors.Is(e, ErrUnknown) {
		t.Fatal("closed snapshot accepted")
	}
}

// PG17-U-STOCK-INPUT-006 IUSR001纯策略分支，不模拟原RO FD/真实alias来源已通过。
func TestNormalOrdinaryStockUdevInputsAdditionalRuleRoots(t *testing.T) {
	original := normalFilesystemObject{Device: 1, Inode: 2, MountID: 3, Readonly: true}
	originals := map[string]normalFilesystemObject{"/usr/lib/udev/rules.d": original}
	for _, tc := range []struct {
		name, canonical string
		object          normalFilesystemObject
		count           int
		want            string
	}{
		{"same-original-nonempty-alias", "/usr/lib/udev/rules.d", original, 28, "original-alias"},
		{"independent-ro-empty", "/usr/local/lib/udev/rules.d", normalFilesystemObject{Device: 1, Inode: 4, MountID: 3, Readonly: true}, 0, "readonly-empty"},
		{"independent-rule-shadow", "/usr/local/lib/udev/rules.d", original, 1, ""},
		{"same-canonical-new-inode", "/usr/lib/udev/rules.d", normalFilesystemObject{Device: 1, Inode: 7, MountID: 3, Readonly: true}, 28, ""},
		{"rw-empty", "/usr/local/lib/udev/rules.d", normalFilesystemObject{Device: 1, Inode: 4, MountID: 3}, 0, ""},
		{"no-original-mount", "/usr/local/lib/udev/rules.d", normalFilesystemObject{Device: 1, Inode: 4, Readonly: true}, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, e := normalOrdinaryStockUdevRuleDirectoryDecision(tc.canonical, tc.object, originals, tc.count)
			if tc.want == "" {
				if e == nil {
					t.Fatal("unsupported rule lookup accepted")
				}
			} else if e != nil || got != tc.want {
				t.Fatalf("decision %s %v", got, e)
			}
		})
	}
}
