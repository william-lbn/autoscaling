//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

func TestNormalStockAssetsExplicitVersionAndRoot(t *testing.T) {
	for _, v := range []uint32{0, 1, 3, 4} {
		if _, e := prepareNormalSystemStockUdevAssets("/", nil, 1000, 1000, v); e == nil {
			t.Fatal("implicit controls accepted")
		}
	}
	if _, e := prepareNormalSystemStockUdevAssets("/", nil, 1000, 1000, 2); e == nil {
		t.Fatal("Host root accepted")
	}
	if _, e := prepareNormalSystemStockUdevAssets("relative", nil, 1000, 1000, 2); e == nil {
		t.Fatal("relative image root accepted")
	}
}
func TestNormalStockAssetsAccountInstallation(t *testing.T) {
	p := []byte("root:x:0:0:root:/root:/bin/sh\npostgres:x:1000:1000:postgres:/var/db/postgres:/bin/sh\n")
	g := []byte("root:x:0:\npostgres:x:1000:\n")
	p2, g2, e := normalStockAssetAccounts(p, g, 1000, 1000)
	if e != nil {
		t.Fatal(e)
	}
	if e = resourceguard.ValidateNormalSystemStockUdevAssetsAccounts(p2, g2, 1000, 1000); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"audio", "cdrom", "dialout", "disk", "input", "kmem", "kvm", "lp", "tape", "tty", "video"} {
		if !bytes.Contains(g2, []byte(name+":x:61")) {
			t.Fatal("fixed missing symbolic group", name)
		}
	}
	a, b, e := normalStockAssetAccounts(p2, g2, 1000, 1000)
	if e != nil || !bytes.Equal(a, p2) || !bytes.Equal(b, g2) {
		t.Fatal("complete account bytes not preserved", e)
	}
	for _, mode := range []string{"reserved group collision", "partial role", "PG identity collision", "supplementary member"} {
		t.Run(mode, func(t *testing.T) {
			x, y := bytes.Clone(p), bytes.Clone(g)
			uid := uint32(1000)
			switch mode {
			case "reserved group collision":
				y = append(y, []byte("other:x:61001:\n")...)
			case "partial role":
				x = append(x, []byte("neon-allow-start:x:60001:60001:role:/nonexistent:/bin/false\n")...)
			case "PG identity collision":
				uid = 60001
			case "supplementary member":
				y = append(y, []byte("other:x:101:neon-clock\n")...)
			}
			if _, _, e := normalStockAssetAccounts(x, y, uid, 1000); e == nil {
				t.Fatal("ambiguous account installation", mode)
			}
		})
	}
}

func normalStockAssetsFixture(t *testing.T) (string, []byte) {
	t.Helper()
	provider := os.Getenv("DSGA_STOCK_ASSET_FIXTURE")
	if provider == "" {
		t.Skip("explicit real APK/provider fixture needed; no synthetic ELF positive")
	}
	root := t.TempDir()
	e := filepath.WalkDir(filepath.Join(provider, "root"), func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(filepath.Join(provider, "root"), path)
		if e != nil {
			return e
		}
		dst := filepath.Join(root, rel)
		st, e := os.Lstat(path)
		if e != nil {
			return e
		}
		if d.IsDir() {
			return os.MkdirAll(dst, 0755)
		}
		if d.Type()&os.ModeSymlink != 0 {
			target, e := os.Readlink(path)
			if e != nil {
				return e
			}
			return os.Symlink(target, dst)
		}
		raw, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		return os.WriteFile(dst, raw, st.Mode().Perm())
	})
	if e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(filepath.Join(provider, "package-lock.json"))
	if e != nil {
		t.Fatal(e)
	}
	return root, raw
}

func TestNormalStockAssetsActualProvider(t *testing.T) {
	root, lock := normalStockAssetsFixture(t)
	dns, e := os.ReadFile(filepath.Join(root, "etc/resolv.conf"))
	if e != nil {
		t.Fatal(e)
	}
	// 原Dockerfile会把该空目录归chrony；新2必须在构建期恢复Root初态。
	if e = os.Chown(filepath.Join(root, "var/lib/chrony"), 123, 123); e != nil {
		t.Fatal(e)
	}
	raw, e := prepareNormalSystemStockUdevAssets(root, lock, 1000, 1000, 2)
	if e != nil {
		t.Fatal(e)
	}
	entries, e := resourceguard.NormalSystemSourceStockUdevManifestFiles(raw)
	if e != nil {
		t.Fatal(e)
	}
	want := resourceguard.NormalSystemSourceStockUdevRequiredFiles()
	delete(want, resourceguard.NormalSystemStockUdevManifestPath)
	dsos := 0
	for _, f := range entries {
		delete(want, f.Path)
		if f.Kind == "loader" {
			dsos++
		}
		b, e := os.ReadFile(filepath.Join(root, f.Path))
		if e != nil || normalDigest(b) != f.SHA256 || uint64(len(b)) != f.Bytes {
			t.Fatal("final asset differs", f.Path, e)
		}
	}
	if len(want) != 0 || dsos == 0 {
		t.Fatal("incomplete final stock inventory", want, dsos)
	}
	var mf map[string]any
	if e = json.Unmarshal(raw, &mf); e != nil {
		t.Fatal(e)
	}
	if mf["package_lock_sha256"] != normalDigest(lock) {
		t.Fatal("lock not actual original bytes")
	}
	for _, p := range []string{resourceguard.NormalSystemStockUdevManifestPath, resourceguard.NormalSystemOrdinaryV2PackageLockPath, normalStockAssetsACLPath} {
		st, e := os.Lstat(filepath.Join(root, p))
		if e != nil || st.Mode().Perm() != 0444 {
			t.Fatal("RO final asset mode", p, e)
		}
	}
	after, e := os.ReadFile(filepath.Join(root, "etc/resolv.conf"))
	if e != nil || !bytes.Equal(dns, after) {
		t.Fatal("original DNS contents changed")
	}
	for _, p := range []string{"/run", "/var/log", "/var/lib/chrony"} {
		st, e := os.Lstat(filepath.Join(root, p))
		if e != nil {
			t.Fatal(e)
		}
		s := st.Sys().(*syscall.Stat_t)
		if s.Uid != 0 || s.Gid != 0 || st.Mode().Perm() != 0755 {
			t.Fatal("not original Root initial anchor", p)
		}
	}
	p, e := os.ReadFile(filepath.Join(root, "etc/passwd"))
	if e != nil {
		t.Fatal(e)
	}
	g, e := os.ReadFile(filepath.Join(root, "etc/group"))
	if e != nil {
		t.Fatal(e)
	}
	if e = resourceguard.ValidateNormalSystemStockUdevAssetsAccounts(p, g, 1000, 1000); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(root, "run/neon-ordinary/allow-start")); !os.IsNotExist(e) {
		t.Fatal("builder precreated runtime private directories")
	}
	again, e := prepareNormalSystemStockUdevAssets(root, lock, 1000, 1000, 2)
	if e != nil || !bytes.Equal(raw, again) {
		t.Fatal("same build input changed on repeat", e)
	}
	t.Logf("actual provider inventory=%d DSO=%d raw lockSHA=%s manifestSHA=%s; static assets only, no Source", len(entries), dsos, normalDigest(lock), normalDigest(raw))
}

func TestNormalStockAssetsActualProviderRejectsDrift(t *testing.T) {
	for _, mode := range []string{"missing-rule", "missing-hwdb", "missing-helper", "extra-rule", "extra-hwdb", "extra-helper", "alternate-rule", "bad-ELF", "missing-DSO", "lock-hash", "unknown-lock-file", "symbolic-ID-collision", "DNS-alias", "nonempty-chrony", "manifest-alias", "loader-override"} {
		t.Run(mode, func(t *testing.T) {
			root, lock := normalStockAssetsFixture(t)
			write := func(p string, b []byte) {
				if e := os.MkdirAll(filepath.Dir(filepath.Join(root, p)), 0755); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(filepath.Join(root, p), b, 0644); e != nil {
					t.Fatal(e)
				}
			}
			switch mode {
			case "missing-rule":
				os.Remove(filepath.Join(root, "lib/udev/rules.d/70-memory.rules"))
			case "missing-hwdb":
				os.Remove(filepath.Join(root, "etc/udev/hwdb.d/20-OUI.hwdb"))
			case "missing-helper":
				os.Remove(filepath.Join(root, "lib/udev/ata_id"))
			case "extra-rule":
				write("lib/udev/rules.d/99-unapproved.rules", []byte("# do not prune\n"))
			case "extra-hwdb":
				write("etc/udev/hwdb.d/99-unapproved.hwdb", []byte("# do not prune\n"))
			case "extra-helper":
				write("lib/udev/other_helper", []byte("uncovered"))
			case "alternate-rule":
				write("usr/local/lib/udev/rules.d/99-uncovered.rules", []byte("# not empty\n"))
			case "bad-ELF":
				write("neonvm/bin/udevd", []byte("not a real ELF"))
			case "missing-DSO":
				os.Remove(filepath.Join(root, "lib/ld-musl-x86_64.so.1"))
			case "lock-hash":
				var l normalStockAssetPackageLock
				if e := json.Unmarshal(lock, &l); e != nil {
					t.Fatal(e)
				}
				l.Files[0].SHA256 = strings.Repeat("ab", 32)
				lock, _ = json.Marshal(l)
			case "unknown-lock-file":
				var l normalStockAssetPackageLock
				if e := json.Unmarshal(lock, &l); e != nil {
					t.Fatal(e)
				}
				f := l.Files[len(l.Files)-1]
				f.Path = "/zzz-uncovered"
				l.Files = append(l.Files, f)
				lock, _ = json.Marshal(l)
			case "symbolic-ID-collision":
				b, _ := os.ReadFile(filepath.Join(root, "etc/group"))
				write("etc/group", append(b, []byte("other:x:61001:\n")...))
			case "DNS-alias":
				os.Remove(filepath.Join(root, "etc/resolv.conf"))
				os.Symlink("/etc/group", filepath.Join(root, "etc/resolv.conf"))
			case "nonempty-chrony":
				write("var/lib/chrony/preserve", []byte("do not remove"))
			case "manifest-alias":
				p := filepath.Join(root, resourceguard.NormalSystemStockUdevManifestPath)
				os.MkdirAll(filepath.Dir(p), 0755)
				os.Symlink("/etc/passwd", p)
			case "loader-override":
				write("etc/ld-musl-x86_64.path", []byte("/uncovered\n"))
			}
			before, _ := os.ReadFile(filepath.Join(root, "etc/passwd"))
			_, e := prepareNormalSystemStockUdevAssets(root, lock, 1000, 1000, 2)
			if e == nil {
				t.Fatal("drift accepted", mode)
			}
			after, _ := os.ReadFile(filepath.Join(root, "etc/passwd"))
			if !bytes.Equal(before, after) {
				t.Fatal("failed preflight modified original accounts")
			}
		})
	}
}
