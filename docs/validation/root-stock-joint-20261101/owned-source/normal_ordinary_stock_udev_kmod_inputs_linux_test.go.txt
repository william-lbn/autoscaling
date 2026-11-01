//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// PG17-U-STOCK-KMOD-001 仅私有临时树类型/全资源拒绝，不伪造生产RO/source。
func TestNormalOrdinaryStockUdevKmodEmptyRoots(t *testing.T) {
	for _, name := range []string{"absent", "empty", "regular-config", "fifo-root", "directory-alias", "dangling-alias", "install-conf", "non-conf-file", "alias-file", "softdep", "index-bin", "module-payload", "hidden-entry", "nested-directory"} {
		t.Run(name, func(t *testing.T) {
			p := t.TempDir()
			parent, e := os.Open(p)
			if e != nil {
				t.Fatal(e)
			}
			defer parent.Close()
			path := filepath.Join(p, "modprobe.d")
			switch name {
			case "absent":
			case "regular-config":
				if e = os.WriteFile(path, []byte("install x /bin/sh -c bad\n"), 0600); e != nil {
					t.Fatal(e)
				}
			case "fifo-root":
				if e = unix.Mkfifo(path, 0600); e != nil {
					t.Fatal(e)
				}
			case "directory-alias":
				target := filepath.Join(p, "other")
				if e = os.Mkdir(target, 0755); e != nil {
					t.Fatal(e)
				}
				if e = os.Symlink(target, path); e != nil {
					t.Fatal(e)
				}
			case "dangling-alias":
				if e = os.Symlink("missing", path); e != nil {
					t.Fatal(e)
				}
			default:
				if e = os.Mkdir(path, 0755); e != nil {
					t.Fatal(e)
				}
				entry := map[string]string{"install-conf": "danger.conf", "non-conf-file": "not-conf", "alias-file": "x.alias", "softdep": "modules.softdep", "index-bin": "modules.dep.bin", "module-payload": "x.ko", "hidden-entry": ".hidden", "nested-directory": "child"}[name]
				if entry != "" {
					if name == "nested-directory" {
						e = os.Mkdir(filepath.Join(path, entry), 0755)
					} else {
						e = os.WriteFile(filepath.Join(path, entry), []byte("resource"), 0600)
					}
					if e != nil {
						t.Fatal(e)
					}
				}
			}
			f, state, e := normalOrdinaryStockUdevKmodEmptyAt(parent, "modprobe.d")
			if name == "absent" || name == "empty" {
				if e != nil || state != name {
					t.Fatal(state, e)
				}
				if f != nil {
					f.Close()
				}
			} else if e == nil {
				if f != nil {
					f.Close()
				}
				t.Fatal("resource/alias accepted")
			}
		})
	}
}

// PG17-U-STOCK-KMOD-002 原runFD缺失证明；任何存在对象均拒且不删除/接管。
func TestNormalOrdinaryStockUdevKmodRunAbsent(t *testing.T) {
	for _, name := range []string{"absent", "directory", "regular", "dangling-alias", "wrong-run-mode"} {
		t.Run(name, func(t *testing.T) {
			p := t.TempDir()
			if e := os.Chmod(p, 0755); e != nil {
				t.Fatal(e)
			}
			run, e := os.Open(p)
			if e != nil {
				t.Fatal(e)
			}
			defer run.Close()
			object, e := normalFilesystemFDObject(int(run.Fd()))
			if e != nil {
				t.Fatal(e)
			}
			q := filepath.Join(p, "modprobe.d")
			switch name {
			case "directory":
				e = os.Mkdir(q, 0755)
			case "regular":
				e = os.WriteFile(q, nil, 0600)
			case "dangling-alias":
				e = os.Symlink("missing", q)
			case "wrong-run-mode":
				e = os.Chmod(p, 0777)
			}
			if e != nil {
				t.Fatal(e)
			}
			e = normalOrdinaryStockUdevKmodRunAbsent(run, object)
			if name == "absent" {
				if e != nil {
					t.Fatal(e)
				}
			} else {
				if e == nil {
					t.Fatal("runtime input admitted")
				}
				if name != "wrong-run-mode" {
					if _, e = os.Lstat(q); e != nil {
						t.Fatal("object deleted or mutated", e)
					}
				}
			}
		})
	}
}

// PG17-U-STOCK-KMOD-003 固定真实supervisor env，无KMOD_LOG；不读取本进程env补资格。
func TestNormalOrdinaryStockUdevKmodEnvironment(t *testing.T) {
	env := normalOrdinaryStockUdevEnvironment()
	if e := validateNormalOrdinaryStockUdevKmodEnvironment(env); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"KMOD_LOG", "MODPROBE_OPTIONS", "extra", "duplicate", "reorder", "path"} {
		t.Run(name, func(t *testing.T) {
			next := append([]string(nil), env...)
			switch name {
			case "KMOD_LOG":
				next = append(next, "KMOD_LOG=debug")
			case "MODPROBE_OPTIONS":
				next = append(next, "MODPROBE_OPTIONS=bad")
			case "extra":
				next = append(next, "ARBITRARY=1")
			case "duplicate":
				next[2] = next[1]
			case "reorder":
				next[0], next[1] = next[1], next[0]
			case "path":
				next[0] = "PATH=/tmp"
			}
			if validateNormalOrdinaryStockUdevKmodEnvironment(next) == nil {
				t.Fatal("changed actual exec environment accepted")
			}
		})
	}
	env[0] = "changed"
	if normalOrdinaryStockUdevEnvironment()[0] == "changed" {
		t.Fatal("global environment slice alias")
	}
}

// PG17-U-STOCK-KMOD-004 精确18c built证明仅决定预期缺失语法，不代表实际ENOENT。
func TestNormalOrdinaryStockUdevKmodKernelBinding(t *testing.T) {
	k, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	if e != nil {
		t.Fatal(e)
	}
	path, e := normalOrdinaryStockUdevKmodReleaseRoot(k.KernelRelease)
	if e != nil || path != "/lib/modules/"+k.KernelRelease {
		t.Fatal(path, e)
	}
	if !normalOrdinaryStockUdevKmodCompressionMayBeAbsent(k) {
		t.Fatal("exact original18c config missing proof")
	}
	for _, name := range []string{"release", "profile", "config", "artifact", "notes"} {
		t.Run(name, func(t *testing.T) {
			next := k
			switch name {
			case "release":
				next.KernelRelease += "-other"
			case "profile":
				next.ProfileID = "other"
			case "config":
				next.KernelConfigSHA256 = strings.Repeat("1", 64)
			case "artifact":
				next.KernelArtifactSHA256 = strings.Repeat("2", 64)
			case "notes":
				next.KernelNotesSHA256 = strings.Repeat("3", 64)
			}
			if normalOrdinaryStockUdevKmodCompressionMayBeAbsent(next) {
				t.Fatal("non-original build inherited missing-attribute proof")
			}
		})
	}
	for _, release := range []string{"", "..", "../../etc", "6.12/foo", "6.12\nother", "6.12\x00", "6.12 other"} {
		if _, e = normalOrdinaryStockUdevKmodReleaseRoot(release); e == nil {
			t.Fatal("unsafe release admitted")
		}
	}
	legacy, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if e != nil {
		t.Fatal(e)
	}
	if normalOrdinaryStockUdevKmodCompressionMayBeAbsent(legacy) {
		t.Fatal("legacy kernel got default absence")
	}
}

// PG17-U-STOCK-KMOD-005 缺原opaque或伪pseudoFS均拒；缓存深副本不产Source。
func TestNormalOrdinaryStockUdevKmodFailClosed(t *testing.T) {
	t.Run("no-original-objects", func(t *testing.T) {
		for _, ctx := range []context.Context{nil, context.Background()} {
			k, e := pinNormalOrdinaryStockUdevKmodInputs(ctx, nil, nil)
			if !errors.Is(e, ErrUnknown) || k != nil {
				t.Fatal(e)
			}
		}
	})
	t.Run("ordinary-file-not-proc", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "cmdline")
		if e := os.WriteFile(p, []byte("not kernel input\n"), 0444); e != nil {
			t.Fatal(e)
		}
		if ref, f, e := normalOrdinaryStockUdevKmodPinPseudo(p, 64<<10, unix.PROC_SUPER_MAGIC); e == nil {
			f.Close()
			t.Fatal("ordinary inode promoted to proc source", ref)
		}
	})
	t.Run("nil-or-closed-recheck", func(t *testing.T) {
		var k *normalOrdinaryStockUdevKmodInputs
		if !errors.Is(k.recheck(context.Background()), ErrUnknown) {
			t.Fatal("nil accepted")
		}
		k = &normalOrdinaryStockUdevKmodInputs{closed: true}
		if !errors.Is(k.recheck(context.Background()), ErrUnknown) {
			t.Fatal("closed accepted")
		}
		k.close()
		k.close()
	})
	t.Run("cached-reference-deep-copy", func(t *testing.T) {
		f, e := os.CreateTemp(t.TempDir(), "model-only")
		if e != nil {
			t.Fatal(e)
		}
		k := &normalOrdinaryStockUdevKmodInputs{inputs: &normalOrdinaryStockUdevInputs{}, dirs: &normalOrdinaryStockUdevRuntimeDirs{}, run: f, ref: normalOrdinaryStockUdevKmodInputReference{Roots: []normalOrdinaryStockUdevKmodRoot{{Path: "model-only"}}, MissingSources: []string{"not-qualified"}}}
		defer k.close()
		a, e := k.snapshot()
		if e != nil {
			t.Fatal(e)
		}
		a.Roots[0].Path = "changed"
		a.MissingSources[0] = "changed"
		b, e := k.snapshot()
		if e != nil || b.Roots[0].Path != "model-only" || b.MissingSources[0] != "not-qualified" {
			t.Fatal("snapshot aliases private data")
		}
	})
}
