//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func stockRuntimeDirsFixture(t *testing.T) (string, *os.File, *os.File, *os.File) {
	t.Helper()
	base, e := os.MkdirTemp("", "stock-runtime-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { os.RemoveAll(base) })
	p := filepath.Join(base, "private-run")
	if e := os.Mkdir(p, 0755); e != nil {
		t.Fatal(e)
	}
	if e := os.Chmod(p, 0755); e != nil {
		t.Fatal(e)
	}
	run, e := os.Open(p)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { run.Close() })
	root, rules, e := normalOrdinaryStockUdevRuntimePrepareAt(context.Background(), run)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { root.Close(); rules.Close() })
	return p, run, root, rules
}
func stockRuntimeWrite(t *testing.T, root, relative string, data []byte, mode os.FileMode) {
	t.Helper()
	path := filepath.Join(root, "udev", relative)
	if e := os.MkdirAll(filepath.Dir(path), 0755); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(path, data, mode); e != nil {
		t.Fatal(e)
	}
	if e := os.Chmod(path, mode); e != nil {
		t.Fatal(e)
	}
}

// PG17-U-STOCK-RUNTIME-001 私有临时树prepare，不访问本机/run/udev。
func TestNormalOrdinaryStockUdevRuntimePrepare(t *testing.T) {
	t.Run("fresh-and-empty-original-rules", func(t *testing.T) {
		_, run, root, rules := stockRuntimeDirsFixture(t)
		if _, e := normalOrdinaryStockUdevRuntimeDirFact(root, 0, 0, 0755); e != nil {
			t.Fatal(e)
		}
		if _, e := normalOrdinaryStockUdevRuntimeDirFact(rules, 0, 0, 0755); e != nil {
			t.Fatal(e)
		}
		again, aRules, e := normalOrdinaryStockUdevRuntimePrepareAt(context.Background(), run)
		if e != nil {
			t.Fatal(e)
		}
		again.Close()
		aRules.Close()
	})
	for _, name := range []string{"preexisting-state", "root-alias", "rules-alias", "wrong-existing-mode", "nonempty-rules"} {
		t.Run(name, func(t *testing.T) {
			p, run, root, rules := stockRuntimeDirsFixture(t)
			path := filepath.Join(p, "udev")
			var e error
			switch name {
			case "preexisting-state":
				if e = os.WriteFile(filepath.Join(path, "queue"), nil, 0600); e != nil {
					t.Fatal(e)
				}
			case "root-alias":
				if e = os.Rename(path, path+"-old"); e != nil {
					t.Fatal(e)
				}
				if e = os.Symlink(path+"-old", path); e != nil {
					t.Fatal(e)
				}
			case "rules-alias":
				q := filepath.Join(path, "rules.d")
				if e = os.Rename(q, q+"-old"); e != nil {
					t.Fatal(e)
				}
				if e = os.Symlink(q+"-old", q); e != nil {
					t.Fatal(e)
				}
			case "wrong-existing-mode":
				if e = unix.Fchmod(int(root.Fd()), 0777); e != nil {
					t.Fatal(e)
				}
			case "nonempty-rules":
				if e = os.WriteFile(filepath.Join(path, "rules.d/99-evil.rules"), []byte("RUN+=bad"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			a, b, e := normalOrdinaryStockUdevRuntimePrepareAt(context.Background(), run)
			if e == nil {
				a.Close()
				b.Close()
				t.Fatal("adopted stale or aliased runtime state")
			}
			_ = rules
		})
	}
}

// PG17-U-STOCK-RUNTIME-002 官方实际五类对象的私有临时树类型正控，无Source/原writer证明。
func TestNormalOrdinaryStockUdevRuntimeTypes(t *testing.T) {
	p, _, root, rules := stockRuntimeDirsFixture(t)
	stockRuntimeWrite(t, p, "data/c10:200", []byte("I:1\n"), 0644|os.ModeSticky)
	stockRuntimeWrite(t, p, `links/\x2fvirtio-ports\x2flog/c10:200`, nil, 0444)
	stockRuntimeWrite(t, p, "tags/uaccess/c10:200", nil, 0444)
	for _, dir := range []string{"watch", "static_node-tags/uaccess"} {
		if e := os.MkdirAll(filepath.Join(p, "udev", dir), 0755); e != nil {
			t.Fatal(e)
		}
	}
	if e := os.Symlink("c10:200", filepath.Join(p, "udev/watch/1")); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink("/dev/null", filepath.Join(p, "udev/static_node-tags/uaccess/null")); e != nil {
		t.Fatal(e)
	}
	entries, e := normalOrdinaryStockUdevRuntimeCollect(context.Background(), root, rules)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		for _, x := range entries {
			x.f.Close()
		}
	}()
	if len(entries) != 13 {
		t.Fatalf("five original type inventory: %d", len(entries))
	}
	for _, id := range []string{"c10:200", "b259:131072", "n3", "+pci:0000:00:1f.2", "+memory:memory0", "+virtio-ports:vport0p1"} {
		if !normalOrdinaryStockUdevRuntimeDeviceID(id) {
			t.Fatal(id)
		}
	}
	for _, id := range []string{"c0:1", "c01:2", "n0", "n03", "n2147483648", "../c1:2", "+pci:", "+a:../b", "c1:2.tmp", "c1:2\n"} {
		if normalOrdinaryStockUdevRuntimeDeviceID(id) {
			t.Fatal("invalid id", id)
		}
	}
}

// PG17-U-STOCK-RUNTIME-003 全预扫先于任何chown，未知/残留/别名均零移交。
func TestNormalOrdinaryStockUdevRuntimeRejectsResidualOrWrongTypes(t *testing.T) {
	for _, name := range []string{"control", "queue", "watch-old", "unknown-top", "database-temp", "data-symlink", "directory-alias", "fifo", "hardlink", "nonempty-link-index", "tag-index-symlink", "watch-regular", "watch-escape", "static-tag-escape", "database-executable", "database-too-large", "new-rules-inode", "rules-shadow"} {
		t.Run(name, func(t *testing.T) {
			p, _, root, rules := stockRuntimeDirsFixture(t)
			stockRuntimeWrite(t, p, "data/c10:200", []byte("I:1\n"), 0644)
			dir := filepath.Join(p, "udev")
			mk := func(rel string) {
				t.Helper()
				if e := os.MkdirAll(filepath.Join(dir, rel), 0755); e != nil {
					t.Fatal(e)
				}
			}
			switch name {
			case "control":
				fd, e := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
				if e != nil {
					t.Fatal(e)
				}
				defer unix.Close(fd)
				if e = unix.Bind(fd, &unix.SockaddrUnix{Name: filepath.Join(dir, "control")}); e != nil {
					t.Fatal(e)
				}
			case "queue":
				stockRuntimeWrite(t, p, "queue", nil, 0644)
			case "watch-old":
				mk("watch.old")
			case "unknown-top":
				mk("unapproved")
			case "database-temp":
				stockRuntimeWrite(t, p, "data/c1:2.tmp", nil, 0644)
			case "data-symlink":
				if e := os.Symlink("/etc/passwd", filepath.Join(dir, "data/c1:2")); e != nil {
					t.Fatal(e)
				}
			case "directory-alias":
				if e := os.Symlink("data", filepath.Join(dir, "watch")); e != nil {
					t.Fatal(e)
				}
			case "fifo":
				if e := unix.Mkfifo(filepath.Join(dir, "data/c1:2"), 0600); e != nil {
					t.Fatal(e)
				}
			case "hardlink":
				if e := os.Link(filepath.Join(dir, "data/c10:200"), filepath.Join(p, "outside-alias")); e != nil {
					t.Fatal(e)
				}
			case "nonempty-link-index":
				stockRuntimeWrite(t, p, "links/encoded/c1:2", []byte("not-empty"), 0444)
			case "tag-index-symlink":
				mk("tags/tag")
				if e := os.Symlink("/dev/null", filepath.Join(dir, "tags/tag/c1:2")); e != nil {
					t.Fatal(e)
				}
			case "watch-regular":
				stockRuntimeWrite(t, p, "watch/1", nil, 0444)
			case "watch-escape":
				mk("watch")
				if e := os.Symlink("../data/c10:200", filepath.Join(dir, "watch/1")); e != nil {
					t.Fatal(e)
				}
			case "static-tag-escape":
				mk("static_node-tags/tag")
				if e := os.Symlink("/dev/../etc/passwd", filepath.Join(dir, "static_node-tags/tag/null")); e != nil {
					t.Fatal(e)
				}
			case "database-executable":
				if e := os.Chmod(filepath.Join(dir, "data/c10:200"), 0700); e != nil {
					t.Fatal(e)
				}
			case "database-too-large":
				if e := os.Truncate(filepath.Join(dir, "data/c10:200"), (1<<20)+1); e != nil {
					t.Fatal(e)
				}
			case "new-rules-inode":
				if e := os.Rename(filepath.Join(dir, "rules.d"), filepath.Join(p, "old-rules")); e != nil {
					t.Fatal(e)
				}
				mk("rules.d")
			case "rules-shadow":
				stockRuntimeWrite(t, p, "rules.d/99-shadow.rules", []byte("RUN+=bad"), 0644)
			}
			if e := normalOrdinaryStockUdevRuntimeHandoffAt(context.Background(), root, rules); e == nil {
				t.Fatal("unknown tree handed off")
			}
			var st unix.Stat_t
			if e := unix.Stat(filepath.Join(dir, "data/c10:200"), &st); e != nil || st.Uid != 0 || st.Gid != 0 {
				t.Fatal("partial chown before full validation", e)
			}
			if _, e := normalOrdinaryStockUdevRuntimeDirFact(root, 0, 0, 0755); e != nil {
				t.Fatal("parent changed on rejection", e)
			}
		})
	}
}

// PG17-U-STOCK-RUNTIME-004 真实内核chown仅私有temp树+symlink自身，不触目标设备/Host目录。
func TestNormalOrdinaryStockUdevRuntimeHandoff(t *testing.T) {
	p, _, root, rules := stockRuntimeDirsFixture(t)
	stockRuntimeWrite(t, p, "data/c10:200", []byte("I:1\n"), 0644)
	if e := os.Mkdir(filepath.Join(p, "udev/watch"), 0755); e != nil {
		t.Fatal(e)
	}
	link := filepath.Join(p, "udev/watch/1")
	if e := os.Symlink("c10:200", link); e != nil {
		t.Fatal(e)
	}
	if e := normalOrdinaryStockUdevRuntimeHandoffAt(context.Background(), root, rules); e != nil {
		t.Fatal(e)
	}
	if _, e := normalOrdinaryStockUdevRuntimeDirFact(root, 0, 60004, 01771); e != nil {
		t.Fatal(e)
	}
	if _, e := normalOrdinaryStockUdevRuntimeDirFact(rules, 0, 0, 0755); e != nil {
		t.Fatal(e)
	}
	for _, n := range []string{"data", "data/c10:200", "watch", "watch/1"} {
		var st unix.Stat_t
		if e := unix.Lstat(filepath.Join(p, "udev", n), &st); e != nil || st.Uid != 60004 || st.Gid != 60004 {
			t.Fatal("not own object handoff", n, e)
		}
	}
	var target unix.Stat_t
	if e := unix.Lstat(filepath.Join(p, "udev/c10:200"), &target); e != unix.ENOENT {
		t.Fatal("symlink target was followed/created", e)
	}
	if e := normalOrdinaryStockUdevRuntimeHandoffAt(context.Background(), root, rules); e == nil {
		t.Fatal("repeated handoff accepted")
	}
}

// PG17-U-STOCK-RUNTIME-005 无原graph/source时不生成生产opaque；取消先于目录变更。
func TestNormalOrdinaryStockUdevRuntimeFailClosed(t *testing.T) {
	t.Run("nil-and-cancelled-context", func(t *testing.T) {
		for _, ctx := range []context.Context{nil, func() context.Context { ctx, cancel := context.WithCancel(context.Background()); cancel(); return ctx }()} {
			if d, e := pinNormalOrdinaryStockUdevRuntimeDirs(ctx); !errors.Is(e, ErrUnknown) || d != nil {
				t.Fatal("context accepted", e)
			}
		}
	})
	t.Run("no-original-object", func(t *testing.T) {
		var d *normalOrdinaryStockUdevRuntimeDirs
		if !errors.Is(d.recheck(context.Background()), ErrUnknown) || !errors.Is(d.handoffAfterOriginalWritersWait(context.Background()), ErrUnknown) {
			t.Fatal("nil object accepted")
		}
		d = &normalOrdinaryStockUdevRuntimeDirs{phase: 1}
		if !errors.Is(d.recheck(context.Background()), ErrUnknown) {
			t.Fatal("model no original pins accepted")
		}
		d.close()
		d.close()
	})
	t.Run("cancel-before-write", func(t *testing.T) {
		_, _, root, rules := stockRuntimeDirsFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if normalOrdinaryStockUdevRuntimeHandoffAt(ctx, root, rules) == nil {
			t.Fatal("cancel accepted")
		}
		if _, e := normalOrdinaryStockUdevRuntimeDirFact(root, 0, 0, 0755); e != nil {
			t.Fatal("cancel changed parent")
		}
	})
}
