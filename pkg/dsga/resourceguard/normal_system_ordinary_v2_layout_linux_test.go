//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func ordinaryV2LayoutFixture(t *testing.T) (*os.File, map[string]normalFilesystemObject, string) {
	t.Helper()
	if os.Getuid() != 0 {
		t.Skip("real Root FD ownership fixture")
	}
	base := t.TempDir()
	for _, s := range []string{"run", "var/log", "var/lib/chrony", "etc"} {
		if e := os.MkdirAll(filepath.Join(base, s), 0755); e != nil {
			t.Fatal(e)
		}
	}
	if e := os.WriteFile(filepath.Join(base, "etc/resolv.conf"), []byte("nameserver 192.0.2.53\n"), 0644); e != nil {
		t.Fatal(e)
	}
	root, e := os.Open(base)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { root.Close() })
	facts := map[string]normalFilesystemObject{}
	for _, s := range []string{"/run", "/var/log", "/var/lib/chrony", "/etc/resolv.conf"} {
		f, e := normalOrdinaryV2OpenAt(int(root.Fd()), s[1:], s != "/etc/resolv.conf", false)
		if e != nil {
			t.Fatal(e)
		}
		facts[s], e = normalFilesystemFDObject(int(f.Fd()))
		f.Close()
		if e != nil {
			t.Fatal(e)
		}
	}
	return root, facts, base
}
func TestOrdinaryV2DirectoryFDPreparationAndDrift(t *testing.T) {
	root, facts, base := ordinaryV2LayoutFixture(t)
	var stages []string
	pins, e := prepareNormalOrdinaryV2PathsAt(context.Background(), root, facts, func(stage string, _ []byte) error { stages = append(stages, stage); return nil })
	if e != nil {
		t.Fatal(e)
	}
	defer pins.close()
	if !reflect.DeepEqual(stages, []string{"intent", "identity", "complete"}) {
		t.Fatal(stages)
	}
	if e = pins.verify(); e != nil {
		t.Fatal(e)
	}
	dns, e := os.ReadFile(filepath.Join(base, "etc/resolv.conf"))
	if e != nil || string(dns) != "nameserver 192.0.2.53\n" {
		t.Fatal("DNS content changed")
	}
	for _, spec := range normalOrdinaryV2Paths() {
		var st unix.Stat_t
		if e = unix.Lstat(base+spec.path, &st); e != nil || st.Uid != spec.uid || st.Gid != spec.gid || st.Mode&07777 != spec.mode {
			t.Fatalf("wrong actual owner/mode %s %v", spec.path, e)
		}
	}
	// 原角色可合法增加自身子目录；nlink不当成固定不变的目录身份。
	if e = os.Mkdir(filepath.Join(base, "run/neon-ordinary/logs/buffer"), 0700); e != nil {
		t.Fatal(e)
	}
	if e = pins.verify(); e != nil {
		t.Fatal(e)
	}
	target := filepath.Join(base, "run/neon-ordinary/allow-start")
	if e = os.Rename(target, target+".old"); e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(target, 0700); e != nil {
		t.Fatal(e)
	}
	if e = unix.Chown(target, 60001, 60001); e != nil {
		t.Fatal(e)
	}
	if pins.verify() == nil {
		t.Fatal("same mode/owner replaced inode accepted")
	}
}
func TestOrdinaryV2DirectoryRejectsUnsafeInitialObjectsBeforeIntent(t *testing.T) {
	for _, which := range []string{"symlink-parent", "fifo-file", "nonempty", "foreign-owner", "anchor-mismatch"} {
		t.Run(which, func(t *testing.T) {
			root, facts, base := ordinaryV2LayoutFixture(t)
			switch which {
			case "symlink-parent":
				if e := os.Symlink("/tmp", filepath.Join(base, "run/neon-ordinary")); e != nil {
					t.Fatal(e)
				}
			case "fifo-file":
				p := filepath.Join(base, "etc/resolv.conf")
				os.Remove(p)
				if e := unix.Mkfifo(p, 0600); e != nil {
					t.Fatal(e)
				}
			case "nonempty":
				if e := os.WriteFile(filepath.Join(base, "var/lib/chrony/old.drift"), []byte("preserve"), 0600); e != nil {
					t.Fatal(e)
				}
			case "foreign-owner":
				if e := os.Chown(filepath.Join(base, "var/lib/chrony"), 60008, 60008); e != nil {
					t.Fatal(e)
				}
			case "anchor-mismatch":
				v := facts["/run"]
				v.Inode++
				facts["/run"] = v
			}
			called := false
			p, e := prepareNormalOrdinaryV2PathsAt(context.Background(), root, facts, func(string, []byte) error { called = true; return nil })
			if p != nil {
				p.close()
			}
			if e == nil || called {
				t.Fatalf("unsafe %s mutated: %v", which, e)
			}
		})
	}
}
func TestOrdinaryV2DirectoryIdentitySyncFailureDoesNotGrantUID(t *testing.T) {
	root, facts, base := ordinaryV2LayoutFixture(t)
	injected := errors.New("identity fsync failed")
	var stages []string
	pins, e := prepareNormalOrdinaryV2PathsAt(context.Background(), root, facts, func(stage string, _ []byte) error {
		stages = append(stages, stage)
		if stage == "identity" {
			return injected
		}
		return nil
	})
	if pins != nil {
		pins.close()
	}
	if !errors.Is(e, injected) || !reflect.DeepEqual(stages, []string{"intent", "identity"}) {
		t.Fatal(e, stages)
	}
	var st unix.Stat_t
	if e = unix.Lstat(filepath.Join(base, "run/neon-ordinary/allow-start"), &st); e != nil || st.Uid != 0 || st.Gid != 0 {
		t.Fatal("owner changed before identity durable", e)
	}
	if _, e = os.Stat(filepath.Join(base, "run/neon-ordinary/allow-start")); e != nil {
		t.Fatal("partial unexpectedly deleted", e)
	}
}
func TestOrdinaryV2DirectorySourceCannotBeForgedFromPlan(t *testing.T) {
	if o, e := prepareNormalSystemOrdinaryV2LayoutLocked(context.Background(), nil, nil, nil, nil, nil, nil); e == nil || o != nil {
		t.Fatal("raw plan made source")
	}
	if _, e := (&normalSystemOrdinaryV2LayoutSource{}).referenceLocked(context.Background(), nil, nil); e == nil {
		t.Fatal("zero source qualified")
	}
	for _, p := range normalOrdinaryV2Paths() {
		if p.path == "/run/udev" || p.path == "/var/db/postgres" || p.path == "/run/neon-dsga" {
			t.Fatal("foreign ownership domain")
		}
	}
}

func TestOrdinaryV2DirectoryHistoryCannotReopen(t *testing.T) {
	for _, stage := range []string{"empty", "intent", "identity", "complete", "bad-type"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			if e := os.Chmod(dir, 0700); e != nil {
				t.Fatal(e)
			}
			state, e := lockState(context.Background(), dir)
			if e != nil {
				t.Fatal(e)
			}
			defer state.close()
			if stage != "empty" {
				name := normalOrdinaryV2LayoutStageName(stage)
				if stage == "bad-type" {
					name = normalOrdinaryV2LayoutStageName("intent")
					if e = os.Symlink("missing", filepath.Join(dir, name)); e != nil {
						t.Fatal(e)
					}
				} else if e = state.createPhysicalFile(name, []byte("{}")); e != nil {
					t.Fatal(e)
				}
			}
			e = normalOrdinaryV2LayoutRejectHistoryLocked(state)
			if stage == "empty" {
				if e != nil {
					t.Fatal("fresh history falsely Unknown", e)
				}
			} else if !errors.Is(e, ErrUnknown) {
				t.Fatal("existing history accepted", e)
			}
		})
	}
}
