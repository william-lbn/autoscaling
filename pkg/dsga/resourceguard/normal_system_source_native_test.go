//go:build linux && dsga_native_normal_system_source

package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 此函数仅在BuildKit独立/out中物化组件夹具，不在Host或真实服务根目录写入。
func TestNativeNormalSystemSourceFixture(t *testing.T) {
	if os.Getenv("DSGA_NATIVE_SYSTEM_FIXTURE_OUTPUT") != "/out/system-fixture" {
		t.Fatal("explicit isolated fixture export required")
	}
	root := "/out/system-fixture"
	if e := os.MkdirAll(root, 0755); e != nil {
		t.Fatal(e)
	}
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	elf, e := os.ReadFile(exe)
	if e != nil {
		t.Fatal(e)
	}
	for _, d := range NormalSystemSourceConfigDirectories() {
		if e := os.MkdirAll(filepath.Join(root, d), 0755); e != nil {
			t.Fatal(e)
		}
	}
	if e := os.MkdirAll(filepath.Join(root, "/rw-parent"), 0755); e != nil {
		t.Fatal(e)
	}
	m := normalSystemModelManifest(t)
	applets := map[string]bool{"/neonvm/bin/touch": true, "/neonvm/bin/cat": true, "/neonvm/bin/sh": true, "/neonvm/bin/udhcpc": true, "/neonvm/bin/acpid": true}
	for i := range m.Files {
		f := &m.Files[i]
		p := filepath.Join(root, f.Path)
		if e := os.MkdirAll(filepath.Dir(p), 0755); e != nil {
			t.Fatal(e)
		}
		var data []byte
		mode := os.FileMode(0644)
		switch f.Kind {
		case "exec":
			data = elf
			mode = 0755
		case "script":
			data = []byte("#!/neonvm/bin/sh\n# fixture only; service not executed\nexit 0\n")
			mode = 0755
		default:
			data = []byte("fixture-only static config\n")
		}
		if f.Path == "/etc/inittab" {
			data = []byte(strings.Join(NormalSystemSourceInitEntries(), "\n") + "\n")
		}
		f.Bytes = uint64(len(data))
		f.SHA256 = physicalDigest(data)
		if applets[f.Path] {
			f.CanonicalPath = "/neonvm/bin/busybox"
			if e := os.Symlink("busybox", p); e != nil {
				t.Fatal(e)
			}
		} else if e := os.WriteFile(p, data, mode); e != nil {
			t.Fatal(e)
		}
	}
	raw, e := json.Marshal(m)
	if e != nil {
		t.Fatal(e)
	}
	p := filepath.Join(root, NormalSystemSourceManifestPath)
	if e := os.MkdirAll(filepath.Dir(p), 0755); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(p, raw, 0644); e != nil {
		t.Fatal(e)
	}
	fmt.Printf("SYSTEM_SOURCE_FIXTURE_BYTES=%d\n", len(raw))
}

// 所有实际挂载/负例写入只发生在显式369独立Guest；不运行任何服务或PG。
func TestNativeNormalSystemSourceComponent(t *testing.T) {
	if os.Getenv("DSGA_NATIVE_SYSTEM_SOURCE") != "isolated-369-system-source-v1" || os.Geteuid() != 0 {
		t.Fatal("explicit isolated Guest root required")
	}
	raw, e := os.ReadFile(NormalSystemSourceManifestPath)
	if e != nil {
		t.Fatal(e)
	}
	var fs unix.Statfs_t
	if e := unix.Statfs("/", &fs); e != nil || fs.Flags&unix.ST_RDONLY == 0 {
		t.Fatal("actual readonly fixture root required")
	}
	var first NormalSystemSourceObservation
	t.Run("actual-readonly-inittab-and-files", func(t *testing.T) {
		var e error
		first, e = CheckNormalSystemSource(context.Background(), raw)
		if e != nil {
			t.Fatal(e)
		}
		if !first.ConfigInventoryObserved || !first.ReadonlyAncestorsObserved || !first.FixedInittabObserved || first.OriginalSourceAuthenticated || first.ELFDependencyClosureVerified || first.FixedArgvEnvironmentExecuted || first.AllSourcesClosed || first.NormalDecreaseAuthorized {
			t.Fatal("component falsely upgraded source")
		}
		b, e := json.Marshal(first)
		if e != nil {
			t.Fatal(e)
		}
		fmt.Printf("DSGA_NATIVE_SYSTEM_SOURCE=%s\n", b)
	})
	remount := func(p string, readonly bool) {
		t.Helper()
		flags := uintptr(unix.MS_REMOUNT)
		if readonly {
			flags |= unix.MS_RDONLY
		}
		if e := unix.Mount("", p, "", flags, ""); e != nil {
			t.Fatal(e)
		}
	}
	t.Run("actual-runtime-command-rejected", func(t *testing.T) {
		remount("/neonvm/runtime", false)
		p := "/neonvm/runtime/command.sh"
		if e := os.WriteFile(p, []byte("fixture override never executed"), 0600); e != nil {
			t.Fatal(e)
		}
		remount("/neonvm/runtime", true)
		defer func() {
			remount("/neonvm/runtime", false)
			if e := os.Remove(p); e != nil {
				t.Fatal(e)
			}
			remount("/neonvm/runtime", true)
		}()
		if _, e := CheckNormalSystemSource(context.Background(), raw); e == nil || !strings.Contains(e.Error(), "override") {
			t.Fatal("runtime shell override accepted", e)
		}
	})

	t.Run("actual-unlisted-glob-config-rejected", func(t *testing.T) {
		p := "/etc/vector/unlisted.yaml"
		remount("/", false)
		if e := os.WriteFile(p, []byte("unlisted fixture never executed"), 0644); e != nil {
			t.Fatal(e)
		}
		remount("/", true)
		defer func() {
			remount("/", false)
			if e := os.Remove(p); e != nil {
				t.Fatal(e)
			}
			remount("/", true)
		}()
		if _, e := CheckNormalSystemSource(context.Background(), raw); e == nil || !strings.Contains(e.Error(), "unlisted") {
			t.Fatal("unlisted glob config accepted", e)
		}
	})

	t.Run("actual-readonly-FIFO-rejected-without-block", func(t *testing.T) {
		p := "/neonvm/config/vector.yaml"
		original, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		remount("/", false)
		if e := os.Remove(p); e != nil {
			t.Fatal(e)
		}
		if e := unix.Mkfifo(p, 0644); e != nil {
			t.Fatal(e)
		}
		remount("/", true)
		defer func() {
			remount("/", false)
			if e := os.Remove(p); e != nil {
				t.Fatal(e)
			}
			if e := os.WriteFile(p, original, 0644); e != nil {
				t.Fatal(e)
			}
			remount("/", true)
		}()
		if _, e := CheckNormalSystemSource(context.Background(), raw); e == nil || !strings.Contains(e.Error(), "regular") {
			t.Fatal("special file accepted", e)
		}
	})

	t.Run("actual-empty-glob-alias-canonical-RW-ancestor-rejected", func(t *testing.T) {
		root := "/rw-parent"
		target := root + "/bucket/target"
		if e := unix.Mount("tmpfs", root, "tmpfs", unix.MS_NODEV|unix.MS_NOEXEC|unix.MS_NOSUID, "size=1m,mode=0755"); e != nil {
			t.Fatal(e)
		}
		defer func() {
			if e := unix.Unmount(root, 0); e != nil {
				t.Fatal(e)
			}
		}()
		if e := os.MkdirAll(target, 0755); e != nil {
			t.Fatal(e)
		}
		if e := unix.Mount(target, target, "", unix.MS_BIND, ""); e != nil {
			t.Fatal(e)
		}
		defer func() {
			if e := unix.Unmount(target, 0); e != nil {
				t.Fatal(e)
			}
		}()
		if e := unix.Mount("", target, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, ""); e != nil {
			t.Fatal(e)
		}
		remount("/", false)
		if e := os.Remove("/etc/vector"); e != nil {
			t.Fatal(e)
		}
		if e := os.Symlink(target, "/etc/vector"); e != nil {
			t.Fatal(e)
		}
		remount("/", true)
		defer func() {
			remount("/", false)
			if e := os.Remove("/etc/vector"); e != nil {
				t.Fatal(e)
			}
			if e := os.Mkdir("/etc/vector", 0755); e != nil {
				t.Fatal(e)
			}
			remount("/", true)
		}()
		// 真实构造必须满足旧logical只读检查和空库存，不能把target直接RW冒充该反例。
		if e := nativeBirthReadonlyPath("/etc/vector/.source-inventory-check"); e != nil {
			t.Fatal("fixture logical chain is not RO", e)
		}
		m, e := DecodeNormalSystemSourceManifest(raw)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = normalSystemConfigInventory("/", m); e != nil {
			t.Fatal("fixture inventory is not empty/valid", e)
		}
		if e = nativeBirthReadonlyPath(target + "/.source-inventory-check"); e == nil {
			t.Fatal("fixture canonical ancestor is not actually RW")
		}
		if _, e = CheckNormalSystemSource(context.Background(), raw); e == nil || !strings.Contains(e.Error(), "read-only") {
			t.Fatal("canonical RW ancestor was accepted", e)
		}
	})
	t.Run("actual-RW-root-rejected", func(t *testing.T) {
		remount("/", false)
		defer remount("/", true)
		if _, e := CheckNormalSystemSource(context.Background(), raw); e == nil || !strings.Contains(e.Error(), "read-only") {
			t.Fatal("RW ancestor accepted", e)
		}
	})
	t.Run("actual-root-login-even-matching-manifest-rejected", func(t *testing.T) {
		original, e := os.ReadFile("/etc/inittab")
		if e != nil {
			t.Fatal(e)
		}
		bad := append(append([]byte(nil), original...), []byte("ttyS0::respawn:/neonvm/bin/agetty --autologin root 115200 ttyS0\n")...)
		remount("/", false)
		if e := os.WriteFile("/etc/inittab", bad, 0644); e != nil {
			t.Fatal(e)
		}
		remount("/", true)
		defer func() {
			remount("/", false)
			if e := os.WriteFile("/etc/inittab", original, 0644); e != nil {
				t.Fatal(e)
			}
			remount("/", true)
		}()
		m, e := DecodeNormalSystemSourceManifest(raw)
		if e != nil {
			t.Fatal(e)
		}
		for i := range m.Files {
			if m.Files[i].Path == "/etc/inittab" {
				m.Files[i].Bytes = uint64(len(bad))
				m.Files[i].SHA256 = physicalDigest(bad)
			}
		}
		modified, e := json.Marshal(m)
		if e != nil {
			t.Fatal(e)
		}
		if _, e := CheckNormalSystemSource(context.Background(), modified); e == nil || !strings.Contains(e.Error(), "inittab") {
			t.Fatal("root login blessed by raw digest", e)
		}
	})
	t.Run("restored-real-reader", func(t *testing.T) {
		again, e := CheckNormalSystemSource(context.Background(), raw)
		if e != nil {
			t.Fatal(e)
		}
		if again.ManifestSHA256 != first.ManifestSHA256 || again.InittabSHA256 != first.InittabSHA256 {
			t.Fatal("restored source mismatch")
		}
	})
}
