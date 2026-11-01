//go:build linux && amd64

package main

import (
	"bytes"
	"debug/elf"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"text/template"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

func normalNativeFixture(t *testing.T) (string, []byte) {
	t.Helper()
	root := normalV3BuilderFixture(t)
	plan := normalTestPlan()
	if e := prepareNormalSystemV3AssetsForControls(root, normalTestJSON(plan), 2); e != nil {
		t.Fatal(e)
	}
	// 只从原Host真实ELF复制测试夹具，debug/elf不接下方任何恶意变异输入。
	seen := map[string]bool{}
	var copyELF func(string, string)
	copyELF = func(source, target string) {
		if seen[target] {
			return
		}
		seen[target] = true
		raw, e := os.ReadFile(source)
		if e != nil {
			t.Fatal(e)
		}
		dest := filepath.Join(root, target)
		if e = os.MkdirAll(filepath.Dir(dest), 0755); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(dest, raw, 0555); e != nil {
			t.Fatal(e)
		}
		f, e := elf.Open(source)
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		for _, p := range f.Progs {
			if p.Type == elf.PT_INTERP {
				b, e := io.ReadAll(p.Open())
				if e != nil || len(b) == 0 {
					t.Fatal(e)
				}
				interp := string(b[:len(b)-1])
				canonical := "/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2"
				copyELF(interp, canonical)
				if e = os.MkdirAll(filepath.Join(root, filepath.Dir(interp)), 0755); e != nil {
					t.Fatal(e)
				}
				if interp != canonical {
					if e = os.Symlink(canonical, filepath.Join(root, interp)); e != nil && !os.IsExist(e) {
						t.Fatal(e)
					}
				}
			}
		}
		needed, e := f.ImportedLibraries()
		if e != nil {
			t.Fatal(e)
		}
		for _, name := range needed {
			found := ""
			for _, dir := range []string{"/lib/x86_64-linux-gnu", "/usr/lib/x86_64-linux-gnu", "/lib", "/usr/lib"} {
				candidate := filepath.Join(dir, name)
				if _, e := os.Stat(candidate); e == nil {
					found = candidate
					break
				}
			}
			if found == "" {
				t.Fatalf("original genuine fixture dependency missing: %s", name)
			}
			copyELF(found, filepath.Join("/lib/x86_64-linux-gnu", name))
		}
	}
	copyELF("/bin/true", "/usr/local/bin/postgres")
	seed := []byte(`{"protocol_version":1,"scope":"neon-native-pre-main-default-loader-v1","pg_major":17,"launch_path":"/usr/local/bin/postgres","postgres_path":"/usr/local/bin/postgres","interpreter_path":"","files":[{"path":"/usr/local/bin/postgres","sha256":"` + strings.Repeat("12", 32) + `"}]}`)
	if e := os.MkdirAll(filepath.Join(root, filepath.Dir(normalNativeBuildPath)), 0755); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(root, normalNativeBuildPath), seed, 0444); e != nil {
		t.Fatal(e)
	}
	system, e := generateNormalSystemManifestForVersion(root, plan, strings.Repeat("ab", 32), 3)
	if e != nil {
		t.Fatal(e)
	}
	return root, append(system, '\n')
}

func TestNormalNativeBirthBuildSelectorAndTemplate(t *testing.T) {
	for _, c := range []struct {
		mode, arch        string
		controls, version uint
		ok                bool
	}{{"legacy", targetArchLinuxArm64, 1, 0, true}, {normalSplitV3BuildMode, targetArchLinuxAmd64, 2, 1, true}, {normalSplitV3BuildMode, targetArchLinuxArm64, 2, 1, false}, {normalSplitV3BuildMode, targetArchLinuxAmd64, 1, 1, false}, {normalSplitBuildMode, targetArchLinuxAmd64, 2, 1, false}, {"legacy", targetArchLinuxAmd64, 2, 1, false}, {normalSplitV3BuildMode, targetArchLinuxAmd64, 2, 2, false}} {
		t.Run(fmt.Sprintf("%s-%s-%d-%d", c.mode, c.arch, c.controls, c.version), func(t *testing.T) {
			if (validateNormalNativeBirthBuildSelection(c.mode, c.arch, c.controls, c.version) == nil) != c.ok {
				t.Fatal("selector differs")
			}
		})
	}
	render := func(version uint32) string {
		var b bytes.Buffer
		c := TemplatesContext{NormalSplit: true, NormalSystemMode: normalSplitV3BuildMode, NormalOrdinaryControlsVersion: 2, NormalNativeBirthVersion: version}
		if e := template.Must(template.New("Dockerfile").Parse(dockerfileVmBuilder)).Execute(&b, c); e != nil {
			t.Fatal(e)
		}
		return b.String()
	}
	old, new := render(0), render(1)
	if strings.Contains(old, "--normal-native-birth-version") {
		t.Fatal("default changed")
	}
	suffix := " --normal-native-birth-version=1 --target-arch=linux/amd64"
	if !strings.Contains(new, suffix) || strings.Replace(new, suffix, "", 1) != old {
		t.Fatal("collector selector changed unrelated template bytes")
	}
}

func TestNormalNativeBirthBuildActualGNUClosureAndExactSystemBytes(t *testing.T) {
	root, system := normalNativeFixture(t)
	if e := publishNormalNativeBirthImage(root, system); e != nil {
		t.Fatal(e)
	}
	actual, e := os.ReadFile(filepath.Join(root, resourceguard.NormalSystemSourceManifestPath))
	if e != nil || !bytes.Equal(actual, system) {
		t.Fatal("System3 original bytes/newline changed", e)
	}
	var manifest struct {
		Interpreter string `json:"interpreter_path"`
		Files       []struct{ Path, SHA256 string }
		Closure     struct {
			SHA string `json:"system_manifest_sha256"`
		} `json:"runtime_closure"`
	}
	raw, e := os.ReadFile(filepath.Join(root, normalNativeBuildPath))
	if e != nil || json.Unmarshal(raw, &manifest) != nil {
		t.Fatal(e)
	}
	if manifest.Interpreter != "/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2" || manifest.Closure.SHA != normalDigest(system) || len(manifest.Files) < 3 {
		t.Fatalf("genuine GNU collector incomplete: %s", raw)
	}
	previous := ""
	for _, f := range manifest.Files {
		if f.Path <= previous {
			t.Fatal("not canonical sorted")
		}
		previous = f.Path
		actual, e := os.ReadFile(filepath.Join(root, f.Path))
		if e != nil || normalDigest(actual) != f.SHA256 {
			t.Fatal("actual collected bytes differ", e)
		}
	}
	if e = publishNormalNativeBirthImage(root, system); e == nil {
		t.Fatal("existing System3 output adopted")
	}
}

func TestNormalNativeBirthBuildRejectsUnapprovedRootInputs(t *testing.T) {
	for _, name := range []string{"cache", "preload", "hwcaps", "RPATH", "shadow", "missingNeeded", "FIFO", "rootEscape", "symlinkLoop", "rootAlias", "PG18", "helperMismatch"} {
		t.Run(name, func(t *testing.T) {
			root, system := normalNativeFixture(t)
			seed, e := os.ReadFile(filepath.Join(root, normalNativeBuildPath))
			if e != nil {
				t.Fatal(e)
			}
			switch name {
			case "cache", "preload":
				p := "/etc/ld.so.cache"
				if name == "preload" {
					p = "/etc/ld.so.preload"
				}
				if e = os.Symlink("/missing", filepath.Join(root, p)); e != nil {
					t.Fatal(e)
				}
			case "hwcaps":
				if e = os.Mkdir(filepath.Join(root, "/lib/x86_64-linux-gnu/glibc-hwcaps"), 0755); e != nil {
					t.Fatal(e)
				}
			case "RPATH":
				pg := filepath.Join(root, "/usr/local/bin/postgres")
				b, e := os.ReadFile(pg)
				if e != nil {
					t.Fatal(e)
				}
				order := binary.LittleEndian
				off := int(order.Uint64(b[32:40]))
				count := int(order.Uint16(b[56:58]))
				done := false
				for i := 0; i < count; i++ {
					ph := b[off+i*56 : off+(i+1)*56]
					if order.Uint32(ph[:4]) != uint32(elf.PT_DYNAMIC) {
						continue
					}
					start := int(order.Uint64(ph[8:16]))
					size := int(order.Uint64(ph[32:40]))
					for j := start; j < start+size; j += 16 {
						if order.Uint64(b[j:j+8]) == uint64(elf.DT_DEBUG) {
							order.PutUint64(b[j:j+8], uint64(elf.DT_RPATH))
							order.PutUint64(b[j+8:j+16], 0)
							done = true
							break
						}
					}
				}
				if !done {
					t.Fatal("genuine fixture lacks dynamic debug slot")
				}
				if e = os.WriteFile(pg, b, 0555); e != nil {
					t.Fatal(e)
				}
			case "shadow":
				if e = os.WriteFile(filepath.Join(root, "/lib/x86_64-linux-gnu/libc.so.6"), []byte("bad first loader object"), 0555); e != nil {
					t.Fatal(e)
				}
			case "missingNeeded":
				if e = os.Remove(filepath.Join(root, "/lib/x86_64-linux-gnu/libc.so.6")); e != nil {
					t.Fatal(e)
				}
			case "FIFO":
				if e = os.Remove(filepath.Join(root, "/lib/x86_64-linux-gnu/libc.so.6")); e != nil {
					t.Fatal(e)
				}
				if e = syscall.Mkfifo(filepath.Join(root, "/lib/x86_64-linux-gnu/libc.so.6"), 0600); e != nil {
					t.Fatal(e)
				}
			case "rootEscape", "symlinkLoop":
				pg := filepath.Join(root, "/usr/local/bin/postgres")
				if e = os.Remove(pg); e != nil {
					t.Fatal(e)
				}
				target := "../../../../../../etc/passwd"
				if name == "symlinkLoop" {
					target = "/usr/local/bin/postgres"
				}
				if e = os.Symlink(target, pg); e != nil {
					t.Fatal(e)
				}
			case "rootAlias":
				alias := root + "-alias"
				if e = os.Symlink(root, alias); e != nil {
					t.Fatal(e)
				}
				defer os.Remove(alias)
				root = alias
			case "PG18":
				if e = os.WriteFile(filepath.Join(root, normalNativeBuildPath), []byte(strings.Replace(string(seed), `"pg_major":17`, `"pg_major":18`, 1)), 0444); e != nil {
					t.Fatal(e)
				}
			case "helperMismatch":
				var m map[string]any
				if e = json.Unmarshal(seed, &m); e != nil {
					t.Fatal(e)
				}
				m["array_helper"] = map[string]any{"protocol_version": 1, "path": "/neonvm/bin/neon-dsga-array-populate", "sha256": strings.Repeat("34", 32)}
				b, _ := json.Marshal(m)
				if e = os.WriteFile(filepath.Join(root, normalNativeBuildPath), b, 0444); e != nil {
					t.Fatal(e)
				}
			}
			if e = publishNormalNativeBirthImage(root, system); e == nil {
				t.Fatal("unapproved root input accepted")
			}
			if _, e = os.Lstat(filepath.Join(root, resourceguard.NormalSystemSourceManifestPath)); !os.IsNotExist(e) {
				t.Fatal("failed collector published System output", e)
			}
		})
	}
}

// 显式绑定原真实发行rootfs/最终清单；只读采集不改变原nonce，也不签VM/Source。
func TestNormalNativeBirthBuildOriginalPG17Rootfs16(t *testing.T) {
	root := os.Getenv("NEON_NATIVE_BIRTH_ORIGINAL_ROOTFS")
	if root == "" {
		t.Skip("requires explicit immutable original PG17 rootfs provider")
	}
	im, e := newNormalNativeImage(root)
	if e != nil {
		t.Fatal(e)
	}
	seed, _, e := im.seed()
	if e != nil {
		t.Fatal(e)
	}
	system, e := os.ReadFile(filepath.Join(root, resourceguard.NormalSystemSourceManifestPath))
	if e != nil {
		t.Fatal(e)
	}
	raw, e := resourceguard.BuildNativeBirthManifestJSON(seed, system, im.canonical, im.open, im.lstat)
	if e != nil {
		t.Fatal(e)
	}
	var got, want struct {
		PGMajor     uint32 `json:"pg_major"`
		Postgres    string `json:"postgres_path"`
		Interpreter string `json:"interpreter_path"`
		Files       []struct {
			Path string `json:"path"`
			SHA  string `json:"sha256"`
		}
	}
	if e = json.Unmarshal(raw, &got); e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(seed, &want); e != nil {
		t.Fatal(e)
	}
	if got.PGMajor != 17 || got.Postgres != "/usr/local/bin/postgres" || len(got.Files) != 16 || !bytes.Equal(mustNativeJSON(t, got), mustNativeJSON(t, want)) {
		t.Fatalf("original independently recorded16 closure differs: %s", raw)
	}
	pg, e := os.ReadFile(filepath.Join(root, got.Postgres))
	if e != nil || normalDigest(pg) != "649f4535d07e682fe5015fd8df8be23f626e73bd772da21b16e1c032721c53c6" {
		t.Fatal("wrong actual PG17 provider", e)
	}
	t.Logf("original PG17 rootfs16 collector only, source=false normal=false manifestSHA=%s", normalDigest(raw))
}
func mustNativeJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
