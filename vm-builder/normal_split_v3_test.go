//go:build linux

package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 复用既有最终库存夹具。仅把旧magic-only guard换成debug/elf可解析的最小
// ELF64结构；它从不执行，不充当正式guard/Root/Source或ELF闭包资格。
func normalV3BuilderFixture(t *testing.T) string {
	t.Helper()
	root := normalFixtureRoot(t)
	b := make([]byte, 64)
	copy(b, []byte{0x7f, 'E', 'L', 'F', 2, 1, 1})
	binary.LittleEndian.PutUint16(b[16:18], 2)
	binary.LittleEndian.PutUint16(b[18:20], 62)
	binary.LittleEndian.PutUint32(b[20:24], 1)
	binary.LittleEndian.PutUint16(b[52:54], 64)
	if e := os.WriteFile(filepath.Join(root, "/neonvm/bin/dsga-resource-guard"), b, 0755); e != nil {
		t.Fatal(e)
	}
	return root
}
func normalV3MustRead(t *testing.T, p string) []byte {
	t.Helper()
	b, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func normalV3SHA(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }

func TestNormalSplitV3SelectionKeepsLegacyAndV2(t *testing.T) {
	raw := normalTestJSON(normalTestPlan())
	for _, mode := range []string{normalSplitBuildMode, normalSplitV3BuildMode} {
		t.Run(mode, func(t *testing.T) {
			if e := validateNormalBuildSelection(mode, targetArchLinuxAmd64, nil, nil, raw); e != nil {
				t.Fatal(e)
			}
			for _, tc := range []struct {
				name, arch string
				spec       *imageSpec
				args       []string
				raw        []byte
			}{
				{"arm", targetArchLinuxArm64, nil, nil, raw}, {"custom-spec", targetArchLinuxAmd64, &imageSpec{}, nil, raw},
				{"build-arg", targetArchLinuxAmd64, nil, []string{"X=y"}, raw}, {"no-plan", targetArchLinuxAmd64, nil, nil, nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if e := validateNormalBuildSelection(mode, tc.arch, tc.spec, tc.args, tc.raw); e == nil {
						t.Fatal("invalid normal selector accepted")
					}
				})
			}
		})
	}
	if e := validateNormalBuildSelection("legacy", targetArchLinuxArm64, &imageSpec{Build: "FROM original"}, []string{"X=y"}, nil); e != nil {
		t.Fatal(e)
	}
	if e := validateNormalBuildSelection("legacy", targetArchLinuxAmd64, nil, nil, raw); e == nil {
		t.Fatal("legacy borrowed normal plan")
	}
	for _, mode := range []string{"normal-split-v2", "NORMAL-SPLIT-V3", "normal-split-v3 ", ""} {
		if e := validateNormalBuildSelection(mode, targetArchLinuxAmd64, nil, nil, raw); e == nil {
			t.Fatal("unknown selector accepted", mode)
		}
	}
}

func TestNormalSplitV3AssetsBindExactOriginalBytes(t *testing.T) {
	root := normalV3BuilderFixture(t)
	// 原raw含尾部空白，声明应绑定准确原文而不是悄悄重编码后的另一个摘要。
	plan := append(normalTestJSON(normalTestPlan()), '\n')
	guard := normalV3MustRead(t, filepath.Join(root, "/neonvm/bin/dsga-resource-guard"))
	if e := prepareNormalSystemV3Assets(root, plan); e != nil {
		t.Fatal(e)
	}
	var broker struct {
		ProtocolVersion        uint32 `json:"protocol_version"`
		ExecutablePath         string `json:"executable_path"`
		ExecutableSHA256       string `json:"executable_sha256"`
		ComputeBuildPlanSHA256 string `json:"compute_build_plan_sha256"`
		ArgvSHA256             string `json:"argv_sha256"`
		EnvironmentSHA256      string `json:"environment_sha256"`
	}
	b := normalV3MustRead(t, filepath.Join(root, resourceguard.NormalSystemRoleBrokerDeclarationPath))
	if e := json.Unmarshal(b, &broker); e != nil {
		t.Fatal(e)
	}
	if broker.ProtocolVersion != 1 || broker.ExecutablePath != "/neonvm/bin/dsga-resource-guard" || broker.ExecutableSHA256 != normalV3SHA(guard) || broker.ComputeBuildPlanSHA256 != normalV3SHA(plan) {
		t.Fatal("broker lost original guard/plan byte binding")
	}
	argv, _ := json.Marshal([]string{"/neonvm/bin/dsga-resource-guard", "system-role-broker"})
	env, _ := json.Marshal([]string{"PATH=/neonvm/bin"})
	if broker.ArgvSHA256 != normalV3SHA(argv) || broker.EnvironmentSHA256 != normalV3SHA(env) {
		t.Fatal("broker vectors differ")
	}
	controls, e := resourceguard.NormalKernelControlSourceManifestJSON()
	if e != nil {
		t.Fatal(e)
	}
	for name, want := range map[string][]byte{normalPlanPath: plan, resourceguard.NormalSystemRoleBrokerDeclarationPath: b, resourceguard.NormalKernelControlSourceManifestPath: controls} {
		got := normalV3MustRead(t, filepath.Join(root, name))
		if !bytes.Equal(got, want) {
			t.Fatal("asset bytes differ", name)
		}
		st, e := os.Lstat(filepath.Join(root, name))
		if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0222 != 0 {
			t.Fatal("asset not fixed RO regular", name, e)
		}
	}
}

func TestNormalSplitV3AssetsRejectOverwriteAndAliases(t *testing.T) {
	for _, name := range []string{normalPlanPath, resourceguard.NormalSystemRoleBrokerDeclarationPath, resourceguard.NormalKernelControlSourceManifestPath} {
		for _, alias := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/alias=%t", name, alias), func(t *testing.T) {
				root := normalV3BuilderFixture(t)
				path := filepath.Join(root, name)
				original := []byte("original object must survive\n")
				target := path
				if alias {
					target = filepath.Join(root, "alias-target")
					if e := os.Symlink(target, path); e != nil {
						t.Fatal(e)
					}
				}
				if e := os.WriteFile(target, original, 0444); e != nil {
					t.Fatal(e)
				}
				if e := prepareNormalSystemV3Assets(root, normalTestJSON(normalTestPlan())); e == nil {
					t.Fatal("existing asset overwritten")
				}
				if !bytes.Equal(normalV3MustRead(t, target), original) {
					t.Fatal("original object modified")
				}
			})
		}
	}
	for _, name := range []string{"/neonvm/config", "/neonvm/bin", "/neonvm/bin/dsga-resource-guard"} {
		t.Run(name, func(t *testing.T) {
			root := normalV3BuilderFixture(t)
			path := filepath.Join(root, name)
			target := path + "-original"
			if e := os.Rename(path, target); e != nil {
				t.Fatal(e)
			}
			if e := os.Symlink(target, path); e != nil {
				t.Fatal(e)
			}
			if e := prepareNormalSystemV3Assets(root, normalTestJSON(normalTestPlan())); e == nil {
				t.Fatal("fixed directory/guard alias accepted")
			}
		})
	}
	root := normalV3BuilderFixture(t)
	if e := prepareNormalSystemV3Assets(root, []byte(`{"protocol_version":1}`)); e == nil {
		t.Fatal("incomplete plan accepted")
	}
	if _, e := os.Stat(filepath.Join(root, normalPlanPath)); !os.IsNotExist(e) {
		t.Fatal("invalid plan produced asset")
	}
}

func TestNormalSplitV3FinalInventoryAndOldDefault(t *testing.T) {
	root := normalV3BuilderFixture(t)
	plan := normalTestPlan()
	raw, e := generateNormalSystemManifest(root, plan, strings.Repeat("ab", 32))
	if e != nil {
		t.Fatal(e)
	}
	old, e := resourceguard.DecodeNormalSystemSourceManifest(raw)
	if e != nil || old.ProtocolVersion != 2 || old.ProfileID != resourceguard.NormalSystemSourceV2ProfileID {
		t.Fatal("default inventory silently upgraded", e)
	}
	if e := prepareNormalSystemV3Assets(root, normalTestJSON(plan)); e != nil {
		t.Fatal(e)
	}
	entries, e := resourceguard.NormalSystemSourceInitEntriesForVersion(3)
	if e != nil {
		t.Fatal(e)
	}
	inittab := []byte(strings.Join(entries, "\n") + "\n")
	if e := os.WriteFile(filepath.Join(root, "/etc/inittab"), inittab, 0444); e != nil {
		t.Fatal(e)
	}
	raw, e = generateNormalSystemManifestForVersion(root, plan, strings.Repeat("ab", 32), 3)
	if e != nil {
		t.Fatal(e)
	}
	m, e := resourceguard.DecodeNormalSystemSourceManifest(raw)
	if e != nil || m.ProtocolVersion != 3 || m.ProfileID != resourceguard.NormalSystemSourceV3ProfileID {
		t.Fatal("wrong v3 final inventory", e)
	}
	var found = map[string]bool{}
	for _, f := range m.Files {
		if f.Path == "/etc/inittab" && f.SHA256 != normalV3SHA(inittab) {
			t.Fatal("final inittab bytes omitted")
		}
		for _, name := range []string{normalPlanPath, resourceguard.NormalSystemRoleBrokerDeclarationPath, resourceguard.NormalKernelControlSourceManifestPath} {
			if f.Path == name {
				found[name] = true
				if f.Kind != "config" || f.CanonicalPath != name || f.SHA256 != normalV3SHA(normalV3MustRead(t, filepath.Join(root, name))) {
					t.Fatal("v3 metadata not exact canonical inventory", name)
				}
			}
		}
	}
	if len(found) != 3 {
		t.Fatal("v3 metadata missing", found)
	}
	for _, version := range []uint32{0, 1, 4} {
		if _, e := generateNormalSystemManifestForVersion(root, plan, strings.Repeat("ab", 32), version); e == nil {
			t.Fatal("unsupported final version accepted", version)
		}
	}
}

func TestNormalSplitV3DockerTemplatePassesExplicitMode(t *testing.T) {
	for _, mode := range []string{"legacy", normalSplitBuildMode, normalSplitV3BuildMode} {
		t.Run(mode, func(t *testing.T) {
			c := TemplatesContext{RootDiskImage: "source@sha256:" + strings.Repeat("ab", 32), NeonvmDaemonImage: "daemon@sha256:" + strings.Repeat("bc", 32), BusyboxImage: "busybox", BusyboxImageTag: "1", AlpineImage: "alpine", AlpineImageTag: "3", NormalSplit: mode != "legacy", NormalSystemMode: mode, NormalUID: 1000, NormalGID: 1000, NormalBaseSHA: strings.Repeat("ab", 32)}
			var b bytes.Buffer
			tw := tar.NewWriter(&b)
			if e := AddTemplatedFileToTar(tw, c, "Dockerfile", dockerfileVmBuilder); e != nil {
				t.Fatal(e)
			}
			if e := tw.Close(); e != nil {
				t.Fatal(e)
			}
			tr := tar.NewReader(&b)
			if _, e := tr.Next(); e != nil {
				t.Fatal(e)
			}
			raw, e := io.ReadAll(tr)
			if e != nil {
				t.Fatal(e)
			}
			text := string(raw)
			if mode == "legacy" {
				if strings.Contains(text, "RUN /normal-manifest-tool") || !strings.Contains(text, "openssh-server") {
					t.Fatal("legacy template changed")
				}
				return
			}
			want := "RUN /normal-manifest-tool --system-mode=" + mode + " --normal-manifest-root=/rootdisk"
			prepare := strings.Index(text, "RUN /rootdisk/neonvm/bin/busybox sh /normal-prepare-root.sh")
			inventory := strings.Index(text, want)
			mkfs := strings.Index(text, "mkfs.ext4 -L vmroot")
			if prepare < 0 || inventory <= prepare || mkfs <= inventory {
				t.Fatal("explicit mode or final inventory order lost")
			}
		})
	}
}

func TestNormalSplitV3ComputeScriptKeepsGateAndOriginalFlock(t *testing.T) {
	flock, e := exec.LookPath("flock")
	if e != nil {
		t.Fatal("test environment needs real util-linux flock", e)
	}
	for _, mode := range []string{"guard-reject", "allow-start-missing", "success", "success-new-lock"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			bin := filepath.Join(root, "bin")
			run := filepath.Join(root, "run")
			if e := os.Mkdir(bin, 0755); e != nil {
				t.Fatal(e)
			}
			if e := os.Mkdir(run, 0755); e != nil {
				t.Fatal(e)
			}
			trace := filepath.Join(root, "trace")
			checked := filepath.Join(root, "checked")
			final := filepath.Join(root, "final")
			lock := filepath.Join(run, "vmstart.lock")
			// 真shell/真flock机制夹具，仅固定路径重映射；不把stub guard当真实Root。
			bb := "#!/bin/sh\nop=$1; shift\ncase \"$op\" in flock) exec " + flock + " \"$@\";; env) exec /usr/bin/env \"$@\";; *) exit 91;; esac\n"
			guard := fmt.Sprintf("#!/bin/sh\ncase \"$1\" in check-guest) /bin/touch %q; [ %q != guard-reject ] || exit 17;; exec-system-role) [ \"$#\" = 2 ] && [ \"$2\" = compute ] || exit 72; [ -z \"${CALLER_INJECTION+x}\" ] || exit 73; exec 8>%q; if %q -n 8; then exit 74; fi; printf 'compute\\n' >>%q; /bin/touch %q;; *) exit 75;; esac\n", checked, mode, lock, flock, trace, final)
			if e := os.WriteFile(filepath.Join(bin, "busybox"), []byte(bb), 0755); e != nil {
				t.Fatal(e)
			}
			if e := os.WriteFile(filepath.Join(bin, "dsga-resource-guard"), []byte(guard), 0755); e != nil {
				t.Fatal(e)
			}
			if strings.HasPrefix(mode, "success") {
				if e := os.WriteFile(filepath.Join(run, "vmstart.allowed"), nil, 0600); e != nil {
					t.Fatal(e)
				}
			}
			script := strings.NewReplacer("/neonvm/bin", bin, "/run/neon-dsga", run).Replace(normalComputeScriptV3())
			if strings.Contains(script, "vmstarter") || strings.Contains(script, "compute_ctl") || strings.Contains(script, "su-exec") {
				t.Fatal("v3 wrapper bypasses broker")
			}
			file := filepath.Join(root, "vmstart")
			if e := os.WriteFile(file, []byte(script), 0755); e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "/bin/sh", file)
			cmd.Env = []string{"CALLER_INJECTION=must-not-survive"}
			var output bytes.Buffer
			cmd.Stdout = &output
			cmd.Stderr = &output
			var held *os.File
			if mode == "success" {
				held, e = os.OpenFile(lock, os.O_CREATE|os.O_RDWR, 0600)
				if e != nil {
					t.Fatal(e)
				}
				defer held.Close()
				if e = syscall.Flock(int(held.Fd()), syscall.LOCK_EX); e != nil {
					t.Fatal(e)
				}
				defer syscall.Flock(int(held.Fd()), syscall.LOCK_UN)
			}
			if e := cmd.Start(); e != nil {
				t.Fatal(e)
			}
			if mode == "success" {
				deadline := time.Now().Add(3 * time.Second)
				for {
					if _, e := os.Stat(checked); e == nil {
						break
					}
					if time.Now().After(deadline) {
						cancel()
						_ = cmd.Wait()
						t.Fatal("check gate not reached", output.String())
					}
					time.Sleep(5 * time.Millisecond)
				}
				if _, e := os.Stat(final); !os.IsNotExist(e) {
					cancel()
					_ = cmd.Wait()
					t.Fatal("compute ignored prior independent OFD lock")
				}
				if e := syscall.Flock(int(held.Fd()), syscall.LOCK_UN); e != nil {
					cancel()
					_ = cmd.Wait()
					t.Fatal(e)
				}
			}
			err := cmd.Wait()
			if !strings.HasPrefix(mode, "success") {
				if err == nil {
					t.Fatal("failed gate/allow-start allowed compute")
				}
				if _, e := os.Stat(final); !os.IsNotExist(e) {
					t.Fatal("failed gate reached client")
				}
				return
			}
			if err != nil {
				t.Fatal("success wrapper", err, output.String())
			}
			if string(normalV3MustRead(t, trace)) != "compute\n" {
				t.Fatal("not exactly one fixed compute request")
			}
			st, e := os.Stat(lock)
			if e != nil || st.Mode().Perm() != 0600 {
				t.Fatal("fixed lock mode differs", e)
			}
		})
	}
}
