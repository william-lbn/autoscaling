//go:build linux && amd64

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"text/template"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

func TestNormalNativePGSelectionKeepsMajorSeparateFromCollector(t *testing.T) {
	pin := strings.Repeat("12", 32)
	for _, c := range []struct {
		name, mode, arch           string
		controls, collector, major uint
		pin                        string
		ok                         bool
	}{
		{"old-legacy", "legacy", targetArchLinuxArm64, 1, 0, 17, "", true},
		{"default-native17", normalSplitV3BuildMode, targetArchLinuxAmd64, 2, 1, 17, "", true},
		{"explicit18", normalSplitV3BuildMode, targetArchLinuxAmd64, 2, 1, 18, pin, true},
		{"unsupported16", normalSplitV3BuildMode, targetArchLinuxAmd64, 2, 1, 16, pin, false},
		{"unsupported19", normalSplitV3BuildMode, targetArchLinuxAmd64, 2, 1, 19, pin, false},
		{"18-not-collector18", normalSplitV3BuildMode, targetArchLinuxAmd64, 2, 18, 18, pin, false},
		{"18-old-mode", normalSplitV3BuildMode, targetArchLinuxAmd64, 2, 0, 18, pin, false},
		{"18-system1", normalSplitBuildMode, targetArchLinuxAmd64, 2, 1, 18, pin, false},
		{"18-controls1", normalSplitV3BuildMode, targetArchLinuxAmd64, 1, 1, 18, pin, false},
		{"18-arm", normalSplitV3BuildMode, targetArchLinuxArm64, 2, 1, 18, pin, false},
		{"18-no-pin", normalSplitV3BuildMode, targetArchLinuxAmd64, 2, 1, 18, "", false},
		{"18-zero-pin", normalSplitV3BuildMode, targetArchLinuxAmd64, 2, 1, 18, strings.Repeat("0", 64), false},
		{"18-uppercase-pin", normalSplitV3BuildMode, targetArchLinuxAmd64, 2, 1, 18, strings.Repeat("AB", 32), false},
		{"pin-with-old-collector", "legacy", targetArchLinuxAmd64, 1, 0, 17, pin, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if (validateNormalNativePGSelection(c.mode, c.arch, c.controls, c.collector, c.major, c.pin) == nil) != c.ok {
				t.Fatal("private major/collector selection differs")
			}
		})
	}
	if flag.Lookup("normal-native-pg-major").DefValue != "17" || flag.Lookup("normal-native-birth-version").DefValue != "0" || flag.Lookup("normal-native-postgres-sha256").DefValue != "" {
		t.Fatal("legacy CLI default changed")
	}
}

func TestNormalNativePGTemplatePreservesPG17DefaultAndPinsPG18(t *testing.T) {
	render := func(major uint32, pin string) string {
		var b bytes.Buffer
		c := TemplatesContext{NormalSplit: true, NormalSystemMode: normalSplitV3BuildMode, NormalOrdinaryControlsVersion: 2, NormalNativeBirthVersion: 1, NormalNativePGMajor: major, NormalNativePostgresSHA: pin}
		if err := template.Must(template.New("Dockerfile").Parse(dockerfileVmBuilder)).Execute(&b, c); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	old, pg17 := render(0, ""), render(17, "")
	if old != pg17 || strings.Contains(pg17, "--normal-native-pg-major") || strings.Contains(pg17, "--normal-native-postgres-sha256") {
		t.Fatal("PG17 default template bytes changed")
	}
	pin := strings.Repeat("12", 32)
	suffix := " --normal-native-pg-major=18 --normal-native-postgres-sha256=" + pin
	pg18 := render(18, pin)
	if !strings.Contains(pg18, suffix) || strings.Replace(pg18, suffix, "", 1) != pg17 {
		t.Fatal("PG18 selection changed unrelated Dockerfile bytes")
	}
}

func TestNormalNativePG17DefaultCollectorBytesUnchanged(t *testing.T) {
	root, system := normalNativeFixture(t)
	im, err := newNormalNativeImage(root)
	if err != nil {
		t.Fatal(err)
	}
	seed, _, err := im.seed()
	if err != nil {
		t.Fatal(err)
	}
	// 原未加major选择的独立采集器仍是PG17输出字节的参考；选择器不改写它。
	original, err := resourceguard.BuildNativeBirthManifestJSON(seed, system, im.canonical, im.open, im.lstat)
	if err != nil {
		t.Fatal(err)
	}
	if err = publishNormalNativeBirthImage(root, system); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(filepath.Join(root, normalNativeBuildPath))
	if err != nil || !bytes.Equal(actual, original) {
		t.Fatal("default PG17 collected manifest bytes changed", err)
	}
}

// GNU测试ELF只验证Builder的选择与原摘要约束，不把/bin/true标成实际PG18
// 认证；真实ed134 PG18资产及Neon插件来自独立原构建锁，另作完整镜像验收。
func TestNormalNativePG18PublicationUsesActualELFPin(t *testing.T) {
	root, system := normalNativeFixture(t)
	seedPath := filepath.Join(root, normalNativeBuildPath)
	raw, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.Replace(raw, []byte(`"pg_major":17`), []byte(`"pg_major":18`), 1)
	if err = os.WriteFile(seedPath, raw, 0444); err != nil {
		t.Fatal(err)
	}
	actualPG, err := os.ReadFile(filepath.Join(root, "/usr/local/bin/postgres"))
	if err != nil {
		t.Fatal(err)
	}
	pin := normalDigest(actualPG)
	if err = publishNormalNativeBirthImageForPG(root, system, 18, pin); err != nil {
		t.Fatal(err)
	}
	final, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = validateNormalNativePGManifest(final, 18, pin); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, resourceguard.NormalSystemSourceManifestPath))
	if err != nil || !bytes.Equal(got, system) {
		t.Fatal("selected major changed original System bytes", err)
	}
}

func TestNormalNativePGMismatchPublishesNoManifest(t *testing.T) {
	for _, fault := range []string{"default17-with-seed18", "18-with-seed17", "wrong-elf-pin", "missing-pin", "unsupported-major"} {
		t.Run(fault, func(t *testing.T) {
			root, system := normalNativeFixture(t)
			seedPath := filepath.Join(root, normalNativeBuildPath)
			seed, err := os.ReadFile(seedPath)
			if err != nil {
				t.Fatal(err)
			}
			major := uint32(18)
			actual, err := os.ReadFile(filepath.Join(root, "/usr/local/bin/postgres"))
			if err != nil {
				t.Fatal(err)
			}
			pin := normalDigest(actual)
			if fault != "18-with-seed17" {
				seed = bytes.Replace(seed, []byte(`"pg_major":17`), []byte(`"pg_major":18`), 1)
			}
			switch fault {
			case "default17-with-seed18":
				major, pin = 17, ""
			case "wrong-elf-pin":
				pin = strings.Repeat("12", 32)
			case "missing-pin":
				pin = ""
			case "unsupported-major":
				major = 19
			}
			if err = os.WriteFile(seedPath, seed, 0444); err != nil {
				t.Fatal(err)
			}
			if err = publishNormalNativeBirthImageForPG(root, system, major, pin); err == nil {
				t.Fatal("mismatched original major/ELF pin accepted")
			}
			got, err := os.ReadFile(seedPath)
			if err != nil || !bytes.Equal(seed, got) {
				t.Fatal("failure rewrote original seed", err)
			}
			if _, err = os.Lstat(filepath.Join(root, resourceguard.NormalSystemSourceManifestPath)); !os.IsNotExist(err) {
				t.Fatal("failure published System manifest", err)
			}
			pending, err := filepath.Glob(filepath.Join(filepath.Dir(seedPath), ".native-build-pending-*"))
			if err != nil || len(pending) != 0 {
				t.Fatal("failure left a staged declaration", err)
			}
		})
	}
}

func TestNormalNativePGManifestRequiresCollectedActualPostgresEntry(t *testing.T) {
	// 该helper只消费已被原采集器规范编码的manifest；这里独立负控其绑定项。
	for _, fault := range []string{"wrong-path", "wrong-major", "bad-pin"} {
		t.Run(fault, func(t *testing.T) {
			pin := strings.Repeat("12", 32)
			manifest := map[string]any{"pg_major": 18, "postgres_path": "/usr/local/bin/postgres", "files": []map[string]string{{"path": "/usr/local/bin/postgres", "sha256": pin}}}
			switch fault {
			case "wrong-path":
				manifest["postgres_path"] = "/other/postgres"
			case "wrong-major":
				manifest["pg_major"] = 17
			case "bad-pin":
				pin = strings.Repeat("34", 32)
			}
			raw, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if validateNormalNativePGManifest(raw, 18, pin) == nil {
				t.Fatal("unbound selected Postgres entry accepted")
			}
		})
	}
}
