package main

import (
	"bytes"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestOrdinaryV2FixedAssetsKeepLegacyAndMarkerContract(t *testing.T) {
	legacyStart := normalComputeScriptV3()
	legacyVector := configVector
	legacyShutdown := scriptNormalShutdown
	for _, v := range []uint32{0, 1, 3, 99} {
		if got, e := normalOrdinaryV2BuildAssets(v); e == nil || got != nil {
			t.Fatalf("version %d accepted", v)
		}
	}
	assets, e := normalOrdinaryV2BuildAssets(2)
	if e != nil {
		t.Fatal(e)
	}
	if len(assets) != 4 {
		t.Fatal(len(assets))
	}
	m := map[string]ordinaryV2FixedAsset{}
	for _, a := range assets {
		if m[a.Path].Path != "" {
			t.Fatal("duplicate")
		}
		m[a.Path] = a
	}
	for _, p := range []string{"/neonvm/bin/vmstart", "/neonvm/bin/vmshutdown"} {
		s := string(m[p].Bytes)
		if !strings.Contains(s, resourceguard.NormalSystemOrdinaryV2AllowMarkerPath) || strings.Contains(s, "/run/neon-dsga/vmstart.allowed") {
			t.Fatal("marker disagreement")
		}
		cmd := exec.Command("/bin/sh", "-n")
		cmd.Stdin = strings.NewReader(s)
		if b, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("shell %v %s", e, b)
		}
	}
	start := string(m["/neonvm/bin/vmstart"].Bytes)
	want := strings.Replace(legacyStart, "/run/neon-dsga/vmstart.allowed", resourceguard.NormalSystemOrdinaryV2AllowMarkerPath, 1)
	if start != want || !strings.Contains(start, "check-guest >/dev/null || exit 1") || !strings.Contains(start, "exec 9>/run/neon-dsga/vmstart.lock") || !strings.Contains(start, "flock -x 9") || !strings.Contains(start, "env -i /neonvm/bin/dsga-resource-guard exec-system-role compute") {
		t.Fatal("lost original gate/flock/empty-env")
	}
	vector := m["/neonvm/config/vector.yaml"].Bytes
	reversed := bytes.Replace(vector, []byte("data_dir: "+resourceguard.NormalSystemOrdinaryV2VectorDataPath+"\n"), []byte("data_dir: /tmp\n"), 1)
	if !bytes.Equal(reversed, []byte(legacyVector)) || !bytes.Equal(m["/neonvm/config/chrony.conf"].Bytes, []byte(configChrony)) {
		t.Fatal("unrelated configuration changed")
	}
	if normalComputeScriptV3() != legacyStart || configVector != legacyVector || scriptNormalShutdown != legacyShutdown {
		t.Fatal("legacy mutated")
	}
	// 每次返回独立字节，不让下一构建继承调用者改过的脚本。
	assets[0].Bytes[0] = 'x'
	again, e := normalOrdinaryV2BuildAssets(2)
	if e != nil || again[0].Bytes[0] != '#' {
		t.Fatal("shared mutable asset")
	}
}

// 仅不可变测试构建导出本函数真实产出的四字节资产供隔离Guest；不接生产Builder模式。
func TestExportOrdinaryV2FixedAssets(t *testing.T) {
	output := os.Getenv("DSGA_ORDINARY_V2_ASSET_EXPORT")
	if output == "" {
		t.Skip("DEV explicit asset export only")
	}
	if !filepath.IsAbs(output) || filepath.Clean(output) != output {
		t.Fatal("canonical DEV output required")
	}
	assets, e := normalOrdinaryV2BuildAssets(2)
	if e != nil {
		t.Fatal(e)
	}
	for _, a := range assets {
		p := filepath.Join(output, a.Path[1:])
		if e = os.MkdirAll(filepath.Dir(p), 0755); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(p, a.Bytes, os.FileMode(a.Mode)); e != nil {
			t.Fatal(e)
		}
	}
}

func normalProductionConfigurationFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, logical := range []string{"/etc", "/neonvm/config"} {
		if e := os.MkdirAll(filepath.Join(root, logical), 0755); e != nil {
			t.Fatal(e)
		}
	}
	for path, raw := range map[string][]byte{
		"/neonvm/config/vector.yaml": []byte("DEV static configuration\n"),
		"/neonvm/config/chrony.conf": []byte("DEV static configuration\n"),
		"/etc/resolv.conf":           []byte("# TEST_ONLY network disabled\n"),
	} {
		if e := os.WriteFile(filepath.Join(root, path), raw, 0644); e != nil {
			t.Fatal(e)
		}
	}
	return root
}

func TestNormalProductionConfigurationsReplaceDEVBeforePublication(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("original root-owned build configuration preparation requires root")
	}
	root := normalProductionConfigurationFixture(t)
	if e := normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, true, strings.Repeat("ab", 32)); e == nil {
		t.Fatal("configuration preparation ran after a base image digest was already supplied")
	}
	if e := normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, false, ""); e == nil {
		t.Fatal("DEV seed passed the original Collector configuration gate")
	}
	if e := normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, true, ""); e != nil {
		t.Fatal(e)
	}
	assets, e := normalOrdinaryV2BuildAssets(2)
	if e != nil {
		t.Fatal(e)
	}
	for _, a := range assets {
		if a.Path != "/neonvm/config/vector.yaml" && a.Path != "/neonvm/config/chrony.conf" {
			continue
		}
		raw, e := os.ReadFile(filepath.Join(root, a.Path))
		if e != nil || !bytes.Equal(raw, a.Bytes) || bytes.Contains(raw, []byte("DEV static configuration")) {
			t.Fatalf("canonical production bytes not materialized: %s: %v", a.Path, e)
		}
		st, e := os.Stat(filepath.Join(root, a.Path))
		if e != nil || st.Mode().Perm() != 0644 {
			t.Fatalf("configuration mode differs: %s: %v", a.Path, e)
		}
	}
	dns, e := os.ReadFile(filepath.Join(root, "/etc/resolv.conf"))
	if e != nil || len(dns) != 0 {
		t.Fatal("test-only resolver was not replaced by an empty original DHCP inode", e)
	}
	// 此时允许base读取的已是真实配置，但不产生System/Native或hosts声明。
	for _, path := range []string{resourceguard.NormalSystemSourceManifestPath, normalNativeBuildPath, "/etc/hosts"} {
		if _, e := os.Lstat(filepath.Join(root, path)); !os.IsNotExist(e) {
			t.Fatalf("pre-base configuration step published an unrelated object: %s: %v", path, e)
		}
	}
	if e := normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, false, ""); e != nil {
		t.Fatal("canonical bytes did not pass subsequent Collector check", e)
	}
	if e := normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, true, ""); e != nil {
		t.Fatal("idempotent preparation failed", e)
	}
	if e := os.WriteFile(filepath.Join(root, "/neonvm/config/vector.yaml"), []byte("sources: {}\nsinks: {}\n"), 0644); e != nil {
		t.Fatal(e)
	}
	if e := normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, false, ""); e == nil {
		t.Fatal("different syntactically plausible configuration passed the fixed contract")
	}
}

func TestNormalProductionConfigurationsPreserveActualResolverAndLegacy(t *testing.T) {
	root := normalProductionConfigurationFixture(t)
	legacy, e := os.ReadFile(filepath.Join(root, "/neonvm/config/vector.yaml"))
	if e != nil {
		t.Fatal(e)
	}
	if e := normalProductionConfigurationPhase(root, normalSplitBuildMode, 1, false, ""); e != nil {
		t.Fatal(e)
	}
	after, e := os.ReadFile(filepath.Join(root, "/neonvm/config/vector.yaml"))
	if e != nil || !bytes.Equal(after, legacy) {
		t.Fatal("legacy selection was changed", e)
	}
	for _, mode := range []string{"legacy", normalSplitBuildMode} {
		if e := normalProductionConfigurationPhase(root, mode, 2, true, ""); e == nil {
			t.Fatal("prepare-only silently selected another mode", mode)
		}
	}
	if e := normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 1, true, ""); e == nil {
		t.Fatal("prepare-only silently upgraded controls1")
	}
	if os.Geteuid() != 0 {
		t.Skip("remaining root-owned file preparation requires root")
	}
	// 文档地址只用于测试，绝不读取执行机器的resolv.conf。
	dns := []byte("nameserver 192.0.2.53\nsearch example.invalid\n")
	if e := os.WriteFile(filepath.Join(root, "/etc/resolv.conf"), dns, 0644); e != nil {
		t.Fatal(e)
	}
	if e := normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, true, ""); e != nil {
		t.Fatal(e)
	}
	after, e = os.ReadFile(filepath.Join(root, "/etc/resolv.conf"))
	if e != nil || !bytes.Equal(after, dns) {
		t.Fatal("actual explicit resolver bytes were copied from Host or changed", e)
	}
}

func TestNormalProductionConfigurationsRejectUnsafeInputsBeforeWrites(t *testing.T) {
	for _, name := range []string{"image-root-alias", "config-parent-alias", "config-alias", "config-fifo", "config-hardlink", "resolver-alias", "unknown-resolver-marker"} {
		t.Run(name, func(t *testing.T) {
			root := normalProductionConfigurationFixture(t)
			vector := filepath.Join(root, "/neonvm/config/vector.yaml")
			switch name {
			case "image-root-alias":
				alias := filepath.Join(t.TempDir(), "alias")
				if e := os.Symlink(root, alias); e != nil {
					t.Fatal(e)
				}
				root = alias
			case "config-parent-alias":
				original := filepath.Join(root, "/neonvm/config")
				target := filepath.Join(root, "/neonvm/original-config")
				if e := os.Rename(original, target); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink("original-config", original); e != nil {
					t.Fatal(e)
				}
			case "config-alias":
				os.Remove(filepath.Join(root, "/neonvm/config/chrony.conf"))
				if e := os.Symlink("vector.yaml", filepath.Join(root, "/neonvm/config/chrony.conf")); e != nil {
					t.Fatal(e)
				}
			case "config-fifo":
				path := filepath.Join(root, "/neonvm/config/chrony.conf")
				os.Remove(path)
				if e := syscall.Mkfifo(path, 0600); e != nil {
					t.Fatal(e)
				}
			case "config-hardlink":
				if e := os.Link(filepath.Join(root, "/neonvm/config/chrony.conf"), filepath.Join(root, "/neonvm/config/second-reference")); e != nil {
					t.Fatal(e)
				}
			case "resolver-alias":
				os.Remove(filepath.Join(root, "/etc/resolv.conf"))
				if e := os.Symlink("../neonvm/config/vector.yaml", filepath.Join(root, "/etc/resolv.conf")); e != nil {
					t.Fatal(e)
				}
			case "unknown-resolver-marker":
				if e := os.WriteFile(filepath.Join(root, "/etc/resolv.conf"), []byte("# TEST_ONLY unknown resolver source\n"), 0644); e != nil {
					t.Fatal(e)
				}
			}
			if e := normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, true, ""); e == nil {
				t.Fatal("unsafe image input accepted")
			}
			raw, e := os.ReadFile(vector)
			if e != nil || !bytes.Equal(raw, []byte("DEV static configuration\n")) {
				t.Fatal("input validation failed after changing another configuration", e)
			}
		})
	}
	for _, root := range []string{"/", "relative", ""} {
		if e := normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, true, ""); e == nil {
			t.Fatal("non-private image root accepted", root)
		}
	}
}
