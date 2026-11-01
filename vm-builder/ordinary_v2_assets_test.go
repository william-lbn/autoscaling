package main

import (
	"bytes"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
