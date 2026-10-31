package resourceguard

import "testing"

// PG17-U-ISO-ASSET-001：真实writer标识的长度边界及六键注入无碰撞。
func TestRuntimeISOAssetNameExactAllowlist(t *testing.T) {
	want := map[string]string{
		"dsga-resource.json":            "dsga-resource.json",
		"dsga-physical-kernel.json":     "dsga-physical-kernel.json",
		"dsga-kernel-bootstrap.json":    "dsga-kernel-bootstrap.json",
		"dsga-prebootstrap-system.json": "dsga-pre-system.json",
		"dsga-normal-split.json":        "dsga-normal-split.json",
		"dsga-native-birth.json":        "dsga-native-birth.json",
	}
	seen := map[string]string{}
	for logical, physical := range want {
		t.Run(logical, func(t *testing.T) {
			got, err := RuntimeISOAssetName(logical)
			if err != nil || got != physical {
				t.Fatalf("mapping %q = %q, %v; want %q", logical, got, err, physical)
			}
			// ISO9660 writer的固定30字节标识包含版本后缀，不能只检查原文件名长度。
			if len(got)+len(";1") > 30 {
				t.Fatalf("physical ISO identifier would truncate: %q", got)
			}
			if prior, ok := seen[got]; ok {
				t.Fatalf("ISO physical collision: %q and %q", prior, logical)
			}
			seen[got] = logical
		})
	}
	if len(want) != 1+len(runtimePolicyAssetOrder) {
		t.Fatal("logical protocol changed without explicit ISO mapping review")
	}
	for _, logical := range runtimePolicyAssetOrder {
		if _, ok := want[logical]; !ok {
			t.Fatalf("unmapped protocol asset %q", logical)
		}
	}
}

// PG17-U-ISO-ASSET-002：短物理名不可反向授为逻辑输入，路径与截断别名均拒绝。
func TestRuntimeISOAssetNameRejectsAliasesAndPaths(t *testing.T) {
	for _, logical := range []string{"", RuntimeISOPreSystemAssetName, "dsga-prebootstrap-syste.json",
		"DSGA-resource.json", " dsga-resource.json", "dsga-resource.json ", "dsga-resource.json\n",
		"/dsga-resource.json", "../dsga-resource.json", "./dsga-resource.json", "sub/dsga-resource.json",
		"sub\\dsga-resource.json", "dsga-resource.json;1", "dsga-resource.json\x00", "unknown.json"} {
		t.Run(logical, func(t *testing.T) {
			got, err := RuntimeISOAssetName(logical)
			if err == nil || got != "" {
				t.Fatalf("unknown logical asset granted mapping: %q = %q, %v", logical, got, err)
			}
		})
	}
}
