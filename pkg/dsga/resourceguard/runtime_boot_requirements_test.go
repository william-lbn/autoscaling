//go:build linux

package resourceguard

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func bootRequirementsFixture(t *testing.T) ([]byte, map[string][]byte) {
	t.Helper()
	d := runtimeTemplatesFullFixture(t)
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	assets, err := RenderRuntimePolicyAssets(raw)
	if err != nil {
		t.Fatal(err)
	}
	assets[RuntimeResourceAssetName] = raw
	return raw, assets
}

// PG17-U-BOOT-001：完整原字节/五阶段/固定369参数；opaque返回值与slice不外泄修改权。
func TestRuntimeBootRequirementsNormalSplit(t *testing.T) {
	raw, assets := bootRequirementsFixture(t)
	req, err := DeriveRuntimeBootRequirements(raw, assets)
	if err != nil {
		t.Fatal(err)
	}
	if !req.Enabled() || req.ProtocolVersion() != 1 || req.ProfileID() != NormalSplitV1PhysicalKernelProfileID || req.BootMemoryUpperBoundBytes() != 256<<20 || req.PIDMaxLimit() != 512 || !req.KernelBuildReference().StrictPageCounterIRQBoundary() {
		t.Fatalf("wrong static requirements: %+v", req)
	}
	var d RuntimeDocument
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	if req.RuntimeDocumentSHA256() != runtimePolicyDigest(raw) || req.ResourceSourceSHA256() != d.SourceSHA256 || req.ConfigSecretUID() != d.ConfigSecretUID {
		t.Fatal("lost exact original references")
	}
	args := req.RequiredKernelArgs()
	if len(args) != 8 {
		t.Fatal(args)
	}
	cmd := "panic=-1 init=/neonvm/bin/init root=/dev/vda rw console=ttyS1 hostname=node1 " + strings.Join(args, " ")
	if err := req.ValidateKernelCommandLine(cmd); err != nil {
		t.Fatal(err)
	}
	args[0] = "mutated"
	if req.RequiredKernelArgs()[0] != ManagedKernelArg {
		t.Fatal("args alias caller")
	}
	for name := range assets {
		assets[name] = nil
	}
	if err := req.ValidateKernelCommandLine(cmd); err != nil {
		t.Fatal("caller map mutation changed private plan", err)
	}
}

// PG17-U-BOOT-002：旧v1不启新模式且准确非canonical原raw仍绑定，不覆盖旧auto选择。
func TestRuntimeBootRequirementsV1NoNewMode(t *testing.T) {
	d := runtimeTemplatesDocumentFixture(t)
	raw, _ := json.Marshal(d)
	raw = append([]byte(" \n"), raw...)
	assets := map[string][]byte{RuntimeResourceAssetName: raw}
	req, err := DeriveRuntimeBootRequirements(raw, assets)
	if err != nil {
		t.Fatal(err)
	}
	if req.Enabled() || len(req.RequiredKernelArgs()) != 0 || req.BootMemoryUpperBoundBytes() != 0 || req.RuntimeDocumentSHA256() != runtimePolicyDigest(raw) {
		t.Fatal("v1 gained normal plan")
	}
	if err := req.ValidateKernelCommandLine("neonvm.dsga=1 memhp_default_state=online memory_hotplug.online_policy=auto-movable memory_hotplug.auto_movable_ratio=301"); err != nil {
		t.Fatal("v1 old selection changed", err)
	}
	if req.ValidateKernelCommandLine("neonvm.dsga=1 "+NormalSplitTopologyKernelArg) == nil {
		t.Fatal("v1 accepted new-mode marker")
	}
	if (RuntimeBootRequirements{}).ValidateKernelCommandLine("") == nil {
		t.Fatal("zero version accepted")
	}
}

// PG17-U-BOOT-003：部分v2、实际资产漏改/未知短名、原raw漂移均拒，绝不降级。
func TestRuntimeBootRequirementsRejectsSourceAndAssets(t *testing.T) {
	for _, name := range []string{"empty_raw", "oversized_raw", "raw_drift", "missing_resource", "missing_policy", "changed_policy", "unknown", "physical_name_as_key", "partial_v2"} {
		t.Run(name, func(t *testing.T) {
			raw, assets := bootRequirementsFixture(t)
			switch name {
			case "empty_raw":
				raw = nil
			case "oversized_raw":
				raw = bytes.Repeat([]byte(" "), (64<<10)+1)
			case "raw_drift":
				raw = append([]byte(" "), raw...)
				assets[RuntimeResourceAssetName] = raw
			case "missing_resource":
				delete(assets, RuntimeResourceAssetName)
			case "missing_policy":
				delete(assets, "dsga-native-birth.json")
			case "changed_policy":
				assets["dsga-normal-split.json"] = append([]byte(" "), assets["dsga-normal-split.json"]...)
			case "unknown":
				assets["unknown.json"] = []byte("{}")
			case "physical_name_as_key":
				assets[RuntimeISOPreSystemAssetName] = assets["dsga-prebootstrap-system.json"]
				delete(assets, "dsga-prebootstrap-system.json")
			case "partial_v2":
				var d RuntimeDocument
				_ = json.Unmarshal(raw, &d)
				ts, _ := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
				ts.Templates = ts.Templates[:4]
				d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(ts.Templates))
				raw, _ = json.Marshal(d)
				var err error
				assets, err = RenderRuntimePolicyAssets(raw)
				if err != nil {
					t.Fatal(err)
				}
				assets[RuntimeResourceAssetName] = raw
			}
			if req, err := DeriveRuntimeBootRequirements(raw, assets); err == nil || req.Enabled() {
				t.Fatal("invalid source/asset granted requirements", name, err)
			}
		})
	}
}

// PG17-U-BOOT-004：最终-append，重复/错值/别名/旧auto/未知阶段/LSM/截断/语法冲突拒。
func TestRuntimeBootRequirementsRejectsKernelOverrides(t *testing.T) {
	raw, assets := bootRequirementsFixture(t)
	req, err := DeriveRuntimeBootRequirements(raw, assets)
	if err != nil {
		t.Fatal(err)
	}
	base := strings.Join(req.RequiredKernelArgs(), " ")
	cases := map[string]string{"empty": "", "NUL": base + "\x00", "truncation": base + strings.Repeat(" ", 2048), "quoted": base + ` hostname="node1"`, "separator": "-- " + base, "old_online": base + " memhp_default_state=online", "auto_policy": base + " memory_hotplug.online_policy=auto-movable", "auto_ratio": base + " memory_hotplug.auto_movable_ratio=301", "unknown_mode": base + " neonvm.dsga.future=1", "lsm": base + " lsm=landlock", "security": base + " security=apparmor", "movablecore": base + " movablecore=1G", "hyphen_alias": base + " memhp-default-state=online_movable", "movable_node": base + " movable_node"}
	for _, arg := range req.RequiredKernelArgs() {
		key, _, _ := strings.Cut(arg, "=")
		cases["missing_"+key] = strings.Replace(base, arg, "", 1)
		cases["duplicate_"+key] = base + " " + arg
		cases["bare_"+key] = strings.Replace(base, arg, key, 1)
	}
	for name, cmd := range cases {
		t.Run(name, func(t *testing.T) {
			if req.ValidateKernelCommandLine(cmd) == nil {
				t.Fatal("unsafe final command line accepted", cmd)
			}
		})
	}
}

// PG17-U-BOOT-005：BootUpper改为另一原显式上界只改计划上界，不生成base/kernelcore事实。
func TestRuntimeBootRequirementsUpperIsNotObservedBase(t *testing.T) {
	raw, assets := bootRequirementsFixture(t)
	first, err := DeriveRuntimeBootRequirements(raw, assets)
	if err != nil {
		t.Fatal(err)
	}
	var d RuntimeDocument
	_ = json.Unmarshal(raw, &d)
	ts, _ := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	var p map[string]json.RawMessage
	_ = json.Unmarshal(ts.Templates[2].Policy, &p)
	p["boot_memory_upper_bytes"] = json.RawMessage(`"536870912"`)
	ts.Templates[2].Policy, _ = json.Marshal(p)
	d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(ts.Templates))
	raw, _ = json.Marshal(d)
	assets, err = RenderRuntimePolicyAssets(raw)
	if err != nil {
		t.Fatal(err)
	}
	assets[RuntimeResourceAssetName] = raw
	second, err := DeriveRuntimeBootRequirements(raw, assets)
	if err != nil {
		t.Fatal(err)
	}
	if second.BootMemoryUpperBoundBytes() != 512<<20 || strings.Join(first.RequiredKernelArgs(), " ") != strings.Join(second.RequiredKernelArgs(), " ") {
		t.Fatal("upper intent changed actual kernel/base selection")
	}
	for _, arg := range second.RequiredKernelArgs() {
		if strings.HasPrefix(arg, "kernelcore=") || strings.HasPrefix(arg, "mem=") {
			t.Fatal("invented base memory from upper intent")
		}
	}
	if first.RuntimeDocumentSHA256() == second.RuntimeDocumentSHA256() {
		t.Fatal("changed original policy bytes lost source binding")
	}
}
