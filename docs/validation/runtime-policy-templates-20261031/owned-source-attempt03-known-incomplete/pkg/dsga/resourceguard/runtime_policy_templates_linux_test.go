//go:build linux

package resourceguard

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func runtimeTemplatesFullFixture(t *testing.T) RuntimeDocument {
	t.Helper()
	ref, err := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if err != nil {
		t.Fatal(err)
	}
	policies := []any{
		map[string]any{"protocol_version": 1, "profile_id": ref.ProfileID, "kernel_release": ref.KernelRelease, "kernel_notes_sha256": ref.KernelNotesSHA256, "kernel_artifact_sha256": ref.KernelArtifactSHA256, "kernel_config_sha256": ref.KernelConfigSHA256},
		map[string]any{"protocol_version": 1, "profile_id": ref.ProfileID, "controls": NormalV1KernelAllocatorControls{PIDMax: 512, MinFreeKBytes: 2048, WatermarkScaleFactor: 10, WatermarkBoostFactor: 0, PerCPUPagelistHighFraction: 16, CompactionProactiveness: 0}, "modules_disabled": 1, "numa_balancing": 0, "demotion_enabled": false},
		map[string]any{"protocol_version": 2, "scope": preSystemScope, "physical_profile_id": ref.ProfileID, "filesystem_profile_sha256": strings.Repeat("ab", 32), "boot_memory_upper_bytes": "268435456"},
		map[string]any{"protocol_version": 1, "scope": normalSplitScope, "physical_profile_id": ref.ProfileID, "private_memory_max_bytes": "67108864", "initial_arrays_memory_max_bytes": "67108864", "private_pids_max_tasks": "64"},
		map[string]any{"protocol_version": 1, "scope": nativeBirthPolicyScope, "build_manifest_sha256": strings.Repeat("cd", 32), "pg_major": 17, "max_buffers": 128, "startup_buffers": 32, "page_bytes": 4096, "element_bytes": [4]uint64{64, 8192, 16, 20}},
	}
	var entries []runtimePolicyTemplate
	for i, p := range policies {
		raw, _ := json.Marshal(p)
		entries = append(entries, runtimePolicyTemplate{Asset: runtimePolicyAssetOrder[i], Policy: raw})
	}
	return runtimeTemplatesSetBlob(runtimeTemplatesDocumentFixture(t), runtimeTemplatesEnvelope(entries))
}
func TestRuntimePolicyTemplatesRenderFullPrefixWithoutUIDCycle(t *testing.T) {
	d := runtimeTemplatesFullFixture(t)
	raw, _ := json.Marshal(d)
	assets, err := RenderRuntimePolicyAssets(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(assets) != 5 {
		t.Fatal("missing assets", assets)
	}
	for _, name := range runtimePolicyAssetOrder {
		var p map[string]any
		if err := json.Unmarshal(assets[name], &p); err != nil {
			t.Fatal(err)
		}
		if p["runtime_document_sha256"] != runtimePolicyDigest(raw) {
			t.Fatal("wrong exact document hash", name)
		}
		if name == "dsga-normal-split.json" || name == "dsga-native-birth.json" {
			if p["resource_source_sha256"] != d.SourceSHA256 {
				t.Fatal("resource bytes normalized", name)
			}
		}
	}
	var kernel NormalV1KernelBootstrapPolicy
	if err := json.Unmarshal(assets["dsga-kernel-bootstrap.json"], &kernel); err != nil {
		t.Fatal(err)
	}
	if kernel.PhysicalOriginPolicySHA256 != runtimePolicyDigest(assets["dsga-physical-kernel.json"]) {
		t.Fatal("kernel hash did not bind effective physical bytes")
	}
	if bytes.Contains(d.PolicyTemplatesJSON, []byte("runtime_document_sha256")) {
		t.Fatal("dynamic reference was fed back into templates")
	}
	again, _ := json.Marshal(d)
	if !bytes.Equal(raw, again) {
		t.Fatal("renderer mutated original document")
	}
	for _, name := range []string{"uid", "template_raw_whitespace", "source_raw_whitespace"} {
		t.Run(name, func(t *testing.T) {
			next := d
			switch name {
			case "uid":
				next.ConfigSecretUID = "second-real-secret-uid"
			case "template_raw_whitespace":
				next.PolicyTemplatesJSON = append([]byte(" \n"), next.PolicyTemplatesJSON...)
				next.PolicyTemplatesSHA256 = runtimePolicyDigest(next.PolicyTemplatesJSON)
			case "source_raw_whitespace":
				next.SourceJSON = append([]byte(" \n"), next.SourceJSON...)
				next.SourceSHA256 = runtimePolicyDigest(next.SourceJSON)
			}
			nextRaw, _ := json.Marshal(next)
			a, err := RenderRuntimePolicyAssets(nextRaw)
			if err != nil {
				t.Fatal(err)
			}
			if runtimePolicyDigest(nextRaw) == runtimePolicyDigest(raw) || bytes.Equal(a[runtimePolicyAssetOrder[0]], assets[runtimePolicyAssetOrder[0]]) {
				t.Fatal("original byte/UID change did not reach effective binding")
			}
		})
	}
	if _, err := RenderRuntimePolicyAssets(append([]byte(" "), raw...)); err == nil {
		t.Fatal("v2 noncanonical document accepted")
	}
	// 模板列表的输入顺序不是依赖顺序；输出固定拓扑。
	ts, _ := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	for i, j := 0, len(ts.Templates)-1; i < j; i, j = i+1, j-1 {
		ts.Templates[i], ts.Templates[j] = ts.Templates[j], ts.Templates[i]
	}
	d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(ts.Templates))
	raw, _ = json.Marshal(d)
	if _, err := RenderRuntimePolicyAssets(raw); err != nil {
		t.Fatal("fixed topology depended on input list order", err)
	}
}
func TestRuntimePolicyTemplatesTypedPolicyRejectsUnsupportedOrOverBudget(t *testing.T) {
	for _, name := range []string{"pg18_unsupported", "native_unknown", "build_hash", "kernel_controls", "kernel_profile", "physical_build", "pre_system_profile", "pre_system_version", "split_memory", "split_pids", "zero_as", "missing_static_budget", "physical_only"} {
		t.Run(name, func(t *testing.T) {
			d := runtimeTemplatesFullFixture(t)
			ts, _ := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
			index := 4
			switch name {
			case "kernel_controls", "kernel_profile":
				index = 1
			case "physical_build", "physical_only":
				index = 0
			case "pre_system_profile", "pre_system_version":
				index = 2
			case "split_memory", "split_pids", "missing_static_budget":
				index = 3
			}
			var p map[string]any
			_ = json.Unmarshal(ts.Templates[index].Policy, &p)
			switch name {
			case "pg18_unsupported":
				p["pg_major"] = 18
			case "native_unknown":
				p["extra"] = true
			case "build_hash":
				p["build_manifest_sha256"] = ""
			case "kernel_controls":
				p["controls"].(map[string]any)["percpu_pagelist_high_fraction"] = 0
			case "kernel_profile":
				p["profile_id"] = NormalV1PhysicalKernelProfileID
			case "physical_build":
				p["kernel_artifact_sha256"] = strings.Repeat("ef", 32)
			case "pre_system_profile":
				p["physical_profile_id"] = NormalV1PhysicalKernelProfileID
			case "pre_system_version":
				p["protocol_version"] = 1
			case "split_memory":
				p["private_memory_max_bytes"] = "536870912"
			case "split_pids":
				p["private_pids_max_tasks"] = "128"
			case "missing_static_budget":
				delete(p, "initial_arrays_memory_max_bytes")
			case "zero_as":
				var source SourceConfig
				_ = json.Unmarshal(d.SourceJSON, &source)
				source.Limits.AddressSpaceMaxBytes = 0
				d.SourceJSON, _ = json.Marshal(source)
				d.SourceSHA256 = runtimePolicyDigest(d.SourceJSON)
			case "physical_only":
				ts.Templates = ts.Templates[:1]
			}
			if name != "physical_only" {
				ts.Templates[index].Policy, _ = json.Marshal(p)
			}
			d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(ts.Templates))
			raw, _ := json.Marshal(d)
			_, err := RenderRuntimePolicyAssets(raw)
			if name == "physical_only" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("invalid effective typed policy accepted")
			}
		})
	}
}
