package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kdomanski/iso9660/util"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 模型只验证Runner ISO消费，不授予真实VM/Secret/Guest/normal资格。
func runtimeISOFixture(t *testing.T, version uint32) map[string][]byte {
	t.Helper()
	s := resourceguard.SourceConfig{
		ProtocolVersion: 1,
		Config:          resourceguard.Config{VMUID: "model-vm", ComputeInstanceID: "model-compute", StateDir: resourceguard.FixedStateDir, SystemCgroup: true},
		IssuerEpoch:     1, BudgetCommitmentID: "model-budget",
		Limits: resourceguard.Limits{MemoryMaxBytes: 512 << 20, OutsideUpperBytes: 128 << 20, SystemMemoryMaxBytes: 64 << 20, PidsMaxTasks: 128, AddressSpaceMaxBytes: 4 << 30,
			HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 0}, {PageSizeKB: 1048576, Pages: 0}}},
		ControlPublicKey: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)),
	}
	source, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	source = append([]byte(" \n"), source...)
	sum := sha256.Sum256(source)
	d := resourceguard.RuntimeDocument{ProtocolVersion: version, Incarnation: "1234567890abcdef1234567890abcdef", ConfigSecretUID: "model-secret", SourceSHA256: hex.EncodeToString(sum[:]), SourceJSON: source}
	if version == 2 {
		policies := []map[string]any{
			{
				"protocol_version": 1, "profile_id": resourceguard.NormalSplitV1PhysicalKernelProfileID,
				"kernel_release":         "6.12.26-william-normal-split-v1-e210f5e3ee94",
				"kernel_notes_sha256":    "7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828",
				"kernel_artifact_sha256": "3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0",
				"kernel_config_sha256":   "11344757b0f979c7fee35e354bd706616c4283deaa67cc30c50747e4bca855be",
			},
			{"protocol_version": 1, "profile_id": resourceguard.NormalSplitV1PhysicalKernelProfileID, "controls": resourceguard.NormalV1KernelAllocatorControls{PIDMax: 512, MinFreeKBytes: 2048, WatermarkScaleFactor: 10, WatermarkBoostFactor: 0, PerCPUPagelistHighFraction: 16, CompactionProactiveness: 0}, "modules_disabled": 1, "numa_balancing": 0, "demotion_enabled": false},
			{"protocol_version": 2, "scope": "pre-bootstrap-system-placement-only", "physical_profile_id": resourceguard.NormalSplitV1PhysicalKernelProfileID, "filesystem_profile_sha256": strings.Repeat("ab", 32), "boot_memory_upper_bytes": "268435456"},
			{"protocol_version": 1, "scope": "normal-split-topology-preparation-only", "physical_profile_id": resourceguard.NormalSplitV1PhysicalKernelProfileID, "private_memory_max_bytes": "67108864", "initial_arrays_memory_max_bytes": "67108864", "private_pids_max_tasks": "64"},
			{"protocol_version": 1, "scope": "neon-native-birth-policy-v1", "build_manifest_sha256": strings.Repeat("cd", 32), "pg_major": 17, "max_buffers": 128, "startup_buffers": 32, "page_bytes": 4096, "element_bytes": [4]uint64{64, 8192, 16, 20}},
		}
		var entries []any
		for i, name := range []string{"dsga-physical-kernel.json", "dsga-kernel-bootstrap.json", "dsga-prebootstrap-system.json", "dsga-normal-split.json", "dsga-native-birth.json"} {
			entries = append(entries, map[string]any{"asset": name, "policy": policies[i]})
		}
		template := map[string]any{"protocol_version": 1, "scope": "neon-dsga-policy-templates-v1", "templates": entries}
		d.PolicyTemplatesJSON, err = json.Marshal(template)
		if err != nil {
			t.Fatal(err)
		}
		sum = sha256.Sum256(d.PolicyTemplatesJSON)
		d.PolicyTemplatesSHA256 = hex.EncodeToString(sum[:])
	}
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if version == 1 {
		raw = append([]byte(" \n"), raw...)
	}
	assets, err := resourceguard.RenderRuntimePolicyAssets(raw)
	if err != nil {
		t.Fatal(err)
	}
	assets[runtimeResourceAsset] = raw
	return assets
}

func cloneRuntimeISOFixture(m map[string][]byte) map[string][]byte {
	next := make(map[string][]byte, len(m))
	for k, v := range m {
		next[k] = append([]byte(nil), v...)
	}
	return next
}

func TestRunnerRuntimeISOAssetsPreserveOriginalBytes(t *testing.T) {
	for _, version := range []uint32{1, 2} {
		name := "v1"
		if version == 2 {
			name = "v2"
		}
		t.Run(name, func(t *testing.T) {
			assets := runtimeISOFixture(t, version)
			before := cloneRuntimeISOFixture(assets)
			ordered, err := validatedRuntimeISOAssets(assets)
			if err != nil {
				t.Fatal(err)
			}
			wantNames := []string{runtimeResourceAsset}
			if version == 2 {
				wantNames = append(wantNames, "dsga-physical-kernel.json", "dsga-kernel-bootstrap.json", "dsga-prebootstrap-system.json", "dsga-normal-split.json", "dsga-native-birth.json")
			}
			if len(ordered) != len(wantNames) {
				t.Fatal("wrong resource/policy set")
			}
			for i, name := range wantNames {
				if ordered[i].name != name {
					t.Fatal("wrong fixed asset order")
				}
			}
			for _, asset := range ordered {
				if !bytes.Equal(asset.data, before[asset.name]) {
					t.Fatalf("lost original bytes %q", asset.name)
				}
				assets[asset.name][0] ^= 1
				if !bytes.Equal(asset.data, before[asset.name]) {
					t.Fatalf("ISO asset aliases caller buffer %q", asset.name)
				}
			}
		})
	}
}

func TestRunnerRuntimeISOAssetsRejectBeforeOutput(t *testing.T) {
	base := runtimeISOFixture(t, 2)
	cases := map[string]func(map[string][]byte){
		"missing_resource":   func(m map[string][]byte) { delete(m, runtimeResourceAsset) },
		"empty_resource":     func(m map[string][]byte) { m[runtimeResourceAsset] = nil },
		"oversized_resource": func(m map[string][]byte) { m[runtimeResourceAsset] = bytes.Repeat([]byte(" "), 64*1024+1) },
		"noncanonical_v2":    func(m map[string][]byte) { m[runtimeResourceAsset] = append([]byte(" "), m[runtimeResourceAsset]...) },
		"missing_policy":     func(m map[string][]byte) { delete(m, "dsga-physical-kernel.json") },
		"empty_policy":       func(m map[string][]byte) { m["dsga-physical-kernel.json"] = nil },
		"modified_policy": func(m map[string][]byte) {
			m["dsga-physical-kernel.json"] = append([]byte(" "), m["dsga-physical-kernel.json"]...)
		},
		"unknown_file": func(m map[string][]byte) { m["extra.json"] = []byte("{}") },
		"path_traversal": func(m map[string][]byte) {
			m["../dsga-physical-kernel.json"] = m["dsga-physical-kernel.json"]
			delete(m, "dsga-physical-kernel.json")
		},
		"extra_stage": func(m map[string][]byte) { m["dsga-native-birth.json"] = []byte("{}") },
		"too_many": func(m map[string][]byte) {
			for _, n := range []string{"a", "b", "c", "d", "e"} {
				m[n] = []byte("{}")
			}
		},
	}
	for _, name := range []string{"dsga-kernel-bootstrap.json", "dsga-prebootstrap-system.json", "dsga-normal-split.json", "dsga-native-birth.json"} {
		cases["missing_"+name] = func(m map[string][]byte) { delete(m, name) }
		cases["modified_"+name] = func(m map[string][]byte) { m[name] = append([]byte(" "), m[name]...) }
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			assets := cloneRuntimeISOFixture(base)
			mutate(assets)
			dir := t.TempDir()
			for _, existing := range []bool{false, true} {
				output := filepath.Join(dir, "runtime.iso")
				sentinel := []byte("original output must stay untouched")
				if existing {
					if err := os.WriteFile(output, sentinel, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if err := createISO9660runtime(output, nil, nil, nil, nil, nil, false, nil, nil, assets); err == nil {
					t.Fatal("invalid asset map accepted")
				}
				actual, err := os.ReadFile(output)
				if existing {
					if err != nil || !bytes.Equal(actual, sentinel) {
						t.Fatal("rejected source truncated original output", err)
					}
				} else if !os.IsNotExist(err) {
					t.Fatal("rejected source created output", err)
				}
			}
		})
	}
	legacy := runtimeISOFixture(t, 1)
	legacy["dsga-physical-kernel.json"] = base["dsga-physical-kernel.json"]
	if _, err := validatedRuntimeISOAssets(legacy); err == nil {
		t.Fatal("v1 gained an unbound policy asset")
	}
}

func TestRunnerRuntimeISOAssetsActualRoundTrip(t *testing.T) {
	for _, version := range []uint32{0, 1, 2} {
		name := []string{"unmanaged", "v1", "v2"}[version]
		t.Run(name, func(t *testing.T) {
			var assets map[string][]byte
			if version != 0 {
				assets = runtimeISOFixture(t, version)
			}
			dir := t.TempDir()
			output := filepath.Join(dir, "runtime.iso")
			if err := createISO9660runtime(output, []string{"/usr/local/bin/postgres"}, nil, nil, nil, nil, false, nil, nil, assets); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(output)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			extracted := filepath.Join(dir, "extracted")
			if err := util.ExtractImageToDirectory(f, extracted); err != nil {
				t.Fatal(err)
			}
			actualEntries, readErr := os.ReadDir(extracted)
			if readErr != nil {
				t.Fatal(readErr)
			}
			var actualNames []string
			for _, entry := range actualEntries {
				actualNames = append(actualNames, entry.Name())
			}
			t.Logf("actual ISO identifiers: %q", actualNames)
			for name, want := range assets {
				diskName := name
				if name == "dsga-prebootstrap-system.json" {
					diskName = "dsga-pre-system.json"
				}
				got, err := os.ReadFile(filepath.Join(extracted, diskName))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("actual ISO asset %s changed: %v", name, err)
				}
			}
			entries, err := os.ReadDir(extracted)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != len(assets)+2 {
				t.Fatalf("actual ISO extra/missing files: %v", entries)
			}
			if _, err := os.Stat(filepath.Join(extracted, "command.sh")); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(extracted, "mounts.sh")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
