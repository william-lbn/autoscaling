package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"k8s.io/apimachinery/pkg/api/resource"
)

// 只核生产Runner选择/argv：模型资产来自真实18c registry与完整政策版本，
// 不执行QEMU，不把512常量或这些测试升为实际Guest内存/normal资格。
func trustedInitialBaseAssets(t *testing.T, mode string, upper uint64) map[string][]byte {
	t.Helper()
	original := runtimeISOFixture(t, 2)
	var document resourceguard.RuntimeDocument
	if err := json.Unmarshal(original[resourceguard.RuntimeResourceAssetName], &document); err != nil {
		t.Fatal(err)
	}
	source, err := document.Source()
	if err != nil {
		t.Fatal(err)
	}
	source.Config.ExecutionModel = mode
	document.SourceJSON, err = json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(document.SourceJSON)
	document.SourceSHA256 = hex.EncodeToString(sum[:])
	var templates struct {
		ProtocolVersion uint32 `json:"protocol_version"`
		Scope           string `json:"scope"`
		Templates       []struct {
			Asset  string          `json:"asset"`
			Policy json.RawMessage `json:"policy"`
		} `json:"templates"`
	}
	if err = json.Unmarshal(document.PolicyTemplatesJSON, &templates); err != nil {
		t.Fatal(err)
	}
	ref, err := resourceguard.LookupPhysicalKernelBuild(resourceguard.NormalSplitMetadataV1PhysicalKernelProfileID)
	if err != nil {
		t.Fatal(err)
	}
	minFree, err := resourceguard.NormalMetadataStableMinFreeKBytes(upper)
	if err != nil {
		t.Fatal(err)
	}
	for i := range templates.Templates {
		var policy map[string]any
		if err = json.Unmarshal(templates.Templates[i].Policy, &policy); err != nil {
			t.Fatal(err)
		}
		switch templates.Templates[i].Asset {
		case "dsga-physical-kernel.json":
			policy["profile_id"] = ref.ProfileID
			policy["kernel_release"] = ref.KernelRelease
			policy["kernel_artifact_sha256"] = ref.KernelArtifactSHA256
			policy["kernel_config_sha256"] = ref.KernelConfigSHA256
			policy["kernel_notes_sha256"] = ref.KernelNotesSHA256
		case "dsga-kernel-bootstrap.json":
			policy["protocol_version"] = 2
			policy["profile_id"] = ref.ProfileID
			policy["max_user_namespaces"] = "0"
			controls := policy["controls"].(map[string]any)
			controls["min_free_kbytes"] = strconv.FormatUint(minFree, 10)
			controls["percpu_pagelist_high_fraction"] = 1024
		case "dsga-prebootstrap-system.json":
			policy["protocol_version"] = 3
			policy["physical_profile_id"] = ref.ProfileID
			policy["boot_memory_upper_bytes"] = strconv.FormatUint(upper, 10)
		case "dsga-normal-split.json":
			policy["protocol_version"] = 4
			policy["physical_profile_id"] = ref.ProfileID
			policy["memory_origin_protocol_version"] = 2
		}
		templates.Templates[i].Policy, err = json.Marshal(policy)
		if err != nil {
			t.Fatal(err)
		}
	}
	document.PolicyTemplatesJSON, err = json.Marshal(templates)
	if err != nil {
		t.Fatal(err)
	}
	sum = sha256.Sum256(document.PolicyTemplatesJSON)
	document.PolicyTemplatesSHA256 = hex.EncodeToString(sum[:])
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	assets, err := resourceguard.RenderRuntimePolicyAssets(raw)
	if err != nil {
		t.Fatal(err)
	}
	assets[resourceguard.RuntimeResourceAssetName] = raw
	return assets
}

func trustedInitialRunnerModel(t *testing.T) (*Config, *vmv1.VirtualMachineSpec, map[string][]byte) {
	t.Helper()
	cfg := runnerBootModelConfig()
	cfg.cpuScalingMode = vmv1.CpuScalingModeSysfs
	assets := trustedInitialBaseAssets(t, resourceguard.TrustedControllerExecutionModel, 512<<20)
	if err := configureRuntimeBootRequirements(cfg, assets); err != nil {
		t.Fatal(err)
	}
	spec := runnerBootModelSpec()
	spec.Guest.MemorySlotSize = resource.MustParse("512Mi")
	spec.Guest.MemorySlots = vmv1.MemorySlots{Min: 2, Use: 2, Max: 16}
	return cfg, spec, assets
}

func TestRunnerTrustedInitialBaseUsesValidatedOriginalSource(t *testing.T) {
	cfg, spec, assets := trustedInitialRunnerModel(t)
	if cfg.runtimeBoot.ProfileID() != resourceguard.NormalSplitMetadataV1PhysicalKernelProfileID || cfg.initialBootstrap.OriginalBaseBytes != 512<<20 {
		t.Fatal("trusted original18c did not select512MiB")
	}
	args, err := initialBootMemoryArguments(cfg, spec)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"-m", "size=536870912b,slots=1,maxmem=8589934592b", "-object", "memory-backend-ram,id=vmem0,size=8053063680b", "-device", "virtio-mem-pci,id=vm0,memdev=vmem0,node=0,block-size=8M,requested-size=0"}
	if strings.Join(args, " ") != strings.Join(want, " ") {
		t.Fatalf("trusted8GiB geometry differs: %q", args)
	}
	// 业务Use/Min可变化，但不能成为原内核base；调用者资产也不能别名篡改保存原raw。
	spec.Guest.MemorySlots.Min = 4
	spec.Guest.MemorySlots.Use = 4
	later, err := initialBootMemoryArguments(cfg, spec)
	if err != nil || strings.Join(later, " ") != strings.Join(want, " ") {
		t.Fatal("business minimum rewrote base", err, later)
	}
	assets[resourceguard.RuntimeResourceAssetName][0] ^= 1
	if _, err = initialBootMemoryArguments(cfg, spec); err != nil {
		t.Fatal("caller assets alias retained original raw", err)
	}
}

func TestRunnerTrustedInitialBaseKeepsLegacySelection(t *testing.T) {
	// 同一真实18c政策族只去掉execution_model时仍选原256；upper512不是base输入。
	cfg := runnerBootModelConfig()
	cfg.cpuScalingMode = vmv1.CpuScalingModeSysfs
	if err := configureRuntimeBootRequirements(cfg, trustedInitialBaseAssets(t, "", 512<<20)); err != nil {
		t.Fatal(err)
	}
	if cfg.initialBootstrap.OriginalBaseBytes != 256<<20 {
		t.Fatal("legacy mode changed by boot upper/18c metadata")
	}
	spec := runnerBootModelSpec()
	spec.Guest.MemorySlotSize = resource.MustParse("512Mi")
	spec.Guest.MemorySlots = vmv1.MemorySlots{Min: 2, Use: 2, Max: 16}
	args, err := initialBootMemoryArguments(cfg, spec)
	if err != nil || !strings.Contains(strings.Join(args, " "), "memory-backend-ram,id=vmem0,size=8321499136b") {
		t.Fatal("legacy256MiB geometry changed", err, args)
	}
}

func TestRunnerTrustedInitialBaseRejectsMixedSourceAndBounds(t *testing.T) {
	cases := map[string]func(*Config, *vmv1.VirtualMachineSpec){
		"legacy-base-on-trusted-source": func(c *Config, _ *vmv1.VirtualMachineSpec) { c.initialBootstrap.OriginalBaseBytes = 256 << 20 },
		"arbitrary-base":                func(c *Config, _ *vmv1.VirtualMachineSpec) { c.initialBootstrap.OriginalBaseBytes = 640 << 20 },
		"changed-original-raw":          func(c *Config, _ *vmv1.VirtualMachineSpec) { c.initialBootstrap.RuntimeJSON[0] ^= 1 },
		"minimum-below-base": func(_ *Config, s *vmv1.VirtualMachineSpec) {
			s.Guest.MemorySlotSize = resource.MustParse("256Mi")
			s.Guest.MemorySlots.Min = 1
		},
		"maximum-not-above-base": func(_ *Config, s *vmv1.VirtualMachineSpec) {
			s.Guest.MemorySlots = vmv1.MemorySlots{Min: 1, Use: 1, Max: 1}
		},
		"off-original-grid": func(_ *Config, s *vmv1.VirtualMachineSpec) { s.Guest.MemorySlotSize = resource.MustParse("536870913") },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			c, s, _ := trustedInitialRunnerModel(t)
			change(c, s)
			if args, err := initialBootMemoryArguments(c, s); err == nil || args != nil {
				t.Fatal("unbound trusted base emitted argv", args)
			}
		})
	}
	t.Run("original-upper-below512", func(t *testing.T) {
		c, _, _ := trustedInitialRunnerModel(t)
		if err := configureRuntimeBootRequirements(c, trustedInitialBaseAssets(t, resourceguard.TrustedControllerExecutionModel, 256<<20)); err == nil {
			t.Fatal("trusted512 exceeded original256 upper")
		}
		if c.runtimeBoot != nil || c.initialBootstrap != nil {
			t.Fatal("rejected source retained a prior executable plan")
		}
	})
}
