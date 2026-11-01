//go:build linux

package resourceguard

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"strconv"
	"strings"
	"testing"
)

func metadataWiringDocument(t *testing.T) RuntimeDocument {
	t.Helper()
	d := runtimeTemplatesFullFixture(t)
	ts, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	require.NoError(t, e)
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	require.NoError(t, e)
	var bootUpper uint64
	for _, template := range ts.Templates {
		if template.Asset == "dsga-prebootstrap-system.json" {
			var p map[string]any
			require.NoError(t, json.Unmarshal(template.Policy, &p))
			value, ok := p["boot_memory_upper_bytes"].(string)
			require.True(t, ok)
			bootUpper, e = strconv.ParseUint(value, 10, 64)
			require.NoError(t, e)
		}
	}
	stableMinFree, e := NormalMetadataStableMinFreeKBytes(bootUpper)
	require.NoError(t, e)
	for i := range ts.Templates {
		p := map[string]any{}
		require.NoError(t, json.Unmarshal(ts.Templates[i].Policy, &p))
		switch ts.Templates[i].Asset {
		case "dsga-physical-kernel.json":
			p["profile_id"] = ref.ProfileID
			p["kernel_release"] = ref.KernelRelease
			p["kernel_artifact_sha256"] = ref.KernelArtifactSHA256
			p["kernel_config_sha256"] = ref.KernelConfigSHA256
			p["kernel_notes_sha256"] = ref.KernelNotesSHA256
		case "dsga-kernel-bootstrap.json":
			p["protocol_version"] = 2
			p["profile_id"] = ref.ProfileID
			p["max_user_namespaces"] = "0"
			controls, ok := p["controls"].(map[string]any)
			require.True(t, ok)
			controls["min_free_kbytes"] = strconv.FormatUint(stableMinFree, 10)
		case "dsga-prebootstrap-system.json":
			p["protocol_version"] = 3
			p["physical_profile_id"] = ref.ProfileID
		case "dsga-normal-split.json":
			p["protocol_version"] = 4
			p["physical_profile_id"] = ref.ProfileID
			p["memory_origin_protocol_version"] = 2
		}
		ts.Templates[i].Policy, e = json.Marshal(p)
		require.NoError(t, e)
	}
	return runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(ts.Templates))
}
func metadataWiringAssets(t *testing.T, d RuntimeDocument) ([]byte, map[string][]byte) {
	t.Helper()
	raw, e := json.Marshal(d)
	require.NoError(t, e)
	a, e := RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	a[RuntimeResourceAssetName] = raw
	return raw, a
}
func TestMetadataWiringExactFiveAssetsAndPrivatePlan(t *testing.T) {
	raw, a := metadataWiringAssets(t, metadataWiringDocument(t))
	plan, e := DeriveRuntimeBootRequirements(raw, a)
	require.NoError(t, e)
	require.EqualValues(t, 2, plan.ProtocolVersion())
	require.Equal(t, NormalSplitMetadataV1PhysicalKernelProfileID, plan.ProfileID())
	require.EqualValues(t, 2, plan.KernelPreparationProtocolVersion())
	require.EqualValues(t, 2, plan.MemoryOriginProtocolVersion())
	require.Equal(t, normalInitialWALFamilyKernelPrepV2, plan.InitialWALFamily())
	require.Len(t, plan.RequiredKernelArgs(), 8)
	line := strings.Join(plan.RequiredKernelArgs(), " ")
	require.NoError(t, plan.ValidateKernelCommandLine(line))
	for _, bad := range []string{strings.Replace(line, NormalV2KernelBootstrapArg, NormalV1KernelBootstrapArg, 1), strings.Replace(line, PrebootstrapSystemMetadataKernelArg, PrebootstrapSystemNormalSplitKernelArg, 1), strings.Replace(line, NormalSplitMetadataTopologyKernelArg, NormalSplitTopologyKernelArg, 1), line + " " + NormalV2KernelBootstrapArg} {
		require.Error(t, plan.ValidateKernelCommandLine(bad))
	}
	// I按J/IRQ/late分别闭合，本次不附加改变调度性能的preempt模式要求。
	require.NoError(t, plan.ValidateKernelCommandLine(line+" preempt=full"))
}
func TestMetadataWiringRejectsCrossVersionPolicyFamilies(t *testing.T) {
	cases := []string{"kernel1", "missingzero", "nonzero", "pre2", "topology3", "selector1", "known-other-profile", "physical-config"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			d := metadataWiringDocument(t)
			ts, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
			require.NoError(t, e)
			for i := range ts.Templates {
				p := map[string]any{}
				require.NoError(t, json.Unmarshal(ts.Templates[i].Policy, &p))
				switch {
				case name == "kernel1" && ts.Templates[i].Asset == "dsga-kernel-bootstrap.json":
					p["protocol_version"] = 1
				case name == "missingzero" && ts.Templates[i].Asset == "dsga-kernel-bootstrap.json":
					delete(p, "max_user_namespaces")
				case name == "nonzero" && ts.Templates[i].Asset == "dsga-kernel-bootstrap.json":
					p["max_user_namespaces"] = "1"
				case name == "pre2" && ts.Templates[i].Asset == "dsga-prebootstrap-system.json":
					p["protocol_version"] = 2
				case name == "topology3" && ts.Templates[i].Asset == "dsga-normal-split.json":
					p["protocol_version"] = 3
				case name == "selector1" && ts.Templates[i].Asset == "dsga-normal-split.json":
					p["memory_origin_protocol_version"] = 1
				case name == "known-other-profile" && ts.Templates[i].Asset == "dsga-normal-split.json":
					p["physical_profile_id"] = NormalSplitV1PhysicalKernelProfileID
				case name == "physical-config" && ts.Templates[i].Asset == "dsga-physical-kernel.json":
					p["kernel_config_sha256"] = strings.Repeat("11", 32)
				}
				ts.Templates[i].Policy, e = json.Marshal(p)
				require.NoError(t, e)
			}
			d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(ts.Templates))
			raw, e := json.Marshal(d)
			require.NoError(t, e)
			a, e := RenderRuntimePolicyAssets(raw)
			if e == nil {
				a[RuntimeResourceAssetName] = raw
				_, e = DeriveRuntimeBootRequirements(raw, a)
			}
			require.Error(t, e)
		})
	}
}
func TestMetadataWiringImmutableTargetsAndLegacySelectors(t *testing.T) {
	raw, a := metadataWiringAssets(t, metadataWiringDocument(t))
	plan, e := DeriveRuntimeBootRequirements(raw, a)
	require.NoError(t, e)
	var doc RuntimeDocument
	require.NoError(t, json.Unmarshal(raw, &doc))
	source, e := doc.Source()
	require.NoError(t, e)
	require.NoError(t, normalImmutableBoundsTarget(plan, source, source.Limits))
	for _, name := range []string{"pids", "System", "AS", "Huge", "fake-plan"} {
		t.Run(name, func(t *testing.T) {
			target := source.Limits
			target.HugePools = append([]HugePool(nil), target.HugePools...)
			candidate := plan
			switch name {
			case "pids":
				target.PidsMaxTasks++
			case "System":
				target.SystemMemoryMaxBytes += 65536
			case "AS":
				target.AddressSpaceMaxBytes += 4096
			case "Huge":
				target.HugePools[0].Pages = 1
			case "fake-plan":
				candidate.version = 1
			}
			require.Error(t, normalImmutableBoundsTarget(candidate, source, target))
		})
	}
	for _, selector := range []uint32{0, 1, 2} {
		d := runtimeTemplatesFullFixture(t)
		ts, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
		require.NoError(t, e)
		if selector > 0 {
			for i := range ts.Templates {
				if ts.Templates[i].Asset == "dsga-normal-split.json" {
					p := map[string]any{}
					require.NoError(t, json.Unmarshal(ts.Templates[i].Policy, &p))
					p["protocol_version"] = selector + 1
					p["memory_origin_protocol_version"] = selector
					ts.Templates[i].Policy, e = json.Marshal(p)
					require.NoError(t, e)
				}
			}
			d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(ts.Templates))
		}
		before, e := json.Marshal(d)
		require.NoError(t, e)
		legacyRaw, legacyAssets := metadataWiringAssets(t, d)
		legacy, e := DeriveRuntimeBootRequirements(legacyRaw, legacyAssets)
		require.NoError(t, e)
		require.EqualValues(t, 1, legacy.ProtocolVersion())
		require.Equal(t, NormalSplitV1PhysicalKernelProfileID, legacy.ProfileID())
		require.Equal(t, selector, legacy.MemoryOriginProtocolVersion())
		require.Equal(t, "", legacy.InitialWALFamily())
		after, e := json.Marshal(d)
		require.NoError(t, e)
		require.Equal(t, before, after)
		require.NotContains(t, strings.Join(legacy.RequiredKernelArgs(), " "), "metadata")
	}
}

func TestMetadataWiringPreSystemRawReaderAndCensusExplicitProfiles(t *testing.T) {
	for _, profile := range []string{NormalV1PhysicalKernelProfileID, NormalSplitV1PhysicalKernelProfileID, NormalSplitMetadataV1PhysicalKernelProfileID} {
		t.Run(profile, func(t *testing.T) {
			ref, e := LookupPhysicalKernelBuild(profile)
			require.NoError(t, e)
			got, e := preSystemOriginalPhysicalProfile(ref.KernelRelease, ref.KernelNotesSHA256)
			require.NoError(t, e)
			require.Equal(t, profile, got)
			require.True(t, preSystemPhysicalProfileSupported(profile))
			_, e = preSystemOriginalPhysicalProfile(ref.KernelRelease, strings.Repeat("00", 32))
			require.Error(t, e)
		})
	}
	for _, profile := range []string{"", NormalV1PIDCappedPhysicalKernelProfileID, "arbitrary-recognized-looking-profile"} {
		t.Run("reject-"+profile, func(t *testing.T) { require.False(t, preSystemPhysicalProfileSupported(profile)) })
	}
	meta, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	require.NoError(t, e)
	old, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	require.NoError(t, e)
	_, e = preSystemOriginalPhysicalProfile(meta.KernelRelease, old.KernelNotesSHA256)
	require.Error(t, e)
	_, e = preSystemOriginalPhysicalProfile(old.KernelRelease, meta.KernelNotesSHA256)
	require.Error(t, e)
}

func TestMetadataWiringStableMinFreeUsesOriginalHardUpper(t *testing.T) {
	for _, test := range []struct{ upper, want uint64 }{{4096, 128}, {128 << 20, 2048}, {256 << 20, 2048}, {512 << 20, 4096}, {1 << 30, 4096}, {2 << 30, 8192}, {4 << 30, 8192}, {16 << 30, 16384}, {^uint64(4095), 262144}} {
		got, e := NormalMetadataStableMinFreeKBytes(test.upper)
		require.NoError(t, e)
		require.Equal(t, test.want, got)
	}
	for _, upper := range []uint64{0, 1, 4095, 4097, ^uint64(0)} {
		_, e := NormalMetadataStableMinFreeKBytes(upper)
		require.Error(t, e)
	}
	d := metadataWiringDocument(t)
	ts, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	require.NoError(t, e)
	for i := range ts.Templates {
		if ts.Templates[i].Asset == "dsga-kernel-bootstrap.json" {
			var p map[string]any
			require.NoError(t, json.Unmarshal(ts.Templates[i].Policy, &p))
			p["controls"].(map[string]any)["min_free_kbytes"] = "128"
			ts.Templates[i].Policy, e = json.Marshal(p)
			require.NoError(t, e)
		}
	}
	d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(ts.Templates))
	raw, e := json.Marshal(d)
	require.NoError(t, e)
	assets, e := RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	assets[RuntimeResourceAssetName] = raw
	_, e = DeriveRuntimeBootRequirements(raw, assets)
	require.Error(t, e, "new plan cannot seal a policy below original hotplug recommendation bound")
}
