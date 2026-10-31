//go:build linux

package resourceguard

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestNormalMemoryOriginPolicyVersionsRequireExplicitOriginalField(t *testing.T) {
	p := NormalSplitTopologyPolicy{ProtocolVersion: 1, Scope: normalSplitScope, PhysicalProfileID: NormalSplitV1PhysicalKernelProfileID, RuntimeDocumentSHA256: strings.Repeat("ab", 32), ResourceSourceSHA256: strings.Repeat("cd", 32), PrivateMemoryMaxBytes: 128 << 20, InitialArraysMemoryMaxBytes: 64 << 20, PrivatePidsMaxTasks: 8}
	raw, e := json.Marshal(p)
	require.NoError(t, e)
	require.NotContains(t, string(raw), "memory_origin_protocol_version")
	var original NormalSplitTopologyPolicy
	require.NoError(t, json.Unmarshal(raw, &original))
	require.Equal(t, p, original)
	for _, version := range []uint32{1, 2, 3} {
		for _, value := range []any{nil, float64(0), float64(1), float64(2), "1", true} {
			b := map[string]any{}
			require.NoError(t, json.Unmarshal(raw, &b))
			b["protocol_version"] = version
			b["memory_origin_protocol_version"] = value
			encoded, e := json.Marshal(b)
			require.NoError(t, e)
			var got NormalSplitTopologyPolicy
			e = json.Unmarshal(encoded, &got)
			if version == 2 && value == float64(1) {
				require.NoError(t, e)
				require.Equal(t, uint32(1), got.MemoryOriginProtocolVersion)
			} else {
				require.Error(t, e)
			}
		}
	}
	missing := strings.Replace(string(raw), `"protocol_version":1`, `"protocol_version":2`, 1)
	require.Error(t, json.Unmarshal([]byte(missing), &original))
	duplicate := strings.Replace(string(raw), `"protocol_version":1`, `"protocol_version":2,"memory_origin_protocol_version":1,"memory_origin_protocol_version":1`, 1)
	require.Error(t, json.Unmarshal([]byte(duplicate), &original))
	alias := strings.Replace(string(raw), `"protocol_version":1`, `"protocol_version":2,"MemoryOriginProtocolVersion":1`, 1)
	require.Error(t, json.Unmarshal([]byte(alias), &original))
}

func TestNormalMemoryOriginPlanOnlyDerivesFromExactRenderedISOAssets(t *testing.T) {
	d := runtimeTemplatesFullFixture(t)
	legacyRaw, e := json.Marshal(d)
	require.NoError(t, e)
	legacyAssets, e := RenderRuntimePolicyAssets(legacyRaw)
	require.NoError(t, e)
	legacyAssets[RuntimeResourceAssetName] = legacyRaw
	legacy, e := DeriveRuntimeBootRequirements(legacyRaw, legacyAssets)
	require.NoError(t, e)
	require.Zero(t, legacy.MemoryOriginProtocolVersion())
	templates, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	require.NoError(t, e)
	found := false
	for i := range templates.Templates {
		if templates.Templates[i].Asset != "dsga-normal-split.json" {
			continue
		}
		found = true
		var fields map[string]any
		require.NoError(t, json.Unmarshal(templates.Templates[i].Policy, &fields))
		fields["protocol_version"] = 2
		fields["memory_origin_protocol_version"] = 1
		templates.Templates[i].Policy, e = json.Marshal(fields)
		require.NoError(t, e)
	}
	require.True(t, found)
	d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(templates.Templates))
	raw, e := json.Marshal(d)
	require.NoError(t, e)
	assets, e := RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	assets[RuntimeResourceAssetName] = raw
	plan, e := DeriveRuntimeBootRequirements(raw, assets)
	require.NoError(t, e)
	require.Equal(t, uint32(1), plan.MemoryOriginProtocolVersion())
	require.Equal(t, legacy.ProfileID(), plan.ProfileID())
	require.Equal(t, legacy.RequiredKernelArgs(), plan.RequiredKernelArgs())
	require.NotEqual(t, legacy.RuntimeDocumentSHA256(), plan.RuntimeDocumentSHA256())
	// 参数或替换asset不能单独开启：必须重派生完整原raw的准确五policy+resource。
	forged := map[string][]byte{}
	for n, b := range legacyAssets {
		forged[n] = append([]byte(nil), b...)
	}
	forged["dsga-normal-split.json"] = assets["dsga-normal-split.json"]
	_, e = DeriveRuntimeBootRequirements(legacyRaw, forged)
	require.Error(t, e)
	_, e = DeriveRuntimeBootRequirements(raw, legacyAssets)
	require.Error(t, e)
}
