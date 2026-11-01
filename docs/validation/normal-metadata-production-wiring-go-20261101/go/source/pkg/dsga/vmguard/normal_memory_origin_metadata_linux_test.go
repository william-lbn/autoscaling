//go:build linux

package vmguard

import (
	"encoding/json"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMetadataWiringHostSelectionUsesCompleteOriginalPlan(t *testing.T) {
	f := runnerBirthFixture(t, false)
	raw, _ := initialBootFullRuntime(t, f)
	var doc resourceguard.RuntimeDocument
	require.NoError(t, json.Unmarshal(raw, &doc))
	var ts struct {
		ProtocolVersion uint32 `json:"protocol_version"`
		Scope           string `json:"scope"`
		Templates       []struct {
			Asset  string          `json:"asset"`
			Policy json.RawMessage `json:"policy"`
		} `json:"templates"`
	}
	require.NoError(t, json.Unmarshal(doc.PolicyTemplatesJSON, &ts))
	ref, e := resourceguard.LookupPhysicalKernelBuild(resourceguard.NormalSplitMetadataV1PhysicalKernelProfileID)
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
	doc.PolicyTemplatesJSON, e = json.Marshal(ts)
	require.NoError(t, e)
	doc.PolicyTemplatesSHA256 = initialBootSHA(doc.PolicyTemplatesJSON)
	raw, e = json.Marshal(doc)
	require.NoError(t, e)
	source := initialBootSource{RuntimeSHA256: initialBootSHA(raw), ResourceSHA256: doc.SourceSHA256, KernelBuildSHA256: ref.KernelArtifactSHA256}
	plan, e := normalMemoryHostPlan(raw, source)
	require.NoError(t, e)
	require.Equal(t, ref.ProfileID, plan.ProfileID())
	selection, e := normalMemoryHostSelection(raw, source)
	require.NoError(t, e)
	require.EqualValues(t, 2, selection)
	bad := source
	old, e := resourceguard.LookupPhysicalKernelBuild(resourceguard.NormalSplitV1PhysicalKernelProfileID)
	require.NoError(t, e)
	bad.KernelBuildSHA256 = old.KernelArtifactSHA256
	_, e = normalMemoryHostPlan(raw, bad)
	require.Error(t, e)
	_, e = normalMemoryHostPlan(append([]byte(" "), raw...), source)
	require.Error(t, e)
}
