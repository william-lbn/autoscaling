package resourceguard

import (
	"encoding/json"
	"strings"
	"testing"
)

// 五元组来自58603b实际artifact/config/notes，不以profile字符串给能力。
func TestPhysicalKernelMetadataExactTupleAndVersionIsolation(t *testing.T) {
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	if e != nil || ref.KernelArtifactSHA256 != "18c9fd3a38bbe5a087b98998c856898e97ee3f0bc68681b13f0287912849e1ad" || ref.KernelConfigSHA256 != "966e615127358d6d0dcf4f6ca884a07702e957ad07a95909cf95feca4aae5441" || ref.KernelNotesSHA256 != "a496a3c44fc0b374f100ba0331a2e21f02d6e79ac999769a8d3ad4d71b76024e" {
		t.Fatal("actual reviewed metadata tuple differs", e)
	}
	if !ref.ControlMetadataGuard() || !ref.StrictPageCounterIRQBoundary() || !physicalKernelHasPIDHardSource(ref.ProfileID) {
		t.Fatal("exact new compiled properties missing")
	}
	for name, mutate := range map[string]func(*PhysicalKernelBuildReference){
		"profile": func(r *PhysicalKernelBuildReference) { r.ProfileID = NormalSplitV1PhysicalKernelProfileID },
		"release": func(r *PhysicalKernelBuildReference) { r.KernelRelease += "-alias" },
		"image":   func(r *PhysicalKernelBuildReference) { r.KernelArtifactSHA256 = strings.Repeat("ab", 32) },
		"config":  func(r *PhysicalKernelBuildReference) { r.KernelConfigSHA256 = strings.Repeat("ab", 32) },
		"notes":   func(r *PhysicalKernelBuildReference) { r.KernelNotesSHA256 = strings.Repeat("ab", 32) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := ref
			mutate(&bad)
			if bad.ControlMetadataGuard() || bad.StrictPageCounterIRQBoundary() {
				t.Fatal("mixed tuple acquired compiled capability")
			}
		})
	}
	for _, id := range []string{"pg17-linux-6.12.26-physical-component-v1", NormalV1PhysicalKernelProfileID, NormalV1PIDCappedPhysicalKernelProfileID, NormalSplitV1PhysicalKernelProfileID} {
		old, e := LookupPhysicalKernelBuild(id)
		if e != nil || old.ControlMetadataGuard() {
			t.Fatal("old profile retroactively acquired metadata guard", id, e)
		}
		raw, e := json.Marshal(old)
		if e != nil {
			t.Fatal(e)
		}
		var fields map[string]json.RawMessage
		if e = json.Unmarshal(raw, &fields); e != nil || len(fields) != 5 {
			t.Fatal("old reference JSON ABI changed", e)
		}
	}
}

func TestPhysicalKernelMetadataObservationRequiresExactLivePair(t *testing.T) {
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	if e != nil {
		t.Fatal(e)
	}
	id, e := physicalKernelObservationBuild(ref.KernelRelease, ref.KernelNotesSHA256)
	if e != nil || id != ref.ProfileID {
		t.Fatal("exact live tuple unrecognized", e)
	}
	old, _ := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	for _, pair := range [][2]string{{ref.KernelRelease, old.KernelNotesSHA256}, {old.KernelRelease, ref.KernelNotesSHA256}, {ref.KernelRelease + "-alias", ref.KernelNotesSHA256}} {
		if _, e := physicalKernelObservationBuild(pair[0], pair[1]); e == nil {
			t.Fatal("mixed live release/notes accepted")
		}
	}
	if _, e := LookupPhysicalKernelBuild(ref.ProfileID + "-alias"); e == nil {
		t.Fatal("unregistered profile accepted")
	}
}
