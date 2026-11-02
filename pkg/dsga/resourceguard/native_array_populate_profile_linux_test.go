//go:build linux

package resourceguard

import (
	"encoding/json"
	"errors"
	"testing"
)

// 只验原runtime到固定构建的绑定；真实release/notes及helper仍由native链验证。
func TestArrayPopulateOriginalKernelProfile(t *testing.T) {
	for _, name := range []string{"metadata", "legacy", "wrong-vm", "wrong-compute", "source-mismatch", "unknown-mode", "trusted-old-profile", "trusted-no-policy", "bad-source-digest"} {
		t.Run(name, func(t *testing.T) {
			d := metadataWiringDocument(t)
			if name == "legacy" || name == "trusted-old-profile" {
				d = runtimeTemplatesFullFixture(t)
			}
			source, e := d.Source()
			if e != nil {
				t.Fatal(e)
			}
			if name != "legacy" {
				source.Config.ExecutionModel = TrustedControllerExecutionModel
			}
			if name == "unknown-mode" {
				source.Config.ExecutionModel = "request-profile"
			}
			d.SourceJSON, e = json.Marshal(source)
			if e != nil {
				t.Fatal(e)
			}
			d.SourceSHA256 = runtimePolicyDigest(d.SourceJSON)
			c := arrayPopulateFixture(t).Context
			c.Original.Identity.VMUID = source.Config.VMUID
			c.Original.Identity.ComputeInstanceID = source.Config.ComputeInstanceID
			switch name {
			case "wrong-vm":
				c.Original.Identity.VMUID += "-other"
			case "wrong-compute":
				c.Original.Identity.ComputeInstanceID += "-other"
			case "trusted-no-policy":
				d.ProtocolVersion = 1
				d.PolicyTemplatesJSON = nil
				d.PolicyTemplatesSHA256 = ""
			case "bad-source-digest":
				d.SourceSHA256 = "bad"
			}
			raw, e := json.Marshal(d)
			if e != nil {
				t.Fatal(e)
			}
			if name == "source-mismatch" {
				source.BudgetCommitmentID += "-other"
			}
			got, e := arrayPopulateKernelBuild(c, source, raw)
			if name != "metadata" && name != "legacy" {
				if e == nil {
					t.Fatal("mismatched original source accepted")
				}
				return
			}
			profile := NormalSplitMetadataV1PhysicalKernelProfileID
			if name == "legacy" {
				profile = NormalSplitV1PhysicalKernelProfileID
			}
			want, e2 := LookupPhysicalKernelBuild(profile)
			if e != nil || e2 != nil || got != want {
				t.Fatalf("fixed original profile differs: %v %v", e, e2)
			}
		})
	}
}
func TestArrayPopulateStagePreservesOriginalError(t *testing.T) {
	original := errors.New("original")
	e := &arrayPopulateStageError{stage: 4, err: original}
	var observed *arrayPopulateStageError
	if !errors.Is(e, original) || !errors.As(e, &observed) || observed.stage != 4 || e.Error() != original.Error() {
		t.Fatal("diagnostic changed original error")
	}
}
