//go:build linux

package resourceguard

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRuntimeGenericComputeRejectsAmbiguousIdentityAndVersion(t *testing.T) {
	raw, _ := bootRequirementsFixture(t)
	var d RuntimeDocument
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	var source SourceConfig
	json.Unmarshal(d.SourceJSON, &source)
	source.Config.ExecutionModel = TrustedControllerExecutionModel
	source.Config.SystemCgroup = true
	if source.Limits.SystemMemoryMaxBytes == 0 {
		source.Limits.SystemMemoryMaxBytes = 64 << 20
	}
	d.SourceJSON, _ = json.Marshal(source)
	d.SourceSHA256 = runtimePolicyDigest(d.SourceJSON)
	d.ProtocolVersion = 3
	d.ComputeStartup = &RuntimeComputeStartup{ProtocolVersion: 1, WorkloadName: "cp-original", NativeConfigSHA256: strings.Repeat("ab", 32), ControlKeyID: "original-key"}
	if _, err := d.Source(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"missing", "wrong-model", "workload-shell", "digest", "key-id", "downgrade"} {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(d)
			var candidate RuntimeDocument
			json.Unmarshal(raw, &candidate)
			switch name {
			case "missing":
				candidate.ComputeStartup = nil
			case "wrong-model":
				s := source
				s.Config.ExecutionModel = ""
				candidate.SourceJSON, _ = json.Marshal(s)
				candidate.SourceSHA256 = runtimePolicyDigest(candidate.SourceJSON)
			case "workload-shell":
				candidate.ComputeStartup.WorkloadName = "$(id)"
			case "digest":
				candidate.ComputeStartup.NativeConfigSHA256 = "AB"
			case "key-id":
				candidate.ComputeStartup.ControlKeyID = "line\n"
			case "downgrade":
				candidate.ProtocolVersion = 2
			}
			if _, err := candidate.Source(); err == nil {
				t.Fatal("invalid generic Compute binding accepted")
			}
		})
	}
	for _, version := range []int{1, 2} {
		raw := []byte(strings.Replace(`{"protocol_version":0,"compute_startup":null}`, "0", string(rune('0'+version)), 1))
		var candidate RuntimeDocument
		if json.Unmarshal(raw, &candidate) == nil {
			t.Fatal("old runtime accepted even null new field")
		}
	}
}
func TestRuntimeGenericComputeTemplateHasOnlyOneIdentitySlot(t *testing.T) {
	base := []string{"/usr/local/bin/compute_ctl", "--compute-id", RuntimeComputeIdentityPlaceholder, "--config", RuntimeComputePublicPath}
	if err := ValidateComputeIdentityTemplate(RuntimeComputeIdentitySource, TrustedControllerExecutionModel, base, nil); err != nil {
		t.Fatal(err)
	}
	for _, extra := range [][]string{{"-i", "another"}, {"--compute-id=another"}, {"--config", "/tmp/other"}, {"--control-plane-uri", "http://other"}, {RuntimeComputeIdentityPlaceholder}, {"--config=/tmp/other"}} {
		if ValidateComputeIdentityTemplate(RuntimeComputeIdentitySource, TrustedControllerExecutionModel, append(append([]string(nil), base...), extra...), nil) == nil {
			t.Fatal("alternate argv accepted")
		}
	}
	if ValidateComputeIdentityTemplate("", TrustedControllerExecutionModel, base, nil) == nil || ValidateComputeIdentityTemplate(RuntimeComputeIdentitySource, "", base, nil) == nil ||
		ValidateComputeIdentityTemplate(RuntimeComputeIdentitySource, TrustedControllerExecutionModel, base, []string{"X=" + RuntimeComputeIdentityPlaceholder}) == nil {
		t.Fatal("unselected or environment template accepted")
	}
}
