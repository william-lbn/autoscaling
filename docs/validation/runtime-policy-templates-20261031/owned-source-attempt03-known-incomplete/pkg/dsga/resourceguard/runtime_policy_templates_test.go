package resourceguard

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func runtimeTemplatesDocumentFixture(t *testing.T) RuntimeDocument {
	t.Helper()
	s := SourceConfig{ProtocolVersion: 1, Config: Config{VMUID: "template-vm", ComputeInstanceID: "template-compute", StateDir: FixedStateDir, SystemCgroup: true}, IssuerEpoch: 1, BudgetCommitmentID: "template-budget",
		Limits:           Limits{MemoryMaxBytes: 512 << 20, OutsideUpperBytes: 128 << 20, SystemMemoryMaxBytes: 64 << 20, PidsMaxTasks: 128, AddressSpaceMaxBytes: 4 << 30, HugePools: []HugePool{{PageSizeKB: 2048, Pages: 0}, {PageSizeKB: 1048576, Pages: 0}}},
		ControlPublicKey: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	d := RuntimeDocument{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ConfigSecretUID: "template-config-uid", SourceJSON: append([]byte(" \n"), raw...)}
	d.SourceSHA256 = runtimePolicyDigest(d.SourceJSON)
	if _, err := d.Source(); err != nil {
		t.Fatal(err)
	}
	return d
}
func runtimeTemplatesSetBlob(d RuntimeDocument, raw []byte) RuntimeDocument {
	d.ProtocolVersion = 2
	d.PolicyTemplatesJSON = append([]byte(nil), raw...)
	d.PolicyTemplatesSHA256 = runtimePolicyDigest(raw)
	return d
}
func runtimeTemplatesEnvelope(entries []runtimePolicyTemplate) []byte {
	raw, _ := json.Marshal(runtimePolicyTemplates{ProtocolVersion: 1, Scope: runtimeTemplatesScope, Templates: entries})
	return raw
}
func TestRuntimePolicyTemplatesV1ExactBytesAndFieldPresence(t *testing.T) {
	d := runtimeTemplatesDocumentFixture(t)
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	legacy := struct {
		ProtocolVersion uint32 `json:"protocol_version"`
		Incarnation     string `json:"incarnation"`
		ConfigSecretUID string `json:"config_secret_uid"`
		SourceSHA256    string `json:"source_sha256"`
		SourceJSON      []byte `json:"source_json"`
	}{d.ProtocolVersion, d.Incarnation, d.ConfigSecretUID, d.SourceSHA256, d.SourceJSON}
	want, _ := json.Marshal(legacy)
	if !bytes.Equal(raw, want) {
		t.Fatal("v1 marshal changed", string(raw), string(want))
	}
	if got, err := RenderRuntimePolicyAssets(append([]byte(" \n"), raw...)); err != nil || len(got) != 0 {
		t.Fatal("legacy whitespace behavior changed", got, err)
	}
	for _, field := range []string{`"policy_templates_json":null`, `"policy_templates_json":""`, `"policy_templates_sha256":null`, `"policy_templates_sha256":""`} {
		t.Run(field, func(t *testing.T) {
			var x RuntimeDocument
			if json.Unmarshal(append(append([]byte(nil), raw[:len(raw)-1]...), []byte(","+field+"}")...), &x) == nil {
				t.Fatal("v1 accepted new field presence")
			}
		})
	}
}
func TestRuntimePolicyTemplatesStrictEnvelopeAndDependencies(t *testing.T) {
	base := runtimeTemplatesEnvelope([]runtimePolicyTemplate{{Asset: runtimePolicyAssetOrder[0], Policy: json.RawMessage(`{}`)}})
	if _, err := parseRuntimePolicyTemplates(base); err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"duplicate_key":    []byte(strings.Replace(string(base), `"protocol_version":1`, `"protocol_version":1,"protocol_version":1`, 1)),
		"unknown_envelope": []byte(strings.Replace(string(base), `"scope":`, `"extra":1,"scope":`, 1)),
		"unknown_entry":    []byte(strings.Replace(string(base), `"asset":`, `"extra":1,"asset":`, 1)),
		"unknown_asset":    runtimeTemplatesEnvelope([]runtimePolicyTemplate{{Asset: "../unexpected.json", Policy: json.RawMessage(`{}`)}}),
		"duplicate_asset":  runtimeTemplatesEnvelope([]runtimePolicyTemplate{{Asset: runtimePolicyAssetOrder[0], Policy: json.RawMessage(`{}`)}, {Asset: runtimePolicyAssetOrder[0], Policy: json.RawMessage(`{}`)}}),
		"empty":            runtimeTemplatesEnvelope(nil), "empty_blob": nil, "oversize": bytes.Repeat([]byte(" "), runtimeTemplatesMaxBytes+1),
		"partial": base[:len(base)-1], "wrong_scope": []byte(strings.Replace(string(base), runtimeTemplatesScope, "other-scope", 1)),
		"wrong_protocol": []byte(strings.Replace(string(base), `"protocol_version":1`, `"protocol_version":2`, 1)),
	}
	for _, policy := range []string{"null", "[]", "true", `{"x":1,"x":2}`} {
		cases["not_object_or_duplicate_"+policy] = runtimeTemplatesEnvelope([]runtimePolicyTemplate{{Asset: runtimePolicyAssetOrder[0], Policy: json.RawMessage(policy)}})
	}
	for _, key := range []string{"runtime_document_sha256", "resource_source_sha256", "physical_origin_policy_sha256", "config_secret_uid", "nonce", "instance_nonce", "guest_boot_id", "origin_sha256", "intent_sha256", "completion_sha256", "policy_templates_json", "policy_templates_sha256"} {
		policy, _ := json.Marshal(map[string]any{"nested": []any{map[string]any{key: nil}}})
		cases["dynamic_"+key] = runtimeTemplatesEnvelope([]runtimePolicyTemplate{{Asset: runtimePolicyAssetOrder[0], Policy: policy}})
	}
	for i, name := range runtimePolicyAssetOrder[1:] {
		entries := []runtimePolicyTemplate{{Asset: name, Policy: json.RawMessage(`{}`)}}
		cases["missing_prefix_"+name] = runtimeTemplatesEnvelope(entries)
		_ = i
	}
	entries := make([]runtimePolicyTemplate, 6)
	for i := range entries {
		entries[i] = runtimePolicyTemplate{Asset: runtimePolicyAssetOrder[0], Policy: json.RawMessage(`{}`)}
	}
	cases["too_many"] = runtimeTemplatesEnvelope(entries)
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			d := runtimeTemplatesSetBlob(runtimeTemplatesDocumentFixture(t), raw)
			if _, err := d.Source(); err == nil {
				t.Fatal("invalid template envelope accepted")
			}
		})
	}
}
func TestRuntimePolicyTemplatesDocumentRejectsRawDriftAndUnknownFields(t *testing.T) {
	blob := runtimeTemplatesEnvelope([]runtimePolicyTemplate{{Asset: runtimePolicyAssetOrder[0], Policy: json.RawMessage(`{}`)}})
	d := runtimeTemplatesSetBlob(runtimeTemplatesDocumentFixture(t), blob)
	if _, err := d.Source(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"blob_drift", "missing_sha", "uppercase_sha", "unknown_version", "source_drift", "source_private_key", "source_extra", "doc_extra", "doc_duplicate"} {
		t.Run(name, func(t *testing.T) {
			x := d
			switch name {
			case "blob_drift":
				x.PolicyTemplatesJSON = append([]byte(" "), blob...)
			case "missing_sha":
				x.PolicyTemplatesSHA256 = ""
			case "uppercase_sha":
				x.PolicyTemplatesSHA256 = strings.ToUpper(x.PolicyTemplatesSHA256)
			case "unknown_version":
				x.ProtocolVersion = 3
			case "source_drift":
				x.SourceJSON = append([]byte(" "), x.SourceJSON...)
			case "source_private_key", "source_extra":
				extra := `,"dsga-policy-templates.json":{}}`
				if name == "source_private_key" {
					extra = `,"control_private_key":"forbidden-model-private-key"}`
				}
				x.SourceJSON = append(append([]byte(nil), x.SourceJSON[:len(x.SourceJSON)-1]...), []byte(extra)...)
				x.SourceSHA256 = runtimePolicyDigest(x.SourceJSON)
			}
			raw, _ := json.Marshal(x)
			if name == "doc_extra" {
				raw = append(raw[:len(raw)-1], []byte(`,"extra":1}`)...)
			}
			if name == "doc_duplicate" {
				raw = []byte(strings.Replace(string(raw), `"protocol_version":2`, `"protocol_version":2,"protocol_version":2`, 1))
			}
			var decoded RuntimeDocument
			err := json.Unmarshal(raw, &decoded)
			if err == nil {
				_, err = decoded.Source()
			}
			if err == nil {
				t.Fatal("invalid source/document accepted")
			}
		})
	}
}
