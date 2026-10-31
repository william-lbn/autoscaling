//go:build linux

package resourceguard

import (
	"encoding/json"
	"strings"
	"testing"
)

func arrayRuntimeDocumentFixture(t *testing.T) RuntimeDocument {
	t.Helper()
	doc := runtimeTemplatesFullFixture(t)
	templates, err := parseRuntimePolicyTemplates(doc.PolicyTemplatesJSON)
	if err != nil {
		t.Fatal(err)
	}
	var policy map[string]any
	if err = json.Unmarshal(templates.Templates[4].Policy, &policy); err != nil {
		t.Fatal(err)
	}
	policy["array_helper_protocol_version"] = 1
	policy["array_helper_path"] = fixedArrayHelperPath
	policy["array_helper_elf_sha256"] = strings.Repeat("ab", 32)
	policy["array_helper_address_space_max_bytes"] = "8388608"
	policy["array_helper_memlock_upper_bytes"] = "1048576"
	policy["array_helper_reserve_bytes"] = "9437184"
	templates.Templates[4].Policy, _ = json.Marshal(policy)
	return runtimeTemplatesSetBlob(doc, runtimeTemplatesEnvelope(templates.Templates))
}

// PG17-U-ARRAY-SOURCE-001：真实canonical renderer/原字节绑定，Source仅静态模型。
func TestArrayAuthorityCapabilityDerivedFromOriginalRuntimeAndHelperABI(t *testing.T) {
	for _, mode := range []string{"legacy-v1", "doc2-old-native", "doc2-new-helper"} {
		t.Run(mode, func(t *testing.T) {
			doc := runtimeTemplatesDocumentFixture(t)
			want := uint32(0)
			if mode == "doc2-old-native" {
				doc = runtimeTemplatesFullFixture(t)
			}
			if mode == "doc2-new-helper" {
				doc = arrayRuntimeDocumentFixture(t)
				want = 1
			}
			raw, err := json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			source, err := doc.Source()
			if err != nil {
				t.Fatal(err)
			}
			got, err := deriveArrayAuthorityRuntimeProtocol(raw, source)
			if err != nil || got != want {
				t.Fatal(got, want, err)
			}
			foreign := source
			foreign.Config.VMUID = "other"
			if _, err = deriveArrayAuthorityRuntimeProtocol(raw, foreign); err == nil {
				t.Fatal("capability borrowed foreign source")
			}
		})
	}
}

// PG17-U-ARRAY-SOURCE-002：声明后缺字段/坏budget/非canonical必须错误，不回0。
func TestArrayAuthorityCapabilityRejectsBrokenDeclaredProfile(t *testing.T) {
	for _, fault := range []string{"missing", "null", "alias", "bad_hash", "oversized_reserve", "missing_prefix", "noncanonical"} {
		t.Run(fault, func(t *testing.T) {
			doc := arrayRuntimeDocumentFixture(t)
			templates, _ := parseRuntimePolicyTemplates(doc.PolicyTemplatesJSON)
			var policy map[string]any
			_ = json.Unmarshal(templates.Templates[4].Policy, &policy)
			switch fault {
			case "missing":
				delete(policy, "array_helper_path")
			case "null":
				policy["array_helper_path"] = nil
			case "alias":
				policy["Array_helper_protocol_version"] = 1
			case "bad_hash":
				policy["array_helper_elf_sha256"] = strings.Repeat("0", 64)
			case "oversized_reserve":
				policy["array_helper_reserve_bytes"] = "134217728"
			}
			templates.Templates[4].Policy, _ = json.Marshal(policy)
			if fault == "missing_prefix" {
				templates.Templates = templates.Templates[1:]
			}
			doc = runtimeTemplatesSetBlob(doc, runtimeTemplatesEnvelope(templates.Templates))
			raw, _ := json.Marshal(doc)
			if fault == "noncanonical" {
				raw = append([]byte(" "), raw...)
			}
			var source SourceConfig
			_ = json.Unmarshal(doc.SourceJSON, &source)
			if _, err := deriveArrayAuthorityRuntimeProtocol(raw, source); err == nil {
				t.Fatal("declared broken helper downgraded")
			}
		})
	}
}
