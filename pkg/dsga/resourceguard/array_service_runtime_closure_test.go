package resourceguard

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// PG17-U-ARRAY-RUNTIME-CLOSURE-001：原manifest raw SHA绑定独立System引用，
// 不扩PG可达ELF集合；metadata解析成功不等于lifecycle/normal证据。
func TestArrayRuntimeClosureOriginalManifestAndLegacyShape(t *testing.T) {
	_, runtime, policyRaw, manifestRaw := nativeBirthSourceFixture(t)
	var old nativeBirthBuildManifest
	if e := json.Unmarshal(manifestRaw, &old); e != nil {
		t.Fatal(e)
	}
	legacy, e := json.Marshal(old)
	if e != nil || bytes.Contains(legacy, []byte("runtime_closure")) {
		t.Fatal("legacy nil shape changed", e)
	}
	var fields map[string]json.RawMessage
	if e = json.Unmarshal(manifestRaw, &fields); e != nil {
		t.Fatal(e)
	}
	fields["runtime_closure"] = json.RawMessage(`{"protocol_version":1,"system_manifest_sha256":"` + strings.Repeat("ab", 32) + `"}`)
	boundRaw, _ := json.Marshal(fields)
	if _, e = decodeNativeBirthSource(runtime, policyRaw, boundRaw); e == nil {
		t.Fatal("new member escaped original BuildManifestSHA")
	}
	var policy nativeBirthPolicy
	if e = json.Unmarshal(policyRaw, &policy); e != nil {
		t.Fatal(e)
	}
	policy.BuildManifestSHA256 = physicalDigest(boundRaw)
	policyRaw, _ = json.Marshal(policy)
	source, e := decodeNativeBirthSource(runtime, policyRaw, boundRaw)
	if e != nil {
		t.Fatal(e)
	}
	if source.manifest.RuntimeClosure == nil || source.manifest.RuntimeClosure.SystemManifestSHA256 != strings.Repeat("ab", 32) || len(source.manifest.Files) != len(old.Files) {
		t.Fatal("independent reference mutated PG Files")
	}
	for i := range old.Files {
		if old.Files[i] != source.manifest.Files[i] {
			t.Fatal("PG closure entry changed")
		}
	}
}

// PG17-U-ARRAY-RUNTIME-CLOSURE-002：声明后所有字段严格；null/别名/空SHA/未知
// 版本不降级为旧nil，不从任何调用者输入选择System manifest路径。
func TestArrayRuntimeClosureRejectsMalformed(t *testing.T) {
	_, _, _, raw := nativeBirthSourceFixture(t)
	var fields map[string]json.RawMessage
	if e := json.Unmarshal(raw, &fields); e != nil {
		t.Fatal(e)
	}
	for _, value := range []string{`null`, `{}`, `{"protocol_version":1}`, `{"protocol_version":1,"system_manifest_sha256":null}`, `{"protocol_version":1,"system_manifest_sha256":""}`, `{"protocol_version":1,"system_manifest_sha256":"` + strings.Repeat("0", 64) + `"}`, `{"protocol_version":2,"system_manifest_sha256":"` + strings.Repeat("ab", 32) + `"}`, `{"Protocol_Version":1,"system_manifest_sha256":"` + strings.Repeat("ab", 32) + `"}`, `{"protocol_version":1,"system_manifest_sha256":"` + strings.Repeat("AB", 32) + `"}`, `{"protocol_version":1,"system_manifest_sha256":"` + strings.Repeat("ab", 32) + `","path":"/tmp/arbitrary"}`} {
		t.Run(value, func(t *testing.T) {
			copy := map[string]json.RawMessage{}
			for k, v := range fields {
				copy[k] = v
			}
			copy["runtime_closure"] = json.RawMessage(value)
			encoded, _ := json.Marshal(copy)
			var manifest nativeBirthBuildManifest
			if json.Unmarshal(encoded, &manifest) == nil {
				t.Fatal("malformed member accepted")
			}
		})
	}
}
