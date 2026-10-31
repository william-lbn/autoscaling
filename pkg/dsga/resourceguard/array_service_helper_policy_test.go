package resourceguard

import (
	"encoding/json"
	"strings"
	"testing"
)

// PG17-U-ARRAY-POLICY-001：旧v1仍省略；新profile必须完整原helper ABI/上界。
func TestArrayHelperOriginalPolicyVersionAndLegacyOmission(t *testing.T) {
	var old nativeBirthPolicy
	if err := validateNativeArrayHelperPolicy(old, false); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(old)
	if strings.Contains(string(raw), "array_helper") {
		t.Fatal("legacy JSON changed")
	}
	if err := validateNativeArrayHelperPolicy(old, true); err == nil {
		t.Fatal("legacy granted new helper profile")
	}
	good := nativeBirthPolicy{ArrayHelperProtocolVersion: 1, ArrayHelperPath: fixedArrayHelperPath, ArrayHelperELFSHA256: strings.Repeat("a", 64), ArrayHelperAddressSpaceMaxBytes: 64 << 20, ArrayHelperMemlockUpperBytes: 1 << 20, ArrayHelperReserveBytes: 65 << 20}
	if err := validateNativeArrayHelperPolicy(good, true); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"version", "path", "sha", "zero_sha", "AS", "AS_align", "lock", "lock_above_AS", "reserve", "reserve_align", "partial"} {
		t.Run(name, func(t *testing.T) {
			p := good
			switch name {
			case "version":
				p.ArrayHelperProtocolVersion = 2
			case "path":
				p.ArrayHelperPath = "/tmp/helper"
			case "sha":
				p.ArrayHelperELFSHA256 = strings.Repeat("A", 64)
			case "zero_sha":
				p.ArrayHelperELFSHA256 = strings.Repeat("0", 64)
			case "AS":
				p.ArrayHelperAddressSpaceMaxBytes = 0
			case "AS_align":
				p.ArrayHelperAddressSpaceMaxBytes++
			case "lock":
				p.ArrayHelperMemlockUpperBytes = 0
			case "lock_above_AS":
				p.ArrayHelperMemlockUpperBytes = p.ArrayHelperAddressSpaceMaxBytes
			case "reserve":
				p.ArrayHelperReserveBytes = p.ArrayHelperAddressSpaceMaxBytes
			case "reserve_align":
				p.ArrayHelperReserveBytes++
			case "partial":
				p = nativeBirthPolicy{ArrayHelperProtocolVersion: 1}
			}
			if err := validateNativeArrayHelperPolicy(p, false); err == nil {
				t.Fatal("incomplete helper original accepted")
			}
		})
	}
}

// PG17-U-ARRAY-POLICY-002：显式helper null/缺字段/大小写别名不能降级旧v1。
func TestArrayHelperPolicyJSONCannotSilentlyDowngrade(t *testing.T) {
	source, _, _, _ := nativeBirthSourceFixture(t)
	original := source.binding.Policy
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var legacy nativeBirthPolicy
	if err = json.Unmarshal(raw, &legacy); err != nil || legacy != original {
		t.Fatal("legacy policy changed", err)
	}
	for _, extra := range []string{`"array_helper_protocol_version":null`, `"array_helper_protocol_version":0`, `"array_helper_path":null`, `"Array_helper_protocol_version":1`, `"array_helper_protocol_version":1,"array_helper_path":"/neonvm/bin/neon-dsga-array-populate"`} {
		body := append(append([]byte(nil), raw[:len(raw)-1]...), []byte(","+extra+"}")...)
		var decoded nativeBirthPolicy
		if err = json.Unmarshal(body, &decoded); err == nil {
			t.Fatalf("helper presence downgraded: %s", extra)
		}
	}
}
