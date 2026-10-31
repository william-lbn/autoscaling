package resourceguard

import (
	"bytes"
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"math"
)

const fixedArrayHelperPath = "/neonvm/bin/neon-dsga-array-populate"

// 纯原创建策略检查，不读取packet，不从RSS/一次current生成AS或收费余量。
func validateNativeArrayHelperPolicy(p nativeBirthPolicy, required bool) error {
	empty := p.ArrayHelperProtocolVersion == 0 && p.ArrayHelperPath == "" && p.ArrayHelperELFSHA256 == "" && p.ArrayHelperAddressSpaceMaxBytes == 0 && p.ArrayHelperMemlockUpperBytes == 0 && p.ArrayHelperReserveBytes == 0
	if empty && !required {
		return nil
	}
	_, digestErr := nativeBirthDigest(p.ArrayHelperELFSHA256)
	as, lock, reserve := uint64(p.ArrayHelperAddressSpaceMaxBytes), uint64(p.ArrayHelperMemlockUpperBytes), uint64(p.ArrayHelperReserveBytes)
	if p.ArrayHelperProtocolVersion != 1 || p.ArrayHelperPath != fixedArrayHelperPath || digestErr != nil || as < 1<<20 || as >= math.MaxInt64 || as%4096 != 0 || lock < 4096 || lock > 1<<20 || lock%4096 != 0 || lock >= as || reserve <= as || reserve >= math.MaxInt64 || reserve%4096 != 0 {
		return fmt.Errorf("original versioned array helper policy missing or unsupported")
	}
	return nil
}

// 新helper组一旦在JSON出现便必须完整且非null；显式null不能被Go零值
// 静默解释为旧profile。plain防递归，原键/重复/别名规则仍由control统一拒绝。
func (p *nativeBirthPolicy) UnmarshalJSON(raw []byte) error {
	type plain nativeBirthPolicy
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	names := []string{"array_helper_protocol_version", "array_helper_path", "array_helper_elf_sha256", "array_helper_address_space_max_bytes", "array_helper_memlock_upper_bytes", "array_helper_reserve_bytes"}
	present := false
	for _, name := range names {
		_, found := fields[name]
		present = present || found
	}
	if present {
		for _, name := range names {
			value, found := fields[name]
			if !found || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return fmt.Errorf("original array helper group missing/null")
			}
		}
		if err := validateNativeArrayHelperPolicy(nativeBirthPolicy(next), true); err != nil {
			return err
		}
	}
	if err := nativeBirthPolicy(next).validate(); err != nil {
		return err
	}
	*p = nativeBirthPolicy(next)
	return nil
}

// helper是独立静态程序，不能伪装成PG DT_NEEDED闭包中的可达库。
// 整个manifest仍由原policy BuildManifestSHA256绑定，六个policy字段另钉上界。
type nativeArrayHelperBuild struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	Path            string `json:"path"`
	SHA256          string `json:"sha256"`
}

func (h nativeArrayHelperBuild) validate() error {
	_, err := nativeBirthDigest(h.SHA256)
	if h.ProtocolVersion != 1 || h.Path != fixedArrayHelperPath || err != nil {
		return fmt.Errorf("native helper original build entry invalid")
	}
	return nil
}
func (h *nativeArrayHelperBuild) UnmarshalJSON(raw []byte) error {
	type plain nativeArrayHelperBuild
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	v := nativeArrayHelperBuild(next)
	if err := v.validate(); err != nil {
		return err
	}
	*h = v
	return nil
}
func (m *nativeBirthBuildManifest) UnmarshalJSON(raw []byte) error {
	type plain nativeBirthBuildManifest
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if value, exists := fields["array_helper"]; exists && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return fmt.Errorf("native helper build entry cannot be null")
	}
	if value, exists := fields["runtime_closure"]; exists && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return fmt.Errorf("native runtime closure cannot be null")
	}
	v := nativeBirthBuildManifest(next)
	if err := v.validate(); err != nil {
		return err
	}
	*m = v
	return nil
}
