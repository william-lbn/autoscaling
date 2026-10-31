package resourceguard

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const (
	PolicyTemplatesSecretKey = "dsga-policy-templates.json"
	RuntimeResourceAssetName = "dsga-resource.json"
	runtimeTemplatesScope    = "neon-dsga-policy-templates-v1"
	runtimeTemplatesMaxBytes = 16 << 10
)

var runtimePolicyAssetOrder = [...]string{
	"dsga-physical-kernel.json", "dsga-kernel-bootstrap.json",
	"dsga-prebootstrap-system.json", "dsga-normal-split.json", "dsga-native-birth.json",
}

type runtimePolicyTemplate struct {
	Asset  string          `json:"asset"`
	Policy json.RawMessage `json:"policy"`
}
type runtimePolicyTemplates struct {
	ProtocolVersion uint32                  `json:"protocol_version"`
	Scope           string                  `json:"scope"`
	Templates       []runtimePolicyTemplate `json:"templates"`
}

func runtimePolicyDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// 模板仅保存创建前静态意图。动态UID/文档引用必须在原Secret已创建并核对后注入；
// 不生成Root nonce，也不接受模板自报的运行时证据。
func rejectRuntimeTemplateReferences(value any) error {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			switch key {
			case "runtime_document_sha256", "resource_source_sha256", "physical_origin_policy_sha256",
				"config_secret_uid", "nonce", "instance_nonce", "guest_boot_id",
				"origin_sha256", "intent_sha256", "completion_sha256",
				"policy_templates_json", "policy_templates_sha256":
				return fmt.Errorf("dynamic reference %q in original template", key)
			}
			if err := rejectRuntimeTemplateReferences(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range v {
			if err := rejectRuntimeTemplateReferences(child); err != nil {
				return err
			}
		}
	}
	return nil
}
func parseRuntimePolicyTemplates(raw []byte) (runtimePolicyTemplates, error) {
	var out runtimePolicyTemplates
	if len(raw) == 0 || len(raw) > runtimeTemplatesMaxBytes {
		return out, fmt.Errorf("original policy templates missing or oversized")
	}
	if err := control.ValidateJSON(raw, runtimeTemplatesMaxBytes, 8192); err != nil {
		return out, err
	}
	if err := control.UnmarshalObject(raw, &out); err != nil {
		return out, err
	}
	if out.ProtocolVersion != 1 || out.Scope != runtimeTemplatesScope || len(out.Templates) == 0 || len(out.Templates) > len(runtimePolicyAssetOrder) {
		return out, fmt.Errorf("invalid policy template envelope")
	}
	seen := map[string]bool{}
	for _, entry := range out.Templates {
		known := false
		for _, name := range runtimePolicyAssetOrder {
			known = known || entry.Asset == name
		}
		if !known || seen[entry.Asset] {
			return out, fmt.Errorf("unknown or duplicate policy asset %q", entry.Asset)
		}
		seen[entry.Asset] = true
		var object map[string]any
		if err := json.Unmarshal(entry.Policy, &object); err != nil || object == nil {
			return out, fmt.Errorf("policy %q is not an object", entry.Asset)
		}
		if err := rejectRuntimeTemplateReferences(object); err != nil {
			return out, err
		}
	}
	// 只支持完整前缀；不能用后阶段孤立策略跳过原前置输入。
	for i, name := range runtimePolicyAssetOrder {
		if seen[name] {
			for _, parent := range runtimePolicyAssetOrder[:i] {
				if !seen[parent] {
					return out, fmt.Errorf("policy %q lacks parent %q", name, parent)
				}
			}
		}
	}
	return out, nil
}
func validateRuntimeTemplateDocument(d RuntimeDocument) error {
	if d.ProtocolVersion == 1 {
		if len(d.PolicyTemplatesJSON) != 0 || d.PolicyTemplatesSHA256 != "" {
			return fmt.Errorf("runtime v1 carries policy templates")
		}
		return nil
	}
	if d.ProtocolVersion != 2 || d.PolicyTemplatesSHA256 != runtimePolicyDigest(d.PolicyTemplatesJSON) {
		return fmt.Errorf("runtime v2 original template bytes differ")
	}
	_, err := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	return err
}

// RenderRuntimePolicyAssets只生成固定ISO文件的准确字节，不发Bootstrap/Birth/normal许可。
// RuntimeDocument包含原模板字节而非渲染结果，实际SecretUID因此不会产生SHA自引用环。
// 调用方必须先完成原UID/owner/immutable核验；此纯函数不能替代Kube授权和原Host预算。
func RenderRuntimePolicyAssets(runtimeRaw []byte) (map[string][]byte, error) {
	if len(runtimeRaw) == 0 || len(runtimeRaw) > 64<<10 {
		return nil, fmt.Errorf("runtime document exceeds bound")
	}
	var d RuntimeDocument
	if err := json.Unmarshal(runtimeRaw, &d); err != nil {
		return nil, err
	}
	source, err := d.Source()
	if err != nil {
		return nil, err
	}
	assets := map[string][]byte{}
	if d.ProtocolVersion == 1 {
		return assets, nil
	}
	canonical, err := json.Marshal(d)
	if err != nil || !bytes.Equal(canonical, runtimeRaw) {
		return nil, fmt.Errorf("runtime v2 must use canonical producer bytes")
	}
	templates, err := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	if err != nil {
		return nil, err
	}
	byName := map[string]json.RawMessage{}
	for _, entry := range templates.Templates {
		byName[entry.Asset] = entry.Policy
	}
	documentSHA := runtimePolicyDigest(runtimeRaw)
	for _, name := range runtimePolicyAssetOrder {
		raw, ok := byName[name]
		if !ok {
			continue
		}
		var policy map[string]json.RawMessage
		if err := json.Unmarshal(raw, &policy); err != nil {
			return nil, err
		}
		inject := func(key, value string) { policy[key], _ = json.Marshal(value) }
		inject("runtime_document_sha256", documentSHA)
		if name == "dsga-normal-split.json" || name == "dsga-native-birth.json" {
			inject("resource_source_sha256", d.SourceSHA256)
		}
		if name == "dsga-kernel-bootstrap.json" {
			inject("physical_origin_policy_sha256", runtimePolicyDigest(assets["dsga-physical-kernel.json"]))
		}
		effective, err := json.Marshal(policy)
		if err != nil {
			return nil, err
		}
		assets[name] = effective
	}
	if err := validateRenderedRuntimePolicyAssets(runtimeRaw, d, source, assets); err != nil {
		return nil, err
	}
	return assets, nil
}
