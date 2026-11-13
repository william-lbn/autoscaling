//go:build linux

package resourceguard

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// helper闭包独立核验；绝不扩PG/interpreter pre-main DT_NEEDED可达集合。
func verifyNativeArrayHelperOriginal(source nativeBirthSource) error {
	p, m := source.binding.Policy, source.manifest
	if p.ArrayHelperProtocolVersion == 0 && m.ArrayHelper == nil {
		return nil
	}
	if err := validateNativeArrayHelperPolicy(p, true); err != nil {
		return err
	}
	if m.ArrayHelper == nil || m.ArrayHelper.validate() != nil || m.ArrayHelper.Path != p.ArrayHelperPath || m.ArrayHelper.SHA256 != p.ArrayHelperELFSHA256 {
		return fmt.Errorf("native helper original build/policy mismatch")
	}
	f, err := nativeBirthOpenRO(fixedArrayHelperPath)
	if err != nil {
		return err
	}
	defer f.Close()
	hash, err := nativeBirthFileHash(f)
	if err != nil || hash != p.ArrayHelperELFSHA256 {
		return fmt.Errorf("native fixed helper original ELF digest differs")
	}
	metadata, err := nativeBirthParseELF(f)
	if err != nil || metadata.Interpreter != "" || len(metadata.Needed) != 0 {
		return fmt.Errorf("native helper must be original independent static ELF")
	}
	_, imageBytes, err := arrayPopulateLockedImage(f)
	if err != nil || imageBytes > uint64(p.ArrayHelperMemlockUpperBytes) {
		return fmt.Errorf("native helper LOAD exceeds original lock upper")
	}
	st, err := f.Stat()
	if err != nil {
		return err
	}
	size := uint64(st.Size())
	fileUpper := (size + 4095) &^ uint64(4095)
	if fileUpper > uint64(p.ArrayHelperReserveBytes-p.ArrayHelperAddressSpaceMaxBytes) {
		return fmt.Errorf("native helper original A reserve lacks full file upper")
	}
	return nil
}

// 生产服务启动只从原只读ISO+封存来源派生能力版本。版本声明只选择必要
// 协议，不表示Registry/预算/normal就绪；缺backend必须Unknown，不得降级。
func arrayAuthorityRuntimeProtocol(sealed SourceConfig) (uint32, error) {
	actual, raw, err := runtimeSource()
	if err != nil {
		return 0, err
	}
	if !reflect.DeepEqual(actual, sealed) {
		return 0, fmt.Errorf("array capability resource source differs")
	}
	if err = matchSealedFile(FixedSealedRuntimePath, raw); err != nil {
		return 0, err
	}
	return deriveArrayAuthorityRuntimeProtocol(raw, sealed)
}
func deriveArrayAuthorityRuntimeProtocol(raw []byte, sealed SourceConfig) (uint32, error) {
	var doc RuntimeDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return 0, err
	}
	actual, err := doc.Source()
	if err != nil || !reflect.DeepEqual(actual, sealed) {
		return 0, fmt.Errorf("array capability original runtime/source mismatch")
	}
	assets, err := RenderRuntimePolicyAssets(raw)
	if err != nil {
		return 0, err
	}
	if doc.ProtocolVersion == 1 {
		return 0, nil
	}
	// 前缀尚未到native的Runtime不启用该阶段，但不得把已声明helper的坏组静默降级。
	native, exists := assets["dsga-native-birth.json"]
	if !exists {
		return 0, nil
	}
	var policy nativeBirthPolicy
	if err = json.Unmarshal(native, &policy); err != nil {
		return 0, err
	}
	if policy.ArrayHelperProtocolVersion == 0 {
		return 0, nil
	}
	if err = validateNativeArrayHelperPolicy(policy, true); err != nil {
		return 0, err
	}
	// v3还渲染public Compute配置。按策略名称核原前缀，public不计入数组能力来源。
	for _, name := range runtimePolicyAssetOrder {
		if len(assets[name]) == 0 {
			return 0, fmt.Errorf("array capability original policy %q missing", name)
		}
	}
	if policy.ResourceSourceSHA256 != doc.SourceSHA256 || policy.RuntimeDocumentSHA256 != physicalDigest(raw) {
		return 0, fmt.Errorf("array capability full original topology/native binding incomplete")
	}
	var split NormalSplitTopologyPolicy
	if err = json.Unmarshal(assets["dsga-normal-split.json"], &split); err != nil {
		return 0, err
	}
	initialBytes := uint64(0)
	for _, element := range policy.ElementBytes {
		initialBytes += (uint64(policy.StartupBuffers)*element + 4095) &^ uint64(4095)
	}
	if uint64(policy.ArrayHelperReserveBytes) > uint64(split.InitialArraysMemoryMaxBytes) || initialBytes > uint64(split.InitialArraysMemoryMaxBytes-policy.ArrayHelperReserveBytes) {
		return 0, fmt.Errorf("original arrays cap lacks startup and fixed helper reserve")
	}
	return 1, nil
}
