package resourceguard

import (
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 独立可选构建引用，沿原policy.BuildManifestSHA256绑定完整原字节。
// 不塞进PG DT_NEEDED Files集合，不重定义System manifest固定路径；旧nil
// JSON完全省略。这里只声明原构建来源，不证明任何角色出生/持续在System，
// 正式Normal仍须独立实际manifest/lifecycle producer核验，缺失不补默认SHA。
type nativeRuntimeClosureBuild struct {
	ProtocolVersion      uint32 `json:"protocol_version"`
	SystemManifestSHA256 string `json:"system_manifest_sha256"`
}

func (c nativeRuntimeClosureBuild) validate() error {
	if c.ProtocolVersion != 1 {
		return fmt.Errorf("unsupported original runtime closure version")
	}
	if _, e := nativeBirthDigest(c.SystemManifestSHA256); e != nil {
		return fmt.Errorf("original runtime System manifest digest: %w", e)
	}
	return nil
}
func (c *nativeRuntimeClosureBuild) UnmarshalJSON(raw []byte) error {
	type plain nativeRuntimeClosureBuild
	var next plain
	if e := control.UnmarshalObject(raw, &next); e != nil {
		return e
	}
	value := nativeRuntimeClosureBuild(next)
	if e := value.validate(); e != nil {
		return e
	}
	*c = value
	return nil
}
