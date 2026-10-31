package resourceguard

import (
	"fmt"
)

// 只引用已经由原PreSys producer建立的完整字节来源。新字段仅见于显式PreSys
// 创建的Bootstrap内部WAL，旧JSON继续省略；不是公开请求/票据或normal许可。
type bootstrapPreSystemReference struct {
	ProtocolVersion  uint32 `json:"protocol_version"`
	IntentSHA256     string `json:"intent_sha256"`
	CompletionSHA256 string `json:"completion_sha256"`
	OriginSHA256     string `json:"origin_sha256"`
	PolicySHA256     string `json:"policy_sha256"`
}

func (v bootstrapPreSystemReference) validate() error {
	if v.ProtocolVersion != 1 || !physicalDigestValid(v.IntentSHA256) || !physicalDigestValid(v.CompletionSHA256) || !physicalDigestValid(v.OriginSHA256) || !physicalDigestValid(v.PolicySHA256) {
		return fmt.Errorf("invalid original pre-System bootstrap reference")
	}
	return nil
}
