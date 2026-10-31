//go:build !linux

package resourceguard

import "fmt"

// 不在非Linux平台复制Linux-only策略规则，也不把模板结构正确当可运行策略。
func validateRenderedRuntimePolicyAssets(_ []byte, _ RuntimeDocument, _ SourceConfig, _ map[string][]byte) error {
	return fmt.Errorf("runtime policy assets unsupported on non-Linux platform")
}
