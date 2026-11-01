//go:build !linux

package resourceguard

import "fmt"

// 实际rootdisk生成仅在Linux执行；其它平台维持可编译，不伪造Inputs2实现。
func ValidateNormalSystemStockUdevAssetsAccounts(passwd, group []byte, pgUID, pgGID uint32) error {
	return fmt.Errorf("stock rootdisk accounts require Linux Inputs2 validator")
}
