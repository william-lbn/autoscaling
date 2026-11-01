//go:build linux

package resourceguard

// 仅复用原Inputs2纯解析；不读取Host账号、不生成账户/Source或修改权限。
func ValidateNormalSystemStockUdevAssetsAccounts(passwd, group []byte, pgUID, pgGID uint32) error {
	if e := normalOrdinaryStockUdevAccounts(passwd, group); e != nil {
		return e
	}
	return ValidateNormalSystemOrdinaryV2InstalledAccounts(passwd, group, pgUID, pgGID)
}
