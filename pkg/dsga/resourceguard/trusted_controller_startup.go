package resourceguard

// 固定可信节点启动表只由构建器的显式部署合同选择，默认v3表仍原样保留。
// PID1先完成vminit/initial-boot；udev-init为once，不阻塞respawn的udevd和Root。
func TrustedControllerInitEntries() []string {
	rows, _ := NormalSystemSourceInitEntriesForVersion(2)
	for i, row := range rows {
		if row == "::wait:/neonvm/bin/sh /neonvm/bin/dsga-resource-service udev-init" {
			rows[i] = "::once:/neonvm/bin/sh /neonvm/bin/dsga-resource-service udev-init"
		}
	}
	return rows
}
