package resourceguard

// v3只描述新的固定启动链/库存；版本号不是Root/FS/角色或normal安全资格。
const NormalSystemSourceV3ProfileID = "neon-dsga-normal-system-source-v3"
const normalSystemV3BrokerDeclarationPath = "/neonvm/config/dsga-normal-role-broker.json"
const normalSystemV3KernelControlPath = "/neonvm/config/dsga-kernel-control-source.json"
const normalSystemV3BuildPlanPath = "/neonvm/config/dsga-normal-build-plan.json"

func normalSystemSourceVersionProfile(version uint32, profile string) bool {
	switch version {
	case 1:
		return profile == NormalSystemSourceProfileID
	case 2:
		return profile == NormalSystemSourceV2ProfileID
	case 3:
		return profile == NormalSystemSourceV3ProfileID
	}
	return false
}
func normalSystemLifecycleManifestVersion(m NormalSystemSourceManifest) bool {
	return (m.ProtocolVersion == 2 || m.ProtocolVersion == 3) && normalSystemSourceVersionProfile(m.ProtocolVersion, m.ProfileID)
}
func normalSystemV3MetadataPaths() []string {
	return []string{normalSystemV3BrokerDeclarationPath, normalSystemV3KernelControlPath, normalSystemV3BuildPlanPath}
}

// BusyBox的env applet由同一个原busybox ELF执行，不增加env alias/任意shell。
// compute保vmstart的allow-start/flock，真正broker payload仍为原ComputeCtl。
// init会把含=的命令隐式交/bin/sh，故client用严格空环境且argv全是固定绝对路径。
// broker/实际目标仍用各自PATH或build-plan环境，不把client空环境泛化到其它角色。
func normalSystemV3InitEntries() []string {
	const prefix = "/neonvm/bin/busybox env -i /neonvm/bin/dsga-resource-guard exec-system-role "
	return []string{
		"::sysinit:/neonvm/bin/vminit",
		"::once:" + prefix + "allow-start",
		"::once:" + prefix + "kernel-version",
		"::respawn:" + prefix + "network",
		"::respawn:" + prefix + "udev",
		"::wait:" + prefix + "udev-init",
		"::respawn:" + prefix + "acpi",
		"::respawn:" + prefix + "logs",
		"::respawn:" + prefix + "clock",
		"::respawn:" + prefix + "daemon",
		"::respawn:" + prefix + "resource",
		"::respawn:/neonvm/bin/vmstart",
		"::shutdown:/neonvm/bin/vmshutdown",
	}
}

func normalSystemV3ClientEnvironment() []string { return []string{} }
