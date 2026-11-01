//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// KernelPrep早于完整FS出生。这里只读取已封存原ISO/build/System清单和实际Layout2，
// 不创建FS WAL、不产opaque完整来源、不补历史或声称未来writer受控。状态盘不同SB
// 的旧路径由原reader保留；本helper只允许唯一固定同SB持久孔。
func normalFilesystemPreflightSameSuperblockState(config Config) error {
	if config.StateDir != FixedStateDir || !config.SystemCgroup {
		return fmt.Errorf("same-superblock state needs fixed original managed directory")
	}
	source, runtimeRaw, e := runtimeSource()
	if e != nil || source.Config != config {
		return fmt.Errorf("same-superblock original runtime differs")
	}
	if _, e = sealedRuntimeSource(); e != nil {
		return e
	}
	assets, e := RenderRuntimePolicyAssets(runtimeRaw)
	if e != nil {
		return e
	}
	requirements, e := DeriveRuntimeBootRequirements(runtimeRaw, func() map[string][]byte {
		out := map[string][]byte{RuntimeResourceAssetName: runtimeRaw}
		for k, v := range assets {
			out[k] = v
		}
		return out
	}())
	accepted := requirements.Enabled() && requirements.ProfileID() == NormalSplitV1PhysicalKernelProfileID
	if requirements.ProfileID() == NormalSplitMetadataV1PhysicalKernelProfileID {
		family, familyErr := normalInitialWALFamilyForRequirements(requirements)
		accepted = familyErr == nil && requirements.version == 2 && family == normalInitialWALFamilyKernelPrepV2
	}
	if e != nil || !accepted {
		return fmt.Errorf("same-superblock state needs canonical explicit normal split source")
	}
	cmd, e := kernelText("/proc/cmdline")
	if e != nil {
		return e
	}
	if e = requirements.ValidateKernelCommandLine(cmd); e != nil {
		return e
	}
	policyRaw, e := nativeBirthReadISO(fixedNativeBirthPolicyPath, 16<<10)
	if e != nil {
		return e
	}
	if !bytes.Equal(policyRaw, assets["dsga-native-birth.json"]) {
		return fmt.Errorf("same-superblock native policy differs from original rendered asset")
	}
	f, e := nativeBirthOpenRO(fixedNativeBirthBuildPath)
	if e != nil {
		return e
	}
	buildRaw, e := nativeBirthReadBounded(f, 32<<10)
	f.Close()
	if e != nil {
		return e
	}
	native, e := decodeNativeBirthSource(runtimeRaw, policyRaw, buildRaw)
	if e != nil || native.resource.Config != config || native.manifest.RuntimeClosure == nil || native.manifest.RuntimeClosure.validate() != nil {
		return fmt.Errorf("same-superblock original runtime/build/System closure missing")
	}
	f, e = nativeBirthOpenRO(NormalSystemSourceManifestPath)
	if e != nil {
		return e
	}
	systemRaw, e := nativeBirthReadBounded(f, 1<<20)
	f.Close()
	if e != nil || physicalDigest(systemRaw) != native.manifest.RuntimeClosure.SystemManifestSHA256 {
		return fmt.Errorf("same-superblock original System digest differs")
	}
	manifest, e := DecodeNormalSystemSourceManifest(systemRaw)
	if e != nil || manifest.ProtocolVersion != 2 && manifest.ProtocolVersion != 3 {
		return fmt.Errorf("same-superblock state requires original System v2/v3")
	}
	if requirements.ProfileID() == NormalSplitMetadataV1PhysicalKernelProfileID && manifest.ProtocolVersion != 3 {
		return fmt.Errorf("metadata state requires explicit System3 inventory")
	}
	// 原只读完整静态文件/配置清单包含vminit准确字节；不反调Native Birth总入口。
	if _, e = CheckNormalSystemSource(context.Background(), systemRaw); e != nil {
		return e
	}
	pid1, e := normalFilesystemPinInitialTask(true)
	if e != nil {
		return e
	}
	defer pid1.close()
	facts, allowed, e := normalFilesystemCurrentGraph(context.Background(), pid1.ns, pid1.rootObject)
	if e != nil {
		return e
	}
	canonical, e := filepath.EvalSymlinks(config.StateDir)
	if e != nil || canonical != config.StateDir {
		return fmt.Errorf("same-superblock state canonical ancestors differ")
	}
	n, e := unix.Open(config.StateDir, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if e != nil {
		return e
	}
	defer unix.Close(n)
	if e = privateRegular(n, unix.S_IFDIR); e != nil {
		return e
	}
	object, e := normalFilesystemFDObject(n)
	if e != nil {
		return e
	}
	expected := uint64(0)
	for _, f := range facts {
		if f.Mount.Point == "/var/lib/neon-dsga" {
			expected = f.Object.MountID
		}
	}
	if object.Filesystem != unix.EXT4_SUPER_MAGIC || object.Readonly || object.Device != pid1.rootObject.Device || object.MountID != expected || !allowed[object.MountID] {
		return fmt.Errorf("same-superblock state is not original RW bind-hole object")
	}
	return nil
}
