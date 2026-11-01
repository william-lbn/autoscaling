//go:build linux

package resourceguard

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"golang.org/x/sys/unix"
)

const normalInitialWALFamilyKernelPrepV2 = "initial8-kernel-prep-userns-v2"

// 固定名字的纯历史投影，不读取目录猜版本，也不创建任何opaque权限。
func normalInitialWALFiles(family string) ([8]string, error) {
	names := initialPrebootGuestFiles
	switch family {
	case "":
	case normalInitialWALFamilyKernelPrepV2:
		names[0], names[1] = normalKernelPrepV2IntentName, normalKernelPrepV2CompleteName
	default:
		return [8]string{}, errors.Join(ErrUnknown, fmt.Errorf("unknown original initial-WAL family"))
	}
	return names, nil
}

func normalInitialWALFamilyForProfileBuild(profile, artifact string) (string, error) {
	switch profile {
	case NormalSplitV1PhysicalKernelProfileID:
		ref, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
		if e == nil && artifact == ref.KernelArtifactSHA256 {
			return "", nil
		}
	case NormalSplitMetadataV1PhysicalKernelProfileID:
		ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
		if e == nil && ref.ControlMetadataGuard() && ref.StrictPageCounterIRQBoundary() && artifact == ref.KernelArtifactSHA256 {
			return normalInitialWALFamilyKernelPrepV2, nil
		}
	}
	return "", errors.Join(ErrUnknown, fmt.Errorf("initial-WAL family needs exact original profile/build pair"))
}

func normalInitialWALFamilyForRequirements(plan RuntimeBootRequirements) (string, error) {
	family, e := normalInitialWALFamilyForProfileBuild(plan.ProfileID(), plan.KernelBuildReference().KernelArtifactSHA256)
	if e != nil {
		return "", e
	}
	if family == "" {
		if plan.version == 1 && plan.Enabled() && plan.memoryOriginProtocol <= 2 {
			return "", nil
		}
	} else if plan.version == 2 && plan.kernelPreparationProtocol == 2 && plan.memoryOriginProtocol == 2 && plan.initialWALFamily == family {
		return family, nil
	}
	return "", errors.Join(ErrUnknown, fmt.Errorf("initial-WAL family differs from explicit boot plan"))
}

func normalInitialWALFamilyForHost(v bootprotocol.NormalMemoryOriginHostReference) (string, error) {
	if e := v.Validate(); e != nil {
		return "", e
	}
	return normalInitialWALFamilyForProfileBuild(v.ProfileID, v.Prepared.Challenge.KernelBuildSHA256)
}

func initialPrebootGuestWitnessFamilyLocked(s *lockedState, c bootprotocol.InitialPrebootChallenge, family string) (initialPrebootGuestWitnessKey, error) {
	var key initialPrebootGuestWitnessKey
	names, e := normalInitialWALFiles(family)
	if e != nil {
		return key, e
	}
	st, e := initialPrebootStateIdentity(s)
	if e != nil {
		return key, e
	}
	var lk unix.Stat_t
	if e = unix.Fstat(int(s.lock.Fd()), &lk); e != nil {
		return key, e
	}
	key.Device, key.Inode, key.LockDevice, key.LockInode = st.Dev, st.Ino, lk.Dev, lk.Ino
	raw, e := json.Marshal(c)
	if e != nil {
		return key, e
	}
	key.ChallengeSHA256 = physicalDigest(raw)
	for i, n := range names {
		raw, id, e := arrayPopulateHistoryRaw(s, n)
		if e != nil {
			return key, e
		}
		key.Files[i], key.SHA256[i] = id, physicalDigest(raw)
	}
	return key, nil
}

// Root FS的窄seam：family来自已校验memory历史投影，不能从当前目录补签。
func normalFilesystemSyncFilesForWitness(w normalFilesystemOriginWitnessKey) ([]string, error) {
	family := ""
	if w.Memory.ProtocolVersion != 0 {
		if _, e := w.Memory.referenceSHA256(); e != nil {
			return nil, e
		}
		family = w.Memory.InitialWALFamily
	}
	initial, e := normalInitialWALFiles(family)
	if e != nil {
		return nil, e
	}
	names := append(append([]string(nil), initial[:]...), normalFilesystemOriginIntentName, normalFilesystemOriginCompleteName)
	if w.Memory.ProtocolVersion == 2 {
		for _, n := range normalMemoryConfirmedFiles() {
			names = append(names, n)
		}
	}
	return names, nil
}

// 供Root在已有typed Runtime上下文中比对历史family；只投影，不认领Source。
func normalFilesystemInitialFamilyFromMemory(m *normalMemoryOriginConfirmedReference) (string, error) {
	if m == nil {
		return "", nil
	}
	if _, e := m.referenceSHA256(); e != nil {
		return "", e
	}
	if _, e := normalInitialWALFiles(m.InitialWALFamily); e != nil {
		return "", e
	}
	return m.InitialWALFamily, nil
}
