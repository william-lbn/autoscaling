//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
)

// 模式只能来自原RO System3库存的同一资产；缺失/畸形新版绝不回退旧Root角色。
// Root/broker/resource/compute都不会因为此模式而隐式安装ordinary约束。
func normalSystemRoleOriginalControlProtocol(ctx context.Context, s *normalSystemLifecycleSource) (uint32, error) {
	if ctx == nil || ctx.Err() != nil || s == nil {
		return 0, ErrUnknown
	}
	if s.manifest.ProtocolVersion != 3 {
		return 1, nil
	}
	var wanted *NormalSystemSourceFile
	for _, v := range s.manifest.Files {
		if v.Path == NormalKernelControlSourceManifestPath {
			q := v
			wanted = &q
			break
		}
	}
	if wanted == nil || wanted.Kind != "config" || wanted.CanonicalPath != NormalKernelControlSourceManifestPath {
		return 0, ErrUnknown
	}
	f, e := nativeBirthOpenRO(wanted.CanonicalPath)
	if e != nil {
		return 0, e
	}
	raw, e := nativeBirthReadBounded(f, 64<<10)
	ce := f.Close()
	if e != nil || ce != nil {
		return 0, errors.Join(ErrUnknown, e, ce)
	}
	if physicalDigest(raw) != wanted.SHA256 || uint64(len(raw)) != wanted.Bytes {
		return 0, ErrUnknown
	}
	var header struct {
		ProtocolVersion uint32 `json:"protocol_version"`
	}
	if json.Unmarshal(raw, &header) != nil {
		return 0, ErrUnknown
	}
	switch header.ProtocolVersion {
	case 1:
		if _, e = decodeNormalKernelControlManifest(raw); e != nil {
			return 0, e
		}
		return 1, nil
	case 2:
		if _, e = DecodeNormalSystemOrdinaryV2Manifest(raw); e != nil {
			return 0, e
		}
		return 2, nil
	}
	return 0, ErrUnknown
}
func normalSystemRoleV2LaunchSpecIfSelected(ctx context.Context, s *normalSystemLifecycleSource, role string) (normalSystemRoleLaunchSpec, bool, error) {
	var zero normalSystemRoleLaunchSpec
	mode, e := normalSystemRoleOriginalControlProtocol(ctx, s)
	if e != nil {
		return zero, false, e
	}
	if mode != 2 {
		return zero, false, nil
	}
	if role == "compute" {
		p, e := newNormalSystemRoleLaunchSpec(ctx, s, role)
		if e != nil {
			return zero, false, e
		}
		p.roleSourceProtocol = 2
		return p, true, nil
	}
	rule, e := NormalSystemOrdinaryV2RolePlan(role)
	if e != nil {
		return zero, false, nil
	}
	if runtime.GOARCH != "amd64" || runtime.Version() != "go1.24.3" {
		return zero, false, fmt.Errorf("ordinary2 original Gate SDK/architecture unsupported")
	}
	sha, e := s.referenceSHA256()
	if e != nil {
		return zero, false, e
	}
	return normalSystemRoleLaunchSpec{roleSourceProtocol: 2, role: role, argv: append([]string(nil), rule.Argv...), env: append([]string{}, rule.Environment...), uid: rule.UID, gid: rule.GID, binding: s.binding, sourceSHA: sha}, true, nil
}
