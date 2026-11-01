//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"sync"
)

// 按原Authority指针保留单次producer和失败tombstone。nonce/路径/当前census
// 不能重建这个对象；关闭/prepare部分失败后，普通调用不会补签或隐式重放。
var normalOrdinaryV2DirectorySources = struct {
	sync.Mutex
	byAuthority map[*normalSystemResourceAuthority]*normalSystemOrdinaryV2LayoutSource
}{byAuthority: map[*normalSystemResourceAuthority]*normalSystemOrdinaryV2LayoutSource{}}

// 后继唯一薄hook在原RunBroker prepared之后、COMMIT/Ready之前调用。此函数
// 自取resource锁；调用者不能在持锁时调用。这里不启动role或改任何cap/预算。
func installNormalSystemOrdinaryV2DirectoryLayout(ctx context.Context, g *Guard, system *normalSystemLifecycleSource, filesystem *normalFilesystemOriginSource, proof *normalSystemRoleBrokerExecProof) (func(), error) {
	mode, e := normalSystemRoleOriginalControlProtocol(ctx, system)
	if e != nil {
		return nil, e
	}
	if mode == 1 {
		return func() {}, nil
	}
	if mode != 2 || g == nil {
		return nil, ErrUnknown
	}
	var source *normalSystemOrdinaryV2LayoutSource
	e = g.withRecord(ctx, func(r *record, s *lockedState) error {
		a := lookupNormalSystemAuthority(g)
		if a == nil {
			return ErrUnknown
		}
		normalOrdinaryV2DirectorySources.Lock()
		if _, exists := normalOrdinaryV2DirectorySources.byAuthority[a]; exists || len(normalOrdinaryV2DirectorySources.byAuthority) >= 64 {
			normalOrdinaryV2DirectorySources.Unlock()
			return ErrUnknown
		}
		normalOrdinaryV2DirectorySources.byAuthority[a] = nil
		normalOrdinaryV2DirectorySources.Unlock()
		var err error
		source, err = prepareNormalSystemOrdinaryV2LayoutLocked(ctx, g, s, r, system, filesystem, proof)
		if err != nil {
			return err
		}
		normalOrdinaryV2DirectorySources.Lock()
		normalOrdinaryV2DirectorySources.byAuthority[a] = source
		normalOrdinaryV2DirectorySources.Unlock()
		return nil
	})
	if e != nil {
		if source != nil {
			source.close()
		}
		return nil, errors.Join(ErrUnknown, e)
	}
	return source.close, nil
}

// 实际prepare-role/second-stop消费此只读入口，不把静态目录表当已准备证书。
// 构造可在candidate期完成；任何角色消费仍要求同一Authority已经COMMIT。
func requireNormalSystemOrdinaryV2DirectoryLayoutLocked(ctx context.Context, a *normalSystemResourceAuthority, s *lockedState, r *record) error {
	if a == nil || a.admission == nil || !a.admission.businessAllowed() {
		return ErrUnknown
	}
	normalOrdinaryV2DirectorySources.Lock()
	source := normalOrdinaryV2DirectorySources.byAuthority[a]
	normalOrdinaryV2DirectorySources.Unlock()
	if source == nil || source.authority != a {
		return ErrUnknown
	}
	_, e := source.referenceLocked(ctx, s, r)
	return e
}
