//go:build linux

package resourceguard

import (
	"context"
	"fmt"
	"reflect"
)

// 生产仅Linux固定ISO/原锁内Origin读者实现；明确模型U可注入同形私有backend。
// 不能调用会再次取resource flock的公开ObservePhysicalKernelOrigin/PreSys入口。
type bootstrapPreSystemBackend interface {
	bootstrapPreSystemInputs(*lockedState) (prebootstrapSystemInputs, error)
}

// 锁序沿正式Bootstrap的单个resource flock；只允许已完成原PreSys同来源/同cap
// 接续，绝不凭System非空或当前cap相等收养。初次与显式Replay在任何资源写前
// 同步原四文件；普通已完成Observe零文件同步/零控制写。原Reference随正式intent
// 先fsync，故恢复不能更换原nonce的PreSys阶段，也不能丢引用回落legacy空域路径。
func (g *Guard) bootstrapPreSystemLocked(ctx context.Context, s *lockedState, c BootstrapCommand, requireEmpty, durable bool) (*bootstrapPreSystemReference, error) {
	backend, ok := g.kernel.(bootstrapPreSystemBackend)
	if !ok {
		return nil, fmt.Errorf("original pre-System bootstrap source unavailable")
	}
	in, err := backend.bootstrapPreSystemInputs(s)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(in.origin.ExpectedBootstrap, c) || sameBootstrapSource(in.source, c) != nil {
		return nil, fmt.Errorf("formal bootstrap differs from original pre-System source")
	}
	result, err := g.prebootstrapSystemLocked(ctx, in, preSystemObserve, s)
	if err != nil {
		return nil, err
	}
	if requireEmpty && !result.Current.PGEmpty {
		return nil, fmt.Errorf("formal bootstrap requires recursively empty original PG")
	}
	ref := &bootstrapPreSystemReference{ProtocolVersion: 1, IntentSHA256: result.IntentSHA256, CompletionSHA256: result.CompletionSHA256, OriginSHA256: in.originSHA, PolicySHA256: in.policySHA}
	if err = ref.validate(); err != nil {
		return nil, err
	}
	if durable {
		for _, name := range []string{physicalOriginDataName, physicalOriginConfirmName, preSystemIntentName, preSystemCompleteName} {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			if err = s.syncPhysicalFile(name); err != nil {
				return nil, fmt.Errorf("%w: original pre-System source sync: %v", ErrUnknown, err)
			}
		}
	}
	return ref, nil
}
