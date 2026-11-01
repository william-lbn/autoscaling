//go:build linux

package resourceguard

import "context"

// 只有包内DEV机制测试设置；生产Background无值。回调只能继续/报错，不能选择来源、
// task/FD/角色或权限。它始终在真实原checked状态产生之后调用，不绕过任何持久门。
type normalSystemRoleV2CheckpointKey struct{}

func normalSystemRoleV2CheckedCheckpoint(ctx context.Context, stage string, p *normalSystemRoleV2Publication) error {
	// finish的独立持久期限ctx不能抹掉原DEV观察者；保存的回调只能观察本次原对象。
	if p != nil && p.checkpoint != nil {
		return p.checkpoint(stage, p)
	}
	return nil
}
