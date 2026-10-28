package resourceguard

import (
	"context"
	"fmt"
	"reflect"
)

// RepairHeld只由受信恢复选择调用。调用者提供已读的完整View及摘要，目标
// 仍由root自己的原日志导出。独立repair栅栏同时覆盖无运行期Command的
// Bootstrap，不能用nil Command把丢回复当作已完成。网络未知后不自动重发，
// 先ObserveHeldRepair；显式再次调用也只能选择同一原来源。
func (c *Client) RepairHeld(ctx context.Context, source View, selector string) (Evidence, error) {
	var evidence Evidence
	digest, err := HeldSourceSHA256(source)
	if err != nil || selector != digest || source.Identity != c.config.Identity || source.IssuerFloorEpoch != c.config.IssuerEpoch {
		return evidence, &ClientError{MayHaveExecuted: false, Code: "invalid_held_repair_source"}
	}
	// 在RPC前固定用于验回执的完整上界，尤其不让调用者在等待HTTP时
	// 改写View内共享HugePools slice，使读回被另一份目标验证。
	limits := heldLimits(source)
	limits.HugePools = append([]HugePool(nil), limits.HugePools...)
	request := HeldRepairRequest{ProtocolVersion: 1, Identity: c.config.Identity, IssuerEpoch: c.config.IssuerEpoch, SourceSHA256: selector}
	c.mu.Lock()
	// 外层GuestReady可能仍因旧回复丢失保留同一个Pending，而Root已完整
	// 落账后又发生约束漂移。允许修复同一完整Held，仍保留该Pending给
	// 原Observe/业务Record；不同命令未知一律不越过，不能借修复清旧WAL。
	pendingDiffers := c.pending != nil && (source.Held == nil || !reflect.DeepEqual(*c.pending, *source.Held))
	if c.busy || pendingDiffers || c.repair != nil && *c.repair != request {
		c.mu.Unlock()
		return evidence, &ClientError{MayHaveExecuted: false, Code: "original_pending"}
	}
	c.busy = true
	c.repair = &request
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.busy = false; c.mu.Unlock() }()
	if err := c.call(ctx, false, "repair-held", request, &evidence); err != nil {
		return Evidence{}, err
	}
	if evidence.matchesLimits(c.config.Identity, limits) != nil || evidence.systemPlacementMatches(limits.SystemMemoryMaxBytes) != nil {
		return Evidence{}, &ClientError{MayHaveExecuted: true, Code: "held_repair_unconfirmed"}
	}
	c.mu.Lock()
	if c.repair != nil && *c.repair == request {
		c.repair = nil
	}
	c.mu.Unlock()
	return evidence, nil
}

// ObserveHeldRepair只读原State/实际完整上界与原Postmaster出生，不发修复
// POST。身份、来源摘要或floor漂移均保留本地未知；不拿新命令的同值读回
// 清旧修复，不把修复成功解释为PG预算/VM热插拔/Host结清已经完成。
func (c *Client) ObserveHeldRepair(ctx context.Context, selector string) (Evidence, error) {
	view, err := c.InspectState(ctx)
	if err != nil {
		return Evidence{}, err
	}
	digest, err := HeldSourceSHA256(view)
	if err != nil || digest != selector || view.IssuerFloorEpoch != c.config.IssuerEpoch {
		return Evidence{}, fmt.Errorf("%w: original held repair observation changed source or floor", ErrUnknown)
	}
	if _, err := view.ConfirmedLimits(); err != nil {
		return Evidence{}, err
	}
	if view.Birth != nil {
		process, err := c.InspectProcess(ctx)
		if err != nil {
			return Evidence{}, err
		}
		if process != *view.Birth {
			return Evidence{}, fmt.Errorf("%w: held repair original Postmaster changed", ErrUnknown)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.busy || c.repair != nil && c.repair.SourceSHA256 != selector {
		return Evidence{}, ErrUnknown
	}
	c.repair = nil
	return view.Actual, nil
}

// PendingHeldRepair返回独立选择副本，仅供诊断和显式恢复；不授予新写许可。
func (c *Client) PendingHeldRepair() *HeldRepairRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.repair == nil {
		return nil
	}
	selected := *c.repair
	return &selected
}
