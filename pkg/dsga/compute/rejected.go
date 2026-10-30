package compute

import (
	"context"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ReconcileRejected只供已有独立持久屏障的可信协调器使用。与完整原生终态
// 对账分开，原Busy/Rejected绝不补成Denied/Completed。调用者先保存屏障，
// 再把本次当前owner fence传入；本方法仍重新查询原Key，不能凭旧Proof清
// Pending。没有网络setter；不持mu跨网络，只清完全相同且无执行在途的一项。
func (client *Client) ReconcileRejected(ctx context.Context, intent control.Request, original control.ControlResponse, retainedFence control.FenceResponse, retainedStatus control.ControlResponse, currentFence control.FenceResponse) (control.ControlResponse, error) {
	fail := func(code string) error {
		return &CallError{MayHaveExecuted: false, Code: code}
	}
	if intent.Key.Instance != client.instance || retainedFence.RequestedEpoch > client.epoch ||
		control.ValidateRejectedResizeBarrier(intent, original, retainedFence, retainedStatus) != nil ||
		currentFence.RequestedEpoch != client.epoch {
		return control.ControlResponse{}, fail("invalid_retained_rejection_barrier")
	}
	client.mu.Lock()
	eligible := !client.inFlight && client.pending != nil && *client.pending == intent
	client.mu.Unlock()
	if !eligible {
		return control.ControlResponse{}, fail("original_rejection_not_pending")
	}
	fact, err := client.Status(ctx, control.Query{ProtocolVersion: 1, Key: intent.Key})
	if err != nil {
		return control.ControlResponse{}, err
	}
	if control.ValidateRejectedResizeBarrier(intent, original, currentFence, fact) != nil {
		return control.ControlResponse{}, fail("original_rejection_barrier_unconfirmed")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.inFlight || client.pending == nil || *client.pending != intent {
		return control.ControlResponse{}, fail("original_rejection_changed_during_read")
	}
	client.pending = nil
	return fact, nil
}
