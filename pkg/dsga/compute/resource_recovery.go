package compute

import (
	"context"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ReconcileRecovery只消费协调器已持久化的独立恢复证明。原WAL未知不改，
// 不发送Resize/Cancel，不推导NoEffect。保存CAS丢回复时调用方不能进入此处；
// 重读证明后仍须当前owner重新Fence并查询完整原Key，不能凭旧Proof清Pending。
// 原生终态和更高floor隔离两种事实分别保持，不能在查询中互相替换。
func (client *Client) ReconcileRecovery(ctx context.Context, intent control.Request, retainedFence control.FenceResponse, retainedStatus control.ControlResponse, currentFence control.FenceResponse) (control.ControlResponse, error) {
	fail := func(code string) error {
		return &CallError{MayHaveExecuted: false, Code: code}
	}
	effect, err := control.ValidateResizeRecovery(intent, retainedFence, retainedStatus)
	q := control.FenceRequest{ProtocolVersion: 1, Instance: client.instance, IssuerEpoch: client.epoch}
	if err != nil || intent.Key.Instance != client.instance || retainedFence.RequestedEpoch > client.epoch ||
		currentFence.ValidateFor(q, client.instance) != nil || currentFence.Disposition != control.Fenced ||
		!control.SameResizeRecoveryBoundary(retainedStatus.Observed, currentFence.Observed) {
		return control.ControlResponse{}, fail("invalid_retained_resize_recovery")
	}
	client.mu.Lock()
	eligible := !client.inFlight && client.pending != nil && *client.pending == intent
	client.mu.Unlock()
	if !eligible {
		return control.ControlResponse{}, fail("original_resize_recovery_not_pending")
	}
	fact, err := client.Status(ctx, control.Query{ProtocolVersion: 1, Key: intent.Key})
	if err != nil {
		return control.ControlResponse{}, err
	}
	currentEffect, err := control.ValidateResizeRecovery(intent, currentFence, fact)
	if err != nil || currentEffect != effect ||
		(retainedStatus.Receipt.Result != nil && (fact.Receipt.Result == nil || !control.SameResizeRecoveryTerminal(*retainedStatus.Receipt.Result, *fact.Receipt.Result))) {
		return control.ControlResponse{}, fail("original_resize_recovery_unconfirmed")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	// 不跨网络持mu。查询期间同原Resize若进入执行，或Pending被换成另一
	// 完整意图，继续保持栅栏；准入/Guest/VM的独立未知从不由本函数清除。
	if client.inFlight || client.pending == nil || *client.pending != intent {
		return control.ControlResponse{}, fail("original_resize_recovery_changed_during_read")
	}
	client.pending = nil
	return fact, nil
}
