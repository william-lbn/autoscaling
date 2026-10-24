package compute

import (
	"context"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ReconcileTerminal只用于可信协调器恢复已经落账的完整原生终态。普通Status/
// Memory仍不清Pending；本方法也不重发Resize、不取消原动作、不重写持久回执。
// expected必须来自原账本，fence必须属于当前固定owner。缺历史/换boot/结果矛盾
// 保留原栅栏，不能从not-found、HTTP200或当前容量推断NoEffect。
func (client *Client) ReconcileTerminal(ctx context.Context, intent control.Request, expected control.ControlResponse, fence control.FenceResponse) (control.ControlResponse, error) {
	fail := func(code string) (control.ControlResponse, error) {
		return control.ControlResponse{}, &CallError{Code: code}
	}
	if intent.Key.Instance != client.instance || intent.Key.IssuerEpoch > client.epoch || expected.ValidateResize(intent, client.instance) != nil || expected.Receipt.Result == nil || expected.Observed.IssuerFloorEpoch > client.epoch || !terminal(expected.Receipt.Result.Operation.Phase) {
		return fail("invalid_original_terminal")
	}
	fenceRequest := control.FenceRequest{ProtocolVersion: 1, Instance: client.instance, IssuerEpoch: client.epoch}
	if fence.ValidateFor(fenceRequest, client.instance) != nil || fence.Disposition != control.Fenced {
		return fail("terminal_reconcile_requires_current_fence")
	}
	client.mu.Lock()
	eligible := !client.inFlight && client.pending != nil && *client.pending == intent
	client.mu.Unlock()
	if !eligible {
		return fail("original_terminal_not_pending")
	}
	// 仅向观察端点发送完整原Key。Status的基础校验不足以比较VM/预算/期限，
	// 因此下面再以完整intent摘要核对，不能把同Key不同context的结果清成已知。
	fact, err := client.Status(ctx, control.Query{ProtocolVersion: 1, Key: intent.Key})
	if err != nil {
		return control.ControlResponse{}, err
	}
	if fact.ValidateResize(intent, client.instance) != nil || fact.Receipt.Disposition != control.ReceiptReplayed || fact.Receipt.Result == nil || fact.Observed.IssuerFloorEpoch != client.epoch || fact.Observed.CapacityGeneration < fence.Observed.CapacityGeneration || fact.Observed.CapacityGeneration < fact.Receipt.Result.CapacityGeneration || !sameTerminal(*expected.Receipt.Result, *fact.Receipt.Result) {
		return fail("original_terminal_unconfirmed")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	// 不跨网络持本地锁。观察期间同原Resize可能被显式重放、另一个请求也可能
	// 被可信调用者保留；只有完全相同原Pending且没有执行在途才可解除这一项。
	// 新准入/资源/VM未知由各自栅栏保留，本方法绝不清它们或释放预算。
	if client.inFlight || client.pending == nil || *client.pending != intent {
		return fail("original_terminal_changed_during_read")
	}
	client.pending = nil
	return fact, nil
}

func terminal(phase control.OperationPhase) bool {
	return phase == control.Committed || phase == control.Denied
}

// Provider、成本、当前floor与准入在后续步骤可继续变化；它们必须由预算域
// 独立核验，不得拿旧快照当现行上限。这里只比较原终态不可改变的逻辑事实，
// 保留原journal的完整审计，不用本次查询覆盖它。Replayed是查询来源标记。
func sameTerminal(before, after control.Snapshot) bool {
	if before.Instance != after.Instance || before.CapacityGeneration != after.CapacityGeneration || before.LowBuffers != after.LowBuffers || before.HighBuffers != after.HighBuffers || before.MaximumBuffers != after.MaximumBuffers || before.StartupSharedUpperBytes != after.StartupSharedUpperBytes || before.Operation == nil || after.Operation == nil {
		return false
	}
	a, b := *before.Operation, *after.Operation
	a.Replayed, b.Replayed = false, false
	return terminal(a.Phase) && a == b
}
