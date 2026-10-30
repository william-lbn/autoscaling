package compute

import (
	"context"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// ReconcileActualBudget只消费已由外层UID/RV持久保存的完整v4实际预算。
// 它不把历史淘汰改成隔离、不改原Unknown、不发送Resize/Cancel/准入setter。
// 原PG未派发时必须没有SDK Pending；已派发只可清完全相同的执行槽，空槽
// 仍完整审计当前事实。调用方继续负责原WAL、门禁和全部非PG来源的认证。
func (client *Client) ReconcileActualBudget(ctx context.Context, intent control.Request, capacity budget.ReconciledCapacity, currentFence control.FenceResponse) (control.ControlResponse, error) {
	var empty control.ControlResponse
	fail := func(code string) error {
		return &CallError{MayHaveExecuted: false, Code: code}
	}
	q := control.FenceRequest{ProtocolVersion: 1, Instance: client.instance, IssuerEpoch: client.epoch}
	if capacity.ProtocolVersion != 4 || capacity.Validate() != nil || capacity.Request != intent || intent.Key.Instance != client.instance || capacity.IssuerEpoch > client.epoch ||
		currentFence.ValidateFor(q, client.instance) != nil || currentFence.Disposition != control.Fenced {
		return empty, fail("invalid_retained_actual_budget")
	}
	fenced, ok := actualBudgetShared(capacity.StartupMainUpperBytes, capacity.StartupArraysUpperBytes, currentFence.Observed)
	if !ok || capacity.ValidateCurrent(fenced, client.epoch, capacity.VMMemoryBytes) != nil {
		return empty, fail("actual_budget_requires_current_capacity_fence")
	}
	client.mu.Lock()
	originalPending := client.pending
	eligible := !client.inFlight && (originalPending == nil || *originalPending == intent) && (capacity.UnchangedPG == nil || originalPending == nil)
	client.mu.Unlock()
	if !eligible {
		return empty, fail("actual_budget_original_slot_not_eligible")
	}
	shared, err := client.SharedCost(ctx)
	if err != nil {
		return control.ControlResponse{}, err
	}
	if capacity.ValidateCurrent(shared, client.epoch, capacity.VMMemoryBytes) != nil || !control.SameResizeRecoveryBoundary(currentFence.Observed, shared.Observed) {
		return empty, fail("actual_budget_current_shared_changed")
	}
	fact, err := client.Status(ctx, control.Query{ProtocolVersion: 1, Key: intent.Key})
	if err != nil {
		return control.ControlResponse{}, err
	}
	observed, ok := actualBudgetShared(shared.StartupMainUpperBytes, shared.StartupArraysUpperBytes, fact.Observed)
	if !ok || fact.ValidateResize(intent, client.instance) != nil || capacity.ValidateCurrent(observed, client.epoch, capacity.VMMemoryBytes) != nil ||
		!control.SameResizeRecoveryBoundary(shared.Observed, fact.Observed) || !actualBudgetSameHead(shared.Observed, fact.Observed) {
		return empty, fail("actual_budget_original_query_changed")
	}
	if capacity.UnchangedPG != nil {
		// 原Planned没有已派发栅栏可清。同owner NotFound是正常只读事实，
		// 但原Key出现结果即与持久未派发选择矛盾；此处不能将其收养。
		if fact.Receipt.Result != nil || (fact.Receipt.Disposition != control.ReceiptNotFound && fact.Receipt.Disposition != control.ReceiptStale && fact.Receipt.Disposition != control.ReceiptResultExpired) {
			return empty, fail("actual_budget_undispatched_pg_has_result")
		}
	} else {
		retained := capacity.RecoveryPG.Status
		if fact.Receipt.Result != nil {
			if retained.Receipt.Result == nil || fact.Receipt.Disposition != control.ReceiptReplayed || !control.SameResizeRecoveryTerminal(*retained.Receipt.Result, *fact.Receipt.Result) || !control.SameResizeRecoveryBoundary(*fact.Receipt.Result, fact.Observed) {
				return empty, fail("actual_budget_original_terminal_changed")
			}
		} else if fact.Receipt.Disposition != control.ReceiptStale && fact.Receipt.Disposition != control.ReceiptResultExpired && (fact.Receipt.Disposition != control.ReceiptNotFound || client.epoch <= intent.Key.IssuerEpoch) {
			// 永久预算已保存完整终态后，明确历史淘汰可以只读恢复；原证明
			// 仍是终态，不降级为isolated_unknown。同floor随意NotFound不够。
			return empty, fail("actual_budget_original_result_not_confirmed")
		}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	// 网络期间不持mu。除完整意图外还固定原槽指针：一次同意图执行即使
	// 已结束、又留下同数值Pending，也替换了槽，不能借前后false抹去它。
	if client.inFlight || client.pending != originalPending || client.pending != nil && *client.pending != intent {
		return empty, fail("actual_budget_original_slot_changed_during_read")
	}
	if originalPending != nil {
		client.pending = nil
	}
	return fact, nil
}

func actualBudgetShared(main, arrays control.Uint64, snapshot control.Snapshot) (control.SharedCostResponse, bool) {
	var empty control.SharedCostResponse
	total := uint64(main)
	for _, array := range snapshot.Arrays {
		if uint64(array.CommittedUpperBytes) > math.MaxUint64-total {
			return empty, false
		}
		total += uint64(array.CommittedUpperBytes)
	}
	return control.SharedCostResponse{ProtocolVersion: 1, VMShrinkPermit: false, StartupMainUpperBytes: main, StartupArraysUpperBytes: arrays, CurrentMainAndArraysUpperBytes: control.Uint64(total), Observed: snapshot}, true
}

func actualBudgetSameHead(a, b control.Snapshot) bool {
	if a.Operation == nil || b.Operation == nil {
		return a.Operation == nil && b.Operation == nil
	}
	return control.SameResizeRecoveryTerminal(a, b)
}
