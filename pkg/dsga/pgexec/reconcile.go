package pgexec

import (
	"context"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// reconcileSettled解决恢复客户端时已落账ResizePG重新成为Pending的问题。
// 外层WAL始终保留，不能在构造器仅看Settled布尔就省略SDK栅栏；先核对完整原
// 命令/审计/结果，再隔离当前owner并只读查询同原终态。旧epoch允许查询，绝不
// 改epoch重发；root未知也不得阻断原PG查询。准入/预算/VM的后续门槛仍独立。
func (e *Executor) reconcileSettled(ctx context.Context, op *journal.Operation) (bool, error) {
	pending := e.client.Pending()
	if pending == nil {
		return false, nil
	}
	if *pending != op.Request || op.Request.Key.Instance != e.instance {
		return false, fmt.Errorf("original SDK intent differs from durable operation")
	}
	for _, step := range op.Steps {
		if step.Action != journal.ResizePG || step.Phase != journal.Settled {
			continue
		}
		command, err := control.DecodeRequest(step.Command)
		var saved executionAudit
		if err != nil || command != op.Request || step.TargetID != e.instance.ComputeInstanceID || step.Observation == nil || control.UnmarshalObject(step.Observation.Data, &saved) != nil || saved.ProtocolVersion != 1 || saved.Response.ValidateResize(op.Request, e.instance) != nil || saved.Response.Receipt.Result == nil {
			return false, fmt.Errorf("durable PG outcome lacks complete original native terminal")
		}
		originalFence := control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: saved.Fence.RequestedEpoch}
		if saved.Fence.RequestedEpoch > e.actor.Epoch || saved.Fence.RequestedEpoch < op.Request.Key.IssuerEpoch || saved.Fence.ValidateFor(originalFence, e.instance) != nil || saved.Fence.Disposition != control.Fenced || saved.Response.Observed.IssuerFloorEpoch != saved.Fence.RequestedEpoch || saved.Response.Observed.CapacityGeneration < saved.Fence.Observed.CapacityGeneration {
			return false, fmt.Errorf("durable PG terminal fence differs from recorded owner")
		}
		phase := saved.Response.Receipt.Result.Operation.Phase
		if !((step.Observation.Result == journal.Completed && phase == control.Committed) || (step.Observation.Result == journal.Rejected && phase == control.Denied)) {
			return false, fmt.Errorf("durable PG outcome contradicts original native terminal")
		}
		fence, err := e.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
		if err != nil {
			return false, err
		}
		if fence.Disposition != control.Fenced {
			return false, nil
		}
		_, err = e.client.ReconcileTerminal(ctx, op.Request, saved.Response, fence)
		return err == nil, err
	}
	return false, nil
}
