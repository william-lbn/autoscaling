// Package admissionexec消费同一持久journal的最终恢复准入步骤，不创建另一操作日志。
// 初始启动预算、实际quiesce与Host/Guest/VM领域仍由对应生产适配器完成，不能跳步。
package admissionexec

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

type Executor struct {
	store    *journal.Store
	client   *compute.Client
	actor    journal.Owner
	instance control.Instance
}

type audit struct {
	ProtocolVersion uint32                     `json:"protocol_version"`
	Query           bool                       `json:"query"`
	Fence           control.FenceResponse      `json:"issuer_fence"`
	Shared          control.SharedCostResponse `json:"shared"`
	Response        control.AdmissionResponse  `json:"response"`
}

func New(store *journal.Store, client *compute.Client, actor journal.Owner) (*Executor, error) {
	if store == nil || client == nil || actor.Validate() != nil {
		return nil, fmt.Errorf("admission executor requires fixed journal/client/owner")
	}
	instance, epoch := client.Binding()
	if actor.Epoch != epoch {
		return nil, fmt.Errorf("admission owner differs from client")
	}
	return &Executor{store: store, client: client, actor: actor, instance: instance}, nil
}

// Advance只推进已存在操作的ResumeAdmission；预算/VM未确认或旧未知存在时不Start。
// 写前CAS必须返回真实票据才发送setter。丢回复后仅按保存的完整请求只读查询，
// NotFound/Expired/冲突均保留原意图，绝不在轮询中生成新revision或自动重发POST。
func (e *Executor) Advance(ctx context.Context) (pgexec.Progress, error) {
	return e.advance(ctx, nil)
}

// ReplayOriginal只供已认证协调入口的显式选择；不由普通轮询/HTTP错误自动触发。
// 限当前owner、同boot已持久派发的原开放意图，重查全预算后发送完全相同字节值。
func (e *Executor) ReplayOriginal(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if key.Validate() != nil {
		return pgexec.AwaitingFact, fmt.Errorf("invalid admission replay key")
	}
	return e.advance(ctx, &key)
}

func (e *Executor) advance(ctx context.Context, replay *control.Key) (pgexec.Progress, error) {
	snapshot, err := e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	state := snapshot.State()
	if state.Owner != e.actor {
		return pgexec.AwaitingFact, fmt.Errorf("admission owner changed")
	}
	if state.Active == nil {
		if replay != nil {
			return pgexec.AwaitingFact, fmt.Errorf("no active original admission")
		}
		return pgexec.Idle, nil
	}
	op := state.Active
	if replay != nil && (*replay != op.Request.Key || op.Origin != e.actor || op.AbortRequested) {
		return pgexec.AwaitingFact, fmt.Errorf("original admission is not eligible for replay")
	}
	var step *journal.Step
	var finalized *journal.Step
	for i := range op.Steps {
		s := &op.Steps[i]
		if s.Action == journal.FinalizeBudget {
			finalized = s
		}
		if s.Phase == journal.Settled {
			if s.Observation.Result != journal.Completed && s.Observation.Result != journal.NoEffect {
				return pgexec.ExternalStep, nil
			}
			continue
		}
		step = s
		break
	}
	if step == nil || step.Action != journal.ResumeAdmission {
		if replay != nil {
			return pgexec.AwaitingFact, fmt.Errorf("no dispatched original admission")
		}
		return pgexec.ExternalStep, nil
	}
	if replay != nil && step.Phase != journal.Unconfirmed {
		return pgexec.AwaitingFact, fmt.Errorf("admission replay requires durable original dispatch")
	}
	if !snapshot.Dispatchable() || op.Request.Key.Instance != e.instance || finalized == nil || finalized.Phase != journal.Settled || finalized.Observation == nil || finalized.Observation.Result != journal.Completed || finalized.TargetID != e.instance.ComputeInstanceID {
		return pgexec.AwaitingFact, fmt.Errorf("admission requires original instance and finalized budget domain")
	}
	if step.Phase == journal.Planned && (op.AbortRequested || op.Origin != e.actor) {
		return pgexec.ExternalStep, nil
	}
	// 旧resize未知禁止新的开放；原准入只读对账仍可达，不能以本地另一pending
	// 隐藏已经发送的门禁结果。查询本身不清旧resize或释放预算。
	if (step.Phase == journal.Planned || replay != nil) && e.client.Pending() != nil {
		return pgexec.AwaitingFact, nil
	}
	var settled budget.Finalized
	// 类型自身的UnmarshalJSON以plain alias执行严格解码；不能把有该方法的
	// 指针直接传给UnmarshalObject，否则会被其递归解码防护拒绝。
	if json.Unmarshal(finalized.Observation.Data, &settled) != nil {
		return pgexec.AwaitingFact, fmt.Errorf("final budget receipt is incomplete")
	}
	fence, err := e.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if fence.Disposition != control.Fenced {
		return pgexec.AwaitingFact, nil
	}
	shared, err := e.client.SharedCost(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if shared.Observed.CapacityGeneration < fence.Observed.CapacityGeneration {
		return pgexec.AwaitingFact, fmt.Errorf("shared cost predates issuer fence")
	}
	vm := snapshot.VirtualMachine()
	if vm.Status.MemorySize == nil {
		return pgexec.AwaitingFact, fmt.Errorf("actual VM memory is unavailable")
	}
	vmMemory, exact := vm.Status.MemorySize.AsInt64()
	if !exact || vmMemory <= 0 || settled.ValidateFor(op.Request, shared, e.actor.Epoch, vm.Status.PodName, vm.Status.RestartCount, uint64(vmMemory)) != nil {
		return pgexec.AwaitingFact, fmt.Errorf("current full budget/PG/Runner/VM fact is not confirmed")
	}
	var request control.AdmissionRequest
	query := step.Phase != journal.Planned && replay == nil
	if step.Phase != journal.Planned {
		if json.Unmarshal(step.Command, &request) != nil || !request.Open || request.Instance != e.instance || request.BudgetCommitmentID != op.Request.BudgetCommitmentID || step.DispatchedBy == nil || request.IssuerEpoch != step.DispatchedBy.Epoch || request.ExpectedCapacityGeneration != shared.Observed.CapacityGeneration || step.TargetID != e.instance.ComputeInstanceID {
			return pgexec.AwaitingFact, fmt.Errorf("original admission command differs from durable operation")
		}
	} else {
		if shared.Observed.AdmissionRevision == control.Uint64(math.MaxUint64) {
			return pgexec.AwaitingFact, fmt.Errorf("admission revision exhausted")
		}
		request = control.AdmissionRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch, Revision: shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: shared.Observed.CapacityGeneration, Open: true, BudgetCommitmentID: op.Request.BudgetCommitmentID}
		pending, closing := e.client.PendingAdmission()
		if pending != nil || closing != nil {
			return pgexec.AwaitingFact, nil
		}
		command, _ := json.Marshal(request)
		var dispatch *journal.Dispatch
		snapshot, dispatch, err = e.store.Start(ctx, snapshot, e.actor, op.Request.Key, journal.ResumeAdmission, e.instance.ComputeInstanceID, command)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		step = &dispatch.Step
	}
	var fact control.AdmissionResponse
	if query {
		fact, err = e.client.AdmissionStatus(ctx, request)
	} else {
		fact, err = e.client.SetAdmission(ctx, request)
	}
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if !fact.Confirms() {
		return pgexec.AwaitingFact, nil
	}
	if fact.Observed.IssuerFloorEpoch != e.actor.Epoch || fact.Observed.CapacityGeneration != shared.Observed.CapacityGeneration || !fact.Observed.AdmissionOpen {
		return pgexec.AwaitingFact, fmt.Errorf("admission receipt was superseded")
	}
	// 同boot主段划分固定，但setter/查询返回的数组占用仍需再次核验，不能把
	// 发送前总额冒充返回时上界。若出现不一致，只保留原写入未知及预算，不能
	// 落成Completed/NoEffect；完整预算生产者仍必须串行化本操作并约束实际资源。
	returned := shared
	returned.Observed = fact.Observed
	total := uint64(returned.StartupMainUpperBytes)
	for _, array := range fact.Observed.Arrays {
		if uint64(array.CommittedUpperBytes) > math.MaxUint64-total {
			return pgexec.AwaitingFact, fmt.Errorf("admission returned allocation overflows")
		}
		total += uint64(array.CommittedUpperBytes)
	}
	returned.CurrentMainAndArraysUpperBytes = control.Uint64(total)
	if settled.ValidateFor(op.Request, returned, e.actor.Epoch, vm.Status.PodName, vm.Status.RestartCount, uint64(vmMemory)) != nil {
		return pgexec.AwaitingFact, fmt.Errorf("admission returned allocation exceeds retained full budget")
	}
	data, _ := json.Marshal(audit{ProtocolVersion: 1, Query: query, Fence: fence, Shared: shared, Response: fact})
	_, err = e.store.Record(ctx, snapshot, e.actor, op.Request.Key, journal.Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: journal.ResumeAdmission, Result: journal.Completed, SourceID: e.instance.ComputeInstanceID, Data: data})
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.Recorded, nil
}
