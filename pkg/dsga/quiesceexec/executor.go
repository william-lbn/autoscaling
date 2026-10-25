// Package quiesceexec将原账本Shrink/Quiesce接到真实关闭准入与同点工作观察。
// 采用持续PG聚合约束覆盖保留状态，不暂停检查点/必要后台，不声称全部内存排空。
package quiesceexec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

type Executor struct {
	store     *journal.Store
	pg        *compute.Client
	resources *resourceguard.Client
	actor     journal.Owner
	instance  control.Instance
	id        resourceguard.Identity
}

func New(store *journal.Store, pg *compute.Client, resources *resourceguard.Client, actor journal.Owner) (*Executor, error) {
	if store == nil || pg == nil || resources == nil || actor.Validate() != nil {
		return nil, fmt.Errorf("quiesce requires original journal/PG/root/owner")
	}
	instance, epoch := pg.Binding()
	id, resourceEpoch := resources.Binding()
	if epoch != actor.Epoch || resourceEpoch != actor.Epoch || id.VMUID != instance.VMUID || id.ComputeInstanceID != instance.ComputeInstanceID || string(store.Binding().VMUID) != id.VMUID {
		return nil, fmt.Errorf("quiesce domains differ")
	}
	return &Executor{store: store, pg: pg, resources: resources, actor: actor, instance: instance, id: id}, nil
}

func (e *Executor) Advance(ctx context.Context) (pgexec.Progress, error) { return e.advance(ctx, nil) }

// 显式重放只允许当前owner已持久的原关闭意图；新owner只查询旧意图，不能借
// 重放改epoch/revision。关门回复丢失也不自动重新POST，更不能改成新ID绕过未知。
func (e *Executor) ReplayOriginal(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if key.Validate() != nil {
		return pgexec.AwaitingFact, fmt.Errorf("invalid quiesce replay key")
	}
	return e.advance(ctx, &key)
}

func (e *Executor) advance(ctx context.Context, replay *control.Key) (pgexec.Progress, error) {
	snapshot, err := e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	state := snapshot.State()
	if state.Owner != e.actor || state.ResourceIdentity == nil || *state.ResourceIdentity != e.id {
		return pgexec.AwaitingFact, fmt.Errorf("quiesce original owner/resource changed")
	}
	if state.Active == nil {
		if replay != nil {
			return pgexec.AwaitingFact, fmt.Errorf("no original quiesce to replay")
		}
		return pgexec.Idle, nil
	}
	op := state.Active
	var step, preparedStep *journal.Step
	for i := range op.Steps {
		s := &op.Steps[i]
		if s.Phase == journal.Settled {
			if s.Observation == nil || s.Observation.Result != journal.Completed {
				return pgexec.ExternalStep, nil
			}
			if s.Action == journal.PrepareBudget {
				preparedStep = s
			}
			continue
		}
		step = s
		break
	}
	if step == nil || step.Action != journal.Quiesce {
		if replay != nil {
			return pgexec.AwaitingFact, fmt.Errorf("no dispatched original quiesce")
		}
		return pgexec.ExternalStep, nil
	}
	if !snapshot.Dispatchable() || op.Request.Kind != control.Shrink || op.Request.Key.Instance != e.instance || preparedStep == nil || preparedStep.TargetID != e.instance.ComputeInstanceID {
		return pgexec.AwaitingFact, fmt.Errorf("quiesce requires original shrink and prepared budget")
	}
	planned := step.Phase == journal.Planned
	if replay != nil && (*replay != op.Request.Key || planned || op.Origin != e.actor || op.AbortRequested) {
		return pgexec.AwaitingFact, fmt.Errorf("original quiesce replay is fenced")
	}
	if planned && (op.Origin != e.actor || op.AbortRequested) {
		return pgexec.ExternalStep, nil
	}
	var prepared budget.Prepared
	if json.Unmarshal(preparedStep.Observation.Data, &prepared) != nil || prepared.ValidateFor(op.Request, e.id) != nil {
		return pgexec.AwaitingFact, fmt.Errorf("quiesce prepared budget is incomplete")
	}
	var closeIntent control.AdmissionRequest
	if !planned {
		if json.Unmarshal(step.Command, &closeIntent) != nil || closeIntent.Open || closeIntent.Instance != e.instance || closeIntent.BudgetCommitmentID != op.Request.BudgetCommitmentID || closeIntent.ExpectedCapacityGeneration != op.Request.ExpectedCapacityGeneration || step.DispatchedBy == nil || closeIntent.IssuerEpoch != step.DispatchedBy.Epoch || closeIntent.IssuerEpoch > e.actor.Epoch || step.TargetID != e.instance.ComputeInstanceID {
			return pgexec.AwaitingFact, fmt.Errorf("original quiesce closing command differs")
		}
	}
	// 只读原命令查询先于资源证明；资源暂不可达不能使已经发送的关闭变成
	// NoEffect。SDK只解除同一准入Pending，外层Unconfirmed仍等待全部事实。
	var fact control.AdmissionResponse
	query := !planned && replay == nil
	if query {
		fact, err = e.pg.AdmissionStatus(ctx, closeIntent)
		if err != nil || !fact.Confirms() {
			return pgexec.AwaitingFact, err
		}
	}
	view, aggregate, err := e.aggregate(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	fence, err := e.pg.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if fence.Disposition != control.Fenced || fence.Observed.CapacityGeneration != op.Request.ExpectedCapacityGeneration {
		return pgexec.AwaitingFact, fmt.Errorf("quiesce native floor/generation changed")
	}
	if planned {
		pending, closing := e.pg.PendingAdmission()
		if pending != nil || closing != nil || e.pg.Pending() != nil || e.resources.Pending() != nil {
			return pgexec.AwaitingFact, fmt.Errorf("quiesce cannot bypass another unknown intent")
		}
		if fence.Observed.AdmissionRevision == control.Uint64(math.MaxUint64) {
			return pgexec.AwaitingFact, fmt.Errorf("closing revision exhausted")
		}
		closeIntent = control.AdmissionRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch, Revision: fence.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: op.Request.ExpectedCapacityGeneration, Open: false, BudgetCommitmentID: op.Request.BudgetCommitmentID}
		// 先核对本策略保留normal额度，不能在关门后才发现所选模式无法降限。
		limits, limitErr := view.ConfirmedLimits()
		if limitErr != nil || prepared.Limits.MemoryMaxBytes < limits.MemoryMaxBytes {
			return pgexec.AwaitingFact, fmt.Errorf("continuous aggregate quiesce cannot authorize memory.max decrease")
		}
		body, marshalErr := json.Marshal(closeIntent)
		if marshalErr != nil {
			return pgexec.AwaitingFact, marshalErr
		}
		var dispatch *journal.Dispatch
		snapshot, dispatch, err = e.store.Start(ctx, snapshot, e.actor, op.Request.Key, journal.Quiesce, e.instance.ComputeInstanceID, body)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		step = &dispatch.Step
		fact, err = e.pg.SetAdmission(ctx, closeIntent)
	} else if replay != nil {
		fact, err = e.pg.SetAdmission(ctx, closeIntent)
	}
	if err != nil || !fact.Confirms() {
		return pgexec.AwaitingFact, err
	}
	// Setter成功不等于Postmaster已ack或前台排空。DSM正式成员内嵌同点的
	// Connection/Prepared/Work；2PC/后台/DSM保留成本由未放宽的原聚合来源覆盖。
	dsm, err := e.pg.DSMState(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	result := budget.Quiesced{ProtocolVersion: 1, Request: op.Request, Close: closeIntent, DSM: dsm, PGAggregate: aggregate}
	if err := result.ValidateFor(prepared, e.actor.Epoch); err != nil {
		return pgexec.AwaitingFact, err
	}
	latest, _, err := e.aggregate(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := aggregate.Match(latest); err != nil {
		return pgexec.AwaitingFact, err
	}
	// 跨系统不假装原子采样；fresh VM读取和最终CAS防Runner/原派发漂移。
	// 后续ResizePG仍须再次核验同一原来源与原关闭，不复用一次成功作永久许可。
	snapshot, err = e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	current := snapshot.State()
	vm := snapshot.VirtualMachine()
	if current.Owner != e.actor || current.Active == nil || current.Active.Request != op.Request || current.ResourceIdentity == nil || *current.ResourceIdentity != e.id || vm.Status.PodName != prepared.RunnerPodName || vm.Status.RestartCount != prepared.RunnerRestartCount || vm.Status.MemorySize == nil {
		return pgexec.AwaitingFact, fmt.Errorf("quiesce original Runner/operation changed")
	}
	memory, exact := vm.Status.MemorySize.AsInt64()
	if !exact || memory <= 0 || uint64(memory) != uint64(prepared.VMBeforeMemoryBytes) {
		return pgexec.AwaitingFact, fmt.Errorf("quiesce VM changed before PG drain")
	}
	currentStep := current.Active.Steps[step.Ordinal-1]
	if currentStep.Action != journal.Quiesce || currentStep.Phase != journal.Unconfirmed || currentStep.CommandSHA256 != step.CommandSHA256 || !bytes.Equal(currentStep.Command, step.Command) {
		return pgexec.AwaitingFact, fmt.Errorf("original quiesce dispatch changed")
	}
	data, err := json.Marshal(result)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	_, err = e.store.Record(ctx, snapshot, e.actor, op.Request.Key, journal.Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: journal.Quiesce, Result: journal.Completed, SourceID: e.instance.ComputeInstanceID, Data: data})
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.Recorded, nil
}

func (e *Executor) aggregate(ctx context.Context) (resourceguard.View, budget.RootAggregate, error) {
	v, err := e.resources.InspectState(ctx)
	if err != nil {
		return v, budget.RootAggregate{}, err
	}
	if v.IssuerFloorEpoch != e.actor.Epoch {
		return v, budget.RootAggregate{}, fmt.Errorf("quiesce resource owner was superseded")
	}
	process, err := e.resources.InspectProcess(ctx)
	if err != nil || v.Birth == nil || process != *v.Birth {
		return v, budget.RootAggregate{}, fmt.Errorf("quiesce original Postmaster birth changed")
	}
	a, err := budget.RootAggregateFromView(v)
	return v, a, err
}
