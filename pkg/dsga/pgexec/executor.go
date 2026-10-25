// Package pgexec 将持久journal的PG步骤接到正式Compute客户端。
// 它只负责PG逻辑容量和Provider事实，不认证owner租约、不执行预算/VM动作；
// 调用方须来自可信协调器。其余步骤必须由各领域适配器核验，不能自动跳过。
package pgexec

import (
	"context"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

type Progress string

const (
	Idle         Progress = "idle"
	ExternalStep Progress = "external_step"
	AwaitingFact Progress = "awaiting_fact"
	Recorded     Progress = "recorded"
	Replayed     Progress = "replayed"
	// 只解除SDK相同原终态的本地Pending，不代表写journal或任何资源完成。
	Reconciled Progress = "reconciled"
)

// Executor固定真实Store、HTTPS客户端、进程owner及boot；没有可注入的“已完成”
// 回调。重建必须显式恢复完整日志/Client.Pending，不能靠换空Client解决未知动作。
// 本对象不持跨网络本地锁：写前/写后以固定UID/RV的apiserver CAS线性化，同owner
// 并发Advance也只有一个Start获票据；查询重复不产生resize副作用。
type Executor struct {
	store     *journal.Store
	client    *compute.Client
	actor     journal.Owner
	instance  control.Instance
	resources *resourceguard.Client
}

type executionAudit struct {
	ProtocolVersion uint32                  `json:"protocol_version"`
	Fence           control.FenceResponse   `json:"issuer_fence"`
	Response        control.ControlResponse `json:"response"`
}

// ReplayOriginal由可信协调器显式调用，Advance从不自动调用。适用于写前记录
// 已持久但原回复丢失，或PG事实已落账后需解除SDK本地pending。只重放本owner/
// 同boot下已经记录的完整原命令，利用PG完整Key/context幂等协议；新owner、取消、
// 尚未Start或VM正在删除均拒绝。它不会重放VM/QMP动作，也不清外层预算/准入状态。
func (executor *Executor) ReplayOriginal(ctx context.Context) (Progress, error) {
	snapshot, err := executor.store.Load(ctx)
	if err != nil {
		return AwaitingFact, err
	}
	state := snapshot.State()
	if !snapshot.Dispatchable() || state.Owner != executor.actor || state.Active == nil ||
		state.Active.Origin != executor.actor || state.Active.AbortRequested ||
		state.Active.Request.Key.Instance != executor.instance {
		return AwaitingFact, fmt.Errorf("original PG intent is not eligible for explicit replay")
	}
	intent := state.Active.Request
	var step *journal.Step
	for i := range state.Active.Steps {
		if state.Active.Steps[i].Action == journal.ResizePG {
			step = &state.Active.Steps[i]
			break
		}
	}
	if step == nil || step.Phase == journal.Planned {
		return AwaitingFact, fmt.Errorf("original PG intent has no durable dispatch record")
	}
	command, err := control.DecodeRequest(step.Command)
	if err != nil || command != intent || step.TargetID != executor.instance.ComputeInstanceID {
		return AwaitingFact, fmt.Errorf("original PG replay command changed")
	}
	var previousPhase control.OperationPhase
	if step.Phase == journal.Settled {
		// Busy/期限拒绝没有原生终态，之后重发可能变成全新执行，绝不能把
		// 已落账Rejected改成成功。只有完整原生终态的幂等重放可解除SDK栅栏。
		var saved executionAudit
		if step.Observation == nil || control.UnmarshalObject(step.Observation.Data, &saved) != nil ||
			saved.ProtocolVersion != 1 || saved.Response.ValidateResize(intent, executor.instance) != nil || saved.Response.Receipt.Result == nil {
			return AwaitingFact, fmt.Errorf("durable PG outcome has no replayable native terminal receipt")
		}
		previousPhase = saved.Response.Receipt.Result.Operation.Phase
		replayableTerminal := (step.Observation.Result == journal.Completed && previousPhase == control.Committed) ||
			(step.Observation.Result == journal.Rejected && previousPhase == control.Denied)
		if !replayableTerminal {
			return AwaitingFact, fmt.Errorf("durable PG outcome cannot be replayed")
		}
	}
	if err := executor.checkResources(ctx); err != nil {
		return AwaitingFact, err
	}
	if intent.Kind == control.Shrink && step.Phase == journal.Unconfirmed && executor.resources != nil {
		if err := executor.checkQuiesce(ctx, state.Active); err != nil {
			return AwaitingFact, err
		}
	}
	fence, err := executor.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: executor.instance, IssuerEpoch: executor.actor.Epoch})
	if err != nil {
		return AwaitingFact, err
	}
	if fence.Disposition != control.Fenced {
		return AwaitingFact, nil
	}
	// 只有调用者选择显式重放才重新POST。任何错误仍保留原journal；PG返回
	// Stale/Conflict/Expired等缺结果状态不能据此关闭旧操作或重建空Client。
	fact, err := executor.client.Resize(ctx, intent)
	if err != nil {
		return AwaitingFact, err
	}
	if step.Phase == journal.Unconfirmed {
		return executor.record(ctx, snapshot, intent, *step, fence, fact, true)
	}
	if err := fact.ValidateResize(intent, executor.instance); err != nil {
		return AwaitingFact, err
	}
	if fact.Observed.IssuerFloorEpoch != executor.actor.Epoch || fact.Observed.CapacityGeneration < fence.Observed.CapacityGeneration || fact.Receipt.Result == nil {
		return AwaitingFact, fmt.Errorf("original PG terminal replay is not confirmed")
	}
	phase := fact.Receipt.Result.Operation.Phase
	if phase != previousPhase {
		return AwaitingFact, fmt.Errorf("original PG terminal replay contradicts durable outcome")
	}
	// 原终态不可重写为这次不同时间的observed快照。这里只确认SDK幂等重放，
	// 不更新历史、不关闭整个操作；后续资源步骤仍按原journal逐项核验。
	return Replayed, nil
}

func New(store *journal.Store, client *compute.Client, actor journal.Owner, resources ...*resourceguard.Client) (*Executor, error) {
	if store == nil || client == nil || actor.Validate() != nil {
		return nil, fmt.Errorf("PG executor requires a journal, client and fixed owner")
	}
	instance, epoch := client.Binding()
	if actor.Epoch != epoch {
		return nil, fmt.Errorf("PG executor owner differs from authenticated client epoch")
	}
	var resource *resourceguard.Client
	if len(resources) > 1 {
		return nil, fmt.Errorf("one original resource client required")
	}
	if len(resources) == 1 {
		resource = resources[0]
		if resource == nil {
			return nil, fmt.Errorf("nil original resource client")
		}
		id, resourceEpoch := resource.Binding()
		if id.VMUID != instance.VMUID || id.ComputeInstanceID != instance.ComputeInstanceID || resourceEpoch != actor.Epoch {
			return nil, fmt.Errorf("PG and resource domains differ")
		}
	}
	return &Executor{store: store, client: client, actor: actor, instance: instance, resources: resource}, nil
}

// Advance每次重读持久状态，仅推进一个PG步骤。返回AwaitingFact表示副作用仍
// 未查明，不是NoEffect/取消成功；调用方有界退避后再查询，禁止自动重发POST。
// ExternalStep保留原状态，交预算/Host/Guest/VM/清理适配器处理，不能作为成功跳步。
func (executor *Executor) Advance(ctx context.Context) (Progress, error) {
	snapshot, err := executor.store.Load(ctx)
	if err != nil {
		return AwaitingFact, err
	}
	state := snapshot.State()
	if state.Owner != executor.actor || state.VMUID != executor.instance.VMUID {
		return AwaitingFact, fmt.Errorf("PG executor journal owner or VM changed")
	}
	if state.Active == nil {
		return Idle, nil
	}
	operation := state.Active
	if reconciled, err := executor.reconcileSettled(ctx, operation); err != nil {
		return AwaitingFact, err
	} else if reconciled {
		return Reconciled, nil
	}
	var step *journal.Step
	for i := range operation.Steps {
		candidate := &operation.Steps[i]
		if candidate.Phase == journal.Settled {
			if candidate.Observation.Result == journal.Rejected {
				return ExternalStep, nil
			}
			continue
		}
		step = candidate
		break
	}
	if step == nil || (step.Action != journal.ResizePG && step.Action != journal.ReleasePG) {
		return ExternalStep, nil
	}
	intent := operation.Request
	if intent.Key.Instance != executor.instance {
		// PG换boot不等于外层旧VM效果消失。这里只冻结，不能改写原请求或将
		// 新boot当前容量借给旧receipt；显式跨boot恢复由上层独立完成。
		return AwaitingFact, fmt.Errorf("PG executor cannot silently rebind an old boot")
	}
	if step.Phase == journal.Planned && (operation.AbortRequested || operation.Origin != executor.actor) {
		return ExternalStep, nil
	}
	if step.Phase == journal.Unconfirmed {
		decoded, err := control.DecodeRequest(step.Command)
		if err != nil || decoded != intent || step.TargetID != executor.instance.ComputeInstanceID {
			return AwaitingFact, fmt.Errorf("PG step command differs from complete original intent")
		}
	}
	if step.Phase == journal.Planned {
		if err := executor.checkResources(ctx); err != nil {
			return AwaitingFact, err
		}
		if intent.Kind == control.Shrink && step.Action == journal.ResizePG && executor.resources != nil {
			if err := executor.checkQuiesce(ctx, operation); err != nil {
				return AwaitingFact, err
			}
		}
	}

	// floor先与签名epoch绑定。它只隔离PG执行，不清账本/预算。旧操作已登记
	// 后允许查询/取消，即使接管Pending也不能抹去其COMMITTED/Provider事实。
	fence, err := executor.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: executor.instance, IssuerEpoch: executor.actor.Epoch})
	if err != nil {
		return AwaitingFact, err
	}
	if fence.Disposition == control.FenceStale {
		return AwaitingFact, fmt.Errorf("PG issuer fence was superseded")
	}
	if step.Phase == journal.Planned {
		if fence.Disposition != control.Fenced {
			return AwaitingFact, nil
		}
		command, err := json.Marshal(intent)
		if err != nil {
			return AwaitingFact, err
		}
		// 必须确认Start写前提交才调用Resize。写成后丢回复也没有派发票据；
		// 下一次只查询原意图，不能以本地构造的Dispatch补发。
		var dispatch *journal.Dispatch
		snapshot, dispatch, err = executor.store.Start(ctx, snapshot, executor.actor, intent.Key, step.Action, executor.instance.ComputeInstanceID, command)
		if err != nil {
			return AwaitingFact, err
		}
		step = &dispatch.Step
		if step.Action == journal.ResizePG {
			fact, err := executor.client.Resize(ctx, intent)
			if err != nil {
				return AwaitingFact, err
			}
			return executor.record(ctx, snapshot, intent, *step, fence, fact, true)
		}
	}

	// Unconfirmed绝不透明重发Resize。取消也只请求业务取消并核验回执，
	// 超时/TooLate/NotFound本身不允许删除未知动作或撤销预算。
	query := control.Query{ProtocolVersion: 1, Key: intent.Key}
	var fact control.ControlResponse
	if step.Action == journal.ResizePG && (operation.AbortRequested || operation.Origin != executor.actor) {
		fact, err = executor.client.Cancel(ctx, query)
	} else {
		fact, err = executor.client.Status(ctx, query)
	}
	if err != nil {
		return AwaitingFact, err
	}
	return executor.record(ctx, snapshot, intent, *step, fence, fact, false)
}

// checkQuiesce只在新Shrink派发前消费同账本的原完整Quiesce事实，并重读原
// root/PG。仅有历史门禁bool、前台idle或任意Completed对象不能推进。未知PG
// 已派发时仍可只读对账，不因另一领域不可达隐藏旧结果。
func (executor *Executor) checkQuiesce(ctx context.Context, op *journal.Operation) error {
	var prepared budget.Prepared
	var quiesced budget.Quiesced
	for _, s := range op.Steps {
		if s.Action != journal.PrepareBudget && s.Action != journal.Quiesce {
			continue
		}
		if s.Phase != journal.Settled || s.Observation == nil || s.Observation.Result != journal.Completed || s.TargetID != executor.instance.ComputeInstanceID {
			return fmt.Errorf("shrink lacks completed original budget/quiesce")
		}
		var err error
		if s.Action == journal.PrepareBudget {
			err = json.Unmarshal(s.Observation.Data, &prepared)
		} else {
			err = json.Unmarshal(s.Observation.Data, &quiesced)
		}
		if err != nil {
			return fmt.Errorf("shrink budget/quiesce source is incomplete")
		}
	}
	if quiesced.Request != op.Request || quiesced.ValidateFor(prepared, executor.actor.Epoch) != nil {
		return fmt.Errorf("shrink quiesce differs from retained original budget")
	}
	view, err := executor.resources.InspectState(ctx)
	if err != nil {
		return err
	}
	if err := quiesced.PGAggregate.Match(view); err != nil {
		return err
	}
	process, err := executor.resources.InspectProcess(ctx)
	if err != nil || process != quiesced.PGAggregate.Birth {
		return fmt.Errorf("shrink original Postmaster birth changed")
	}
	dsm, err := executor.client.DSMState(ctx)
	if err != nil {
		return err
	}
	// 前台状态会变化，重新核验而不把旧快照覆盖历史审计。2PC/DSM/背景保留
	// 计数不要求清0；原聚合限额持续持有，不能据此压低normal上限。
	latest := quiesced
	latest.DSM = dsm
	return latest.ValidateFor(prepared, executor.actor.Epoch)
}

// 新PG POST的最低门槛，不能替代PrepareBudget/Host/Guest/排空的完整证明。
// 观察未知PG结果不经过此门槛，避免root不确定时连原查询也被阻塞；显式重放
// 仍可能形成新效果，所以同样必须核对当前资源floor和持续存在的已确认约束。
func (executor *Executor) checkResources(ctx context.Context) error {
	if executor.resources == nil {
		return nil
	}
	if executor.resources.Pending() != nil {
		return resourceguard.ErrUnknown
	}
	view, err := executor.resources.InspectState(ctx)
	if err != nil {
		return err
	}
	if view.IssuerFloorEpoch != executor.actor.Epoch {
		return fmt.Errorf("resource owner floor differs before PG dispatch")
	}
	_, err = view.ConfirmedLimits()
	return err
}

func (executor *Executor) record(ctx context.Context, snapshot *journal.Snapshot, intent control.Request, step journal.Step, fence control.FenceResponse, fact control.ControlResponse, fromResize bool) (Progress, error) {
	// Status/Cancel客户端只按Key核验；这里必须再重算完整PG context摘要，
	// 防止同Key但VM目标/预算/期限不同的回执被写成持久完成事实。
	if err := fact.ValidateResize(intent, executor.instance); err != nil {
		return AwaitingFact, err
	}
	// 当前观察不得比刚取得的同boot隔离事实倒退；历史receipt自身可更旧。
	// 更高floor表示本执行者已被替代，即使journal尚未观察到也停止写后推进。
	if fact.Observed.IssuerFloorEpoch != executor.actor.Epoch || fact.Observed.CapacityGeneration < fence.Observed.CapacityGeneration {
		return AwaitingFact, fmt.Errorf("PG observation predates the fence or issuer was superseded")
	}
	result := journal.Completed
	if step.Action == journal.ResizePG {
		if fact.Receipt.Result == nil {
			if !fromResize || (fact.Receipt.Disposition != control.ReceiptBusy && fact.Receipt.Disposition != control.ReceiptRejected) {
				return AwaitingFact, nil
			}
			// 只有本次执行明确拒绝可记Rejected；NotFound/Expired/Stale/Conflict
			// 不能推断原效果不存在。Rejected仍须外层预算/准入清理，不是NoEffect。
			result = journal.Rejected
		} else {
			switch fact.Receipt.Result.Operation.Phase {
			case control.Preparing:
				return AwaitingFact, nil
			case control.Denied:
				result = journal.Rejected
			case control.Committed:
				// 这里只记逻辑容量提交。即使Provider仍在途也绝不把下一步
				// ReleasePG/Guest/VM动作视为完成，成本和未知区间保留原回执。
			default:
				return AwaitingFact, fmt.Errorf("unknown PG operation phase")
			}
		}
	} else {
		if !releaseCurrent(intent, fence, fact) {
			return AwaitingFact, nil
		}
		if !releaseVerified(intent, fence, fact) {
			// PG串行执行者已确认退出且仍是本操作，Provider不会靠反复查询
			// 自行重试。unsupported/失败/工作区保留记Rejected，要求外层按
			// 原成本做预算与准入清理；不是NoEffect，也不销掉未知分配成本。
			result = journal.Rejected
		}
	}
	audit, err := json.Marshal(executionAudit{ProtocolVersion: 1, Fence: fence, Response: fact})
	if err != nil {
		return AwaitingFact, err
	}
	// 保存两个完整原回执而非截断/拼接事实；超出既定32KiB审计预算则Record
	// 拒绝并保持Unconfirmed，不能为了腾空间删除活动意图或其原预算。
	_, err = executor.store.Record(ctx, snapshot, executor.actor, intent.Key, journal.Observation{
		Key: intent.Key, RequestSHA256: snapshot.State().Active.RequestSHA256,
		CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: step.Action,
		Result: result, SourceID: step.TargetID, Data: audit,
	})
	if err != nil {
		return AwaitingFact, err
	}
	return Recorded, nil
}

// releaseVerified只核验当前同一操作/容量下的四数组Provider释放；不能把历史
// 释放回执与regrow后的当前分配拼接。PG事实仍不构成Guest/Host或VM缩容许可。
// 此处false不表示NoEffect；是否可持久记Rejected还须releaseCurrent证明旧PG
// 执行者已退出且观察仍属于本操作。原成本始终保留在完整回执与预算中。
func releaseVerified(intent control.Request, fence control.FenceResponse, fact control.ControlResponse) bool {
	if !releaseCurrent(intent, fence, fact) {
		return false
	}
	observed := fact.Observed
	for _, array := range observed.Arrays {
		p := array.Provider
		if array.AllocationUnknown || array.ReleaseState != control.ReleaseVerified || array.ReleaseKey == nil ||
			*array.ReleaseKey != intent.Key || array.ReleaseCapacityGeneration != observed.CapacityGeneration ||
			!p.HasResult || p.AllocationMayBePartial || p.ReleaseMayBePartial || p.SystemErrno != 0 {
			return false
		}
		if p.Status == control.ProviderNoop && p.RequestedBytes == 0 && p.AffectedBytes == 0 {
			continue
		}
		if p.Status != control.ProviderOK || !p.PhysicalReleaseVerified || p.RequestedOffset != p.AffectedOffset ||
			p.RequestedBytes == 0 || p.RequestedBytes != p.AffectedBytes {
			return false
		}
	}
	return true
}

func releaseCurrent(intent control.Request, fence control.FenceResponse, fact control.ControlResponse) bool {
	observed := fact.Observed
	hash, err := intent.ContextSHA256()
	if intent.Kind != control.Shrink || fence.Disposition != control.Fenced || fact.Receipt.Result == nil ||
		fact.Receipt.Result.Operation.Phase != control.Committed || observed.Operation == nil ||
		observed.Operation.Key != intent.Key || observed.Operation.Phase != control.Committed ||
		err != nil || observed.Operation.ContextSHA256 != hash || observed.Operation.Kind != intent.Kind ||
		observed.Operation.ExpectedCapacityGeneration != intent.ExpectedCapacityGeneration ||
		observed.HighBuffers != intent.TargetBuffers || observed.CapacityGeneration != fact.Receipt.Result.CapacityGeneration ||
		observed.IssuerFloorEpoch != fence.RequestedEpoch {
		return false
	}
	return true
}
