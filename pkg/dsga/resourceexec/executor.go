// Package resourceexec将原VM账本GuestReady步骤接到独立Guest资源执行器。
// 本域确认目标硬约束并生产PG聚合上界来源；完整外部预算/Host/VM/准入仍分别核验。
package resourceexec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"

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

// Audit保留实际GuestReady完整来源供后续结算核验；JSON格式与既有v2相同。
// 导出不提供创建Completed的权限，消费者仍必须来自原可信账本和认证root复核。
type Audit struct {
	ProtocolVersion uint32                     `json:"protocol_version"`
	Query           bool                       `json:"query"`
	Command         resourceguard.Command      `json:"command"`
	Evidence        resourceguard.Evidence     `json:"evidence"`
	Fence           control.FenceResponse      `json:"issuer_fence"`
	Shared          control.SharedCostResponse `json:"shared"`
	PGAggregate     budget.RootAggregate       `json:"pg_aggregate"`
	RunnerPodName   string                     `json:"runner_pod_name"`
	RunnerRestarts  int32                      `json:"runner_restarts"`
	VMMemoryBytes   control.Uint64             `json:"vm_memory_bytes"`
}

// 保留包内名称，避免改变原写入流程或历史序列化字段。
type audit = Audit

func (v *Audit) UnmarshalJSON(b []byte) error {
	type plain Audit
	var p plain
	if err := control.UnmarshalObject(b, &p); err != nil {
		return err
	}
	*v = Audit(p)
	return nil
}

func New(store *journal.Store, pg *compute.Client, resources *resourceguard.Client, actor journal.Owner) (*Executor, error) {
	if store == nil || pg == nil || resources == nil || actor.Validate() != nil {
		return nil, fmt.Errorf("resource executor requires original journal/PG/Guest/owner")
	}
	instance, pgEpoch := pg.Binding()
	id, resourceEpoch := resources.Binding()
	if actor.Epoch != pgEpoch || actor.Epoch != resourceEpoch || id.VMUID != instance.VMUID || id.ComputeInstanceID != instance.ComputeInstanceID || string(store.Binding().VMUID) != instance.VMUID {
		return nil, fmt.Errorf("resource executor domains differ")
	}
	return &Executor{store: store, pg: pg, resources: resources, actor: actor, instance: instance, id: id}, nil
}

func (e *Executor) Advance(ctx context.Context) (pgexec.Progress, error) { return e.advance(ctx, nil) }

// 显式重放独立于PG/准入重放。Key只作选择提示，发送值始终取自原WAL；新owner
// 只能Observe旧epoch，不能借重放修改epoch/PID/限额。不存在定时自动重放入口。
func (e *Executor) ReplayOriginal(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if key.Validate() != nil {
		return pgexec.AwaitingFact, fmt.Errorf("invalid resource replay key")
	}
	return e.advance(ctx, &key)
}

func guestStep(op *journal.Operation) (*journal.Step, *journal.Step) {
	var prepared *journal.Step
	for i := range op.Steps {
		s := &op.Steps[i]
		if s.Phase == journal.Settled {
			// 本领域不能从NoEffect/Rejected预算或前驱拼出成功路径。
			if s.Observation == nil || s.Observation.Result != journal.Completed {
				return nil, nil
			}
			if s.Action == journal.PrepareBudget {
				prepared = s
			}
			continue
		}
		if s.Action == journal.GuestReady {
			return s, prepared
		}
		break
	}
	return nil, nil
}

func (e *Executor) advance(ctx context.Context, replay *control.Key) (pgexec.Progress, error) {
	snapshot, err := e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	state := snapshot.State()
	if state.Owner != e.actor || state.ResourceIdentity == nil || *state.ResourceIdentity != e.id {
		return pgexec.AwaitingFact, fmt.Errorf("resource executor owner/original Guest changed")
	}
	if state.Active == nil {
		if replay != nil {
			return pgexec.AwaitingFact, fmt.Errorf("no active original resource intent")
		}
		return pgexec.Idle, nil
	}
	op := state.Active
	step, source := guestStep(op)
	if step == nil {
		if replay != nil {
			return pgexec.AwaitingFact, fmt.Errorf("no pending original GuestReady")
		}
		return pgexec.ExternalStep, nil
	}
	if op.Request.Key.Instance != e.instance || source == nil || source.TargetID != e.instance.ComputeInstanceID || !snapshot.Dispatchable() {
		return pgexec.AwaitingFact, fmt.Errorf("GuestReady requires original instance and prepared budget")
	}
	if replay != nil && (*replay != op.Request.Key || step.Phase != journal.Unconfirmed || op.Origin != e.actor || op.AbortRequested) {
		return pgexec.AwaitingFact, fmt.Errorf("original resource intent is not eligible for replay")
	}
	if step.Phase == journal.Planned && (op.Origin != e.actor || op.AbortRequested) {
		return pgexec.ExternalStep, nil
	}
	var prepared budget.Prepared
	if json.Unmarshal(source.Observation.Data, &prepared) != nil || prepared.ValidateFor(op.Request, e.id) != nil {
		return pgexec.AwaitingFact, fmt.Errorf("original prepared budget is incomplete")
	}
	view, err := e.resources.InspectState(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if view.IssuerFloorEpoch != e.actor.Epoch {
		return pgexec.AwaitingFact, fmt.Errorf("resource floor differs from current owner")
	}
	if op.Request.Kind == control.Shrink && (step.Phase == journal.Planned || replay != nil) {
		// 未确认Original仍须保留旧Held/Bootstrap的normal额度；显式重放也
		// 不能越过安全降限缺口。只读原Observe保持可达，不把旧效果藏起来。
		held := view.Bootstrap.Limits.MemoryMaxBytes
		if view.Held != nil {
			held = view.Held.MemoryMaxBytes
		}
		if prepared.Limits.MemoryMaxBytes < held {
			return pgexec.AwaitingFact, fmt.Errorf("continuous aggregate cannot authorize memory.max decrease")
		}
	}
	var command resourceguard.Command
	planned := step.Phase == journal.Planned
	if !planned {
		if json.Unmarshal(step.Command, &command) != nil || command.Validate() != nil || step.DispatchedBy == nil || command.IssuerEpoch != step.DispatchedBy.Epoch || command.IssuerEpoch > e.actor.Epoch || step.TargetID != e.instance.ComputeInstanceID || !commandMatches(command, prepared) {
			return pgexec.AwaitingFact, fmt.Errorf("GuestReady command differs from complete original budget")
		}
		if err := e.resources.RetainOriginal(command); err != nil {
			return pgexec.AwaitingFact, err
		}
	}
	var evidence resourceguard.Evidence
	query := !planned && replay == nil
	if query {
		// 必须先观察原root意图，即使PG暂不可达也不能自动重发或替换PID。
		// Observe成功只解除SDK相同Pending；下面全套预算/PG/VM检查尚未成功
		// 时外层仍保留Unconfirmed，不会提前落成GuestReady或VM完成。
		evidence, err = e.resources.Observe(ctx, command)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
	}
	fence, shared, err := e.native(ctx, prepared)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if _, err := checkVM(snapshot, prepared); err != nil {
		return pgexec.AwaitingFact, err
	}
	if planned {
		if e.resources.Pending() != nil {
			return pgexec.AwaitingFact, resourceguard.ErrUnknown
		}
		if _, err := view.ConfirmedLimits(); err != nil {
			return pgexec.AwaitingFact, err
		}
		if err := checkGuest(view, prepared); err != nil {
			return pgexec.AwaitingFact, err
		}
		process, processErr := e.resources.InspectProcess(ctx)
		if processErr != nil {
			return pgexec.AwaitingFact, processErr
		}
		if view.Birth == nil || process != *view.Birth {
			return pgexec.AwaitingFact, fmt.Errorf("original Postmaster birth changed")
		}
		revision := control.Uint64(1)
		if view.Original != nil {
			if view.Original.Revision == control.Uint64(math.MaxUint64) {
				return pgexec.AwaitingFact, fmt.Errorf("resource revision exhausted")
			}
			revision = view.Original.Revision + 1
		}
		// 独立资源代次取自原root WAL，绝不借PG revision/gen生成。完整命令
		// 先经原VM UID/RV CAS持久化；丢回复没有票据，绝不调用下面Apply。
		command = resourceguard.Command{ProtocolVersion: 1, Identity: e.id, IssuerEpoch: e.actor.Epoch, Revision: revision, BudgetCommitmentID: op.Request.BudgetCommitmentID, PostmasterPID: process.PostmasterPID, PostmasterStartTicks: process.PostmasterStartTicks, MemoryMaxBytes: prepared.Limits.MemoryMaxBytes, PidsMaxTasks: prepared.Limits.PidsMaxTasks, SystemMemoryMaxBytes: prepared.Limits.SystemMemoryMaxBytes, OutsideUpperBytes: prepared.Limits.OutsideUpperBytes, HugePools: append([]resourceguard.HugePool(nil), prepared.Limits.HugePools...)}
		body, marshalErr := json.Marshal(command)
		if marshalErr != nil || command.Validate() != nil {
			return pgexec.AwaitingFact, fmt.Errorf("invalid complete resource command")
		}
		// Start及Apply必须写入外层err，供下方统一处理。上面的出生查询和
		// 序列化分别用processErr/marshalErr；任一处用“新变量, err :=”都会
		// 在本分支遮蔽外层err，使真实发送失败被零值Evidence掩盖为预算
		// 不符。未知仍须保留原WAL/SDK Pending，并把准确ClientError交给
		// 协调器决定只读恢复，不能因此将本次动作落成成功或自动重发。
		var dispatch *journal.Dispatch
		snapshot, dispatch, err = e.store.Start(ctx, snapshot, e.actor, op.Request.Key, journal.GuestReady, e.instance.ComputeInstanceID, body)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		step = &dispatch.Step
		evidence, err = e.resources.Apply(ctx, command)
	} else if replay != nil {
		if err := checkGuest(view, prepared); err != nil {
			return pgexec.AwaitingFact, err
		}
		// 两种显式恢复都保留完整原意图。root保留同命令则原样Replay；若
		// root尚未收到，Deliver发固定Apply并由root锁内再校验连续代次。
		// 此分支不推导NoEffect；旧epoch/未知另一命令仍由最终root拒绝。
		if view.Original != nil && reflect.DeepEqual(*view.Original, command) {
			evidence, err = e.resources.ReplayOriginal(ctx, command)
		} else {
			evidence, err = e.resources.DeliverOriginal(ctx, command)
		}
	}
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if evidence.ValidateFor(command) != nil || evidence.GuestMemTotalBytes < prepared.GuestUsableMemoryBytes {
		return pgexec.AwaitingFact, fmt.Errorf("resource response cannot confirm original target Guest budget")
	}
	// 写后重新读PG和root，不能拿写前PG采样配写后资源上限拼出完成。root
	// 当前Original/Held/出生身份必须仍是本次完整命令，任何新代次/漂移均冻结。
	fence, shared, err = e.native(ctx, prepared)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	view, err = e.resources.InspectState(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if view.IssuerFloorEpoch != e.actor.Epoch || view.Original == nil || !reflect.DeepEqual(*view.Original, command) || view.Held == nil || !reflect.DeepEqual(*view.Held, command) || !view.Applied || checkGuest(view, prepared) != nil || view.Birth.PostmasterPID != command.PostmasterPID || view.Birth.PostmasterStartTicks != command.PostmasterStartTicks {
		return pgexec.AwaitingFact, fmt.Errorf("resource original completion was superseded")
	}
	if _, err := view.ConfirmedLimits(); err != nil {
		return pgexec.AwaitingFact, err
	}
	// fresh Store读取将PG/root核验后出现的VM/Runner漂移也纳入最后CAS。只
	// 接受同一Unconfirmed命令；其他执行者已落账时不覆盖/重写其审计事实。
	snapshot, err = e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	current := snapshot.State()
	if current.Owner != e.actor || current.Active == nil || current.Active.Request != op.Request || current.ResourceIdentity == nil || *current.ResourceIdentity != e.id {
		return pgexec.AwaitingFact, fmt.Errorf("resource operation changed before completion CAS")
	}
	currentStep, _ := guestStep(current.Active)
	if currentStep == nil || currentStep.Phase != journal.Unconfirmed || currentStep.CommandSHA256 != step.CommandSHA256 || !bytes.Equal(currentStep.Command, step.Command) {
		return pgexec.AwaitingFact, fmt.Errorf("original resource dispatch changed before completion")
	}
	vmBytes, err := checkVM(snapshot, prepared)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	// 将本次已核验root采样的聚合限额来源随GuestReady一起落账；这不是跨root/
	// Kube/PG的原子快照，后续预算动作仍须重新Match认证View，不能缓存成永久
	// 许可。不从声明的Outside或瞬时占用补系统预算；旧审计v1也不升级成新证书。
	aggregate, err := budget.RootAggregateFromView(view)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	data, err := json.Marshal(audit{ProtocolVersion: 2, Query: query, Command: command, Evidence: view.Actual, Fence: fence, Shared: shared, PGAggregate: aggregate, RunnerPodName: prepared.RunnerPodName, RunnerRestarts: prepared.RunnerRestartCount, VMMemoryBytes: control.Uint64(vmBytes)})
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	_, err = e.store.Record(ctx, snapshot, e.actor, op.Request.Key, journal.Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: journal.GuestReady, Result: journal.Completed, SourceID: step.TargetID, Data: data})
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.Recorded, nil
}

func commandMatches(c resourceguard.Command, p budget.Prepared) bool {
	return c.Identity == p.Identity && c.BudgetCommitmentID == p.Request.BudgetCommitmentID && c.MemoryMaxBytes == p.Limits.MemoryMaxBytes && c.PidsMaxTasks == p.Limits.PidsMaxTasks && c.SystemMemoryMaxBytes == p.Limits.SystemMemoryMaxBytes && c.OutsideUpperBytes == p.Limits.OutsideUpperBytes && reflect.DeepEqual(c.HugePools, p.Limits.HugePools)
}

func checkGuest(v resourceguard.View, p budget.Prepared) error {
	// 原创建冻结的任务数也属于完整限额，不接受Prepare遗漏/变更后才让root晚拒绝。
	// 这只是任务数量硬约束，不能在此乘估算RSS冒充私有内存预算。

	if p.Limits.PidsMaxTasks != v.Bootstrap.Limits.PidsMaxTasks || p.Limits.SystemMemoryMaxBytes != v.Bootstrap.Limits.SystemMemoryMaxBytes || v.Identity != p.Identity || v.Birth == nil || v.Actual.GuestMemTotalBytes < p.GuestUsableMemoryBytes || p.Limits.OutsideUpperBytes < v.Bootstrap.Limits.OutsideUpperBytes || len(v.Actual.HugePools) != len(p.Limits.HugePools) {
		return fmt.Errorf("current Guest/source cannot contain prepared target")
	}
	for i, h := range v.Actual.HugePools {
		if h.PageSizeKB != p.Limits.HugePools[i].PageSizeKB {
			return fmt.Errorf("prepared limits omit or replace an actual HugeTLB hstate")
		}
	}
	return nil
}

func checkVM(s *journal.Snapshot, p budget.Prepared) (uint64, error) {
	vm := s.VirtualMachine()
	if !s.Dispatchable() || vm.Status.MemorySize == nil || vm.Status.PodName != p.RunnerPodName || vm.Status.RestartCount != p.RunnerRestartCount {
		return 0, fmt.Errorf("original Runner or actual VM memory is unavailable")
	}
	n, exact := vm.Status.MemorySize.AsInt64()
	expected := p.VMBeforeMemoryBytes
	if p.Request.Kind == control.Grow {
		expected = p.Request.TargetVMMemoryBytes
	}
	if !exact || n <= 0 || uint64(n) != uint64(expected) {
		return 0, fmt.Errorf("GuestReady VM memory differs from required action order")
	}
	return uint64(n), nil
}

func (e *Executor) native(ctx context.Context, p budget.Prepared) (control.FenceResponse, control.SharedCostResponse, error) {
	fence, err := e.pg.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return fence, control.SharedCostResponse{}, err
	}
	if fence.Disposition != control.Fenced {
		return fence, control.SharedCostResponse{}, resourceguard.ErrUnknown
	}
	shared, err := e.pg.SharedCost(ctx)
	if err == nil && (shared.Observed.CapacityGeneration < fence.Observed.CapacityGeneration || p.ValidateCurrent(shared, e.actor.Epoch) != nil) {
		err = fmt.Errorf("current native cost/generation does not confirm prepared GuestReady")
	}
	return fence, shared, err
}
