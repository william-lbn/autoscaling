// Package budgetexec结算同一VM持久账本中的预算步骤；不创建第二份操作日志、
// 不写资源/QMP/Host额度、不将任意外部声明补成可信完成。未由可信上游生产的
// Outside/Host/最终VM事实保持阻断，生产Prepare及最终VM执行器仍须各自落实约束。
package budgetexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

type Executor struct {
	store    *journal.Store
	pg       *compute.Client
	root     *resourceguard.Client
	actor    journal.Owner
	instance control.Instance
	id       resourceguard.Identity
	claims   *budgetclaims.Reader
	runner   *vmguard.Reader
}

func New(store *journal.Store, pg *compute.Client, root *resourceguard.Client, actor journal.Owner, claims *budgetclaims.Reader, runners ...*vmguard.Reader) (*Executor, error) {
	if store == nil || pg == nil || root == nil || claims == nil || actor.Validate() != nil {
		return nil, fmt.Errorf("budget finalization requires original store/PG/root/claims/owner")
	}
	instance, epoch := pg.Binding()
	id, resourceEpoch := root.Binding()
	if epoch != actor.Epoch || resourceEpoch != epoch || instance.VMUID != string(store.Binding().VMUID) || id.VMUID != instance.VMUID || id.ComputeInstanceID != instance.ComputeInstanceID {
		return nil, fmt.Errorf("budget domains differ")
	}
	if len(runners) > 1 || len(runners) == 1 && runners[0] == nil {
		return nil, fmt.Errorf("one original Runner reader required")
	}
	var runner *vmguard.Reader
	if len(runners) == 1 {
		runner = runners[0]
	}
	return &Executor{store: store, pg: pg, root: root, actor: actor, instance: instance, id: id, claims: claims, runner: runner}, nil
}

// command固定第一次结算时的完整Finalized与所有原前驱摘要。此步骤只有本地CAS
// 副作用，Unknown可以自动重复只读复核，但不重算/替换命令、更换来源或新发资源
// 请求。CAS回复丢失先重读原账本；不能把Finalize未知扩大成PG/VM重新执行。
type command = budget.FinalizationCommand

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func steps(op *journal.Operation) (map[journal.Action]*journal.Step, *journal.Step) {
	prior := map[journal.Action]*journal.Step{}
	for i := range op.Steps {
		s := &op.Steps[i]
		if s.Phase != journal.Settled {
			return prior, s
		}
		if s.Observation == nil || s.Observation.Result != journal.Completed {
			return nil, nil
		}
		prior[s.Action] = s
	}
	return prior, nil
}

func (e *Executor) Advance(ctx context.Context) (pgexec.Progress, error) {
	return e.advance(ctx, false, false)
}

// CheckBeforeFinalization只复核完整资源前驱和当前Root/PG/Host/Outside/Runner，
// 不Start/Record、不替换永久预算。用于最终结算前异常关闭；已持久的Finalize
// 未知仍逐字核验。原生fence是唯一允许的PG写，不能据此推导目标预算已完成。
func (e *Executor) CheckBeforeFinalization(ctx context.Context, s *journal.Snapshot) error {
	if s == nil {
		return fmt.Errorf("pending finalization check requires original snapshot")
	}
	if _, err := s.State().PendingFinalizationGenerationForClosure(); err != nil {
		return err
	}
	if err := e.sameSnapshot(ctx, s); err != nil {
		return err
	}
	progress, err := e.advance(ctx, false, true)
	if err == nil && progress != pgexec.Idle {
		err = fmt.Errorf("pending finalization check did not verify original resource boundary")
	}
	return err
}

// RetainOriginal只显式升级仍在Active中的旧v2完整结算。调用方须先查询未知
// 原准入，不能让来源失联挡住只读查询。本函数重核所有原前驱/来源，不改旧回执
// 或外部资源；成功只做一次同VM UID/RV长期预算保存。历史不自动收养。
func (e *Executor) RetainOriginal(ctx context.Context) (pgexec.Progress, error) {
	return e.advance(ctx, true, false)
}

func (e *Executor) advance(ctx context.Context, retain, inspect bool) (pgexec.Progress, error) {
	snapshot, err := e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	state := snapshot.State()
	if state.Owner != e.actor || state.ResourceIdentity == nil || *state.ResourceIdentity != e.id {
		return pgexec.AwaitingFact, fmt.Errorf("budget owner or original resource changed")
	}
	if state.Active == nil {
		return pgexec.Idle, nil
	}
	op := state.Active
	prior, step := steps(op)
	if retain {
		if state.ProtocolVersion != 2 || state.BudgetHeld != nil || prior[journal.FinalizeBudget] == nil {
			return pgexec.AwaitingFact, fmt.Errorf("only original active v2 settlement may be explicitly retained")
		}
		step = prior[journal.FinalizeBudget]
	} else if step == nil || step.Action != journal.FinalizeBudget {
		return pgexec.ExternalStep, nil
	}
	if !snapshot.Dispatchable() || op.Request.Key.Instance != e.instance {
		return pgexec.AwaitingFact, fmt.Errorf("budget original VM/PG not dispatchable")
	}
	if !inspect && step.Phase == journal.Planned && (op.AbortRequested || op.Origin != e.actor) {
		return pgexec.ExternalStep, nil
	}
	// 接管不能新派发旧owner的结算，但可只读完成已持久原结算。原Finalized的
	// issuer保持原请求epoch，当前PG/root floor另行核验，不能改epoch冒充新命令。
	preparedStep, guestStep, vmStep := prior[journal.PrepareBudget], prior[journal.GuestReady], prior[journal.ResizeVM]
	if preparedStep == nil || guestStep == nil || vmStep == nil || prior[journal.ResizePG] == nil {
		return pgexec.AwaitingFact, fmt.Errorf("budget original predecessor missing")
	}
	var p budget.Prepared
	var guest resourceexec.Audit
	var originalGuestCommand resourceguard.Command
	var vmFact budget.VMSettlement
	if preparedStep.TargetID != e.instance.ComputeInstanceID || json.Unmarshal(preparedStep.Observation.Data, &p) != nil || p.ValidateFor(op.Request, e.id) != nil || p.OutsideClaim == nil {
		return pgexec.AwaitingFact, fmt.Errorf("complete original prepared budget/claim missing")
	}
	if json.Unmarshal(guestStep.Command, &originalGuestCommand) != nil || json.Unmarshal(guestStep.Observation.Data, &guest) != nil || !reflect.DeepEqual(originalGuestCommand, guest.Command) || guestStep.TargetID != e.instance.ComputeInstanceID || guestStep.DispatchedBy == nil || guest.Command.IssuerEpoch != guestStep.DispatchedBy.Epoch || guest.Command.BudgetCommitmentID != op.Request.BudgetCommitmentID || guest.ProtocolVersion != 2 || guest.PGAggregate.Validate() != nil || guest.PGAggregate.Identity != e.id || guest.PGAggregate.Held == nil || !reflect.DeepEqual(*guest.PGAggregate.Held, guest.Command) || guest.Evidence.ValidateFor(guest.Command) != nil || guest.RunnerPodName != p.RunnerPodName || guest.RunnerRestarts != p.RunnerRestartCount {
		return pgexec.AwaitingFact, fmt.Errorf("original GuestReady audit incomplete")
	}
	if json.Unmarshal(vmStep.Observation.Data, &vmFact) != nil || vmStep.TargetID != vmFact.Host.RunnerPodUID || vmFact.ValidateFor(p, guest.PGAggregate, e.store.Binding().Incarnation) != nil {
		return pgexec.AwaitingFact, fmt.Errorf("original final VM settlement missing")
	}
	if op.Request.Kind == control.Shrink {
		s := prior[journal.ObserveVM]
		if s == nil || s.TargetID != vmStep.TargetID || !reflect.DeepEqual(s.Observation.Data, vmStep.Observation.Data) {
			return pgexec.AwaitingFact, fmt.Errorf("shrink final VM readback differs from original settlement")
		}
	}
	if err := e.checkRunner(ctx, p, preparedStep, guestStep, vmStep, vmFact); err != nil {
		return pgexec.AwaitingFact, err
	}
	observed, err := e.claims.Read(ctx, snapshot, p, vmFact.HostClaim)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if observed.Host != vmFact.Host {
		return pgexec.AwaitingFact, fmt.Errorf("held Host differs from final VM receipt")
	}
	if op.Request.Kind == control.Grow {
		s := prior[journal.ReserveHost]
		var host budget.HostReservation
		if s == nil || s.TargetID != vmFact.Host.SourceID || json.Unmarshal(s.Observation.Data, &host) != nil || host != vmFact.Host {
			return pgexec.AwaitingFact, fmt.Errorf("grow lacks original completed Host reservation")
		}
	}
	fence, err := e.pg.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil || fence.Disposition != control.Fenced {
		if err == nil {
			err = resourceguard.ErrUnknown
		}
		return pgexec.AwaitingFact, err
	}
	shared, err := e.pg.SharedCost(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if op.Request.Kind == control.Shrink && p.ValidateCurrent(shared, e.actor.Epoch) != nil {
		return pgexec.AwaitingFact, fmt.Errorf("final shrink provider release remains unverified")
	}
	if shared.Observed.CapacityGeneration < fence.Observed.CapacityGeneration {
		return pgexec.AwaitingFact, fmt.Errorf("PG cost predates budget fence")
	}
	view, err := e.root.InspectState(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if view.IssuerFloorEpoch != e.actor.Epoch || view.Actual.GuestMemTotalBytes < p.GuestUsableMemoryBytes || e.root.Pending() != nil {
		return pgexec.AwaitingFact, resourceguard.ErrUnknown
	}
	if err = guest.PGAggregate.Match(view); err != nil {
		return pgexec.AwaitingFact, err
	}
	process, err := e.root.InspectProcess(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if process != guest.PGAggregate.Birth {
		return pgexec.AwaitingFact, fmt.Errorf("budget original PG birth changed")
	}
	limits, err := view.ConfirmedLimits()
	if err != nil || !reflect.DeepEqual(limits, p.Limits) {
		return pgexec.AwaitingFact, fmt.Errorf("budget target constraints not currently confirmed")
	}
	vm := snapshot.VirtualMachine()
	if vm.Status.MemorySize == nil {
		return pgexec.AwaitingFact, fmt.Errorf("final VM memory unavailable")
	}
	memory, exact := vm.Status.MemorySize.AsInt64()
	if !exact || memory <= 0 || uint64(memory) != uint64(vmFact.ActualMemoryBytes) {
		return pgexec.AwaitingFact, fmt.Errorf("final VM status differs from verified QMP settlement")
	}
	final := budget.Finalized{ProtocolVersion: 1, Request: op.Request, IssuerEpoch: op.Request.Key.IssuerEpoch, RunnerPodName: p.RunnerPodName, RunnerRestartCount: p.RunnerRestartCount, HostReservationID: vmFact.Host.SourceID, GuestUsableMemoryBytes: p.GuestUsableMemoryBytes, Shared: shared, SystemOutsidePG: p.SystemOutsidePG, Aggregate: &guest.PGAggregate.Bound, Transition: p.Transition, Safety: p.Safety, Sources: &budget.SettlementSources{OutsideClaim: *p.OutsideClaim, HostClaim: vmFact.HostClaim, VMReceiptSHA256: digest(vmStep.Observation.Data), Runner: vmFact.Runner}, VMShrinkPermit: false, PrivateAndDSM: nil, LFCOutsideShared: nil, HugePoolOutsideShared: nil}
	if step.Phase == journal.Planned && final.ValidateFor(op.Request, shared, e.actor.Epoch, vm.Status.PodName, vm.Status.RestartCount, uint64(memory)) != nil {
		return pgexec.AwaitingFact, fmt.Errorf("complete final budget does not fit original target")
	}
	original := command{ProtocolVersion: 1, Finalized: final, PreparedSHA256: digest(preparedStep.Observation.Data), GuestSHA256: digest(guestStep.Observation.Data), VMSHA256: digest(vmStep.Observation.Data)}
	if step.Phase != journal.Planned {
		var saved command
		if json.Unmarshal(step.Command, &saved) != nil || saved.ProtocolVersion != 1 || saved.PreparedSHA256 != original.PreparedSHA256 || saved.GuestSHA256 != original.GuestSHA256 || saved.VMSHA256 != original.VMSHA256 || saved.Finalized.Sources == nil || saved.Finalized.HostReservationID != vmFact.Host.SourceID || saved.Finalized.GuestUsableMemoryBytes != p.GuestUsableMemoryBytes || !reflect.DeepEqual(saved.Finalized.Sources, final.Sources) || saved.Finalized.Aggregate == nil || *saved.Finalized.Aggregate != guest.PGAggregate.Bound || saved.Finalized.SystemOutsidePG != p.SystemOutsidePG || saved.Finalized.Transition != p.Transition || saved.Finalized.Safety != p.Safety || saved.Finalized.ValidateFor(op.Request, shared, e.actor.Epoch, vm.Status.PodName, vm.Status.RestartCount, uint64(memory)) != nil || step.TargetID != e.instance.ComputeInstanceID {
			return pgexec.AwaitingFact, fmt.Errorf("retained original budget settlement differs")
		}
		original = saved
	}
	if err = original.Validate(); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err = e.claims.Recheck(ctx, snapshot, p, vmFact.HostClaim, observed); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err = e.sameSnapshot(ctx, snapshot); err != nil {
		return pgexec.AwaitingFact, err
	}
	if inspect {
		return pgexec.Idle, nil
	}
	if step.Phase == journal.Planned {
		data, err := json.Marshal(original)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		var dispatch *journal.Dispatch
		snapshot, dispatch, err = e.store.Start(ctx, snapshot, e.actor, op.Request.Key, journal.FinalizeBudget, e.instance.ComputeInstanceID, data)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		step = &dispatch.Step
	}
	// 这里没有外部资源写，但写前CAS之后仍重新读取所有原承诺/Root/VM，以免
	// source在网络等待或CAS过程中被替换。失败仅保留Unconfirmed，不执行回滚。
	if err = e.claims.Recheck(ctx, snapshot, p, vmFact.HostClaim, observed); err != nil {
		return pgexec.AwaitingFact, err
	}
	latest, err := e.root.InspectState(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if latest.IssuerFloorEpoch != e.actor.Epoch || guest.PGAggregate.Match(latest) != nil {
		return pgexec.AwaitingFact, fmt.Errorf("budget original resource changed after write-ahead CAS")
	}
	actualProcess, err := e.root.InspectProcess(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if actualProcess != guest.PGAggregate.Birth {
		return pgexec.AwaitingFact, fmt.Errorf("original Postmaster changed during settlement")
	}
	actualShared, err := e.pg.SharedCost(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if original.Finalized.ValidateFor(op.Request, actualShared, e.actor.Epoch, vm.Status.PodName, vm.Status.RestartCount, uint64(memory)) != nil {
		return pgexec.AwaitingFact, fmt.Errorf("PG allocation changed during settlement")
	}
	if err = e.sameSnapshot(ctx, snapshot); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := e.checkRunner(ctx, p, preparedStep, guestStep, vmStep, vmFact); err != nil {
		return pgexec.AwaitingFact, err
	}
	data, err := json.Marshal(original.Finalized)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if retain {
		// 旧Completed必须等于原完整WAL的Finalized。不能用当前较低的Shared成本
		// 重写旧数据；保留原字节以及当时owner，CAS只增加长期持有字段/版本。
		var recorded budget.Finalized
		if step.Observation == nil || json.Unmarshal(step.Observation.Data, &recorded) != nil || !reflect.DeepEqual(recorded, original.Finalized) {
			return pgexec.AwaitingFact, fmt.Errorf("original completed finalization differs from retained command")
		}
		_, err = e.store.RetainFinalizedBudget(ctx, snapshot, e.actor, op.Request.Key)
	} else {
		_, err = e.store.Record(ctx, snapshot, e.actor, op.Request.Key, journal.Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: journal.FinalizeBudget, Result: journal.Completed, SourceID: e.instance.ComputeInstanceID, Data: data})
	}
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.Recorded, nil
}

func (e *Executor) sameSnapshot(ctx context.Context, original *journal.Snapshot) error {
	latest, err := e.store.Load(ctx)
	if err != nil {
		return err
	}
	if !latest.Dispatchable() || latest.ResourceVersion() != original.ResourceVersion() || latest.State().Owner != e.actor {
		return fmt.Errorf("budget VM/journal changed during verification")
	}
	return nil
}
