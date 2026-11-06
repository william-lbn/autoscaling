// Package vmexec把Agent的原ResizeVM/ObserveVM接到最终Runner持久消费域。
// 本包只写既有VM UID/RV意图和核验后的原回执，不直接发送QMP、改spec.Use、
// 创建预算声明或把HTTP/VM状态当完成。Host/预算生产者须已经真实持有原来源。
package vmexec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
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
	claims   *budgetclaims.Reader
	runner   *vmguard.Reader
	actor    journal.Owner
	instance control.Instance
	id       resourceguard.Identity
}

func New(store *journal.Store, pg *compute.Client, root *resourceguard.Client, claims *budgetclaims.Reader, runner *vmguard.Reader, actor journal.Owner) (*Executor, error) {
	if store == nil || pg == nil || root == nil || claims == nil || runner == nil || actor.Validate() != nil {
		return nil, fmt.Errorf("VM executor requires original journal/PG/root/Host/Runner/owner")
	}
	instance, epoch := pg.Binding()
	id, rootEpoch := root.Binding()
	if instance.VMUID != string(store.Binding().VMUID) || id.VMUID != instance.VMUID || id.ComputeInstanceID != instance.ComputeInstanceID || epoch != actor.Epoch || rootEpoch != epoch {
		return nil, fmt.Errorf("VM executor domains differ")
	}
	return &Executor{store: store, pg: pg, root: root, claims: claims, runner: runner, actor: actor, instance: instance, id: id}, nil
}

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

func (e *Executor) sameSnapshot(ctx context.Context, s *journal.Snapshot) error {
	latest, err := e.store.Load(ctx)
	if err != nil {
		return err
	}
	if !latest.Dispatchable() || latest.ResourceVersion() != s.ResourceVersion() || latest.State().Owner != e.actor {
		return fmt.Errorf("VM original journal/Runner/owner changed during verification")
	}
	return nil
}

func (e *Executor) prepared(op *journal.Operation, prior map[journal.Action]*journal.Step) (budget.Prepared, error) {
	var p budget.Prepared
	s := prior[journal.PrepareBudget]
	if s == nil || s.Observation == nil || s.TargetID != e.instance.ComputeInstanceID || json.Unmarshal(s.Observation.Data, &p) != nil || p.ValidateFor(op.Request, e.id) != nil || p.OutsideClaim == nil {
		return p, fmt.Errorf("VM lacks complete original Prepare/Outside source")
	}
	return p, nil
}

// native核当前认证PG/root，而非重用此前GuestReady的过期采样。Grow尚未执行
// 未来PG grow，Shrink须原Provider释放、原Guest限额、原关闭门禁仍有效。root
// 出生与PG实例都固定；任何partial/未知或较新资源命令均保留外层未知。
func (e *Executor) native(ctx context.Context, p budget.Prepared, prior map[journal.Action]*journal.Step, afterVM bool) (budget.RootAggregate, error) {
	return e.nativeWithShared(ctx, p, prior, afterVM, nil)
}

// nativeWithShared只在本次完整原生/Root/Process审计成功后交付同轮Shared，
// 不缓存许可、不以观察值替换已持久的Prepared或原Runner命令。
func (e *Executor) nativeWithShared(ctx context.Context, p budget.Prepared, prior map[journal.Action]*journal.Step, afterVM bool, audited *control.SharedCostResponse) (budget.RootAggregate, error) {
	var aggregate budget.RootAggregate
	fence, err := e.pg.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return aggregate, err
	}
	if fence.Disposition != control.Fenced {
		return aggregate, resourceguard.ErrUnknown
	}
	shared, err := e.pg.SharedCost(ctx)
	if err != nil {
		return aggregate, err
	}
	if shared.Observed.CapacityGeneration < fence.Observed.CapacityGeneration || p.ValidateCurrent(shared, e.actor.Epoch) != nil {
		return aggregate, fmt.Errorf("VM current original PG cost/provider is unconfirmed")
	}
	view, err := e.root.InspectState(ctx)
	if err != nil {
		return aggregate, err
	}
	if view.Identity != e.id || view.IssuerFloorEpoch != e.actor.Epoch || e.root.Pending() != nil {
		return aggregate, resourceguard.ErrUnknown
	}
	limits, err := view.ConfirmedLimits()
	if err != nil {
		return aggregate, err
	}
	process, err := e.root.InspectProcess(ctx)
	if err != nil {
		return aggregate, err
	}
	if view.Birth == nil || process != *view.Birth {
		return aggregate, fmt.Errorf("VM original Postmaster birth changed")
	}
	if afterVM && view.Actual.GuestMemTotalBytes < p.GuestUsableMemoryBytes {
		return aggregate, fmt.Errorf("actual Guest MemTotal has not reached prepared usable budget")
	}
	aggregate, err = budget.RootAggregateFromView(view)
	if err != nil {
		return aggregate, err
	}
	if p.Request.Kind == control.Grow {
		if audited != nil {
			*audited = shared
		}
		return aggregate, nil
	}
	s := prior[journal.GuestReady]
	q := prior[journal.Quiesce]
	if s == nil || s.Observation == nil || q == nil || q.Observation == nil || prior[journal.ResizePG] == nil || prior[journal.ReleasePG] == nil {
		return aggregate, fmt.Errorf("VM shrink original PG/Guest predecessors missing")
	}
	var guest resourceexec.Audit
	var cmd resourceguard.Command
	var quiesced budget.Quiesced
	if json.Unmarshal(s.Command, &cmd) != nil || json.Unmarshal(s.Observation.Data, &guest) != nil || guest.ProtocolVersion != 2 || !reflect.DeepEqual(cmd, guest.Command) || guest.Command.BudgetCommitmentID != p.Request.BudgetCommitmentID || guest.PGAggregate.Identity != e.id || guest.PGAggregate.Match(view) != nil || guest.Evidence.ValidateFor(cmd) != nil || !reflect.DeepEqual(limits, p.Limits) || guest.RunnerPodName != p.RunnerPodName || guest.RunnerRestarts != p.RunnerRestartCount || guest.VMMemoryBytes != p.VMBeforeMemoryBytes || json.Unmarshal(q.Observation.Data, &quiesced) != nil || quiesced.ValidateFor(p, p.Request.Key.IssuerEpoch) != nil {
		return aggregate, fmt.Errorf("VM shrink current resources differ from original complete Guest/Quiesce")
	}
	// 关闭原门禁后仍须新采样前台排空，后台/DSM/2PC由持续聚合硬约束覆盖，
	// 不将其计数强制清零，也不把一次RSS当未来私有内存承诺。
	dsm, err := e.pg.DSMState(ctx)
	if err != nil {
		return aggregate, err
	}
	c := dsm.Prepared.Connections
	work := c.Work
	current := work.Observed
	if !c.Enabled || !c.PrivateManagementReady || !c.Acknowledged || !c.PublicConnectionsBlocked || !work.ObserverExcluded || !work.ForegroundIdle || current.AdmissionOpen || current.AdmissionRevision != quiesced.Close.Revision || current.IssuerFloorEpoch != e.actor.Epoch || current.CapacityGeneration != shared.Observed.CapacityGeneration || p.ValidateCurrent(control.SharedCostResponse{ProtocolVersion: shared.ProtocolVersion, VMShrinkPermit: false, StartupMainUpperBytes: shared.StartupMainUpperBytes, StartupArraysUpperBytes: shared.StartupArraysUpperBytes, CurrentMainAndArraysUpperBytes: shared.CurrentMainAndArraysUpperBytes, Observed: current}, e.actor.Epoch) != nil {
		return aggregate, fmt.Errorf("VM shrink native public gate/foreground is no longer quiescent")
	}
	if audited != nil {
		*audited = shared
	}
	return aggregate, nil
}

func (e *Executor) command(p budget.Prepared, prior map[journal.Action]*journal.Step, birth vmguard.Birth, ref, host budget.ObjectRef, h budget.HostReservation) (vmguard.Command, error) {
	raw, err := json.Marshal(h)
	if err != nil {
		return vmguard.Command{}, err
	}
	c := vmguard.Command{ProtocolVersion: 1, Identity: birth.Identity, BindingClaim: ref, HostClaim: host, Request: p.Request, BeforeBytes: p.VMBeforeMemoryBytes, PreparedSHA256: vmguard.SourceDigest(prior[journal.PrepareBudget].Observation.Data), HostSHA256: vmguard.SourceDigest(raw), GuestSHA256: ""}
	if p.Request.Kind == control.Shrink {
		if prior[journal.GuestReady] == nil {
			return c, fmt.Errorf("VM original Guest predecessor missing")
		}
		c.GuestSHA256 = vmguard.SourceDigest(prior[journal.GuestReady].Observation.Data)
	}
	return c, c.Validate()
}

func hostHeld(h budget.HostReservation, c vmguard.Command) bool {
	held := max(c.BeforeBytes, c.Request.TargetVMMemoryBytes)
	return uint64(h.HostOverheadBytes) <= math.MaxUint64-uint64(held) && h.ReservedMemoryBytes >= held+h.HostOverheadBytes && h.RunnerPodUID == c.Identity.RunnerPodUID && h.NodeUID == c.Identity.NodeUID
}

func (e *Executor) Advance(ctx context.Context) (pgexec.Progress, error) {
	return e.advance(ctx, nil)
}

// AdvanceWithAudit保留原Advance；仅在本轮完整Prepared/Host/PG/Root/Process/
// Runner及同快照复核成功后同步回调。回调失败不Start/Record、不派发或重放；
// Runner异步未知仍沿原路径返回，绝不以续租回调补造Completed。
func (e *Executor) AdvanceWithAudit(ctx context.Context, after func(context.Context, *journal.Snapshot, control.SharedCostResponse) error) (pgexec.Progress, error) {
	if after == nil {
		return pgexec.AwaitingFact, fmt.Errorf("VM audit requires explicit completion callback")
	}
	return e.advance(ctx, after)
}

func (e *Executor) advance(ctx context.Context, after func(context.Context, *journal.Snapshot, control.SharedCostResponse) error) (pgexec.Progress, error) {
	s, err := e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	state := s.State()
	if state.Owner != e.actor || state.ResourceIdentity == nil || *state.ResourceIdentity != e.id || state.PGInstance == nil || *state.PGInstance != e.instance {
		return pgexec.AwaitingFact, fmt.Errorf("VM original owner/PG/Guest binding changed")
	}
	if state.Active == nil {
		return pgexec.Idle, nil
	}
	op := state.Active
	prior, step := steps(op)
	if step == nil || (step.Action != journal.ResizeVM && step.Action != journal.ObserveVM) {
		return pgexec.ExternalStep, nil
	}
	if !s.Dispatchable() || op.Request.Key.Instance != e.instance {
		return pgexec.AwaitingFact, fmt.Errorf("VM original instance is not dispatchable")
	}
	if step.Phase == journal.Planned && (op.AbortRequested || op.Origin != e.actor) {
		return pgexec.ExternalStep, nil
	}
	if op.AbortRequested && step.Phase == journal.Unconfirmed && step.Action == journal.ResizeVM {
		if handled, progress, err := e.cancelledBeforeSend(ctx, s, *step); handled {
			return progress, err
		}
	}
	p, err := e.prepared(op, prior)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if step.Action == journal.ObserveVM {
		return e.observeWithAudit(ctx, s, p, prior, step, after)
	}
	if step.Phase == journal.Planned {
		birth, ref, pub, err := e.runner.Inspect(ctx, nil, e.actor)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		if pub.State.Pending != nil || pub.Actual.RequestedBytes != p.VMBeforeMemoryBytes || pub.Actual.ActualBytes != p.VMBeforeMemoryBytes {
			return pgexec.AwaitingFact, vmguard.ErrUnknown
		}
		hostRef, err := e.claims.HostReference(ctx, op.Request)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		observed, err := e.claims.Read(ctx, s, p, hostRef)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		c, err := e.command(p, prior, birth, ref, hostRef, observed.Host)
		if err != nil || !hostHeld(observed.Host, c) {
			return pgexec.AwaitingFact, fmt.Errorf("VM original command/Host hold differs from before and target")
		}
		if op.Request.Kind == control.Grow {
			var h budget.HostReservation
			source := prior[journal.ReserveHost]
			if source == nil || source.Observation == nil || json.Unmarshal(source.Observation.Data, &h) != nil || h != observed.Host || source.TargetID != h.SourceID {
				return pgexec.AwaitingFact, fmt.Errorf("VM grow lacks completed original Host reservation")
			}
		}
		var shared control.SharedCostResponse
		aggregate, err := e.nativeWithShared(ctx, p, prior, false, &shared)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		if err := e.claims.Recheck(ctx, s, p, hostRef, observed); err != nil {
			return pgexec.AwaitingFact, err
		}
		if _, _, latest, err := e.runner.Inspect(ctx, &ref, e.actor); err != nil || latest.State.Pending != nil || latest.Actual.RequestedBytes != c.BeforeBytes || latest.Actual.ActualBytes != c.BeforeBytes {
			if err == nil {
				err = vmguard.ErrUnknown
			}
			return pgexec.AwaitingFact, err
		}
		if err := e.sameSnapshot(ctx, s); err != nil {
			return pgexec.AwaitingFact, err
		}
		if after != nil {
			// Grow尚未GuestReady，保原Prepare Root；Shrink已由nativeWithShared核新Guest Root。
			if p.Request.Kind == control.Grow {
				if err := samePreparedRoot(p, prior, aggregate); err != nil {
					return pgexec.AwaitingFact, err
				}
			}
			if err := after(ctx, s, shared); err != nil {
				return pgexec.AwaitingFact, err
			}
		}
		raw, err := json.Marshal(c)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		// Runner以自己的GET确认此原CAS后才执行。Agent没有第二个发送动作：
		// CAS回复丢失时保留Unconfirmed，恢复只能读取这个完整原命令/原回执。
		_, _, err = e.store.Start(ctx, s, e.actor, op.Request.Key, journal.ResizeVM, c.Identity.RunnerPodUID, raw)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		return pgexec.Recorded, nil
	}
	var c vmguard.Command
	if json.Unmarshal(step.Command, &c) != nil || c.Validate() != nil || c.Request != op.Request || step.TargetID != c.Identity.RunnerPodUID || step.DispatchedBy == nil || step.DispatchedBy.Epoch != c.Request.Key.IssuerEpoch {
		return pgexec.AwaitingFact, fmt.Errorf("original typed ResizeVM command changed")
	}
	// 已可能执行先查询原Runner，再访问外部Host/Root/PG。查询成功仍不等于
	// 外层Completed，下面独立重核所有域；缺来源保留未知，不自动重放。
	proof, err := e.runner.ObserveOriginal(ctx, c, step.CommandSHA256, e.actor)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	observed, err := e.claims.Read(ctx, s, p, c.HostClaim)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	expected, err := e.command(p, prior, proof.Birth, c.BindingClaim, c.HostClaim, observed.Host)
	if err != nil || expected != c || !hostHeld(observed.Host, c) {
		return pgexec.AwaitingFact, fmt.Errorf("runner original sources differ from complete current Host/Prepare/Guest")
	}
	var shared control.SharedCostResponse
	aggregate, err := e.nativeWithShared(ctx, p, prior, true, &shared)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	fact := budget.VMSettlement{ProtocolVersion: 1, VMShrinkPermit: false, Request: p.Request, Identity: p.Identity, HostClaim: c.HostClaim, Host: observed.Host, RunnerRestartCount: p.RunnerRestartCount, RunnerFenceEpoch: p.Request.Key.IssuerEpoch, RunnerCommandID: proof.Sources.CommandSHA256, GuestUsableMemoryBytes: p.GuestUsableMemoryBytes, ActualMemoryBytes: proof.Actual.ActualBytes, QMPObservedMemoryBytes: proof.Actual.ActualBytes, PGResourceSourceID: "", NoPendingCommands: true, MigrationBlocked: true, Runner: &proof.Sources}
	if p.Request.Kind == control.Shrink {
		fact.PGResourceSourceID = aggregate.Bound.SourceID
	}
	if err := fact.ValidateFor(p, aggregate, e.store.Binding().Incarnation); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := e.claims.Recheck(ctx, s, p, c.HostClaim, observed); err != nil {
		return pgexec.AwaitingFact, err
	}
	if _, err := e.runner.ObserveOriginal(ctx, c, step.CommandSHA256, e.actor); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := e.sameSnapshot(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	if after != nil {
		if p.Request.Kind == control.Grow {
			if err := samePreparedRoot(p, prior, aggregate); err != nil {
				return pgexec.AwaitingFact, err
			}
		}
		if err := after(ctx, s, shared); err != nil {
			return pgexec.AwaitingFact, err
		}
	}
	raw, err := json.Marshal(fact)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	_, err = e.store.Record(ctx, s, e.actor, op.Request.Key, journal.Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: step.TargetID, Data: raw})
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.Recorded, nil
}

// ObserveVM是同一原ResizeVM回执的再次当前验证，不制造另一个Runner写命令。
// 数据逐字节保留，预算域可核对两步同一结论；新的外部事实只用于拒绝/等待。
func (e *Executor) observe(ctx context.Context, s *journal.Snapshot, p budget.Prepared, prior map[journal.Action]*journal.Step, step *journal.Step) (pgexec.Progress, error) {
	return e.observeWithAudit(ctx, s, p, prior, step, nil)
}

// ObserveVM只复核原已完成ResizeVM，不派发QMP；nil保原恢复/观察入口。
func (e *Executor) observeWithAudit(ctx context.Context, s *journal.Snapshot, p budget.Prepared, prior map[journal.Action]*journal.Step, step *journal.Step, after func(context.Context, *journal.Snapshot, control.SharedCostResponse) error) (pgexec.Progress, error) {
	resize := prior[journal.ResizeVM]
	if resize == nil || resize.Observation == nil {
		return pgexec.AwaitingFact, fmt.Errorf("original ResizeVM result missing")
	}
	var c vmguard.Command
	var fact budget.VMSettlement
	if json.Unmarshal(resize.Command, &c) != nil || c.Validate() != nil || json.Unmarshal(resize.Observation.Data, &fact) != nil || fact.Runner == nil || fact.Runner.JournalSHA256 != resize.CommandSHA256 || fact.Request != p.Request || resize.TargetID != c.Identity.RunnerPodUID {
		return pgexec.AwaitingFact, fmt.Errorf("typed original ResizeVM/Runner source missing")
	}
	proof, err := e.runner.ObserveOriginal(ctx, c, resize.CommandSHA256, e.actor)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if proof.Sources != *fact.Runner {
		return pgexec.AwaitingFact, fmt.Errorf("original Runner receipt source changed before ObserveVM")
	}
	observed, err := e.claims.Read(ctx, s, p, c.HostClaim)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if observed.Host != fact.Host {
		return pgexec.AwaitingFact, fmt.Errorf("original ObserveVM Host changed")
	}
	var shared control.SharedCostResponse
	aggregate, err := e.nativeWithShared(ctx, p, prior, true, &shared)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := fact.ValidateFor(p, aggregate, e.store.Binding().Incarnation); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := e.claims.Recheck(ctx, s, p, c.HostClaim, observed); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := e.sameSnapshot(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	if step.Phase == journal.Planned {
		if after != nil {
			if p.Request.Kind == control.Grow {
				if err := samePreparedRoot(p, prior, aggregate); err != nil {
					return pgexec.AwaitingFact, err
				}
			}
			if err := after(ctx, s, shared); err != nil {
				return pgexec.AwaitingFact, err
			}
		}
		var dispatch *journal.Dispatch
		s, dispatch, err = e.store.Start(ctx, s, e.actor, p.Request.Key, journal.ObserveVM, resize.TargetID, resize.Command)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		step = &dispatch.Step
	} else if step.TargetID != resize.TargetID || !bytes.Equal(step.Command, resize.Command) {
		return pgexec.AwaitingFact, fmt.Errorf("original ObserveVM command changed")
	}
	if _, err := e.runner.ObserveOriginal(ctx, c, resize.CommandSHA256, e.actor); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := e.sameSnapshot(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	if after != nil {
		if p.Request.Kind == control.Grow {
			if err := samePreparedRoot(p, prior, aggregate); err != nil {
				return pgexec.AwaitingFact, err
			}
		}
		if err := after(ctx, s, shared); err != nil {
			return pgexec.AwaitingFact, err
		}
	}
	_, err = e.store.Record(ctx, s, e.actor, p.Request.Key, journal.Observation{Key: p.Request.Key, RequestSHA256: s.State().Active.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: step.TargetID, Data: append([]byte(nil), resize.Observation.Data...)})
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.Recorded, nil
}

// Grow的GuestReady尚未派发，仍在服务PG的Root必须是原Prepare完整绑定。
// 摘要只含身份、Bootstrap/Birth/Held和硬限Bound，不含GuestMemTotal、VM内存、
// RSS或RV；VM真实增大不会伪装旧Guest容量，也不能借换Root来源延长服务。
// 原Advance无回调保持原领域行为；此门只保护新增的有限续租检查点。
func samePreparedRoot(p budget.Prepared, prior map[journal.Action]*journal.Step, aggregate budget.RootAggregate) error {
	step := prior[journal.PrepareBudget]
	var original budget.PrepareCommand
	if step == nil || json.Unmarshal(step.Command, &original) != nil || original.Validate() != nil {
		return fmt.Errorf("VM audit lacks original complete Prepare Root binding")
	}
	expected := p
	expected.OutsideClaim = nil // 原Prepare完成只增加正式创建的Outside对象引用。
	if !reflect.DeepEqual(original.Prepared, expected) {
		return fmt.Errorf("VM audit original Prepared differs from completed source")
	}
	digest, err := budget.RootDigest(aggregate)
	if err != nil || digest != original.RootSHA256 {
		return fmt.Errorf("VM audit current Root differs from original Prepare source")
	}
	return nil
}
