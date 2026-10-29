// Package initialexec将完整启动预算与首次准入接到真实PG/root/Runner/Host读者。
// 不生成Resize或VM完成，不写Guest限额/Host，不签发平台资格。没有明确资格
// 只保留旧路径；一旦已有首次WAL，必须使用其原来源恢复，不能重新发现替换。
package initialexec

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

type Executor struct {
	store     *journal.Store
	pg        *compute.Client
	resources *resourceguard.Client
	claims    *budgetclaims.Reader
	runner    *vmguard.Reader
	actor     journal.Owner
	source    *budget.QualificationSource
}

func New(store *journal.Store, pg *compute.Client, resources *resourceguard.Client, claims *budgetclaims.Reader, runner *vmguard.Reader, actor journal.Owner, source *budget.QualificationSource) (*Executor, error) {
	if store == nil || pg == nil || resources == nil || claims == nil || runner == nil || actor.Validate() != nil || source != nil && source.Validate() != nil {
		return nil, fmt.Errorf("initial producer requires original trusted domains")
	}
	instance, epoch := pg.Binding()
	id, resourceEpoch := resources.Binding()
	if epoch != actor.Epoch || resourceEpoch != epoch || instance.VMUID != string(store.Binding().VMUID) || id.VMUID != instance.VMUID || id.ComputeInstanceID != instance.ComputeInstanceID {
		return nil, fmt.Errorf("initial producer domains differ")
	}
	var selected *budget.QualificationSource
	if source != nil {
		v := *source
		selected = &v
	}
	return &Executor{store: store, pg: pg, resources: resources, claims: claims, runner: runner, actor: actor, source: selected}, nil
}

type observed struct {
	initial    budget.Initial
	fence      control.FenceResponse
	shared     control.SharedCostResponse
	root       budget.RootAggregate
	vmBytes    control.Uint64
	guestBytes control.Uint64
	sources    *budgetclaims.InitialSources
}

// observe不持跨域锁，使用认证通道核当前事实；写入以同VM UID/RV CAS串行化，
// native最终再核自己的floor。首次发布仅接受原Bootstrap及初始PG容量；恢复
// 用完整原预算，不按当前配置重造预算。Source/状态不一致保持旧持有，不写限额。
func (e *Executor) observe(ctx context.Context, s *journal.Snapshot, source budget.QualificationSource, retained *budget.Initial) (observed, error) {
	var o observed
	m, vm := s.State(), s.VirtualMachine()
	instance, _ := e.pg.Binding()
	id, _ := e.resources.Binding()
	if !s.Dispatchable() || m.Owner != e.actor || m.PGInstance == nil || *m.PGInstance != instance || m.ResourceIdentity == nil || *m.ResourceIdentity != id || e.pg.Pending() != nil || e.resources.Pending() != nil {
		return o, fmt.Errorf("initial producer lacks original live bound PG/Guest")
	}
	q, err := e.claims.Qualification(ctx, source)
	if err != nil {
		return o, err
	}
	birth, birthRef, p, err := e.runner.Inspect(ctx, &q.RunnerBirth, e.actor)
	if err != nil {
		return o, err
	}
	raw, err := json.Marshal(birth.Identity)
	if err != nil {
		return o, err
	}
	if birthRef != q.RunnerBirth || vmguard.SourceDigest(raw) != q.RunnerIdentitySHA256 || birth.Identity.RunnerPodUID != q.RunnerPodUID || birth.Identity.NodeUID != q.NodeUID || p.State.Pending != nil || p.Actual.RequestedBytes != p.Actual.ActualBytes || vm.Status.MemorySize == nil {
		return o, fmt.Errorf("initial producer original Runner is not settled")
	}
	if retained == nil && (p.State.Last != nil || p.State.HighWater != nil) {
		return o, fmt.Errorf("new initial budget cannot adopt prior VM memory commands")
	}
	if p.State.Last != nil {
		// 持续复核不能仅凭当前数值认领旧未发送/未知VM动作。原域结果必须
		// 已完成或不可重放取消，完整不可变回执和当前实际值再次认证。
		last := p.State.Last
		confirmed, err := e.runner.ObserveAbortOriginal(ctx, last.Intent.Command, last.Intent.JournalSHA256, e.actor)
		if err != nil {
			return o, err
		}
		if !confirmed.Receipt.Complete() && !confirmed.Receipt.CancelledBeforeSend() {
			return o, vmguard.ErrUnknown
		}
	}
	actual, exact := vm.Status.MemorySize.AsInt64()
	if !exact || actual <= 0 || control.Uint64(actual) != p.Actual.ActualBytes {
		return o, fmt.Errorf("initial VM status differs from native actual capacity")
	}
	view, err := e.resources.InspectState(ctx)
	if err != nil {
		return o, err
	}
	if _, err := view.ConfirmedLimits(); err != nil {
		return o, err
	}
	process, err := e.resources.InspectProcess(ctx)
	if err != nil {
		return o, err
	}
	if view.Identity != id || view.IssuerFloorEpoch != e.actor.Epoch || view.Birth == nil || process != *view.Birth {
		return o, fmt.Errorf("initial root floor or original Postmaster birth changed")
	}
	root, err := budget.RootAggregateFromView(view)
	if err != nil {
		return o, err
	}
	fence, err := e.pg.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return o, err
	}
	shared, err := e.pg.SharedCost(ctx)
	if err != nil {
		return o, err
	}
	if fence.Disposition != control.Fenced || !reflect.DeepEqual(fence.Observed, shared.Observed) {
		return o, fmt.Errorf("initial native fence and shared cost differ")
	}
	var i budget.Initial
	var sources *budgetclaims.InitialSources
	if retained != nil {
		i = *retained
		if err := i.MatchCurrent(shared, root, e.actor.Epoch, control.Uint64(actual), view.Actual.GuestMemTotalBytes); err != nil {
			return o, err
		}
		sources, err = e.claims.ReadInitialBudget(ctx, s, i)
	} else {
		sources, err = e.claims.ReadInitialPlacement(ctx, s, source, control.Uint64(actual))
		if err == nil {
			target, targetErr := q.Target(control.Uint64(actual))
			if targetErr != nil {
				return o, targetErr
			}
			system, transition, safety := target.Bounds(source)
			i = budget.Initial{ProtocolVersion: 1, VMShrinkPermit: false, Instance: instance, Incarnation: e.store.Binding().Incarnation, IssuerEpoch: e.actor.Epoch, Qualification: source, PGAggregate: root, Shared: shared, VMMemoryBytes: control.Uint64(actual), GuestUsableMemoryBytes: target.GuestUsableMemoryBytes, RunnerBirth: birthRef, RunnerIdentitySHA256: vmguard.SourceDigest(raw), RunnerPodName: q.RunnerPodName, RunnerPodUID: q.RunnerPodUID, RunnerRestartCount: q.RunnerRestartCount, NodeName: q.NodeName, NodeUID: q.NodeUID, HostProfileID: sources.HostProfileID, HostMemoryBytes: sources.HostMemoryBytes, HostOverheadBytes: sources.HostOverheadBytes, SystemOutsidePG: system, Transition: transition, Safety: safety}
			if err := i.MatchQualification(q); err != nil {
				return o, err
			}
			if view.Actual.GuestMemTotalBytes < i.GuestUsableMemoryBytes {
				return o, fmt.Errorf("initial actual Guest usable memory is below qualified guarantee")
			}
		}
	}
	if err != nil {
		return o, err
	}
	if !reflect.DeepEqual(q, sources.Qualification) {
		return o, fmt.Errorf("original qualification changed across initial source reads")
	}
	return observed{initial: i, fence: fence, shared: shared, root: root, vmBytes: control.Uint64(actual), guestBytes: view.Actual.GuestMemTotalBytes, sources: sources}, nil
}

func (e *Executor) Advance(ctx context.Context) (pgexec.Progress, error) { return e.advance(ctx, "") }

// CheckRetained复用首次预算生产者的完整来源核验，但从不开放或记录原开放
// 已完成。供服务安全门禁在首次开放未知时使用：即使Status尚未找到原命令，
// 也不能跳过Root/资格/Runner/PG预算检查，任由迟到开放在坏来源上开始服务。
// observe中的Fence只认证当前owner，不作为关闭或旧请求NoEffect证明。
func (e *Executor) CheckRetained(ctx context.Context, s *journal.Snapshot) error {
	i, err := s.State().InitialBudgetFor()
	if err != nil {
		return err
	}
	_, err = e.observe(ctx, s, i.Qualification, &i)
	return err
}

// selector仅是原WAL命令摘要，不携带预算/revision等替换参数。只有原派发
// Session仍为owner才可原样重放；普通轮询和NotFound从不自动投递此选择。
func (e *Executor) ReplayOriginal(ctx context.Context, selector string) (pgexec.Progress, error) {
	if len(selector) != 64 {
		return pgexec.AwaitingFact, fmt.Errorf("invalid original first admission selector")
	}
	return e.advance(ctx, selector)
}

func (e *Executor) advance(ctx context.Context, selector string) (pgexec.Progress, error) {
	s, err := e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != e.actor {
		return pgexec.AwaitingFact, fmt.Errorf("initial producer owner changed")
	}
	// 已存在Resize交各原领域推进：若它先于初始生产，首次服务仍只能在其
	// 完整Finalize后开放，不在这里补造一次初始成功。普通/实际预算已有时
	// 也不能清掉它们重新开机。已有InitialHeld未知由模型保证没有Active。
	if m.Active != nil || m.BudgetHeld != nil || m.CapacityHeld != nil {
		if selector != "" {
			return pgexec.AwaitingFact, fmt.Errorf("original first admission is not pending")
		}
		return pgexec.ExternalStep, nil
	}
	if m.InitialHeld == nil {
		if selector != "" {
			return pgexec.AwaitingFact, fmt.Errorf("original first admission WAL is absent")
		}
		if e.source == nil {
			return pgexec.ExternalStep, nil
		}
		if m.HighWater != nil || len(m.History) != 0 || m.ExpiredResults != 0 {
			return pgexec.AwaitingFact, fmt.Errorf("initial producer cannot reset prior operation history")
		}
		o, err := e.observe(ctx, s, *e.source, nil)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		if err := e.claims.RecheckInitialBudget(ctx, s, o.initial, o.sources); err != nil {
			return pgexec.AwaitingFact, err
		}
		_, err = e.store.BindInitialBudget(ctx, s, e.actor, o.initial)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		return pgexec.Recorded, nil
	}
	i, err := m.InitialBudgetFor()
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	a := m.InitialHeld.Admission
	if m.InitialAdmissionReady() {
		if selector != "" {
			return pgexec.AwaitingFact, fmt.Errorf("first admission already settled")
		}
		_, err := e.observe(ctx, s, i.Qualification, &i)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		return pgexec.Idle, nil
	}
	var c budget.InitialAdmissionCommand
	var fact control.AdmissionResponse
	query := a != nil
	if a != nil {
		if json.Unmarshal(a.Command, &c) != nil || c.ValidateFor(i) != nil {
			return pgexec.AwaitingFact, fmt.Errorf("original first admission command is malformed")
		}
		if selector != "" && (selector != a.CommandSHA256 || a.Origin != e.actor) {
			return pgexec.AwaitingFact, fmt.Errorf("first admission replay differs from original dispatcher/command")
		}
		// 先查原未知，即使Root/资格暂不可达也不掩盖原生结果。查询不等于
		// 完整预算落账；其后仍须认证全部来源。普通NotFound只保留原WAL。
		fact, err = e.pg.AdmissionStatus(ctx, c.Request)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		if !fact.Confirms() && selector == "" {
			return pgexec.AwaitingFact, resourceguard.ErrUnknown
		}
		if !fact.Confirms() {
			query = false
		}
	} else if selector != "" {
		return pgexec.AwaitingFact, fmt.Errorf("first admission is not dispatched")
	}
	o, err := e.observe(ctx, s, i.Qualification, &i)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if a == nil {
		if err := e.canOpen(o); err != nil {
			return pgexec.AwaitingFact, err
		}
		digest, err := i.Digest()
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		c = budget.InitialAdmissionCommand{ProtocolVersion: 1, InitialSHA256: digest, Request: control.AdmissionRequest{ProtocolVersion: 1, Instance: i.Instance, IssuerEpoch: e.actor.Epoch, Revision: 1, Open: true, ExpectedCapacityGeneration: 1, BudgetCommitmentID: i.PGAggregate.Bootstrap.BudgetCommitmentID}}
		var ticket *journal.InitialDispatch
		s, ticket, err = e.store.StartInitialAdmission(ctx, s, e.actor, c)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		if ticket == nil {
			return pgexec.AwaitingFact, fmt.Errorf("first admission dispatch ticket is missing")
		}
		// 保存原命令后再核全部来源；漂移留下Unconfirmed，不用新预算替换。
		o, err = e.observe(ctx, s, i.Qualification, &i)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
	}
	if !query {
		if err := e.canOpen(o); err != nil {
			return pgexec.AwaitingFact, err
		}
		if err := e.claims.RecheckInitialBudget(ctx, s, i, o.sources); err != nil {
			return pgexec.AwaitingFact, err
		}
		fact, err = e.pg.SetAdmission(ctx, c.Request)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		if !fact.Confirms() {
			return pgexec.AwaitingFact, resourceguard.ErrUnknown
		}
	}
	latest, err := e.observe(ctx, s, i.Qualification, &i)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if query {
		// 新owner第一次Status可能仍看到旧floor，随后observe中的Fence
		// 才推进到当前epoch。必须再只读原命令，不能拿旧回复拼当前审计，
		// 更不能因这次正常时序差换owner或重发setter。若维护门禁已前进，
		// 新Status不再确认原开放就保留未知，不能自动覆盖维护选择。
		fact, err = e.pg.AdmissionStatus(ctx, c.Request)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		if !fact.Confirms() {
			return pgexec.AwaitingFact, resourceguard.ErrUnknown
		}
	}
	if err := e.claims.RecheckInitialBudget(ctx, s, i, o.sources); err != nil {
		return pgexec.AwaitingFact, err
	}
	receipt := budget.InitialAdmissionReceipt{ProtocolVersion: 1, Command: c, Query: query, Fence: latest.fence, Shared: latest.shared, Response: fact, PGAggregate: latest.root, ResourceEpoch: e.actor.Epoch, VMMemoryBytes: latest.vmBytes, GuestUsableMemoryBytes: latest.guestBytes}
	if err := receipt.ValidateFor(i, c, e.actor.Epoch); err != nil {
		return pgexec.AwaitingFact, err
	}
	_, err = e.store.RecordInitialAdmission(ctx, s, e.actor, receipt)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.Recorded, nil
}

func (e *Executor) canOpen(o observed) error {
	if o.shared.Observed.AdmissionOpen || o.shared.Observed.AdmissionRevision != 0 {
		return fmt.Errorf("first admission cannot override an advanced external gate")
	}
	if original, closeIntent := e.pg.PendingAdmission(); closeIntent != nil || original != nil && (original.Instance != o.initial.Instance || original.Revision != 1 || original.IssuerEpoch != e.actor.Epoch || !original.Open || original.ExpectedCapacityGeneration != 1 || original.BudgetCommitmentID != o.initial.PGAggregate.Bootstrap.BudgetCommitmentID) {
		return fmt.Errorf("first admission cannot cross another SDK unknown intent")
	}
	return nil
}
