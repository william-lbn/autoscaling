// Package admissionexec消费同一持久journal的最终恢复准入步骤，不创建另一操作日志。
// 初始启动预算、实际quiesce与Host/Guest/VM领域仍由对应生产适配器完成，不能跳步。
package admissionexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

type Executor struct {
	store     *journal.Store
	client    *compute.Client
	actor     journal.Owner
	instance  control.Instance
	resources *resourceguard.Client
	claims    *budgetclaims.Reader
	runner    *vmguard.Reader
}

type audit struct {
	ProtocolVersion uint32                     `json:"protocol_version"`
	Query           bool                       `json:"query"`
	Fence           control.FenceResponse      `json:"issuer_fence"`
	Shared          control.SharedCostResponse `json:"shared"`
	Response        control.AdmissionResponse  `json:"response"`
	PGAggregate     budget.RootAggregate       `json:"pg_aggregate"`
	ResourceEpoch   control.Uint64             `json:"resource_issuer_epoch"`
}

func New(store *journal.Store, client *compute.Client, actor journal.Owner, resources ...*resourceguard.Client) (*Executor, error) {
	if store == nil || client == nil || actor.Validate() != nil {
		return nil, fmt.Errorf("admission executor requires fixed journal/client/owner")
	}
	instance, epoch := client.Binding()
	if actor.Epoch != epoch {
		return nil, fmt.Errorf("admission owner differs from client")
	}
	if len(resources) > 1 {
		return nil, fmt.Errorf("admission requires at most one original resource client")
	}
	var root *resourceguard.Client
	if len(resources) == 1 {
		root = resources[0]
	}
	if root != nil {
		id, epoch := root.Binding()
		if id.VMUID != instance.VMUID || id.ComputeInstanceID != instance.ComputeInstanceID || epoch != actor.Epoch {
			return nil, fmt.Errorf("admission resource domain differs from owner/Compute")
		}
	}
	// nil仅保留历史原命令的只读可达性。任何新开放、显式重放或完成落账
	// 都必须在下面核验已持久绑定的原root；不能把缺资源域当作兼容放行。
	return &Executor{store: store, client: client, actor: actor, instance: instance, resources: root, claims: nil, runner: nil}, nil
}

// NewWithClaims保留仅含Outside/Host来源的历史领域接线；实际Agent使用下面
// 的NewWithRunner。旧New仅保留已有模块/历史读取，不能消费带新Sources
// 但缺少可信读者的结算；本构造器也不能借缺Runner读者降级当前预算。
func NewWithClaims(store *journal.Store, client *compute.Client, actor journal.Owner, claims *budgetclaims.Reader, resources *resourceguard.Client) (*Executor, error) {
	if claims == nil {
		return nil, fmt.Errorf("production admission requires original reservation reader")
	}
	e, err := New(store, client, actor, resources)
	if err != nil {
		return nil, err
	}
	e.claims = claims
	return e, nil
}

// 实际生产入口在预算/Guest来源之外必须核原Runner持久回执。旧构造器保留历史
// 读取和已有领域测试，不允许无读者消费新Runner来源。
func NewWithRunner(store *journal.Store, client *compute.Client, actor journal.Owner, claims *budgetclaims.Reader, resources *resourceguard.Client, runner *vmguard.Reader) (*Executor, error) {
	if runner == nil {
		return nil, fmt.Errorf("production admission requires original Runner reader")
	}
	e, err := NewWithClaims(store, client, actor, claims, resources)
	if err != nil {
		return nil, err
	}
	e.runner = runner
	return e, nil
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
	if replay == nil && op.AbortRequested {
		if reconciledCleanup(op) {
			_, progress, err := e.advanceReconciledAbort(ctx, snapshot, nil, false)
			return progress, err
		}
		if handled, progress, err := e.advanceCommittedAbort(ctx, snapshot, nil); handled {
			return progress, err
		}
		if handled, progress, err := e.advanceUnchangedAbort(ctx, snapshot, nil, false); handled {
			return progress, err
		}
		if handled, progress, err := e.advanceReconciledAbort(ctx, snapshot, nil, false); handled {
			return progress, err
		}
	}
	if replay != nil && (*replay != op.Request.Key || op.Origin != e.actor || op.AbortRequested) {
		return pgexec.AwaitingFact, fmt.Errorf("original admission is not eligible for replay")
	}
	var step *journal.Step
	var finalized *journal.Step
	var admitted *journal.Step
	for i := range op.Steps {
		s := &op.Steps[i]
		if s.Action == journal.FinalizeBudget {
			finalized = s
		}
		if s.Action == journal.ResumeAdmission {
			admitted = s
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
	// 正常完成不能停在所有步骤Settled、Active却永久占槽的状态。只有正式
	// Runner/来源读者齐全的生产执行器承担结案；旧领域构造器保留原边界。
	// 结案独立一轮，复用下面完整原开放查询和PG/Root/Host/Runner双重核验，
	// 绝不从历史Completed直接Finish，更不重发setter或释放任何资源承诺。
	finishing := step == nil && !op.AbortRequested && replay == nil && e.runner != nil && e.claims != nil
	if finishing {
		step = admitted
		if step == nil || step.Phase != journal.Settled || step.Observation == nil || step.Observation.Result != journal.Completed {
			return pgexec.AwaitingFact, fmt.Errorf("normal completion lacks original completed admission")
		}
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
	var request control.AdmissionRequest
	var fact control.AdmissionResponse
	query := step.Phase != journal.Planned && replay == nil
	if step.Phase != journal.Planned {
		if json.Unmarshal(step.Command, &request) != nil || request.Validate() != nil || !request.Open || request.Instance != e.instance || request.BudgetCommitmentID != op.Request.BudgetCommitmentID || step.DispatchedBy == nil || request.IssuerEpoch != step.DispatchedBy.Epoch || request.ExpectedCapacityGeneration != settled.Shared.Observed.CapacityGeneration || step.TargetID != e.instance.ComputeInstanceID {
			return pgexec.AwaitingFact, fmt.Errorf("original admission command differs from durable operation")
		}
	}
	if finishing {
		if err := validateCompletionAudit(*step, request, settled); err != nil {
			return pgexec.AwaitingFact, err
		}
	}
	if query {
		// 原开放可能已经生效，先查询完整原命令，即使root/PG此刻失联或约束
		// 漂移也不能隐藏它。原生确认可解除同一SDK未知，但在全部预算/VM再
		// 核验前，外层WAL仍是Unconfirmed，绝不重发、释放预算或提前完成。
		fact, err = e.client.AdmissionStatus(ctx, request)
		if err != nil || !fact.Confirms() {
			return pgexec.AwaitingFact, err
		}
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
	if step.Phase != journal.Planned {
		if request.ExpectedCapacityGeneration != shared.Observed.CapacityGeneration {
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
	}
	aggregateBefore, err := e.checkRoot(ctx, snapshot, settled)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	claimObservation, prepared, err := e.checkClaims(ctx, snapshot, settled, aggregateBefore)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	// PG/root/Kube不是原子快照；网络等待不持锁。再次直接读取同一VM并
	// 要求RV未变，封住采样期间Runner/owner/原意图漂移；新写仍需Start
	// 的UID/RV CAS，显式重放也不得跳过此复核。外部长期承诺不由此补造。
	if err := e.sameSnapshot(ctx, snapshot); err != nil {
		return pgexec.AwaitingFact, err
	}
	if e.claims != nil {
		if state.ProtocolVersion == 2 && state.BudgetHeld == nil {
			// 仅旧Active原结算的显式在线升级。未知原开放已经先查询；完整来源
			// 不可用时保留原WAL，不能补写。升级先返回Recorded，由下一轮独立
			// 准入；绝不能在长期预算CAS尚未确认时发送新的开放。
			var finalizer *budgetexec.Executor
			if e.runner != nil {
				finalizer, err = budgetexec.New(e.store, e.client, e.resources, e.actor, e.claims, e.runner)
			} else {
				finalizer, err = budgetexec.New(e.store, e.client, e.resources, e.actor, e.claims)
			}
			if err != nil {
				return pgexec.AwaitingFact, err
			}
			return finalizer.RetainOriginal(ctx)
		}
		held, err := state.BudgetFor(op.Request)
		if err != nil || !reflect.DeepEqual(held, settled) {
			return pgexec.AwaitingFact, fmt.Errorf("production admission requires same atomically retained complete budget")
		}
	}
	if step.Phase == journal.Planned {
		command, _ := json.Marshal(request)
		var dispatch *journal.Dispatch
		snapshot, dispatch, err = e.store.Start(ctx, snapshot, e.actor, op.Request.Key, journal.ResumeAdmission, e.instance.ComputeInstanceID, command)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		step = &dispatch.Step
	}
	if !query {
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
	// Setter成功不等于资源仍归原来源：写后重核原root/Birth及同一VM版本。
	// 相同数值的新Held也会改变来源摘要，不能收养为原完整预算。失败保留
	// 已派发原命令供下次只读查询，不补发关闭/开放、不改原epoch或预算。
	aggregate, err := e.checkRoot(ctx, snapshot, settled)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if claimObservation != nil {
		if err := e.claims.Recheck(ctx, snapshot, prepared, settled.Sources.HostClaim, claimObservation); err != nil {
			return pgexec.AwaitingFact, err
		}
	}
	if _, _, err := e.checkClaims(ctx, snapshot, settled, aggregate); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := e.sameSnapshot(ctx, snapshot); err != nil {
		return pgexec.AwaitingFact, err
	}
	if finishing {
		// 只关闭完整原操作，并同CAS建立永久预算关闭边界供Host结清。CAS
		// 丢回复时不补写回执、不重造Key；下轮Load若Active已空即读取原结果。
		// 若失败未提交，仍保留原Active，必须重新核验后才可再尝试同一Finish。
		_, err = e.store.Finish(ctx, snapshot, e.actor, op.Request.Key)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		return pgexec.Recorded, nil
	}
	data, _ := json.Marshal(audit{ProtocolVersion: 2, Query: query, Fence: fence, Shared: shared, Response: fact, PGAggregate: aggregate, ResourceEpoch: e.actor.Epoch})
	_, err = e.store.Record(ctx, snapshot, e.actor, op.Request.Key, journal.Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: journal.ResumeAdmission, Result: journal.Completed, SourceID: e.instance.ComputeInstanceID, Data: data})
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.Recorded, nil
}

// checkRoot只确认Finalized中PG聚合这一来源仍真实持续；不把Outside声明升级
// 为已认证系统上界，也不认证Host承诺/免OOM。后续完整生产者须独立完成那些域。
// 调用前已经ValidateFor，原总量/去重成立；仍显式检查本函数的Outside求和溢出。
func (e *Executor) checkRoot(ctx context.Context, snapshot *journal.Snapshot, full budget.Finalized) (budget.RootAggregate, error) {
	if e.resources == nil || snapshot.State().ResourceIdentity == nil || full.Aggregate == nil {
		return budget.RootAggregate{}, fmt.Errorf("admission requires original authenticated aggregate resource domain")
	}
	id, _ := e.resources.Binding()
	if *snapshot.State().ResourceIdentity != id {
		return budget.RootAggregate{}, fmt.Errorf("admission original Guest binding changed")
	}
	if e.resources.Pending() != nil {
		// 原root命令仍待独立Observe，不能拿当前聚合读回清另一个SDK未知。
		// 外层原准入查询已在本函数之前执行，保持两领域的只读恢复可达。
		return budget.RootAggregate{}, resourceguard.ErrUnknown
	}
	view, err := e.resources.InspectState(ctx)
	if err != nil {
		return budget.RootAggregate{}, err
	}
	if view.IssuerFloorEpoch != e.actor.Epoch || view.Actual.GuestMemTotalBytes < full.GuestUsableMemoryBytes {
		return budget.RootAggregate{}, fmt.Errorf("admission resource owner or usable Guest changed")
	}
	aggregate, err := budget.RootAggregateFromView(view)
	if err != nil {
		return budget.RootAggregate{}, err
	}
	if aggregate.Bound != *full.Aggregate {
		return budget.RootAggregate{}, fmt.Errorf("admission original aggregate source was replaced")
	}
	limits, err := view.ConfirmedLimits()
	if err != nil {
		return budget.RootAggregate{}, err
	}
	outside := uint64(0)
	for _, b := range []budget.Bound{full.SystemOutsidePG, full.Transition, full.Safety} {
		if uint64(b.UpperBytes) > math.MaxUint64-outside {
			return budget.RootAggregate{}, fmt.Errorf("admission outside commitments overflow")
		}
		outside += uint64(b.UpperBytes)
	}
	if outside != uint64(limits.OutsideUpperBytes) {
		return budget.RootAggregate{}, fmt.Errorf("admission outside reservation differs from original root source")
	}
	process, err := e.resources.InspectProcess(ctx)
	if err != nil {
		return budget.RootAggregate{}, err
	}
	if process != aggregate.Birth {
		return budget.RootAggregate{}, fmt.Errorf("admission original Postmaster birth changed")
	}
	return aggregate, nil
}

func (e *Executor) sameSnapshot(ctx context.Context, original *journal.Snapshot) error {
	latest, err := e.store.Load(ctx)
	if err != nil {
		return err
	}
	if !latest.Dispatchable() || latest.ResourceVersion() != original.ResourceVersion() || latest.State().Owner != e.actor {
		return fmt.Errorf("admission VM/Runner/original journal changed during verification")
	}
	return nil
}

// checkClaims不把原Finalize回执当成永久活性证明。Missing/删后重建/正在释放均
// 在新开放前拒绝；已发送开放的原只读查询在本检查之前仍可达，外层保持Unknown。
func (e *Executor) checkClaims(ctx context.Context, s *journal.Snapshot, f budget.Finalized, a budget.RootAggregate) (*budgetclaims.Observed, budget.Prepared, error) {
	var p budget.Prepared
	if e.claims == nil {
		if f.Sources != nil {
			return nil, p, fmt.Errorf("new settlement requires authenticated reservation reader")
		}
		return nil, p, nil
	}
	if f.Sources == nil || f.Sources.Validate() != nil {
		return nil, p, fmt.Errorf("production admission requires original persistent budget sources")
	}
	op := s.State().Active
	if op == nil {
		return nil, p, fmt.Errorf("original budget operation missing")
	}
	var vmStep *journal.Step
	for i := range op.Steps {
		step := &op.Steps[i]
		if step.Action == journal.PrepareBudget {
			if step.Phase != journal.Settled || step.Observation == nil || step.Observation.Result != journal.Completed || json.Unmarshal(step.Observation.Data, &p) != nil {
				return nil, p, fmt.Errorf("prepared source unavailable")
			}
		}
		if step.Action == journal.ResizeVM {
			vmStep = step
		}
	}
	if p.ValidateFor(f.Request, a.Identity) != nil || p.OutsideClaim == nil || *p.OutsideClaim != f.Sources.OutsideClaim || p.SystemOutsidePG != f.SystemOutsidePG || p.Transition != f.Transition || p.Safety != f.Safety || p.GuestUsableMemoryBytes != f.GuestUsableMemoryBytes || vmStep == nil || vmStep.Phase != journal.Settled || vmStep.Observation == nil || vmStep.Observation.Result != journal.Completed {
		return nil, p, fmt.Errorf("final budget differs from retained prepared/VM source")
	}
	hash := sha256.Sum256(vmStep.Observation.Data)
	var fact budget.VMSettlement
	if hex.EncodeToString(hash[:]) != f.Sources.VMReceiptSHA256 || json.Unmarshal(vmStep.Observation.Data, &fact) != nil || fact.ValidateFor(p, a, s.VirtualMachine().Spec.DSGABootstrap.Incarnation) != nil || fact.HostClaim != f.Sources.HostClaim || fact.Host.SourceID != f.HostReservationID || vmStep.TargetID != fact.Host.RunnerPodUID {
		return nil, p, fmt.Errorf("original final VM receipt was replaced")
	}
	observed, err := e.claims.Read(ctx, s, p, f.Sources.HostClaim)
	if err != nil {
		return nil, p, err
	}
	if !reflect.DeepEqual(observed.Host, fact.Host) {
		return nil, p, fmt.Errorf("final VM Host commitment changed")
	}
	if e.runner != nil {
		if f.Sources.Runner == nil || fact.Runner == nil || *f.Sources.Runner != *fact.Runner {
			return nil, p, fmt.Errorf("production admission lacks original retained Runner sources")
		}
		var original budget.FinalizationCommand
		var final *journal.Step
		for i := range op.Steps {
			if op.Steps[i].Action == journal.FinalizeBudget {
				final = &op.Steps[i]
				break
			}
		}
		if final == nil || json.Unmarshal(final.Command, &original) != nil || original.Validate() != nil || !reflect.DeepEqual(original.Finalized, f) {
			return nil, p, fmt.Errorf("original finalization command changed before Runner verification")
		}
		proof, err := e.runner.CheckRetained(ctx, *fact.Runner, f.Request, original.PreparedSHA256, original.GuestSHA256, f.Sources.HostClaim, e.actor)
		if err != nil {
			return nil, p, err
		}
		var cmd vmguard.Command
		if json.Unmarshal(vmStep.Command, &cmd) != nil || cmd != proof.Receipt.Intent.Command || fact.Runner.JournalSHA256 != vmStep.CommandSHA256 || proof.Actual.ActualBytes != fact.ActualMemoryBytes {
			return nil, p, fmt.Errorf("admission original VM/Runner command differs")
		}
	} else if f.Sources.Runner != nil || fact.Runner != nil {
		return nil, p, fmt.Errorf("runner-backed admission requires authenticated original reader")
	}
	return observed, p, nil
}
