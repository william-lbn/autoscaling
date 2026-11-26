package owner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"reflect"
	"time"
)

// 只能在原生产 worker 启动前由可信配置固定来源。管理选择只含原来源内的ID，
// 不接收预算数字、Root命令、Host许可或新身份。恢复旧操作始终读取其原Plan。
func (w *PGWorker) ConfigureCacheAllocations(source budget.QualificationSource) error {
	if w.running.Load() || w.cacheAllocationSource != nil || source.Validate() != nil || w.productionCore == nil ||
		w.resourceOwner == nil || w.prepare == nil || w.finalize == nil || w.qualification == nil {
		return fmt.Errorf("cache allocations require an unstarted complete original production worker")
	}
	if _, err := budgetclaims.New(w.productionCore, w.session.binding); err != nil {
		return err
	}
	frozen := source
	w.cacheAllocationSource = &frozen
	return nil
}

func (w *PGWorker) cacheClaims() (*budgetclaims.Reader, error) {
	if w.productionCore == nil {
		return nil, fmt.Errorf("cache allocation original Kubernetes client unavailable")
	}
	return budgetclaims.New(w.productionCore, w.session.binding)
}

func (w *PGWorker) beginCacheAllocation(ctx context.Context, request control.Request, choice control.CacheAllocationChoice) (*journal.Snapshot, journal.Disposition, error) {
	if choice.Validate() != nil || w.resourceOwner == nil || w.client == nil {
		return nil, journal.Stale, fmt.Errorf("cache allocation source is not configured")
	}
	if err := w.session.Check(ctx); err != nil {
		return nil, journal.Stale, err
	}
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return nil, journal.Stale, err
	}
	m := s.State()
	// 原接受的重试先读冻结计划，不借当前来源或新观测改写旧选择。
	var existing *journal.Operation
	if m.Active != nil && m.Active.Request.Key == request.Key {
		existing = m.Active
	}
	for i := range m.History {
		if m.History[i].Operation.Request.Key == request.Key {
			existing = &m.History[i].Operation
		}
	}
	if existing != nil {
		p := existing.AllocationPlan
		if p == nil || existing.Request != request || p.Scope != choice.Scope ||
			p.Transition.From.ID != choice.FromAllocationID || p.Transition.To.ID != choice.ToAllocationID {
			return s, journal.Conflict, nil
		}
		return w.session.store.BeginAllocation(ctx, s, w.session.Actor(), *p, uint64(time.Now().UnixMilli()))
	}
	if w.cacheAllocationSource == nil {
		return nil, journal.Stale, fmt.Errorf("new cache allocation source is not configured")
	}
	if !s.Dispatchable() || m.Active != nil || m.ServiceClosure != nil || m.Owner != w.session.Actor() ||
		request.Key.IssuerEpoch != m.Owner.Epoch || m.ResourceIdentity == nil || m.PGInstance == nil ||
		request.Key.Instance != *m.PGInstance || w.client.Pending() != nil || w.client.PendingLFCAllocation() != nil {
		return nil, journal.Stale, fmt.Errorf("cache allocation requires idle original bound domains")
	}
	claims, err := w.cacheClaims()
	if err != nil {
		return nil, journal.Stale, err
	}
	transition, err := claims.AllocationIDs(ctx, *w.cacheAllocationSource, choice.FromAllocationID, choice.ToAllocationID)
	if err != nil {
		return nil, journal.Stale, err
	}
	if transition.Base != *w.qualification {
		return nil, journal.Stale, fmt.Errorf("allocation parent differs from production qualification")
	}
	native, err := w.client.LFCAllocationState(ctx)
	if err != nil {
		return nil, journal.Stale, err
	}
	pg, err := w.client.Memory(ctx)
	if err != nil {
		return nil, journal.Stale, err
	}
	if native.Phase == "unconfirmed" || native.Observed.SharedLimitBytes != transition.From.LFCLimitBytes ||
		native.Observed.PrewarmActive || pg.Observed.LowBuffers != pg.Observed.HighBuffers ||
		pg.Observed.HighBuffers != transition.From.Buffers || pg.Observed.CapacityGeneration != request.ExpectedCapacityGeneration ||
		native.Observed.CapacityGeneration != pg.Observed.CapacityGeneration {
		return nil, journal.Stale, fmt.Errorf("allocation original PG/LFC vector changed or remains unknown")
	}
	p := budget.AllocationPlan{ProtocolVersion: 1, Scope: choice.Scope, Request: request, Resource: *m.ResourceIdentity,
		Transition: transition, ExpectedLFCGeneration: native.Observed.LFCGeneration,
		ExpectedLFCSequence: native.Observed.LFCSequence, LFCMaximumBytes: native.Observed.MaxBytes,
		LFCGranuleBytes: native.Observed.ChunkBytes, HoldCompleteParent: true}
	if p.Validate() != nil {
		return nil, journal.Stale, fmt.Errorf("allocation plan does not match exact original target")
	}
	if _, err = w.auditCacheAllocation(ctx, s, p, false); err != nil {
		return nil, journal.Stale, err
	}
	latest, err := w.session.store.Load(ctx)
	if err != nil {
		return nil, journal.Stale, err
	}
	if latest.ResourceVersion() != s.ResourceVersion() {
		return nil, journal.Stale, fmt.Errorf("cache plan changed before original acceptance")
	}
	millis := time.Now().UnixMilli()
	if millis <= 0 {
		return nil, journal.Stale, fmt.Errorf("invalid cache allocation wall clock")
	}
	return w.session.store.BeginAllocation(ctx, latest, w.session.Actor(), p, uint64(millis))
}

// 审核原来源、Root实际硬限和Runner实际内存；不消费LFC文件长度或配置值结算。
// 持有预算的来源在原正式Reader复核，旧预算和新Prepare仍属于各自原生命周期。
func (w *PGWorker) auditCacheAllocation(ctx context.Context, s *journal.Snapshot, p budget.AllocationPlan, active bool) (journal.CacheAllocationAudit, error) {
	var audit journal.CacheAllocationAudit
	if err := w.session.Check(ctx); err != nil {
		return audit, err
	}
	claims, err := w.cacheClaims()
	if err != nil {
		return audit, err
	}
	from, err := p.Transition.From.Selector()
	if err != nil {
		return audit, err
	}
	to, err := p.Transition.To.Selector()
	if err != nil {
		return audit, err
	}
	actual, err := claims.AllocationTransition(ctx, p.Transition.Source, from, to)
	if err != nil || !reflect.DeepEqual(actual, p.Transition) {
		return audit, fmt.Errorf("original exact allocation source changed")
	}
	base, err := claims.Qualification(ctx, p.Transition.Base)
	if err != nil || base.Identity != p.Resource {
		return audit, fmt.Errorf("original cache parent resource changed")
	}
	view, err := w.resourceOwner.client.InspectState(ctx)
	if err != nil {
		return audit, err
	}
	limits, err := view.ConfirmedLimits()
	if err != nil || view.Identity != p.Resource || !reflect.DeepEqual(limits, p.Transition.PeakLimits) ||
		view.IssuerFloorEpoch != w.session.Actor().Epoch || view.Birth == nil {
		return audit, fmt.Errorf("cache allocation lost original complete normal budget")
	}
	process, err := w.resourceOwner.client.InspectProcess(ctx)
	if err != nil || process != *view.Birth {
		return audit, fmt.Errorf("cache allocation original Root process changed")
	}
	bootstrap, err := json.Marshal(view.Bootstrap)
	if err != nil || vmguard.SourceDigest(bootstrap) != base.RootBootstrapSHA256 {
		return audit, fmt.Errorf("cache allocation original bootstrap changed")
	}
	vm := s.VirtualMachine()
	if vm.Status.PodName != base.RunnerPodName || vm.Status.RestartCount != base.RunnerRestartCount || vm.Status.Node != base.NodeName {
		return audit, fmt.Errorf("cache allocation qualified Runner changed")
	}
	runner, err := vmguard.NewReader(w.productionCore, w.session.binding, string(w.session.leaseUID))
	if err != nil {
		return audit, err
	}
	birth, ref, publication, err := runner.Inspect(ctx, &base.RunnerBirth, w.session.Actor())
	if err != nil {
		return audit, err
	}
	rawIdentity, err := json.Marshal(birth.Identity)
	if err != nil || ref != base.RunnerBirth || vmguard.SourceDigest(rawIdentity) != base.RunnerIdentitySHA256 ||
		birth.Identity.NodeUID != base.NodeUID || birth.Identity.RunnerPodUID != base.RunnerPodUID ||
		publication.State.Pending != nil || publication.Actual.RequestedBytes != publication.Actual.ActualBytes ||
		publication.Actual.ActualBytes != p.Transition.Parent.VMMemoryBytes || vm.Status.MemorySize == nil {
		return audit, fmt.Errorf("cache allocation lost actual original same-VM Runner capacity")
	}
	memory, exact := vm.Status.MemorySize.AsInt64()
	if !exact || memory <= 0 || control.Uint64(memory) != publication.Actual.ActualBytes {
		return audit, fmt.Errorf("cache allocation VM status differs from actual")
	}
	// 首次服务有独立持久来源，不能用Active存在绕过其未知；普通头沿原正式读取。
	if s.State().InitialHeld == nil {
		if active {
			_, err = claims.ReadActiveHeld(ctx, s)
		} else {
			_, err = claims.ReadCurrent(ctx, s)
		}
		if err != nil {
			return audit, err
		}
	} else {
		initial, e := s.State().InitialBudgetFor()
		if e != nil || !s.State().InitialAdmissionReady() || initial.Qualification != p.Transition.Base ||
			initial.VMMemoryBytes != p.Transition.Parent.VMMemoryBytes {
			return audit, fmt.Errorf("cache allocation original initial budget remains unknown or changed")
		}
		if _, err = claims.ReadInitialPlacement(ctx, s, initial.Qualification, initial.VMMemoryBytes); err != nil {
			return audit, err
		}
	}
	raw, err := json.Marshal(view)
	if err != nil {
		return audit, err
	}
	digest := sha256.Sum256(raw)
	audit = journal.CacheAllocationAudit{ProtocolVersion: 1, NormalIdentity: p.Resource, NormalLimits: limits, RootStateSHA256: hex.EncodeToString(digest[:])}
	latest, err := w.session.store.Load(ctx)
	if err != nil {
		return audit, err
	}
	if latest.ResourceVersion() != s.ResourceVersion() || latest.State().Owner != w.session.Actor() || !latest.Dispatchable() {
		return audit, fmt.Errorf("cache allocation original journal changed during audit")
	}
	return audit, nil
}

func (w *PGWorker) advanceCacheAllocation(ctx context.Context) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Active == nil || m.Active.AllocationPlan == nil {
		return pgexec.ExternalStep, nil
	}
	op := m.Active
	var step *journal.Step
	for i := range op.Steps {
		if op.Steps[i].Phase != journal.Settled {
			step = &op.Steps[i]
			break
		}
	}
	if step == nil || step.Action != journal.RepartitionLFCBefore && step.Action != journal.RepartitionLFCAfter {
		return pgexec.ExternalStep, nil
	}
	if step.Phase == journal.Planned && (op.AbortRequested || op.Origin != w.session.Actor()) {
		return pgexec.ExternalStep, nil
	}
	p := *op.AllocationPlan
	audit, err := w.auditCacheAllocation(ctx, s, p, true)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	command, err := op.LFCCommand(*step)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	var fact control.LFCAllocationResponse
	if step.Phase == journal.Planned {
		fence, e := w.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: p.Request.Key.Instance, IssuerEpoch: w.session.Actor().Epoch})
		if e != nil {
			return pgexec.AwaitingFact, e
		}
		if fence.Disposition != control.Fenced {
			return pgexec.AwaitingFact, nil
		}
		if fence.Observed.CapacityGeneration != command.ExpectedCapacityGeneration {
			return pgexec.AwaitingFact, fmt.Errorf("cache native PG capacity changed before dispatch")
		}
		data, _ := json.Marshal(command)
		hash, _ := p.SHA256()
		var dispatch *journal.Dispatch
		s, dispatch, err = w.session.store.Start(ctx, s, w.session.Actor(), op.Request.Key, step.Action, "cache-"+hash, data)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		if dispatch == nil {
			return pgexec.AwaitingFact, fmt.Errorf("cache original dispatch was not acknowledged")
		}
		// 凭据调用仍实时Check；写前CAS丢回复绝不进入此分支。
		fact, err = w.client.AllocateLFC(ctx, command)
	} else {
		fact, err = w.client.LFCAllocationStatus(ctx, command)
	}
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if fact.ValidateRequest(command) != nil {
		return pgexec.AwaitingFact, fmt.Errorf("cache response changed original native intent")
	}
	result := journal.Completed
	if !fact.AppliedFor(command) {
		if fact.Disposition == "rejected" || fact.Disposition == "busy" || fact.Disposition == "deadline_expired" {
			result = journal.Rejected
		} else {
			return pgexec.AwaitingFact, nil
		}
	} else {
		// 只读执行连接握手既等待此前SQL退出，又解除本地旧SDK pending。
		fact, err = w.client.AcknowledgeLFCAllocation(ctx, command)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		if !fact.AppliedFor(command) || step.Action == journal.RepartitionLFCBefore && !fact.SynchronousExitFor(command) {
			return pgexec.AwaitingFact, nil
		}
	}
	// 外部执行和最终CAS之间再读实际Root完整父限制及同一份原资格。
	audit, err = w.auditCacheAllocation(ctx, s, p, true)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	audit.Response = fact
	data, err := json.Marshal(audit)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	current := s.State()
	var saved journal.Step
	for _, value := range current.Active.Steps {
		if value.Ordinal == step.Ordinal {
			saved = value
			break
		}
	}
	observation := journal.Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: saved.CommandSHA256,
		Ordinal: saved.Ordinal, Action: saved.Action, Result: result, SourceID: saved.TargetID, Data: data}
	_, err = w.session.store.Record(ctx, s, w.session.Actor(), op.Request.Key, observation)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.Recorded, nil
}
