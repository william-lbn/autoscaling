package owner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 只组合当前writer确切Grow在途时的原已付目标。它不是另一预算生产者，
// 不能据WAL MAY/observer Preparing自行制造live task，也不缓存一次审计续租。
// Shrink的before/target/Quiesce/release顺序尚未由本入口证明，继续原同步路径。
type pgInflightGrowProof struct {
	prepareCommand                                    budget.PrepareCommand
	prepared                                          budget.Prepared
	guest                                             resourceexec.Audit
	vm                                                budget.VMSettlement
	host                                              budget.HostReservation
	preparedStep, hostStep, vmStep, guestStep, pgStep *journal.Step
}

func (w *PGWorker) pgInflightAuditSelected(s *journal.Snapshot) bool {
	if w.executor == nil || !w.executor.InFlightEnabled() || s == nil {
		return false
	}
	m := s.State()
	if m.Active == nil {
		return false
	}
	// 异步能力只针对Grow；C执行器保持Shrink同步，不能让不支持kind落部分审计。
	if m.Active.Request.Kind != control.Grow {
		return w.executor.MatchInFlight(s, m.Active.Request)
	}
	for _, step := range m.Active.Steps {
		if step.Action == journal.ResizePG && step.Phase == journal.Unconfirmed {
			return true
		}
	}
	return w.executor.MatchInFlight(s, m.Active.Request)
}

func pgInflightGrowBoundary(s *journal.Snapshot, actor journal.Owner, instance control.Instance) (*pgInflightGrowProof, error) {
	fail := func() (*pgInflightGrowProof, error) {
		return nil, fmt.Errorf("inflight PG paid audit lost exact original Grow boundary")
	}
	if s == nil || !s.Dispatchable() {
		return fail()
	}
	m := s.State()
	op := m.Active
	if m.Validate() != nil || m.Owner != actor || op == nil || op.Origin != actor || op.AbortRequested || op.Cleanup != nil || m.ServiceClosure != nil || m.ResourceIdentity == nil || m.PGInstance == nil || *m.PGInstance != instance {
		return fail()
	}
	r := op.Request
	if r.Validate() != nil || r.Kind != control.Grow || r.Key.Instance != instance || r.Key.IssuerEpoch != actor.Epoch || uint64(r.DeadlineUnixMillis) <= uint64(time.Now().UnixMilli()) {
		return fail()
	}
	order := []journal.Action{journal.PrepareBudget, journal.ReserveHost, journal.ResizeVM, journal.GuestReady, journal.ResizePG, journal.FinalizeBudget, journal.ResumeAdmission}
	if len(op.Steps) != len(order) {
		return fail()
	}
	for i := range op.Steps {
		step := &op.Steps[i]
		if step.Action != order[i] || step.Ordinal != control.Uint64(i+1) {
			return fail()
		}
		if i < 5 {
			if step.DispatchedBy == nil || *step.DispatchedBy != actor || len(step.Command) == 0 {
				return fail()
			}
		}
		if i < 4 && (step.Phase != journal.Settled || step.Observation == nil || step.Observation.Result != journal.Completed) {
			return fail()
		}
		if i > 4 && (step.Phase != journal.Planned || len(step.Command) != 0 || step.Observation != nil || step.DispatchedBy != nil) {
			return fail()
		}
	}
	p := &pgInflightGrowProof{preparedStep: &op.Steps[0], hostStep: &op.Steps[1], vmStep: &op.Steps[2], guestStep: &op.Steps[3], pgStep: &op.Steps[4]}
	decoded, err := control.DecodeRequest(p.pgStep.Command)
	if err != nil || decoded != r || p.pgStep.TargetID != instance.ComputeInstanceID || (p.pgStep.Phase != journal.Unconfirmed && p.pgStep.Phase != journal.Settled) {
		return fail()
	}
	if p.pgStep.Phase == journal.Settled {
		var audit struct {
			ProtocolVersion uint32                  `json:"protocol_version"`
			Fence           control.FenceResponse   `json:"issuer_fence"`
			Response        control.ControlResponse `json:"response"`
		}
		if p.pgStep.Observation == nil || p.pgStep.Observation.Result != journal.Completed || control.UnmarshalObject(p.pgStep.Observation.Data, &audit) != nil || audit.ProtocolVersion != 1 || audit.Response.ValidateResize(r, instance) != nil || audit.Response.Receipt.Result == nil || audit.Response.Receipt.Result.Operation == nil || audit.Response.Receipt.Result.Operation.Phase != control.Committed {
			return fail()
		}
	}
	if p.preparedStep.TargetID != instance.ComputeInstanceID || p.guestStep.TargetID != instance.ComputeInstanceID || json.Unmarshal(p.preparedStep.Observation.Data, &p.prepared) != nil || p.prepared.ValidateFor(r, *m.ResourceIdentity) != nil || p.prepared.OutsideClaim == nil {
		return fail()
	}
	if json.Unmarshal(p.preparedStep.Command, &p.prepareCommand) != nil || p.prepareCommand.Validate() != nil || p.prepareCommand.OutsideName != p.prepared.OutsideClaim.Name {
		return fail()
	}
	unpublished := p.prepared
	unpublished.OutsideClaim = nil
	if !reflect.DeepEqual(unpublished, p.prepareCommand.Prepared) {
		return fail()
	}
	if json.Unmarshal(p.hostStep.Observation.Data, &p.host) != nil || p.host.ValidateFor(p.prepared, m.Incarnation) != nil {
		return fail()
	}
	if json.Unmarshal(p.vmStep.Observation.Data, &p.vm) != nil || p.vm.Runner == nil || p.vm.Host != p.host {
		return fail()
	}
	if json.Unmarshal(p.guestStep.Observation.Data, &p.guest) != nil || p.guest.ProtocolVersion != 2 || p.guest.Command.Validate() != nil || p.guest.Evidence.ValidateFor(p.guest.Command) != nil || p.guest.PGAggregate.Validate() != nil || p.guest.PGAggregate.Identity != *m.ResourceIdentity || p.guest.PGAggregate.Held == nil || !reflect.DeepEqual(*p.guest.PGAggregate.Held, p.guest.Command) {
		return fail()
	}
	command, err := json.Marshal(p.guest.Command)
	if err != nil || !bytes.Equal(command, p.guestStep.Command) || p.guest.Command.Identity != *m.ResourceIdentity || p.guest.Command.IssuerEpoch != actor.Epoch || p.guest.Command.BudgetCommitmentID != r.BudgetCommitmentID || p.guest.RunnerPodName != p.prepared.RunnerPodName || p.guest.RunnerRestarts != p.prepared.RunnerRestartCount || p.guest.VMMemoryBytes != r.TargetVMMemoryBytes || p.guest.Evidence.GuestMemTotalBytes < p.prepared.GuestUsableMemoryBytes {
		return fail()
	}
	c := p.guest.Command
	limits := p.prepared.Limits
	if c.MemoryMaxBytes != limits.MemoryMaxBytes || c.PidsMaxTasks != limits.PidsMaxTasks || c.SystemMemoryMaxBytes != limits.SystemMemoryMaxBytes || c.AddressSpaceMaxBytes != limits.AddressSpaceMaxBytes || c.OutsideUpperBytes != limits.OutsideUpperBytes || !reflect.DeepEqual(c.HugePools, limits.HugePools) {
		return fail()
	}
	if p.guest.Fence.ValidateFor(control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: actor.Epoch}, instance) != nil || p.guest.Fence.Disposition != control.Fenced || p.prepared.ValidateCurrent(p.guest.Shared, actor.Epoch) != nil {
		return fail()
	}
	if p.vm.ValidateFor(p.prepared, p.guest.PGAggregate, m.Incarnation) != nil {
		return fail()
	}
	var hc struct {
		ProtocolVersion uint32           `json:"protocol_version"`
		Request         control.Request  `json:"request"`
		Claim           budget.ObjectRef `json:"claim"`
		PreparedSHA256  string           `json:"prepared_sha256"`
		HostSHA256      string           `json:"host_sha256"`
	}
	if control.UnmarshalObject(p.hostStep.Command, &hc) != nil || hc.ProtocolVersion != 1 || hc.Request != r || hc.Claim != p.vm.HostClaim || hc.PreparedSHA256 != vmguard.SourceDigest(p.preparedStep.Observation.Data) || hc.HostSHA256 != vmguard.SourceDigest(p.hostStep.Observation.Data) || p.hostStep.TargetID != p.host.SourceID {
		return fail()
	}
	var vmc vmguard.Command
	if json.Unmarshal(p.vmStep.Command, &vmc) != nil || vmc.Validate() != nil || vmc.Request != r || vmc.BeforeBytes != p.prepared.VMBeforeMemoryBytes || vmc.BindingClaim != p.vm.Runner.BirthClaim || vmc.HostClaim != p.vm.HostClaim || p.vmStep.CommandSHA256 != p.vm.Runner.JournalSHA256 || p.vmStep.TargetID != vmc.Identity.RunnerPodUID || vmc.Identity.RunnerPodUID != p.host.RunnerPodUID || vmc.Identity.NodeUID != p.host.NodeUID {
		return fail()
	}
	return p, nil
}

// 原ABI6 Grow在每批Populate调用前先把worst page upper写入committed_upper，
// 再置AllocationUnknown。这里只认该已预付范围，不把Unknown抹掉或作完成证明。
// p.Shared必须仍是原稳定、known的Prepare输入；通用ValidateCurrent完全不变。
func validatePGInflightGrowUpper(p budget.Prepared, current control.SharedCostResponse, epoch control.Uint64, normal control.Uint64) error {
	if current.ValidateFor(p.Request.Key.Instance) != nil || current.StartupMainUpperBytes != p.Shared.StartupMainUpperBytes || current.StartupArraysUpperBytes != p.Shared.StartupArraysUpperBytes {
		return fmt.Errorf("inflight PG actual shared startup/cost envelope changed")
	}
	return validatePGInflightGrowSnapshot(p, current.Observed, epoch, normal)
}

// Memory/Native observer原样包含四数组事实；用原已认证不可变main加实际每项upper，
// 逐项及求和均核原已付上界。不把跨调用进展误当源丢失，也不凭Key单独授权。
func validatePGInflightGrowSnapshot(p budget.Prepared, s control.Snapshot, epoch control.Uint64, normal control.Uint64) error {
	r := p.Request
	baseline := p.Shared.Observed
	if r.Kind != control.Grow || p.ValidateFor(r, p.Identity) != nil || s.Validate() != nil || s.Instance != r.Key.Instance || s.IssuerFloorEpoch != epoch || epoch != r.Key.IssuerEpoch || s.MaximumBuffers != baseline.MaximumBuffers || s.StartupSharedUpperBytes != baseline.StartupSharedUpperBytes || s.AdmissionOpen != baseline.AdmissionOpen || s.AdmissionRevision != baseline.AdmissionRevision || s.Operation == nil {
		return fmt.Errorf("inflight PG lacks original precharged range context")
	}
	op := s.Operation
	hash, err := r.ContextSHA256()
	if err != nil || op.Key != r.Key || op.Kind != r.Kind || op.ContextSHA256 != hash || op.TargetBuffers != r.TargetBuffers || op.ExpectedCapacityGeneration != r.ExpectedCapacityGeneration || op.CancelRequested || op.Replayed || op.Reason != control.ReasonNone {
		return fmt.Errorf("inflight PG operation differs from original live command")
	}
	switch op.Phase {
	case control.Preparing:
		if s.CapacityGeneration != r.ExpectedCapacityGeneration || s.LowBuffers != baseline.LowBuffers || s.HighBuffers != baseline.HighBuffers {
			return fmt.Errorf("inflight Preparing changed original published prefix")
		}
	case control.Committed:
		if r.ExpectedCapacityGeneration == control.Uint64(math.MaxUint64) || s.CapacityGeneration != r.ExpectedCapacityGeneration+1 || s.LowBuffers != r.TargetBuffers || s.HighBuffers != r.TargetBuffers || !op.CancelClosed {
			return fmt.Errorf("inflight committed observer does not match original target")
		}
	default:
		return fmt.Errorf("inflight PG phase is not bounded Preparing or committed observation")
	}
	quote, err := budget.GrowSharedUpper(p.Shared, r.TargetBuffers)
	if err != nil || quote > p.TargetSharedUpperBytes || p.TargetSharedUpperBytes > normal {
		return fmt.Errorf("inflight PG worst charged aggregate exceeds paid hard limit")
	}
	factor := (uint64(r.TargetBuffers) + uint64(baseline.HighBuffers) - 1) / uint64(baseline.HighBuffers)
	paid := make(map[control.ArrayKind]uint64, 4)
	original := make(map[control.ArrayKind]control.ArrayFact, 4)
	for _, a := range baseline.Arrays {
		if a.AllocationUnknown || uint64(a.CommittedUpperBytes) > math.MaxUint64/factor {
			return fmt.Errorf("inflight PG baseline range is unknown or overflowed")
		}
		paid[a.Array] = uint64(a.CommittedUpperBytes) * factor
		original[a.Array] = a
	}
	aggregate := uint64(p.Shared.StartupMainUpperBytes)
	for _, a := range s.Arrays {
		u, ok := paid[a.Array]
		if !ok || a.CommittedUpperBytes < original[a.Array].CommittedUpperBytes || uint64(a.CommittedUpperBytes) > u {
			return fmt.Errorf("inflight PG array exceeds original paid range")
		}
		// Grow只增长预付尾部；历史release/Provider字段必须仍属原基线，
		// 不能把并发Shrink或另一范围的回执拼成本次Populate许可。
		if uint64(a.CommittedUpperBytes) > math.MaxUint64-aggregate {
			return fmt.Errorf("inflight PG current aggregate overflows")
		}
		aggregate += uint64(a.CommittedUpperBytes)
		o := original[a.Array]
		if a.ReleaseState != o.ReleaseState || !reflect.DeepEqual(a.ReleaseKey, o.ReleaseKey) || a.ReleaseCapacityGeneration != o.ReleaseCapacityGeneration || a.Provider != o.Provider {
			return fmt.Errorf("inflight PG array release scope changed from original baseline")
		}
	}
	if aggregate > uint64(p.TargetSharedUpperBytes) || aggregate > uint64(normal) {
		return fmt.Errorf("inflight PG current aggregate exceeds original paid cap")
	}
	return nil
}

// 同一次审计允许原单Grow合法前进；已发布不得回退、各数组已计费用不得下降。
// AllocationUnknown可随原Populate变化，所有值仍须独立通过同一完整paid校验。
func pgInflightGrowProgress(before, after control.Snapshot) error {
	if before.Operation == nil || after.Operation == nil || after.CapacityGeneration < before.CapacityGeneration || before.Operation.Phase == control.Committed && after.Operation.Phase != control.Committed {
		return fmt.Errorf("inflight PG published capacity regressed during audit")
	}
	previous := make(map[control.ArrayKind]control.ArrayFact, 4)
	for _, a := range before.Arrays {
		previous[a.Array] = a
	}
	for _, a := range after.Arrays {
		p, ok := previous[a.Array]
		if !ok || a.CommittedUpperBytes < p.CommittedUpperBytes {
			return fmt.Errorf("inflight PG precharged array cost regressed during audit")
		}
	}
	if before.Operation.Phase == control.Committed && !reflect.DeepEqual(before.Arrays, after.Arrays) {
		return fmt.Errorf("inflight PG array changed after committed publication")
	}
	return nil
}

func (w *PGWorker) checkPGInflightRunner(ctx context.Context, s *journal.Snapshot, p *pgInflightGrowProof) (vmguard.Confirmed, error) {
	proof, err := w.safety.runner.CheckRetained(ctx, *p.vm.Runner, p.prepared.Request, vmguard.SourceDigest(p.preparedStep.Observation.Data), vmguard.SourceDigest(p.guestStep.Observation.Data), p.vm.HostClaim, w.session.Actor())
	if err != nil {
		return proof, err
	}
	var cmd vmguard.Command
	if json.Unmarshal(p.vmStep.Command, &cmd) != nil || proof.Receipt.Intent.Command != cmd || proof.Actual.ActualBytes != p.vm.ActualMemoryBytes || proof.Sources.CommandSHA256 != p.vm.RunnerCommandID || proof.Actual.RequestedBytes != proof.Actual.ActualBytes {
		return proof, fmt.Errorf("inflight PG original Runner current receipt changed")
	}
	vm := s.VirtualMachine()
	if vm.Status.MemorySize == nil {
		return proof, ErrServiceSafety
	}
	actual, exact := vm.Status.MemorySize.AsInt64()
	if !exact || actual <= 0 || control.Uint64(actual) != p.prepared.Request.TargetVMMemoryBytes || control.Uint64(actual) != proof.Actual.ActualBytes {
		return proof, fmt.Errorf("inflight PG target VM actual capacity changed")
	}
	return proof, nil
}

func pgInflightRootMatches(p *pgInflightGrowProof, view resourceguard.View, process resourceguard.ProcessIdentity, epoch control.Uint64) bool {
	return view.Identity == p.prepared.Identity && view.Birth != nil && process == *view.Birth && view.IssuerFloorEpoch == epoch && view.Applied && view.Held != nil && view.Original != nil && reflect.DeepEqual(*view.Held, p.guest.Command) && reflect.DeepEqual(*view.Original, p.guest.Command) && (view.Cancelled == nil || view.Cancelled.Revision < p.guest.Command.Revision) && view.Actual.GuestMemTotalBytes >= p.prepared.GuestUsableMemoryBytes && p.guest.PGAggregate.Match(view) == nil
}

func (w *PGWorker) inspectPGInflightService(ctx context.Context, s *journal.Snapshot) error {
	if ctx == nil || ctx.Err() != nil || ctx.Value(recoveryLeaseScopeKey{}) != nil || w.session == nil || w.executor == nil || !w.executor.InFlightEnabled() || w.safety == nil || w.resourceOwner == nil || w.qualification == nil {
		return ErrServiceSafety
	}
	instance, epoch := w.Binding()
	p, err := pgInflightGrowBoundary(s, w.session.Actor(), instance)
	if err != nil {
		return err
	}
	r := p.prepared.Request
	if p.prepareCommand.Qualification != *w.qualification {
		return ErrServiceSafety
	}
	if !w.executor.MatchInFlight(s, r) {
		return fmt.Errorf("inflight PG has no exact same live bounded writer task")
	}
	if pending, closing := w.client.PendingAdmission(); pending != nil || closing != nil {
		return ErrServiceSafety
	}
	if pending := w.client.Pending(); pending != nil && *pending != r {
		return ErrServiceSafety
	}
	if w.resourceOwner.client.Pending() != nil || w.resourceOwner.client.ArrayAuthorityPending() != nil {
		return ErrServiceSafety
	}
	if err = w.session.Check(ctx); err != nil {
		return err
	}
	m := s.State()
	q, err := w.safety.claims.Qualification(ctx, *w.qualification)
	if err != nil {
		return err
	}
	target, err := q.Target(r.TargetVMMemoryBytes)
	if err != nil {
		return err
	}
	system, transition, safety := target.Bounds(*w.qualification)
	if q.Identity != p.prepared.Identity || q.Incarnation != m.Incarnation || target.GuestUsableMemoryBytes != p.prepared.GuestUsableMemoryBytes || !reflect.DeepEqual(target.Limits, p.prepared.Limits) || p.prepared.SystemOutsidePG != system || p.prepared.Transition != transition || p.prepared.Safety != safety {
		return fmt.Errorf("inflight PG paid target qualification changed")
	}
	var initial *budgetclaims.InitialSources
	var held *budgetclaims.Current
	var original budget.Initial
	if m.InitialHeld != nil {
		original, err = m.InitialBudgetFor()
		if err == nil {
			initial, err = w.safety.claims.ReadInitialBudget(ctx, s, original)
		}
	} else {
		held, err = w.safety.claims.ReadActiveHeld(ctx, s)
	}
	if err != nil {
		return err
	}
	observed, err := w.safety.claims.Read(ctx, s, p.prepared, p.vm.HostClaim)
	if err != nil {
		return err
	}
	if observed.Host != p.host {
		return ErrServiceSafety
	}
	runner, err := w.checkPGInflightRunner(ctx, s, p)
	if err != nil {
		return err
	}
	view, err := w.resourceOwner.client.InspectState(ctx)
	if err != nil {
		return err
	}
	process, err := w.resourceOwner.client.InspectProcess(ctx)
	if err != nil {
		return err
	}
	if !pgInflightRootMatches(p, view, process, epoch) {
		return fmt.Errorf("inflight PG lacks actual settled target Guest/Root hard boundary")
	}
	limits, err := view.ConfirmedLimits()
	if err != nil || !reflect.DeepEqual(limits, p.prepared.Limits) {
		return fmt.Errorf("inflight PG current target hard limits changed")
	}
	root, err := budget.RootAggregateFromView(view)
	if err != nil || p.vm.ValidateFor(p.prepared, root, m.Incarnation) != nil {
		return ErrServiceSafety
	}
	boot, err := json.Marshal(view.Bootstrap)
	if err != nil || q.RootBootstrapSHA256 != vmguard.SourceDigest(boot) || q.RunnerPodName != s.VirtualMachine().Status.PodName || q.RunnerPodUID != runner.Birth.Identity.RunnerPodUID || q.NodeName != s.VirtualMachine().Status.Node || q.NodeUID != runner.Birth.Identity.NodeUID || q.RunnerRestartCount != s.VirtualMachine().Status.RestartCount {
		return ErrServiceSafety
	}
	shared, err := w.client.SharedCost(ctx)
	if err != nil {
		return err
	}
	if err = validatePGInflightGrowUpper(p.prepared, shared, epoch, limits.MemoryMaxBytes); err != nil {
		return err
	}
	memory, err := w.client.Memory(ctx)
	if err != nil || validatePGInflightGrowSnapshot(p.prepared, memory.Observed, epoch, limits.MemoryMaxBytes) != nil || pgInflightGrowProgress(shared.Observed, memory.Observed) != nil {
		return fmt.Errorf("inflight PG changed across full observer capture")
	}
	if pending := w.client.Pending(); pending == nil && memory.Observed.Operation.Phase != control.Committed || pending != nil && *pending != r {
		return ErrServiceSafety
	}
	// 所有实际来源在同次审计末再次读取；没有一次成功后台心跳或旧proof缓存。
	if initial != nil {
		err = w.safety.claims.RecheckInitialBudget(ctx, s, original, initial)
	} else {
		err = w.safety.claims.RecheckActiveHeld(ctx, s, held)
	}
	if err != nil {
		return err
	}
	if err = w.safety.claims.Recheck(ctx, s, p.prepared, p.vm.HostClaim, observed); err != nil {
		return err
	}
	latestQ, err := w.safety.claims.Qualification(ctx, *w.qualification)
	if err != nil || !reflect.DeepEqual(q, latestQ) {
		return ErrServiceSafety
	}
	latestRoot, err := w.resourceOwner.client.InspectState(ctx)
	if err != nil {
		return ErrServiceSafety
	}
	latestProcess, err := w.resourceOwner.client.InspectProcess(ctx)
	if err != nil || latestProcess != process || !pgInflightRootMatches(p, latestRoot, latestProcess, epoch) {
		return ErrServiceSafety
	}
	latestRunner, err := w.checkPGInflightRunner(ctx, s, p)
	if err != nil || !reflect.DeepEqual(runner, latestRunner) {
		return ErrServiceSafety
	}
	latest, err := w.client.SharedCost(ctx)
	if err != nil || validatePGInflightGrowUpper(p.prepared, latest, epoch, limits.MemoryMaxBytes) != nil || pgInflightGrowProgress(memory.Observed, latest.Observed) != nil {
		return fmt.Errorf("inflight PG latest actual paid range invalid or regressed")
	}
	if pending := w.client.Pending(); pending == nil && latest.Observed.Operation.Phase != control.Committed || pending != nil && *pending != r {
		return ErrServiceSafety
	}
	if w.client.AdmissionPolicy() == control.AdmissionPolicyEnforcedBudget {
		if err := w.checkEnforcedObservation(ctx, s, latest.Observed); err != nil {
			return err
		}
		// 同一个在途执行器和业务命令期限仍是必要条件，管理续租不参与。
		if ctx.Err() != nil || !w.executor.MatchInFlight(s, r) || uint64(r.DeadlineUnixMillis) <= uint64(time.Now().UnixMilli()) {
			return ErrServiceSafety
		}
		return nil
	}
	fact, err := w.observeAdmissionLease(ctx)
	if err != nil {
		return err
	}
	if !fact.Armed || fact.Expired || fact.LeaseEpoch != epoch || !reflect.DeepEqual(fact.Observed, latest.Observed) {
		return ErrServiceSafety
	}
	if ctx.Err() != nil || !w.executor.MatchInFlight(s, r) || uint64(r.DeadlineUnixMillis) <= uint64(time.Now().UnixMilli()) {
		return ErrServiceSafety
	}
	// 保留原helper Session.Check→末独立VM Load/RV，再沿原Native期限CAS；
	// 这不是跨API原子事务。回复变化/未知仍拒绝，不把可能续租的请求标成NoEffect。
	return w.renewAuditedAdmissionLease(ctx, s, fact, &latest.Observed)
}
