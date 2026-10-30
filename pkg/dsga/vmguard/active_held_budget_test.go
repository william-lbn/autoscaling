package vmguard

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 永久头只经真实Store的Begin/Start/Record/Finish安装；Runner使用真实WAL、
// flock/fsync和固定Kube来源读者。PG/Root/Host为显式完整领域合同，QMP为
// memoryFixture；本组只认证旧预算读取，不能称Linux热拔或整机E2E。
type activeHeldFixture struct {
	f         *runnerFixture
	reader    *Reader
	command   Command
	prepared  budget.Prepared
	host      budget.HostReservation
	root      budget.RootAggregate
	confirmed Confirmed
	final     budget.FinalizationCommand
}

func newActiveHeldFixture(t *testing.T, actual bool) *activeHeldFixture {
	t.Helper()
	ctx := context.Background()
	f := newRunnerFixture(t)
	c, outer := f.beginGrow(t)
	f.runner.engine.native = &memoryFixture{mu: sync.Mutex{}, dir: f.config.StateDir, actual: Observation{RequestedBytes: c.BeforeBytes, ActualBytes: c.BeforeBytes}, writes: 0, apply: true, err: nil, entered: nil, release: nil}
	require.NoError(t, f.runner.Tick(ctx))
	reader, err := NewReader(f.core.CoreV1(), f.store.Binding(), "lease-uid")
	require.NoError(t, err)
	proof, err := reader.ObserveOriginal(ctx, c, outer, f.actor)
	require.NoError(t, err)
	s, err := f.store.Load(ctx)
	require.NoError(t, err)
	var p budget.Prepared
	var h budget.HostReservation
	for _, step := range s.State().Active.Steps {
		switch step.Action {
		case journal.PrepareBudget:
			require.NoError(t, json.Unmarshal(step.Observation.Data, &p))
		case journal.ReserveHost:
			require.NoError(t, json.Unmarshal(step.Observation.Data, &h))
		}
	}
	// 该完整Root只为预算合同，不由Reader认证；真实Runner来源独立取得。
	rootCommand := resourceguard.Command{ProtocolVersion: 1, Identity: p.Identity, IssuerEpoch: f.actor.Epoch, Revision: 1, BudgetCommitmentID: c.Request.BudgetCommitmentID, PostmasterPID: 123, PostmasterStartTicks: 456, MemoryMaxBytes: p.Limits.MemoryMaxBytes, OutsideUpperBytes: p.Limits.OutsideUpperBytes, HugePools: p.Limits.HugePools, PidsMaxTasks: p.Limits.PidsMaxTasks, SystemMemoryMaxBytes: p.Limits.SystemMemoryMaxBytes, AddressSpaceMaxBytes: p.Limits.AddressSpaceMaxBytes}
	view := resourceguard.View{ProtocolVersion: 1, Identity: p.Identity, IssuerFloorEpoch: f.actor.Epoch, Bootstrap: resourceguard.BootstrapCommand{ProtocolVersion: 1, Identity: p.Identity, IssuerEpoch: f.actor.Epoch, BudgetCommitmentID: "original-bootstrap", Limits: p.Limits}, BootstrapApplied: true, Birth: &resourceguard.ProcessIdentity{ProtocolVersion: 1, Identity: p.Identity, PostmasterPID: rootCommand.PostmasterPID, PostmasterStartTicks: rootCommand.PostmasterStartTicks, AddressSpaceMaxBytes: 0}, Original: &rootCommand, Held: &rootCommand, Applied: true, Cancelled: nil, Actual: resourceguard.Evidence{ProtocolVersion: 1, Identity: p.Identity, MemoryMaxBytes: rootCommand.MemoryMaxBytes, MemoryCurrentBytes: 4 << 20, GuestMemTotalBytes: c.Request.TargetVMMemoryBytes, HugePools: p.Limits.HugePools, WholePoolUpperBytes: 16 << 20, HugeOvercommitDisabled: true, CgroupPopulated: true, VMShrinkPermit: false, SwapMaxBytes: 0, SwapCurrentBytes: 0, HugeSurplusPages: 0, TaskLimit: nil, AddressSpace: nil, System: nil}, VMShrinkPermit: false, SystemPlacementObserved: false}
	root, err := budget.RootAggregateFromView(view)
	require.NoError(t, err)
	var noFinal budget.FinalizationCommand
	x := &activeHeldFixture{f: f, reader: reader, command: c, prepared: p, host: h, root: root, confirmed: proof, final: noFinal}
	x.record(t, journal.ResizeVM, journal.Completed, map[string]bool{"explicit_outer_vm_audit_fixture": true})
	if actual {
		x.installCapacity(t)
	} else {
		x.installFinalized(t)
	}
	s, err = f.store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active)
	require.NotNil(t, s.State().BudgetBoundary)
	_, err = reader.CheckCurrentBudget(ctx, s.State())
	require.NoError(t, err, "原closed-only API仍应认证完整真实来源")
	_, err = reader.CheckActiveHeldBudget(ctx, s.State())
	require.Error(t, err, "新API只认证Active仍持旧预算的状态")
	return x
}

func (x *activeHeldFixture) record(t *testing.T, action journal.Action, result journal.Result, data any) {
	t.Helper()
	ctx := context.Background()
	s, err := x.f.store.Load(ctx)
	require.NoError(t, err)
	m := s.State()
	var selected journal.Step
	for _, step := range m.Active.Steps {
		if step.Action == action {
			selected = step
		}
	}
	if action == journal.AbortReconcile {
		selected = *m.Active.Cleanup
	}
	raw, err := json.Marshal(data)
	require.NoError(t, err)
	_, err = x.f.store.Record(ctx, s, x.f.actor, m.Active.Request.Key, journal.Observation{Key: m.Active.Request.Key, RequestSHA256: m.Active.RequestSHA256, CommandSHA256: selected.CommandSHA256, Ordinal: selected.Ordinal, Action: selected.Action, Result: result, SourceID: selected.TargetID, Data: raw})
	require.NoError(t, err)
}

func (x *activeHeldFixture) finish(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	s, err := x.f.store.Load(ctx)
	require.NoError(t, err)
	_, err = x.f.store.Finish(ctx, s, x.f.actor, s.State().Active.Request.Key)
	require.NoError(t, err)
}

func (x *activeHeldFixture) installFinalized(t *testing.T) {
	t.Helper()
	p, c := x.prepared, x.command
	shared := p.Shared
	shared.Observed.CapacityGeneration++
	shared.Observed.LowBuffers, shared.Observed.HighBuffers = c.Request.TargetBuffers, c.Request.TargetBuffers
	digest, err := c.Request.ContextSHA256()
	require.NoError(t, err)
	shared.Observed.Operation = &control.OperationFact{Key: c.Request.Key, Kind: c.Request.Kind, Phase: control.Committed, Reason: control.ReasonNone, TargetBuffers: c.Request.TargetBuffers, ExpectedCapacityGeneration: c.Request.ExpectedCapacityGeneration, ContextSHA256: digest, CancelClosed: true, CancelRequested: false, Replayed: false}
	final := budget.Finalized{ProtocolVersion: 1, VMShrinkPermit: false, Request: c.Request, IssuerEpoch: x.f.actor.Epoch, SettlementEpoch: 0, RunnerPodName: p.RunnerPodName, RunnerRestartCount: p.RunnerRestartCount, HostReservationID: x.host.SourceID, GuestUsableMemoryBytes: p.GuestUsableMemoryBytes, Shared: shared, SystemOutsidePG: p.SystemOutsidePG, Aggregate: &x.root.Bound, PrivateAndDSM: nil, LFCOutsideShared: nil, HugePoolOutsideShared: nil, Transition: p.Transition, Safety: p.Safety, Sources: &budget.SettlementSources{OutsideClaim: *p.OutsideClaim, HostClaim: c.HostClaim, VMReceiptSHA256: strings.Repeat("c", 64), Runner: &x.confirmed.Sources}}
	x.final = budget.FinalizationCommand{ProtocolVersion: 1, Finalized: final, PreparedSHA256: c.PreparedSHA256, GuestSHA256: strings.Repeat("d", 64), VMSHA256: final.Sources.VMReceiptSHA256}
	require.NoError(t, x.final.Validate())
	for _, action := range []journal.Action{journal.GuestReady, journal.ResizePG} {
		x.f.completePrior(t, c.Request, action, c.Request.Key.Instance.ComputeInstanceID, map[string]bool{"explicit_prior_domain_fixture": true})
	}
	x.f.completePriorCommand(t, c.Request, journal.FinalizeBudget, c.Request.Key.Instance.ComputeInstanceID, x.final, final)
	x.f.completePrior(t, c.Request, journal.ResumeAdmission, c.Request.Key.Instance.ComputeInstanceID, map[string]bool{"explicit_admission_fixture": true})
	x.finish(t)
}

func (x *activeHeldFixture) installCapacity(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	p, c := x.prepared, x.command
	native, err := CapacitySource(x.confirmed)
	require.NoError(t, err)
	cm, err := x.f.core.CoreV1().ConfigMaps(x.f.store.Binding().Namespace).Get(ctx, p.OutsideClaim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var outside budget.OutsideReservation
	require.NoError(t, json.Unmarshal([]byte(cm.Data[budgetclaims.ClaimKey]), &outside))
	shared := p.Shared
	shared.Observed.AdmissionOpen = true
	capacity, err := budget.Reconcile(budget.ReconcileInputs{Prepared: p, Shared: shared, Root: x.root, Outside: outside, OutsideClaim: *p.OutsideClaim, Host: x.host, HostClaim: c.HostClaim, VM: native, InitialVM: nil, GuestUsableMemoryBytes: p.GuestUsableMemoryBytes, IssuerEpoch: x.f.actor.Epoch})
	require.NoError(t, err)
	s, err := x.f.store.Load(ctx)
	require.NoError(t, err)
	s, err = x.f.store.RequestAbort(ctx, s, x.f.actor, c.Request.Key)
	require.NoError(t, err)
	command := budget.ReconciledAbortCommand{ProtocolVersion: 1, Capacity: capacity, PreparedSHA256: c.PreparedSHA256, Admission: nil}
	receipt := budget.ReconciledAbortReceipt{ProtocolVersion: 1, Command: command, Query: true, Fence: control.FenceResponse{ProtocolVersion: 1, RequestedInstance: c.Request.Key.Instance, RequestedEpoch: x.f.actor.Epoch, Disposition: control.Fenced, Observed: shared.Observed, VMShrinkPermit: false}, Shared: shared, Response: nil, PGAggregate: x.root, ResourceEpoch: x.f.actor.Epoch}
	require.NoError(t, receipt.ValidateFor(command, x.f.actor.Epoch, x.f.actor.Epoch))
	raw, err := json.Marshal(command)
	require.NoError(t, err)
	_, _, err = x.f.store.StartCleanup(ctx, s, x.f.actor, c.Request.Key, c.Request.Key.Instance.ComputeInstanceID, raw)
	require.NoError(t, err)
	x.record(t, journal.AbortReconcile, journal.Completed, receipt)
	x.finish(t)
}

func (x *activeHeldFixture) beginNext(t *testing.T, start bool) journal.Model {
	t.Helper()
	ctx := context.Background()
	s, err := x.f.store.Load(ctx)
	require.NoError(t, err)
	request := x.command.Request
	request.Key = *s.State().HighWater
	request.Key.Revision++
	request.Key.OperationID.Low++
	request.BudgetCommitmentID += "-next"
	source, err := s.State().CurrentRetainedSource()
	require.NoError(t, err)
	request.ExpectedCapacityGeneration, err = source.CapacityGeneration()
	require.NoError(t, err)
	request.TargetBuffers += 512
	request.TargetVMMemoryBytes = 3 << 30
	s, disposition, err := x.f.store.Begin(ctx, s, x.f.actor, request, uint64(time.Now().UnixMilli()))
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	if start {
		s, _, err = x.f.store.Start(ctx, s, x.f.actor, request.Key, journal.PrepareBudget, request.Key.Instance.ComputeInstanceID, []byte(`{"explicit_prepare_intent_fixture":true}`))
		require.NoError(t, err)
	}
	return s.State()
}

func TestActiveHeldBudgetAuthenticatesOriginalAnchorsWithoutOpeningIdleAPI(t *testing.T) {
	for _, actual := range []bool{false, true} {
		for _, start := range []bool{false, true} {
			name := "finalized"
			if actual {
				name = "capacity"
			}
			if start {
				name += "-captured-prior"
			} else {
				name += "-global"
			}
			t.Run(name, func(t *testing.T) {
				x := newActiveHeldFixture(t, actual)
				m := x.beginNext(t, start)
				before := m.Clone()
				proof, err := x.reader.CheckActiveHeldBudget(context.Background(), m)
				require.NoError(t, err)
				require.Equal(t, x.confirmed.Sources, proof.Budget.Sources)
				require.Nil(t, proof.Cancellation)
				require.Equal(t, before, m, "只读API不能清Active或重写关闭链")
				_, err = x.reader.CheckCurrentBudget(context.Background(), m)
				require.Error(t, err, "旧API必须维持closed-only")
			})
		}
	}
}

func TestActiveHeldBudgetRejectsMissingAnchorAndSameSizeReplacement(t *testing.T) {
	for _, mode := range []string{"missing-anchor", "wrong-digest", "wrong-recorded-sequence", "uncaptured-dispatch", "replacement-receipt-uid"} {
		t.Run(mode, func(t *testing.T) {
			x := newActiveHeldFixture(t, false)
			m := x.beginNext(t, true)
			switch mode {
			case "missing-anchor":
				m.Active.PriorBudgetBoundary = nil
			case "wrong-digest":
				m.Active.PriorBudgetBoundary.FinalizationSHA256 = strings.Repeat("e", 64)
			case "wrong-recorded-sequence":
				m.Active.PriorBudgetBoundary.RecordedSequence++
			case "uncaptured-dispatch":
				m.BudgetBoundary, m.Active.PriorBudgetBoundary = m.Active.PriorBudgetBoundary, nil
			case "replacement-receipt-uid":
				ctx := context.Background()
				cm, err := x.f.core.CoreV1().ConfigMaps(x.f.store.Binding().Namespace).Get(ctx, x.confirmed.Sources.ReceiptClaim.Name, metav1.GetOptions{})
				require.NoError(t, err)
				cm.UID += "-same-size-replacement"
				err = x.f.core.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace)
				require.NoError(t, err)
			}
			require.NoError(t, m.Validate(), "对抗例保持模型结构有效，仅精确来源/anchor不符")
			_, err := x.reader.CheckActiveHeldBudget(context.Background(), m)
			require.Error(t, err, "同一实际数值不能替代原永久预算来源")
		})
	}
}

// 第二个Grow通过生产Runner显式取消，保留第一个完整预算；旧取消的
// NoEffect与准入清理仍各自落账，不修改原成功，也不把第三个Active的
// 目标或Key用作第二个原命令已经关闭的证明。
func TestActiveHeldBudgetAuthenticatesOldCancellationOnlyWithinCapturedThrough(t *testing.T) {
	ctx := context.Background()
	x := newActiveHeldFixture(t, false)
	stale := x.beginNext(t, false)
	request := stale.Active.Request
	c := x.command
	c.Request, c.BeforeBytes = request, x.command.Request.TargetVMMemoryBytes
	p := x.prepared
	p.Request, p.VMBeforeMemoryBytes, p.GuestUsableMemoryBytes, p.Shared = request, c.BeforeBytes, request.TargetVMMemoryBytes-(32<<20), x.final.Finalized.Shared
	quote, err := budget.GrowSharedUpper(p.Shared, request.TargetBuffers)
	require.NoError(t, err)
	p.TargetSharedUpperBytes = quote
	require.NoError(t, p.ValidateFor(request, p.Identity))
	x.f.completePrior(t, request, journal.PrepareBudget, request.Key.Instance.ComputeInstanceID, p)
	host := x.host
	host.Request, host.ReservedMemoryBytes = request, request.TargetVMMemoryBytes+host.HostOverheadBytes
	x.f.completePrior(t, request, journal.ReserveHost, host.SourceID, host)
	preparedRaw, err := json.Marshal(p)
	require.NoError(t, err)
	hostRaw, err := json.Marshal(host)
	require.NoError(t, err)
	c.PreparedSHA256, c.HostSHA256 = SourceDigest(preparedRaw), SourceDigest(hostRaw)
	raw, err := json.Marshal(c)
	require.NoError(t, err)
	s, err := x.f.store.Load(ctx)
	require.NoError(t, err)
	s, dispatch, err := x.f.store.Start(ctx, s, x.f.actor, request.Key, journal.ResizeVM, c.Identity.RunnerPodUID, raw)
	require.NoError(t, err)
	_, err = x.f.store.RequestAbort(ctx, s, x.f.actor, request.Key)
	require.NoError(t, err)
	require.NoError(t, x.f.runner.Tick(ctx))
	cancelled, err := x.reader.ObserveAbortOriginal(ctx, c, dispatch.Step.CommandSHA256, x.f.actor)
	require.NoError(t, err)
	require.True(t, cancelled.Receipt.CancelledBeforeSend())
	_, err = x.reader.CheckActiveHeldBudget(ctx, stale)
	require.Error(t, err, "真实取消仍不能越过读取快照原来捕获的关闭Through")
	cancelRaw, err := json.Marshal(cancelled.Receipt)
	require.NoError(t, err)
	x.record(t, journal.ResizeVM, journal.NoEffect, cancelled.Receipt)
	s, err = x.f.store.Load(ctx)
	require.NoError(t, err)
	command := budget.UnchangedAbortCommand{ProtocolVersion: 1, Request: request, FinalizationSHA256: s.State().BudgetHeld.Finalization.CommandSHA256, CancelledVMSHA256: SourceDigest(cancelRaw), CancellationSources: cancelled.Sources, Admission: nil}
	shared := x.final.Finalized.Shared
	shared.Observed.AdmissionOpen = true
	shared.Observed.AdmissionRevision++
	receipt := budget.UnchangedAbortReceipt{ProtocolVersion: 1, Command: command, Query: true, Fence: control.FenceResponse{ProtocolVersion: 1, RequestedInstance: request.Key.Instance, RequestedEpoch: x.f.actor.Epoch, Disposition: control.Fenced, Observed: shared.Observed, VMShrinkPermit: false}, Shared: shared, Response: nil, PGAggregate: x.root, ResourceEpoch: x.f.actor.Epoch}
	require.NoError(t, receipt.ValidateFor(command, x.final.Finalized, x.f.actor.Epoch, x.f.actor.Epoch))
	raw, err = json.Marshal(command)
	require.NoError(t, err)
	cleanup, _, err := x.f.store.StartCleanup(ctx, s, x.f.actor, request.Key, request.Key.Instance.ComputeInstanceID, raw)
	require.NoError(t, err)
	_, err = x.reader.CheckActiveHeldBudget(ctx, cleanup.State())
	require.ErrorContains(t, err, "without cleanup")
	x.record(t, journal.AbortReconcile, journal.Completed, receipt)
	x.finish(t)
	m := x.beginNext(t, true)
	original := m.Clone()
	proof, err := x.reader.CheckActiveHeldBudget(ctx, m)
	require.NoError(t, err)
	require.Equal(t, x.confirmed.Sources, proof.Budget.Sources)
	require.NotNil(t, proof.Cancellation)
	require.Equal(t, cancelled.Sources, proof.Cancellation.Sources)
	require.Equal(t, request.Key, m.Active.PriorBudgetBoundary.ThroughKey)
	require.Equal(t, original, m)
	_, err = x.reader.CheckCurrentBudget(ctx, m)
	require.Error(t, err)
}

func TestActiveHeldBudgetRejectsInitialWithoutInventingResize(t *testing.T) {
	ctx := context.Background()
	f := runnerBirthFixtureForCompute(t, true, "compute-1")
	bindRunnerServiceBudget(t, f)
	_, _ = f.beginGrow(t)
	s, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, s.State().InitialHeld)
	reader, err := NewReader(f.core.CoreV1(), f.store.Binding(), "lease-uid")
	require.NoError(t, err)
	before := s.State()
	_, err = reader.CheckActiveHeldBudget(ctx, before)
	require.ErrorContains(t, err, "initial retained budget requires original boot reader")
	require.Equal(t, before, s.State())
}
