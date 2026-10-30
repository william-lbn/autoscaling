package journal

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 永久头、关闭、原操作与覆盖均经生产Model推进。PG/Root/Runner为明确
// 完整领域夹具；本组不启动资源执行，真实来源认证由owner组合另证。
func retainedRecoveryBase(t *testing.T, kind string) (Model, control.SharedCostResponse, budget.RootAggregate) {
	t.Helper()
	var m Model
	var shared control.SharedCostResponse
	var root budget.RootAggregate
	switch kind {
	case "initial":
		i, c, r := initialDomain(t)
		m = initialBoundModel(t)
		var err error
		m, err = m.StartInitialAdmission(m.Owner, c)
		require.NoError(t, err)
		m, err = m.RecordInitialAdmission(m.Owner, r)
		require.NoError(t, err)
		shared, root = r.Shared, i.PGAggregate
	case "finalized":
		var f budget.Finalized
		m, f, root = committedAbortModel(t)
		m = heldFinish(t, m)
		shared = f.Shared
	case "capacity":
		var c budget.ReconciledAbortCommand
		var r budget.ReconciledAbortReceipt
		m, c, r = partialCapacityModel(t)
		m = recordPartialCapacity(t, m, c, r)
		var err error
		m, err = m.Finish(m.Owner, m.Active.Request.Key)
		require.NoError(t, err)
		shared, root = r.Shared, r.PGAggregate
	case "capacity-recovered":
		var live control.AdmissionLeaseResponse
		m, live = reopenedCleanupRecovery(t, "actual", true, false, true)
		var err error
		m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
		require.NoError(t, err)
		shared, root = m.CapacityHeld.Recovery.Proof.Current, m.CapacityHeld.Recovery.Proof.PGAggregate
		shared.Observed = live.Observed
	case "actual-recovered":
		var live control.AdmissionLeaseResponse
		m, live = reopenedActualRecovery(t, "completed")
		var err error
		m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
		require.NoError(t, err)
		shared, root = m.CapacityHeld.ActiveRecovery.Proof.Current, m.CapacityHeld.ActiveRecovery.Proof.Root
		shared.Observed = live.Observed
	case "finalization-recovered":
		var command budget.FinalizationCommand
		m, command, shared = finalizationRecoveryModel(t, true, true)
		var i budget.Initial
		require.NoError(t, json.Unmarshal(m.InitialHeld.Budget, &i))
		root = i.PGAggregate
		command.Finalized.Aggregate = &root.Bound
		_, _, _, selector := m.ServiceClosure.Selection()
		var err error
		m, err = m.RecordServiceFinalization(m.Owner, selector, command, shared)
		require.NoError(t, err)
		before := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: shared.Observed.CapacityGeneration, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Renewed: false, Armed: true, Expired: false, DeadlineNS: 200, LeaseEpoch: m.Owner.Epoch, TimeoutMS: 30000, Observed: shared.Observed}
		m, err = m.StartServiceReopen(m.Owner, selector, before)
		require.NoError(t, err)
		m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
		require.NoError(t, err)
		before.Observed = m.ServiceClosure.Reopen.Receipt.Observed
		m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, before)
		require.NoError(t, err)
		shared.Observed = before.Observed
	case "capacity-initial-vm":
		var c budget.ReconciledAbortCommand
		var r budget.ReconciledAbortReceipt
		m, c, r = initialCapacityDomain(t, true)
		m = recordPartialCapacity(t, m, c, r)
		var err error
		m, err = m.Finish(m.Owner, m.Active.Request.Key)
		require.NoError(t, err)
		shared, root = r.Shared, r.PGAggregate
	default:
		t.Fatalf("unknown retained domain %q", kind)
	}
	return m, shared, root
}

func closeRetainedOperation(t *testing.T, m Model, shared control.SharedCostResponse, root budget.RootAggregate, unknown, abort bool, kind control.Kind) (Model, ServiceRetainedRecovery) {
	t.Helper()
	revision := uint64(1)
	if m.HighWater != nil {
		revision = uint64(m.HighWater.Revision) + 1
	}
	req := testRequest(m, revision, kind)
	req.Key.Instance = *m.PGInstance
	req.ExpectedCapacityGeneration = shared.Observed.CapacityGeneration
	req.BudgetCommitmentID = "new-unapplied-target-" + string(req.Kind)
	req.TargetBuffers = 2048
	m = beginModel(t, m, req)
	var err error
	if unknown {
		m, err = m.Start(m.Owner, req.Key, PrepareBudget, req.Key.Instance.ComputeInstanceID, testCommand)
		require.NoError(t, err)
	}
	if abort {
		m, err = m.RequestAbort(m.Owner, req.Key)
		require.NoError(t, err)
	}
	closed := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: shared.Observed.CapacityGeneration, Open: false, BudgetCommitmentID: req.BudgetCommitmentID}
	m, err = m.StartActiveServiceClosure(m.Owner, closed)
	require.NoError(t, err)
	digest, err := closed.ContextSHA256()
	require.NoError(t, err)
	shared.Observed.AdmissionOpen = false
	shared.Observed.AdmissionRevision = closed.Revision
	shared.Observed.IssuerFloorEpoch = m.Owner.Epoch
	m, err = m.RecordServiceClosure(m.Owner, control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: closed, ContextSHA256: digest, Disposition: control.AdmissionApplied, Observed: shared.Observed}, false)
	require.NoError(t, err)
	source, err := m.RetainedServiceSource()
	require.NoError(t, err)
	var p ServiceRetainedRecovery
	p.Source, p.Current, p.Root = source, shared, root
	p.Fence = control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: *m.PGInstance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: shared.Observed}
	p.Anchor = m.BudgetBoundary
	if m.Active.PriorBudgetBoundary != nil {
		p.Anchor = m.Active.PriorBudgetBoundary
	}
	if i := source.Initial; i != nil {
		p.InitialVM = &budget.InitialVMSources{Instance: i.Instance, BirthClaim: i.RunnerBirth, StateClaim: runnerSources().StateClaim, IdentitySHA256: i.RunnerIdentitySHA256, MemoryBytes: i.VMMemoryBytes}
	} else if c := source.Capacity; c != nil {
		if c.InitialVM != nil {
			v := *c.InitialVM
			p.InitialVM = &v
		} else {
			v := c.VM
			p.NativeVM = &v
		}
	} else {
		f, err := source.finalizationCommand()
		require.NoError(t, err)
		p.NativeVM = &budget.NativeCapacity{Sources: *f.Finalized.Sources.Runner, Request: f.Finalized.Request, BeforeBytes: f.Finalized.Request.TargetVMMemoryBytes / 2, Cancelled: false, PreparedSHA256: f.PreparedSHA256, GuestSHA256: "", HostClaim: f.Finalized.Sources.HostClaim}
	}
	return m, p
}

func selectRetainedRecovery(t *testing.T, m Model, p ServiceRetainedRecovery) Model {
	t.Helper()
	_, _, _, selector := m.ServiceClosure.Selection()
	next, err := m.RecordServiceRetainedRecovery(m.Owner, selector, p)
	require.NoError(t, err)
	require.NoError(t, next.CheckStableServiceRecovery())
	return next
}

func reopenRetainedRecovery(t *testing.T, m Model) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	p := m.ServiceClosure.RetainedRecovery
	before := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: p.Current.Observed.CapacityGeneration, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Renewed: false, Armed: true, Expired: false, DeadlineNS: 200, LeaseEpoch: m.Owner.Epoch, TimeoutMS: 30000, Observed: p.Current.Observed}
	before.Observed.IssuerFloorEpoch = m.Owner.Epoch
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err := m.StartServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
	require.NoError(t, err)
	before.Observed = m.ServiceClosure.Reopen.Receipt.Observed
	return m, before
}

func TestRetainedRecoveryAtomicallyPreservesPriorBudgetAndOriginalPrepare(t *testing.T) {
	for _, domain := range []string{"initial", "finalized", "finalization-recovered", "capacity", "capacity-recovered", "actual-recovered", "capacity-initial-vm"} {
		for _, unknown := range []bool{false, true} {
			for _, kind := range []control.Kind{control.Grow, control.Shrink} {
				name := domain + "/" + string(kind)
				if unknown {
					name += "/unknown"
				} else {
					name += "/planned"
				}
				t.Run(name, func(t *testing.T) {
					m, shared, root := retainedRecoveryBase(t, domain)
					m, p := closeRetainedOperation(t, m, shared, root, unknown, false, kind)
					original := m.Clone()
					m = selectRetainedRecovery(t, m, p)
					require.Equal(t, original.Active, m.Active)
					require.Equal(t, original.InitialHeld, m.InitialHeld)
					require.Equal(t, original.BudgetHeld, m.BudgetHeld)
					require.Equal(t, original.CapacityHeld, m.CapacityHeld)
					require.Equal(t, original.ServiceClosure.Request, m.ServiceClosure.Request)
					_, err := m.Finish(m.Owner, m.Active.Request.Key)
					require.Error(t, err)
					m, live := reopenRetainedRecovery(t, m)
					id, err := p.Source.BudgetCommitmentID()
					require.NoError(t, err)
					require.Equal(t, id, m.ServiceClosure.Reopen.Request.BudgetCommitmentID)
					require.NotEqual(t, m.Active.Request.BudgetCommitmentID, id)
					done, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
					require.NoError(t, err)
					require.EqualValues(t, 22, done.ProtocolVersion)
					require.Nil(t, done.Active)
					require.Nil(t, done.ServiceClosure)
					require.Equal(t, *original.Active, done.RetainedCoverage.Record.Operation)
					require.Equal(t, RecoveredWithRetainedBudget, done.RetainedCoverage.Record.Outcome)
					require.Equal(t, original.InitialHeld, done.InitialHeld)
					require.Equal(t, original.BudgetHeld, done.BudgetHeld)
					require.Equal(t, original.CapacityHeld, done.CapacityHeld)
					if domain == "initial" {
						require.Nil(t, done.BudgetBoundary)
					} else {
						require.Equal(t, original.Active.Request.Key, done.BudgetBoundary.ThroughKey)
						require.Equal(t, done.Sequence, done.BudgetBoundary.ThroughSequence)
						require.Equal(t, p.Anchor.SucceededSequence, done.BudgetBoundary.SucceededSequence)
					}
					_, raw, err := encode(done, MaxEncodedBytes)
					require.NoError(t, err)
					decoded, err := decode(raw, done.VMUID, done.Incarnation)
					require.NoError(t, err)
					require.Equal(t, done, decoded)
				})
			}
		}
	}
}

func TestRetainedRecoveryRejectsChangedSourceOrCrossedResourceBoundary(t *testing.T) {
	base, shared, root := retainedRecoveryBase(t, "initial")
	m, p := closeRetainedOperation(t, base, shared, root, true, false, control.Grow)
	for _, mode := range []string{"selector", "source-digest", "source-sequence", "source-boot", "source-pod", "current-generation", "current-root", "current-cost", "floor", "metadata", "fake-anchor", "mixed-native"} {
		t.Run(mode, func(t *testing.T) {
			q := *cloneServiceRetainedRecovery(&p)
			_, _, _, selector := m.ServiceClosure.Selection()
			switch mode {
			case "selector":
				selector = strings.Repeat("f", 64)
			case "source-digest":
				q.Source.SourceSHA256 = strings.Repeat("f", 64)
			case "source-sequence":
				q.Source.RecordedSequence++
			case "source-boot":
				q.Source.Initial.Incarnation = "ffffffffffffffffffffffffffffffff"
			case "source-pod":
				q.Source.Initial.RunnerPodName = "different-original-pod"
			case "current-generation":
				q.Current.Observed.CapacityGeneration++
			case "current-root":
				q.Root.Bootstrap.Limits.HugePools[0].Pages++
			case "current-cost":
				q.Current.Observed.Arrays[0].AllocationUnknown = true
			case "floor":
				q.Fence.RequestedEpoch++
			case "metadata":
				q.RecordedSequence = 1
			case "fake-anchor":
				q.Anchor = &ClosedBudgetBoundary{ProtocolVersion: 1, BudgetKey: m.Active.Request.Key, FinalizationSHA256: q.Source.SourceSHA256, RecordedSequence: 1, SucceededSequence: 2, ThroughKey: m.Active.Request.Key, ThroughSequence: 3}
			case "mixed-native":
				var v budget.NativeCapacity
				q.NativeVM = &v
			}
			_, err := m.RecordServiceRetainedRecovery(m.Owner, selector, q)
			require.Error(t, err)
		})
	}
	for _, action := range []Action{ReserveHost, ResizeVM, GuestReady, ResizePG} {
		t.Run(string(action), func(t *testing.T) {
			other, closed, fact := activeClosureModel(t, control.Grow, action, true)
			other, err := other.StartActiveServiceClosure(other.Owner, closed)
			require.NoError(t, err)
			other, err = other.RecordServiceClosure(other.Owner, fact, false)
			require.NoError(t, err)
			_, err = other.RetainedServiceSource()
			require.Error(t, err)
		})
	}
}

func TestRetainedRecoverySurvivesTakeoverAbortAndStrictCodec(t *testing.T) {
	m, shared, root := retainedRecoveryBase(t, "initial")
	m, p := closeRetainedOperation(t, m, shared, root, true, true, control.Grow)
	m = selectRetainedRecovery(t, m, p)
	frozen := cloneOperation(*m.Active)
	saved := cloneServiceRetainedRecovery(m.ServiceClosure.RetainedRecovery)
	m, err := m.Takeover(m.Owner, "higher-retained-owner")
	require.NoError(t, err)
	require.Equal(t, saved, m.ServiceClosure.RetainedRecovery)
	m, live := reopenRetainedRecovery(t, m)
	done, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
	require.NoError(t, err)
	require.True(t, done.RetainedCoverage.Record.Operation.AbortRequested)
	require.Equal(t, frozen, done.RetainedCoverage.Record.Operation)
	clone := done.Clone()
	clone.RetainedCoverage.Record.ServiceCoverage.RetainedRecovery.Source.Initial.PGAggregate.Bootstrap.Limits.HugePools[0].Pages++
	require.NoError(t, done.Validate())
	bad := done.Clone()
	bad.RetainedCoverage = nil
	require.Error(t, bad.Validate())
	bad.ProtocolVersion = 21
	require.Error(t, bad.Validate(), "降版不能丢永久原Prepare Unknown")
	raw, err := json.Marshal(saved)
	require.NoError(t, err)
	for _, wire := range [][]byte{append([]byte(`{"unexpected":true,`), raw[1:]...), append([]byte(`{"origin":{},`), raw[1:]...), []byte(`null`)} {
		var decoded ServiceRetainedRecovery
		require.Error(t, json.Unmarshal(wire, &decoded))
	}
}

func TestRetainedRecoveryHistoryEvictionAndRepeatedCyclesRemainBounded(t *testing.T) {
	m, shared, root := retainedRecoveryBase(t, "initial")
	var takeoverErr error
	m, takeoverErr = m.Takeover(m.Owner, "retained-retirement-owner")
	require.NoError(t, takeoverErr)
	m, p := closeRetainedOperation(t, m, shared, root, true, false, control.Grow)
	m = selectRetainedRecovery(t, m, p)
	m, live := reopenRetainedRecovery(t, m)
	m, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
	require.NoError(t, err)
	original := cloneRetainedCoverage(m.RetainedCoverage)
	for i := uint64(2); i <= HistoryLimit+3; i++ {
		req := testRequest(m, i, control.Grow)
		m = beginModel(t, m, req)
		m, err = m.RequestAbort(m.Owner, req.Key)
		require.NoError(t, err)
		m, err = m.Finish(m.Owner, req.Key)
		require.NoError(t, err)
	}
	require.Positive(t, m.ExpiredResults)
	require.Equal(t, original, m.RetainedCoverage)
	shared = original.Record.ServiceCoverage.RetainedRecovery.Current
	shared.Observed = live.Observed
	for cycle := 0; cycle < 3; cycle++ {
		m, p = closeRetainedOperation(t, m, shared, root, true, false, control.Grow)
		m = selectRetainedRecovery(t, m, p)
		m, live = reopenRetainedRecovery(t, m)
		m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
		require.NoError(t, err)
		require.EqualValues(t, cycle+2, m.RetainedCoverage.Cycle)
		require.NotNil(t, m.RetainedCoverage.Previous)
		context, err := recoveryContext(m.ServiceLifecycle.Context)
		require.NoError(t, err)
		require.Nil(t, context.RetainedCoverage)
		require.NotEmpty(t, m.ServiceLifecycle.ContextRetainedSHA256)
		shared = m.RetainedCoverage.Record.ServiceCoverage.RetainedRecovery.Current
		shared.Observed = live.Observed
		_, wire, err := encode(m, MaxEncodedBytes)
		require.NoError(t, err)
		m, err = decode(wire, m.VMUID, m.Incarnation)
		require.NoError(t, err)
	}
	for _, epoch := range []control.Uint64{m.RetainedCoverage.Previous.Key.IssuerEpoch - 1, m.RetainedCoverage.Record.ServiceCoverage.RetainedRecovery.ClosedOrigin.Epoch + 1} {
		bad := m.Clone()
		bad.RetainedCoverage.Previous.Open.IssuerEpoch = epoch
		require.NoError(t, bad.RetainedCoverage.Previous.Open.Validate(), "对抗例只改变合法issuer，确保命中新退休上下界")
		require.ErrorContains(t, bad.Validate(), "strict superseding native boundary")
	}
	bad := m.Clone()
	bad.ServiceLifecycle.ContextRetainedSHA256 = ""
	require.Error(t, bad.Validate(), "删除外层退休摘要不能抹掉旧slot")
	bad = m.Clone()
	context, err := recoveryContext(bad.ServiceLifecycle.Context)
	require.NoError(t, err)
	context.RetainedContextSHA256 = ""
	bad.ServiceLifecycle.Context, err = json.Marshal(context)
	require.NoError(t, err)
	bad.ServiceLifecycle.ContextSHA256 = rawDigest(bad.ServiceLifecycle.Context)
	require.Error(t, bad.Validate(), "重算Context摘要后仍拒绝删除内层原slot退休证明")
	bad = m.Clone()
	bad.RetainedContextSHA256 = strings.Repeat("f", 64)
	require.Error(t, bad.Validate(), "冻结Context摘要不得挂到当前顶层Model")
}

func TestStoreRetainedRecoveryUsesExactCASAndLostReplies(t *testing.T) {
	for _, mode := range []string{"ok", "lost-proof", "stale-proof", "recreated-proof", "lost-complete", "stale-complete", "recreated-complete"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, shared, root := retainedRecoveryBase(t, "initial")
			m, p := closeRetainedOperation(t, m, shared, root, true, false, control.Grow)
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(ctx)
			require.NoError(t, err)
			if mode == "stale-proof" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"concurrent": "update"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-proof"
			if mode == "recreated-proof" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "replacement-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			_, _, _, selector := m.ServiceClosure.Selection()
			selected, err := store.RecordServiceRetainedRecovery(ctx, s, m.Owner, selector, p)
			if mode == "stale-proof" || mode == "recreated-proof" {
				require.Error(t, err)
				require.Nil(t, selected)
				return
			}
			if mode == "lost-proof" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				require.Nil(t, selected)
				loaded, err := store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, m.Active, loaded.State().Active)
				require.NotNil(t, loaded.State().ServiceClosure.RetainedRecovery)
				return
			}
			require.NoError(t, err)
			opening, live := reopenRetainedRecovery(t, selected.State())
			_, wire, err = encode(opening, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err = store.Load(ctx)
			require.NoError(t, err)
			if mode == "stale-complete" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"concurrent": "update"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-complete"
			if mode == "recreated-complete" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "replacement-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			done, err := store.CompleteServiceReopen(ctx, s, m.Owner, opening.ServiceClosure.Reopen.CommandSHA256, live)
			if mode == "stale-complete" || mode == "recreated-complete" {
				require.Error(t, err)
				require.Nil(t, done)
				return
			}
			if mode == "lost-complete" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				require.Nil(t, done)
			} else {
				require.NoError(t, err)
			}
			loaded, err := store.Load(ctx)
			require.NoError(t, err)
			require.Nil(t, loaded.State().Active)
			require.Nil(t, loaded.State().ServiceClosure)
			require.NotNil(t, loaded.State().RetainedCoverage)
			require.Equal(t, *m.Active, loaded.State().RetainedCoverage.Record.Operation)
		})
	}
}

func TestRetainedRecoveryKeepsAuditAfterOrdinaryBudgetReplacement(t *testing.T) {
	m, shared, root := retainedRecoveryBase(t, "initial")
	m, p := closeRetainedOperation(t, m, shared, root, true, false, control.Grow)
	m = selectRetainedRecovery(t, m, p)
	m, live := reopenRetainedRecovery(t, m)
	done, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
	require.NoError(t, err)
	old := done.Clone()
	coverage := cloneRetainedCoverage(done.RetainedCoverage)
	next, obs := heldStart(t, done, uint64(done.HighWater.Revision)+1, runnerSources())
	next, err = next.Record(next.Owner, obs.Key, obs)
	require.NoError(t, err)
	next = heldFinish(t, next)
	require.Nil(t, next.InitialHeld)
	require.Equal(t, coverage, next.RetainedCoverage)
	require.Greater(t, next.BudgetHeld.RecordedSequence, coverage.Record.ClosedSequence)
	require.NoError(t, next.Validate())
	bad := next.Clone()
	bad.BudgetHeld, bad.InitialHeld, bad.BudgetBoundary = nil, old.InitialHeld, nil
	require.Error(t, bad.Validate(), "真实新头不能降回已被替换的旧Source")
	for count := 0; count < HistoryLimit+1; count++ {
		r := testRequest(next, uint64(next.HighWater.Revision)+1, control.Grow)
		next = beginModel(t, next, r)
		next, err = next.RequestAbort(next.Owner, r.Key)
		require.NoError(t, err)
		next, err = next.Finish(next.Owner, r.Key)
		require.NoError(t, err)
	}
	require.NoError(t, next.Validate())
	require.Equal(t, coverage, next.RetainedCoverage)
	tampered := next.Clone()
	proof := tampered.RetainedCoverage.Record.ServiceCoverage.RetainedRecovery
	proof.Source.SourceSHA256 = strings.Repeat("f", 64)
	tampered.RetainedCoverage.Record.ServiceCoverage.FinalizationSHA256 = proof.Source.SourceSHA256
	require.NoError(t, proof.ValidateFor(tampered.RetainedCoverage.Record.Operation))
	require.Error(t, tampered.Validate(), "历史淘汰/旧头替换后仍须逐字绑定生命周期中的原覆盖")
}

func TestRetainedRecoveryRejectsIdentityDeletionAndMetadataInjection(t *testing.T) {
	m, shared, root := retainedRecoveryBase(t, "initial")
	m, p := closeRetainedOperation(t, m, shared, root, true, false, control.Grow)
	_, _, _, selector := m.ServiceClosure.Selection()
	for _, change := range []func(*ServiceRetainedRecovery){func(p *ServiceRetainedRecovery) { p.Origin.HolderID = "untrusted-owner" }, func(p *ServiceRetainedRecovery) { p.ClosedOrigin.HolderID = "untrusted-close-owner" }, func(p *ServiceRetainedRecovery) { p.Closed.BudgetCommitmentID = "untrusted-close" }} {
		q := *cloneServiceRetainedRecovery(&p)
		change(&q)
		_, err := m.RecordServiceRetainedRecovery(m.Owner, selector, q)
		require.Error(t, err)
	}
	m = selectRetainedRecovery(t, m, p)
	m, live := reopenRetainedRecovery(t, m)
	done, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
	require.NoError(t, err)
	for _, change := range []func(*Model){func(m *Model) { m.ResourceIdentity = nil }, func(m *Model) { m.PGInstance = nil }, func(m *Model) { m.RetainedCoverage.Record.ServiceCoverage = nil }} {
		bad := done.Clone()
		change(&bad)
		require.NotPanics(t, func() { require.Error(t, bad.Validate()) })
	}
}

func TestRetainedRecoveryInitialNativeTerminalKeepsCancellationWitness(t *testing.T) {
	m, shared, root := retainedRecoveryBase(t, "initial")
	old := testRequest(m, 1, control.Grow)
	m = beginModel(t, m, old)
	var err error
	m, err = m.RequestAbort(m.Owner, old.Key)
	require.NoError(t, err)
	m, err = m.Finish(m.Owner, old.Key)
	require.NoError(t, err)
	oldAt := m.Sequence
	m, p := closeRetainedOperation(t, m, shared, root, true, false, control.Grow)
	i := p.Source.Initial
	old.TargetVMMemoryBytes = i.VMMemoryBytes * 2
	sources := runnerSources()
	sources.BirthClaim = i.RunnerBirth
	p.InitialVM = nil
	p.NativeVM = &budget.NativeCapacity{Sources: sources, Request: old, BeforeBytes: i.VMMemoryBytes, Cancelled: true, PreparedSHA256: strings.Repeat("a", 64), GuestSHA256: "", HostClaim: budget.ObjectRef{Name: "prior-native-host", UID: "prior-native-host-uid"}}
	p.NativeVMCancellation = &RetainedRunnerCancellation{Owner: m.Owner, VMSequence: oldAt - 1, VMResourceVersion: "original-vm-rv"}
	positive := selectRetainedRecovery(t, m, p)
	require.True(t, positive.ServiceClosure.RetainedRecovery.NativeVM.Cancelled)
	for _, mode := range []string{"missing-witness", "late-witness", "missing-rv", "different-birth"} {
		t.Run(mode, func(t *testing.T) {
			q := *cloneServiceRetainedRecovery(&p)
			switch mode {
			case "missing-witness":
				q.NativeVMCancellation = nil
			case "late-witness":
				q.NativeVMCancellation.VMSequence = m.ServiceClosure.StartedSequence
			case "missing-rv":
				q.NativeVMCancellation.VMResourceVersion = ""
			case "different-birth":
				q.NativeVM.Sources.BirthClaim.UID = "replacement-birth"
			}
			_, _, _, selector := m.ServiceClosure.Selection()
			_, err := m.RecordServiceRetainedRecovery(m.Owner, selector, q)
			require.Error(t, err)
		})
	}
	completed := *cloneServiceRetainedRecovery(&p)
	completed.NativeVM.Cancelled = false
	completed.NativeVM.Request.TargetVMMemoryBytes = i.VMMemoryBytes
	completed.NativeVM.BeforeBytes = i.VMMemoryBytes / 2
	completed.NativeVMCancellation = nil
	require.NoError(t, completed.NativeVM.Validate())
	_ = selectRetainedRecovery(t, m, completed)
	completed.NativeVMCancellation = p.NativeVMCancellation
	_, _, _, selector := m.ServiceClosure.Selection()
	_, err = m.RecordServiceRetainedRecovery(m.Owner, selector, completed)
	require.Error(t, err, "Completed不可携带取消见证")
}

func TestRetainedRecoveryCancellationTailChecksExactKeyAndSequenceBounds(t *testing.T) {
	m, f, receipt := unchangedAbortModel(t)
	witnessAt := m.Sequence - 1
	obs := observation(m, *m.Active.Cleanup, Completed)
	raw, err := json.Marshal(receipt)
	require.NoError(t, err)
	obs.Data = raw
	m, err = m.Record(m.Owner, m.Active.Request.Key, obs)
	require.NoError(t, err)
	m, err = m.Finish(m.Owner, m.Active.Request.Key)
	require.NoError(t, err)
	m, p := closeRetainedOperation(t, m, receipt.Shared, receipt.PGAggregate, true, false, control.Grow)
	p.TailVM = &budget.NativeCapacity{Sources: receipt.Command.CancellationSources, Request: receipt.Command.Request, BeforeBytes: f.Request.TargetVMMemoryBytes, Cancelled: true, PreparedSHA256: strings.Repeat("a", 64), GuestSHA256: "", HostClaim: f.Sources.HostClaim}
	p.TailVMCancellation = &RetainedRunnerCancellation{Owner: m.Owner, VMSequence: witnessAt, VMResourceVersion: "original-cancel-rv"}
	positive := selectRetainedRecovery(t, m, p)
	require.NotNil(t, positive.ServiceClosure.RetainedRecovery.TailVM)
	for _, mode := range []string{"equal-budget-sequence", "equal-through-sequence", "aliased-through-key", "past-through", "changed-state", "missing-witness"} {
		t.Run(mode, func(t *testing.T) {
			q := *cloneServiceRetainedRecovery(&p)
			switch mode {
			case "equal-budget-sequence":
				q.TailVMCancellation.VMSequence = q.Source.RecordedSequence
			case "equal-through-sequence":
				q.TailVMCancellation.VMSequence = q.Anchor.ThroughSequence
			case "aliased-through-key":
				q.TailVM.Request.Key.OperationID.Low++
			case "past-through":
				q.TailVM.Request.Key.Revision++
			case "changed-state":
				q.TailVM.Sources.StateClaim.UID = "another-original-state"
			case "missing-witness":
				q.TailVMCancellation = nil
			}
			_, _, _, selector := m.ServiceClosure.Selection()
			_, err := m.RecordServiceRetainedRecovery(m.Owner, selector, q)
			require.Error(t, err)
		})
	}
}

func TestRetainedRecoveryRearmRecloseRetryKeepsOriginalTargetAndUnknown(t *testing.T) {
	m, shared, root := retainedRecoveryBase(t, "initial")
	m, p := closeRetainedOperation(t, m, shared, root, true, false, control.Grow)
	m = selectRetainedRecovery(t, m, p)
	original := m.Clone()
	var err error
	m, err = m.Takeover(m.Owner, "retained-lease-recovery-owner")
	require.NoError(t, err)
	before := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: p.Current.Observed.CapacityGeneration, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Renewed: false, Armed: true, Expired: true, DeadlineNS: 100, LeaseEpoch: m.Owner.Epoch - 1, TimeoutMS: 30000, Observed: p.Current.Observed}
	before.Observed.IssuerFloorEpoch = m.Owner.Epoch
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err = m.StartServiceRearm(m.Owner, selector, before)
	require.NoError(t, err)
	m, err = m.RecordServiceRearm(m.Owner, rearmFact(t, m))
	require.NoError(t, err)
	m, live := reopenRetainedRecovery(t, m)
	live.Observed = m.ServiceClosure.Reopen.Receipt.Observed
	// 已确认的新开放再被独立关闭，重试仅派生旧预算ID的更高revision。
	m, err = m.StartServiceReopenClosure(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
	require.NoError(t, err)
	closed := reopenClosureFact(t, m)
	m, err = m.RecordServiceReopenClosure(m.Owner, closed, false)
	require.NoError(t, err)
	live.Observed = closed.ClosedBy.Observed
	_, _, _, retrySelector := m.ServiceClosure.Reopen.Closing.Close.Selection()
	m, err = m.RetryServiceReopen(m.Owner, retrySelector, live)
	require.NoError(t, err)
	require.Equal(t, original.ServiceClosure.Request, m.ServiceClosure.Request)
	require.Equal(t, original.Active, m.Active)
	require.Equal(t, original.ServiceClosure.RetainedRecovery, m.ServiceClosure.RetainedRecovery)
	m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
	require.NoError(t, err)
	live.Observed = m.ServiceClosure.Reopen.Receipt.Observed
	m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
	require.NoError(t, err)
	require.Equal(t, *original.Active, m.RetainedCoverage.Record.Operation)
	// 后继空闲服务周期不退休slot；Context内以明确摘要替代旧slot原文。
	oldSlot := cloneRetainedCoverage(m.RetainedCoverage)
	id, err := p.Source.BudgetCommitmentID()
	require.NoError(t, err)
	q := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: live.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: live.Observed.CapacityGeneration, Open: false, BudgetCommitmentID: id}
	m, err = m.StartServiceClosure(m.Owner, q)
	require.NoError(t, err)
	fact := serviceReopenFactForRequest(t, q, live.Observed)
	m, err = m.RecordServiceClosure(m.Owner, fact, false)
	require.NoError(t, err)
	live.Observed = fact.Observed
	_, _, _, selector = m.ServiceClosure.Selection()
	m, err = m.StartServiceReopen(m.Owner, selector, live)
	require.NoError(t, err)
	m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
	require.NoError(t, err)
	live.Observed = m.ServiceClosure.Reopen.Receipt.Observed
	m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
	require.NoError(t, err)
	require.Equal(t, oldSlot, m.RetainedCoverage)
	require.NotEmpty(t, m.ServiceLifecycle.ContextRetainedSHA256)
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	_, err = decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
}

func TestRetainedRecoveryFrozenContextCannotBecomeCurrentModel(t *testing.T) {
	m, shared, root := retainedRecoveryBase(t, "initial")
	m, p := closeRetainedOperation(t, m, shared, root, true, false, control.Grow)
	m = selectRetainedRecovery(t, m, p)
	m, live := reopenRetainedRecovery(t, m)
	m, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
	require.NoError(t, err)
	for count := 0; count < HistoryLimit+1; count++ {
		r := testRequest(m, uint64(m.HighWater.Revision)+1, control.Grow)
		m = beginModel(t, m, r)
		m, err = m.RequestAbort(m.Owner, r.Key)
		require.NoError(t, err)
		m, err = m.Finish(m.Owner, r.Key)
		require.NoError(t, err)
	}
	require.NotNil(t, m.RetainedCoverage)
	require.Nil(t, m.InitialHeld.Covered)
	require.Nil(t, m.BudgetHeld)
	require.Nil(t, m.CapacityHeld)
	for _, r := range m.History {
		require.Nil(t, r.ServiceCoverage, "必须真实淘汰原独立覆盖，不能靠旧History拒绝顶层投影")
	}
	shared.Observed = live.Observed
	m, p = closeRetainedOperation(t, m, shared, root, true, false, control.Grow)
	m = selectRetainedRecovery(t, m, p)
	m, _ = reopenRetainedRecovery(t, m)
	frozen := m.Clone()
	slot, err := json.Marshal(frozen.RetainedCoverage)
	require.NoError(t, err)
	frozen.ProtocolVersion, frozen.ServiceLifecycle, frozen.RetainedCoverage = 20, nil, nil
	frozen.RetainedContextSHA256 = rawDigest(slot)
	frozen.ServiceClosure.FormatVersion = 0
	raw, err := json.Marshal(frozen)
	require.NoError(t, err)
	internal, err := recoveryContext(raw)
	require.NoError(t, err, "专用内部入口必须能认证真实合法冻结上下文")
	require.NoError(t, internal.validateCore())
	require.ErrorContains(t, internal.Validate(), "frozen retained context cannot become current journal")
	var current Model
	require.ErrorContains(t, json.Unmarshal(raw, &current), "frozen retained context cannot become current journal", "顶层解码不可把有摘要而无slot的合法v20上下文收养为当前账本")
}
