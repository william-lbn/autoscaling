package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 只验证生产Model/Store/严格codec：原Initial提供有限pids/system完整预算，
// PG/Root/Host/Runner事实为明确领域输入，不将此处的原生回执引用当成网络
// 或Linux物理认证。未知VM始终保留原Step，独立Proof不修改原Abort/目标。
func actualRecoveryDomain(t *testing.T, mode string) (Model, budget.ReconciledCapacity, control.SharedCostResponse, budget.RootAggregate) {
	t.Helper()
	i, first, opened := initialDomain(t)
	m := initialBoundModel(t)
	m, err := m.StartInitialAdmission(m.Owner, first)
	require.NoError(t, err)
	m, err = m.RecordInitialAdmission(m.Owner, opened)
	require.NoError(t, err)
	r := testRequest(m, 1, control.Grow)
	r.TargetBuffers, r.TargetVMMemoryBytes = 1024, 2<<30
	m = beginModel(t, m, r)
	outsideRef := budget.ObjectRef{Name: "actual-target-outside", UID: "actual-target-outside-uid"}
	upper, err := budget.GrowSharedUpper(opened.Shared, r.TargetBuffers)
	require.NoError(t, err)
	p := budget.Prepared{ProtocolVersion: 1, VMShrinkPermit: false, Request: r, Identity: i.PGAggregate.Identity, RunnerPodName: i.RunnerPodName, RunnerRestartCount: i.RunnerRestartCount, VMBeforeMemoryBytes: i.VMMemoryBytes, GuestUsableMemoryBytes: (2 << 30) - (256 << 20), Shared: opened.Shared, TargetSharedUpperBytes: upper, Limits: i.PGAggregate.Bootstrap.Limits, SystemOutsidePG: i.SystemOutsidePG, Transition: i.Transition, Safety: i.Safety, OutsideClaim: &outsideRef}
	require.NoError(t, p.ValidateFor(r, p.Identity))
	hostRef := budget.ObjectRef{Name: "actual-host", UID: "actual-host-uid"}
	host := budget.HostReservation{ProtocolVersion: 2, VMShrinkPermit: false, Request: r, Identity: p.Identity, Incarnation: m.Incarnation, SourceID: "actual-host-source", RunnerPodName: i.RunnerPodName, RunnerPodUID: i.RunnerPodUID, NodeName: i.NodeName, NodeUID: i.NodeUID, ReservedMemoryBytes: r.TargetVMMemoryBytes + i.HostOverheadBytes, HostOverheadBytes: i.HostOverheadBytes, HoldUntilExplicitSettlement: true}
	var preparedRaw []byte
	for _, action := range []Action{PrepareBudget, ReserveHost} {
		target, body := r.Key.Instance.ComputeInstanceID, any(p)
		if action == ReserveHost {
			target, body = host.SourceID, host
		}
		m, err = m.Start(m.Owner, r.Key, action, target, testCommand)
		require.NoError(t, err)
		step := m.Active.Steps[0]
		for _, candidate := range m.Active.Steps {
			if candidate.Action == action {
				step = candidate
			}
		}
		obs := observation(m, step, Completed)
		obs.Data, err = json.Marshal(body)
		require.NoError(t, err)
		if action == PrepareBudget {
			preparedRaw = obs.Data
		}
		m, err = m.Record(m.Owner, r.Key, obs)
		require.NoError(t, err)
	}
	var native budget.NativeCapacity
	var initialVM *budget.InitialVMSources
	if mode == "planned" {
		initialVM = &budget.InitialVMSources{Instance: i.Instance, BirthClaim: i.RunnerBirth, StateClaim: budget.ObjectRef{Name: "actual-initial-state", UID: "actual-initial-state-uid"}, IdentitySHA256: i.RunnerIdentitySHA256, MemoryBytes: i.VMMemoryBytes}
	} else {
		var command serviceVMProjection
		command.ProtocolVersion, command.BindingClaim, command.HostClaim, command.Request = 1, i.RunnerBirth, hostRef, r
		command.BeforeBytes, command.PreparedSHA256 = i.VMMemoryBytes, rawDigest(preparedRaw)
		hostRaw, err := json.Marshal(host)
		require.NoError(t, err)
		command.HostSHA256 = rawDigest(hostRaw)
		command.Identity.VMUID, command.Identity.Incarnation = m.VMUID, m.Incarnation
		command.Identity.RunnerPodUID, command.Identity.RunnerBootID, command.Identity.NodeUID = i.RunnerPodUID, testIncarnation, i.NodeUID
		command.Identity.MinimumBytes, command.Identity.MaximumBytes, command.Identity.BlockBytes = 512<<20, 4<<30, 128<<20
		raw, err := json.Marshal(command)
		require.NoError(t, err)
		m, err = m.Start(m.Owner, r.Key, ResizeVM, i.RunnerPodUID, raw)
		require.NoError(t, err)
		step, _, err := m.OriginalServiceVM()
		require.NoError(t, err)
		hash := sha256.Sum256(append([]byte("neon-dsga-runner-memory-v1\x00"), raw...))
		native = budget.NativeCapacity{Sources: budget.RunnerSources{BirthClaim: i.RunnerBirth, StateClaim: budget.ObjectRef{Name: "actual-state", UID: "actual-state-uid"}, ReceiptClaim: budget.ObjectRef{Name: "actual-receipt", UID: "actual-receipt-uid"}, CommandSHA256: hex.EncodeToString(hash[:]), JournalSHA256: step.CommandSHA256}, Request: r, BeforeBytes: i.VMMemoryBytes, Cancelled: mode == "cancelled", PreparedSHA256: command.PreparedSHA256, GuestSHA256: "", HostClaim: hostRef}
	}
	closed := control.AdmissionRequest{ProtocolVersion: 1, Instance: i.Instance, IssuerEpoch: m.Owner.Epoch, Revision: first.Request.Revision + 1, ExpectedCapacityGeneration: r.ExpectedCapacityGeneration, Open: false, BudgetCommitmentID: r.BudgetCommitmentID}
	m, err = m.StartActiveServiceClosure(m.Owner, closed)
	require.NoError(t, err)
	current := opened.Shared
	current.Observed.AdmissionOpen, current.Observed.AdmissionRevision = false, closed.Revision
	hash, err := closed.ContextSHA256()
	require.NoError(t, err)
	m, err = m.RecordServiceClosure(m.Owner, control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: closed, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: current.Observed}, false)
	require.NoError(t, err)
	if mode != "planned" {
		_, _, _, selector := m.ServiceClosure.Selection()
		m, err = m.StartServiceVMRecovery(m.Owner, selector, native.Cancelled)
		require.NoError(t, err)
		var witness *ServiceVMCancellation
		if native.Cancelled {
			witness = &ServiceVMCancellation{Owner: m.Owner, VMSequence: m.Sequence, VMResourceVersion: "actual-domain-cancellation-rv"}
		}
		m, err = m.RecordServiceVMRecovery(m.Owner, selector, native, witness)
		require.NoError(t, err)
	}
	var noOutside budget.OutsideReservation
	inputs := budget.ReconcileInputs{Prepared: p, Shared: current, Root: i.PGAggregate, Outside: noOutside, OutsideClaim: budget.ObjectRef{Name: "", UID: ""}, Host: host, HostClaim: hostRef, VM: native, InitialVM: initialVM, GuestUsableMemoryBytes: i.GuestUsableMemoryBytes, IssuerEpoch: m.Owner.Epoch}
	var capacity budget.ReconciledCapacity
	if mode == "completed" {
		inputs.Outside = budget.OutsideReservation{ProtocolVersion: 1, VMShrinkPermit: false, Request: r, Identity: p.Identity, Incarnation: m.Incarnation, ResidualProfileID: "actual-domain-target", SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true, Qualification: nil}
		inputs.OutsideClaim, inputs.GuestUsableMemoryBytes = outsideRef, p.GuestUsableMemoryBytes
		capacity, err = budget.ReconcileServiceUnchanged(inputs, p.Shared)
	} else {
		capacity, err = budget.ReconcileInitialServiceUnchanged(inputs, i, p.Shared)
	}
	require.NoError(t, err)
	return m, capacity, current, i.PGAggregate
}

func selectedActualRecovery(t *testing.T, mode string) Model {
	t.Helper()
	m, capacity, current, root := actualRecoveryDomain(t, mode)
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err := m.RecordServiceActualRecovery(m.Owner, selector, capacity, current, root)
	require.NoError(t, err)
	return m
}

func reopenedActualRecovery(t *testing.T, mode string) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	m := selectedActualRecovery(t, mode)
	require.NoError(t, m.CheckStableServiceRecovery())
	_, _, _, selector := m.ServiceClosure.Selection()
	current := m.ServiceClosure.ActualRecovery.Current
	before := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: current.Observed.CapacityGeneration, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Armed: true, Expired: false, Renewed: false, DeadlineNS: 200, LeaseEpoch: m.Owner.Epoch, TimeoutMS: 30000, Observed: current.Observed}
	m, err := m.StartServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
	require.NoError(t, err)
	before.Observed = m.ServiceClosure.Reopen.Receipt.Observed
	return m, before
}

func TestActualServiceRecoveryInstallsIndependentBudgetAtomically(t *testing.T) {
	for _, mode := range []string{"planned", "cancelled", "completed"} {
		t.Run(mode, func(t *testing.T) {
			original, capacity, current, root := actualRecoveryDomain(t, mode)
			require.Error(t, original.CheckStableServiceRecovery(), "单领域证明不能替代完整实际预算")
			_, _, _, selector := original.ServiceClosure.Selection()
			selected, err := original.RecordServiceActualRecovery(original.Owner, selector, capacity, current, root)
			require.NoError(t, err)
			require.Equal(t, original.Active, selected.Active)
			require.Equal(t, original.InitialHeld, selected.InitialHeld)
			require.Nil(t, selected.CapacityHeld, "保存独立实际预算不提前替换永久头")
			require.False(t, selected.Active.AbortRequested)
			_, err = selected.RecordServiceActualRecovery(selected.Owner, selector, capacity, current, root)
			require.Error(t, err, "已保存的实际预算不可被后读来源改写")
			capacity.UnchangedPG.Observed.Arrays[0].CommittedUpperBytes++
			current.Observed.Arrays[0].CommittedUpperBytes++
			root.Bootstrap.Limits.HugePools[0].Pages++
			require.NoError(t, selected.Validate(), "保存Proof后调用者改输入不能污染已保存的完整来源")
			_, err = selected.Finish(selected.Owner, selected.Active.Request.Key)
			require.Error(t, err)
			opening, live := reopenedActualRecovery(t, mode)
			frozen := opening.Clone()
			done, err := opening.CompleteServiceReopen(opening.Owner, opening.ServiceClosure.Reopen.CommandSHA256, live)
			require.NoError(t, err)
			require.Nil(t, done.Active)
			require.Nil(t, done.ServiceClosure)
			require.Nil(t, done.InitialHeld)
			require.Nil(t, done.BudgetHeld)
			require.EqualValues(t, 3, done.CapacityHeld.ProtocolVersion)
			var noCleanup Step
			require.Equal(t, noCleanup, done.CapacityHeld.Cleanup)
			require.Nil(t, done.CapacityHeld.Recovery)
			require.Equal(t, *frozen.Active, done.CapacityHeld.ActiveRecovery.Original)
			require.Equal(t, *frozen.ServiceClosure.ActualRecovery, done.CapacityHeld.ActiveRecovery.Proof)
			record := done.History[len(done.History)-1]
			require.Equal(t, RecoveredWithActualBudget, record.Outcome)
			require.Equal(t, *frozen.Active, record.Operation)
			require.Equal(t, frozen.ServiceClosure.ActualRecovery, record.ServiceCoverage.ActualRecovery)
			require.NoError(t, done.CheckClosedCapacity(frozen.Active.Request))
			stored, err := done.CurrentCapacity()
			require.NoError(t, err)
			require.Equal(t, frozen.ServiceClosure.ActualRecovery.Capacity, stored)
			digest, err := done.CapacitySourceSHA256()
			require.NoError(t, err)
			require.Equal(t, frozen.ServiceClosure.ActualRecovery.CapacitySHA256, digest)
			require.Equal(t, digest, done.BudgetBoundary.FinalizationSHA256)
			require.Less(t, done.CapacityHeld.RecordedSequence, done.BudgetBoundary.SucceededSequence)
			_, wire, err := encode(done, MaxEncodedBytes)
			require.NoError(t, err)
			loaded, err := decode(wire, done.VMUID, done.Incarnation)
			require.NoError(t, err)
			require.Equal(t, done, loaded)
			stored.UnchangedPG.Observed.Arrays[0].CommittedUpperBytes++
			if stored.InitialOutside != nil {
				stored.InitialOutside.PGAggregate.Bootstrap.Limits.HugePools[0].Pages++
			}
			again, err := done.CurrentCapacity()
			require.NoError(t, err)
			require.Equal(t, frozen.ServiceClosure.ActualRecovery.Capacity, again)
			cloned := done.Clone()
			cloned.CapacityHeld.ActiveRecovery.Original.Steps[0].Command[0] ^= 1
			cloned.CapacityHeld.ActiveRecovery.Proof.Current.Observed.Arrays[0].CommittedUpperBytes++
			require.NoError(t, done.Validate(), "深拷贝修改不污染原永久证明")
		})
	}
}

func TestActualServiceRecoveryRejectsProofAndCodecSubstitution(t *testing.T) {
	m := selectedActualRecovery(t, "cancelled")
	for _, mode := range []string{"old-origin", "operation", "capacity", "digest", "no-vm", "vm-source", "root", "unknown", "old-version"} {
		t.Run(mode, func(t *testing.T) {
			bad := m.Clone()
			p := bad.ServiceClosure.ActualRecovery
			switch mode {
			case "old-origin":
				p.Origin.HolderID += "-aliased"
			case "operation":
				p.OperationSHA256 = strings.Repeat("0", 64)
			case "capacity":
				p.Capacity.VMMemoryBytes--
			case "digest":
				p.CapacitySHA256 = strings.Repeat("0", 64)
			case "no-vm":
				p.VM = nil
			case "vm-source":
				p.VM.Capacity.Sources.ReceiptClaim.UID += "-replaced"
			case "root":
				p.Root.Bootstrap.Limits.SystemMemoryMaxBytes = 0
			case "unknown":
				bad.Active.AbortRequested = true
			case "old-version":
				bad.ProtocolVersion = 15
			}
			require.Error(t, bad.Validate())
			raw, err := json.Marshal(bad)
			require.NoError(t, err)
			var loaded Model
			require.Error(t, json.Unmarshal(raw, &loaded), "严格解码不能把损坏Proof投影成旧预算")
		})
	}
	raw, err := json.Marshal(m.ServiceClosure.ActualRecovery)
	require.NoError(t, err)
	for _, invalid := range []string{strings.Replace(string(raw), `"capacity_sha256":`, `"capacity_sha256":"duplicate","capacity_sha256":`, 1), strings.Replace(string(raw), `"capacity_sha256":`, `"unsupported":true,"capacity_sha256":`, 1)} {
		var decoded ServiceActualRecovery
		require.Error(t, json.Unmarshal([]byte(invalid), &decoded))
	}
}

func TestStoreActualServiceRecoveryAndCompletionRequireExactCAS(t *testing.T) {
	for _, stage := range []string{"record", "complete"} {
		for _, mode := range []string{"normal", "lost-reply", "stale-rv", "recreated-vm"} {
			t.Run(stage+"/"+mode, func(t *testing.T) {
				api, store := newStoreAPI(t)
				_ = initializeStore(t, api, store)
				m, capacity, current, root := actualRecoveryDomain(t, "cancelled")
				var live control.AdmissionLeaseResponse
				if stage == "complete" {
					m, live = reopenedActualRecovery(t, "cancelled")
				}
				_, wire, err := encode(m, MaxEncodedBytes)
				require.NoError(t, err)
				api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
				s, err := store.Load(context.Background())
				require.NoError(t, err)
				if mode == "stale-rv" {
					api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"fresh": "rv"} })
				}
				api.mu.Lock()
				api.failAfterCommit = mode == "lost-reply"
				if mode == "recreated-vm" {
					api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "replacement-vm"; api.mu.Unlock() }
				}
				api.mu.Unlock()
				var result *Snapshot
				if stage == "record" {
					_, _, _, selector := m.ServiceClosure.Selection()
					result, err = store.RecordServiceActualRecovery(context.Background(), s, m.Owner, selector, capacity, current, root)
				} else {
					result, err = store.CompleteServiceReopen(context.Background(), s, m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
				}
				if mode == "stale-rv" || mode == "recreated-vm" {
					require.Error(t, err)
					require.Nil(t, result)
					api.mu.Lock()
					unchanged := api.vm.Annotations[control.JournalAnnotation]
					api.mu.Unlock()
					require.Equal(t, wire, unchanged, "条件失败不能半安装预算或单独清原Unknown")
					return
				}
				if mode == "lost-reply" {
					var lost *UnconfirmedWrite
					require.ErrorAs(t, err, &lost)
					require.Nil(t, result)
				} else {
					require.NoError(t, err)
				}
				loaded, err := store.Load(context.Background())
				require.NoError(t, err)
				if stage == "record" {
					require.Equal(t, m.Active, loaded.State().Active)
					require.Equal(t, m.InitialHeld, loaded.State().InitialHeld)
					require.Nil(t, loaded.State().CapacityHeld)
					require.NotNil(t, loaded.State().ServiceClosure.ActualRecovery)
				} else {
					done := loaded.State()
					require.Nil(t, done.Active)
					require.Nil(t, done.ServiceClosure)
					require.EqualValues(t, 3, done.CapacityHeld.ProtocolVersion)
					require.Equal(t, *m.Active, done.CapacityHeld.ActiveRecovery.Original)
					require.Equal(t, RecoveredWithActualBudget, done.History[len(done.History)-1].Outcome)
					require.NoError(t, done.CheckClosedCapacity(m.Active.Request))
				}
			})
		}
	}
}

func TestActualServiceRecoveryBudgetSurvivesSuccessorsAndNormalReplacement(t *testing.T) {
	m, live := reopenedActualRecovery(t, "completed")
	original := m.Active.Request
	m, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
	require.NoError(t, err)
	held := m.Clone().CapacityHeld
	m, err = m.Takeover(m.Owner, "actual-budget-successor")
	require.NoError(t, err)
	require.Equal(t, held, m.CapacityHeld, "更高owner接管保留原未知/原独立观察者和预算SHA")
	for revision := uint64(2); revision <= HistoryLimit+3; revision++ {
		r := testRequest(m, revision, control.Grow)
		m = beginModel(t, m, r)
		m, err = m.RequestAbort(m.Owner, r.Key)
		require.NoError(t, err)
		m, err = m.Finish(m.Owner, r.Key)
		require.NoError(t, err)
	}
	require.NotZero(t, m.ExpiredResults)
	require.Equal(t, held, m.CapacityHeld)
	require.NoError(t, m.CheckClosedCapacity(original))
	require.Equal(t, *m.HighWater, m.BudgetBoundary.ThroughKey)
	r := testRequest(m, HistoryLimit+4, control.Grow)
	r.ExpectedCapacityGeneration = held.ActiveRecovery.Proof.Capacity.CapacityGeneration
	r.TargetBuffers, r.TargetVMMemoryBytes = 2048, held.ActiveRecovery.Proof.Capacity.VMMemoryBytes
	m = beginModel(t, m, r)
	command := heldCommand(t, r)
	sources := runnerSources()
	command.Finalized.Sources.Runner = &sources
	for _, originalStep := range m.Active.Steps {
		if originalStep.Action == FinalizeBudget {
			raw, err := json.Marshal(command)
			require.NoError(t, err)
			m, err = m.Start(m.Owner, r.Key, FinalizeBudget, r.Key.Instance.ComputeInstanceID, raw)
			require.NoError(t, err)
			require.Equal(t, held, m.CapacityHeld, "Finalize未知不能先撤销旧实际预算")
			step := m.Active.Steps[int(originalStep.Ordinal)-1]
			obs := observation(m, step, Completed)
			obs.Data, err = json.Marshal(command.Finalized)
			require.NoError(t, err)
			m, err = m.Record(m.Owner, r.Key, obs)
			require.NoError(t, err)
			require.Nil(t, m.CapacityHeld)
			require.NotNil(t, m.BudgetHeld)
			continue
		}
		m, err = m.Start(m.Owner, r.Key, originalStep.Action, "explicit-actual-successor-domain", testCommand)
		require.NoError(t, err)
		m, err = m.Record(m.Owner, r.Key, observation(m, m.Active.Steps[int(originalStep.Ordinal)-1], Completed))
		require.NoError(t, err)
	}
	m, err = m.Finish(m.Owner, r.Key)
	require.NoError(t, err)
	require.NoError(t, m.CheckClosedBudget(r))
	_, err = m.CurrentCapacity()
	require.Error(t, err)
}
