package journal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 这里只构造完整账本合同，资源域仍是显式协议夹具；完整来源认证另由
// 生产执行器场景证明，不能将此Model测试称作真实PG或物理资源E2E。
func finalizationRecoveryModel(t *testing.T, planned, takeover bool) (Model, budget.FinalizationCommand, control.SharedCostResponse) {
	t.Helper()
	m, q, closed := pendingFinalizeClosureModel(t, planned)
	command := heldCommand(t, m.Active.Request)
	command.Finalized.Sources.Runner = ptrRunner(runnerSources())
	for _, step := range m.Active.Steps {
		switch step.Action {
		case PrepareBudget:
			command.PreparedSHA256 = rawDigest(step.Observation.Data)
		case GuestReady:
			command.GuestSHA256 = rawDigest(step.Observation.Data)
		case ResizeVM:
			command.VMSHA256 = rawDigest(step.Observation.Data)
			command.Finalized.Sources.VMReceiptSHA256 = command.VMSHA256
		}
	}
	if !planned {
		s := &m.Active.Steps[len(m.Active.Steps)-2]
		*s = Step{Ordinal: s.Ordinal, Action: FinalizeBudget, TargetID: "", Command: nil, CommandSHA256: "", Phase: Planned, DispatchedBy: nil, Observation: nil}
		raw, err := json.Marshal(command)
		require.NoError(t, err)
		m, err = m.Start(m.Owner, m.Active.Request.Key, FinalizeBudget, m.PGInstance.ComputeInstanceID, raw)
		require.NoError(t, err)
	}
	m, err := m.StartServiceClosure(m.Owner, q)
	require.NoError(t, err)
	m, err = m.RecordServiceClosure(m.Owner, closed, false)
	require.NoError(t, err)
	if takeover {
		m, err = m.Takeover(m.Owner, "independent-budget-owner")
		require.NoError(t, err)
	}
	current := command.Finalized.Shared
	current.Observed = closed.Observed
	current.Observed.IssuerFloorEpoch = m.Owner.Epoch
	if planned {
		command.Finalized.Shared = current
		command.Finalized.SettlementEpoch = m.Owner.Epoch
	}
	require.NoError(t, command.Validate())
	return m, command, current
}

func ptrRunner(r budget.RunnerSources) *budget.RunnerSources { return &r }

func TestServiceFinalizationPreservesUnknownAndAtomicallyCompletesBudget(t *testing.T) {
	for _, mode := range []struct {
		name     string
		planned  bool
		takeover bool
	}{{"planned", true, false}, {"unknown", false, false}, {"planned-takeover", true, true}, {"unknown-takeover", false, true}} {
		t.Run(mode.name, func(t *testing.T) {
			m, command, current := finalizationRecoveryModel(t, mode.planned, mode.takeover)
			original := m.Clone()
			_, _, _, selector := m.ServiceClosure.Selection()
			m, err := m.RecordServiceFinalization(m.Owner, selector, command, current)
			require.NoError(t, err)
			require.Equal(t, original.Active, m.Active)
			require.Equal(t, original.InitialHeld, m.InitialHeld)
			require.Nil(t, m.BudgetHeld, "独立证据不提前替换当前预算")
			require.NoError(t, m.CheckStableServiceRecovery())
			before := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: current.Observed.CapacityGeneration, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Renewed: false, Armed: true, Expired: false, DeadlineNS: 200, LeaseEpoch: m.Owner.Epoch, TimeoutMS: 30000, Observed: current.Observed}
			m, err = m.StartServiceReopen(m.Owner, selector, before)
			require.NoError(t, err)
			m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
			require.NoError(t, err)
			before.Observed = m.ServiceClosure.Reopen.Receipt.Observed
			done, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, before)
			require.NoError(t, err)
			require.Nil(t, done.Active)
			require.Nil(t, done.InitialHeld)
			require.Nil(t, done.ServiceClosure)
			require.Equal(t, *original.Active, done.History[len(done.History)-1].Operation)
			require.Equal(t, *original.Active, done.BudgetHeld.Recovery.Original)
			require.Equal(t, RecoverFinalization, done.BudgetHeld.Finalization.Action)
			require.EqualValues(t, 2, done.BudgetHeld.ProtocolVersion)
			require.NoError(t, done.CheckClosedBudget(original.Active.Request))
			f, err := done.BudgetFor(original.Active.Request)
			require.NoError(t, err)
			require.Equal(t, command.Finalized, f)
			_, wire, err := encode(done, MaxEncodedBytes)
			require.NoError(t, err)
			decoded, err := decode(wire, done.VMUID, done.Incarnation)
			require.NoError(t, err)
			require.Equal(t, done, decoded)
			for _, mutate := range []func(*Model){
				func(m *Model) { m.BudgetHeld.Recovery = nil },
				func(m *Model) { m.BudgetHeld.ProtocolVersion = 1 },
				func(m *Model) { m.BudgetHeld.Finalization.Action = FinalizeBudget },
				func(m *Model) { m.History[0].ServiceCoverage.Finalization = nil },
				func(m *Model) { m.ServiceLifecycle = nil },
				func(m *Model) { m.ServiceLifecycle, m.ProtocolVersion = nil, 5 },
				func(m *Model) {
					m.BudgetHeld.Recovery.Original.Steps[len(m.BudgetHeld.Recovery.Original.Steps)-2].Phase = Settled
				},
			} {
				n := done.Clone()
				mutate(&n)
				require.Error(t, n.Validate())
			}
		})
	}
}

func TestServiceFinalizationRejectsChangedOriginalAndCurrent(t *testing.T) {
	m, command, current := finalizationRecoveryModel(t, false, true)
	_, _, _, selector := m.ServiceClosure.Selection()
	for _, mode := range []string{"selector", "source", "pg-floor", "request", "predecessor", "unknown-command"} {
		t.Run(mode, func(t *testing.T) {
			var proof ServiceFinalization
			proof.Command, proof.Current = command, current
			c := *cloneServiceFinalization(&proof)
			selected := selector
			switch mode {
			case "selector":
				selected = "wrong"
			case "source":
				c.Command.Finalized.Sources.HostClaim.UID = "changed"
			case "pg-floor":
				c.Current.Observed.IssuerFloorEpoch--
			case "request":
				c.Command.Finalized.Request.BudgetCommitmentID = "changed"
			case "predecessor":
				c.Command.GuestSHA256 = c.Command.PreparedSHA256 + "x"
			case "unknown-command":
				c.Command.Finalized.SettlementEpoch = m.Owner.Epoch
			}
			_, err := m.RecordServiceFinalization(m.Owner, selected, c.Command, c.Current)
			require.Error(t, err)
		})
	}
}

func TestStoreServiceFinalizationKeepsOriginalOnLostCAS(t *testing.T) {
	for _, mode := range []string{"normal", "lost-record", "stale-rv", "recreated-vm"} {
		t.Run(mode, func(t *testing.T) {
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, command, current := finalizationRecoveryModel(t, false, true)
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(context.Background())
			require.NoError(t, err)
			if mode == "stale-rv" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"changed": "true"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-record"
			if mode == "recreated-vm" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "other-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			_, _, _, selector := m.ServiceClosure.Selection()
			result, err := store.RecordServiceFinalization(context.Background(), s, m.Owner, selector, command, current)
			if mode == "stale-rv" || mode == "recreated-vm" {
				require.Error(t, err)
				require.Nil(t, result)
				return
			}
			if mode == "lost-record" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
			} else {
				require.NoError(t, err)
			}
			loaded, err := store.Load(context.Background())
			require.NoError(t, err)
			require.Equal(t, m.Active, loaded.State().Active)
			require.Equal(t, m.InitialHeld, loaded.State().InitialHeld)
			require.Nil(t, loaded.State().BudgetHeld)
			require.NotNil(t, loaded.State().ServiceClosure.Finalization)
			returned := loaded.State()
			returned.ServiceClosure.Finalization.Command.Finalized.Sources.HostClaim.UID = "caller-mutation"
			again, err := store.Load(context.Background())
			require.NoError(t, err)
			require.NotEqual(t, "caller-mutation", again.State().ServiceClosure.Finalization.Command.Finalized.Sources.HostClaim.UID)
		})
	}
}

func TestServiceFinalizationStrictDecodeAndInputIsolation(t *testing.T) {
	m, command, current := finalizationRecoveryModel(t, true, true)
	_, _, _, selector := m.ServiceClosure.Selection()
	next, err := m.RecordServiceFinalization(m.Owner, selector, command, current)
	require.NoError(t, err)
	proof := cloneServiceFinalization(next.ServiceClosure.Finalization)
	command.Finalized.Sources.Runner.StateClaim.UID = "caller-source-mutation"
	command.Finalized.Shared.Observed.Arrays[0].CommittedUpperBytes++
	current.Observed.Arrays[0].CommittedUpperBytes++
	require.Equal(t, proof, next.ServiceClosure.Finalization, "输入Command/Current和返回模型不能共享可变指针")
	cloned := next.Clone()
	cloned.ServiceClosure.Finalization.Current.Observed.Arrays[0].CommittedUpperBytes++
	cloned.ServiceClosure.Finalization.Command.Finalized.Aggregate.UpperBytes++
	require.Equal(t, proof, next.ServiceClosure.Finalization)
	raw, err := json.Marshal(proof)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	for key := range fields {
		t.Run(key, func(t *testing.T) {
			var copied map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &copied))
			delete(copied, key)
			invalid, err := json.Marshal(copied)
			require.NoError(t, err)
			var p ServiceFinalization
			require.Error(t, json.Unmarshal(invalid, &p))
		})
	}
	for _, tail := range []string{`,"closed_sequence":"1"}`, `,"unknown":0}`} {
		var p ServiceFinalization
		require.Error(t, json.Unmarshal(append(append([]byte(nil), raw[:len(raw)-1]...), []byte(tail)...), &p))
	}
	for _, change := range []func(*Model){
		func(m *Model) { m.ServiceClosure.Finalization.Current.Observed.AdmissionOpen = true },
		func(m *Model) { m.ServiceClosure.Finalization.Closed.Revision++ },
		func(m *Model) {
			m.ServiceClosure.Finalization.ClosedSequence = m.ServiceClosure.Finalization.RecordedSequence
		},
	} {
		bad := next.Clone()
		change(&bad)
		require.Error(t, bad.Validate())
	}
}

func reopenedServiceFinalization(t *testing.T) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	m, command, current := finalizationRecoveryModel(t, false, true)
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err := m.RecordServiceFinalization(m.Owner, selector, command, current)
	require.NoError(t, err)
	before := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: current.Observed.CapacityGeneration, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Renewed: false, Armed: true, Expired: false, DeadlineNS: 200, LeaseEpoch: m.Owner.Epoch, TimeoutMS: 30000, Observed: current.Observed}
	m, err = m.StartServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
	require.NoError(t, err)
	before.Observed = m.ServiceClosure.Reopen.Receipt.Observed
	return m, before
}

// 与只读Proof落账不同，完成CAS必须同时撤销旧预算、安装独立预算并关闭
// 原操作。覆盖RV冲突、VM重建及已提交丢回复，不能出现只清Active的半完成。
func TestStoreServiceFinalizationCompletionIsAtomic(t *testing.T) {
	for _, mode := range []string{"normal", "lost-record", "stale-rv", "recreated-vm"} {
		t.Run(mode, func(t *testing.T) {
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, current := reopenedServiceFinalization(t)
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(context.Background())
			require.NoError(t, err)
			if mode == "stale-rv" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"concurrent": "update"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-record"
			if mode == "recreated-vm" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "other-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			result, err := store.CompleteServiceReopen(context.Background(), s, m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
			if mode == "stale-rv" || mode == "recreated-vm" {
				require.Error(t, err)
				require.Nil(t, result)
				api.mu.Lock()
				unchanged := api.vm.Annotations[control.JournalAnnotation]
				api.mu.Unlock()
				require.Equal(t, wire, unchanged)
				return
			}
			if mode == "lost-record" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
			}
			loaded, err := store.Load(context.Background())
			require.NoError(t, err)
			done := loaded.State()
			require.Nil(t, done.Active)
			require.Nil(t, done.ServiceClosure)
			require.Nil(t, done.InitialHeld)
			require.Equal(t, *m.Active, done.BudgetHeld.Recovery.Original)
			require.Equal(t, *m.Active, done.History[0].Operation)
			require.NoError(t, done.CheckClosedBudget(m.Active.Request))
		})
	}
}

func TestServiceFinalizationSurvivesHistoryEvictionAndLaterServiceCycles(t *testing.T) {
	m, current := reopenedServiceFinalization(t)
	original := cloneOperation(*m.Active)
	done, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
	require.NoError(t, err)
	// 先跨过结果缓存上限，再完成后继服务周期。最近Lifecycle此时不再
	// 包含旧恢复上下文，永久预算内的完整原Unknown必须独立保留且可解码。
	for revision := uint64(2); revision <= HistoryLimit+3; revision++ {
		request := testRequest(done, revision, control.Grow)
		request.ExpectedCapacityGeneration = current.Observed.CapacityGeneration
		var disposition Disposition
		done, disposition, err = done.Begin(done.Owner, request, 1)
		require.NoError(t, err)
		require.Equal(t, Accepted, disposition)
		done, err = done.RequestAbort(done.Owner, request.Key)
		require.NoError(t, err)
		done, err = done.Finish(done.Owner, request.Key)
		require.NoError(t, err)
	}
	require.Positive(t, uint64(done.ExpiredResults))
	for cycle := 0; cycle < 2; cycle++ {
		q := control.AdmissionRequest{ProtocolVersion: 1, Instance: *done.PGInstance, IssuerEpoch: done.Owner.Epoch, Revision: current.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: current.Observed.CapacityGeneration, Open: false, BudgetCommitmentID: original.Request.BudgetCommitmentID}
		done, err = done.StartServiceClosure(done.Owner, q)
		require.NoError(t, err)
		fact := serviceReopenFactForRequest(t, q, current.Observed)
		done, err = done.RecordServiceClosure(done.Owner, fact, false)
		require.NoError(t, err)
		current.Observed = fact.Observed
		done, err = done.StartServiceReopen(done.Owner, done.ServiceClosure.CommandSHA256, current)
		require.NoError(t, err)
		done, err = done.RecordServiceReopen(done.Owner, reopenFact(t, done), false)
		require.NoError(t, err)
		current.Observed = done.ServiceClosure.Reopen.Receipt.Observed
		done, err = done.CompleteServiceReopen(done.Owner, done.ServiceClosure.Reopen.CommandSHA256, current)
		require.NoError(t, err)
		require.Equal(t, original, done.BudgetHeld.Recovery.Original)
		require.NoError(t, done.CheckClosedBudget(original.Request))
		_, wire, encodeErr := encode(done, MaxEncodedBytes)
		require.NoError(t, encodeErr)
		done, err = decode(wire, done.VMUID, done.Incarnation)
		require.NoError(t, err)
	}
}
