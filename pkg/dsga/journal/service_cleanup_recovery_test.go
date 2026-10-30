package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 原资源事实为明确协议夹具；本组只验证生产Model/Store/codec，不把
// 结构合法当作真实PG/Root/Runner来源认证。网络消费者另由owner组合验证。
func cleanupRecoveryModel(t *testing.T, kind string, opening, settled, takeover bool) (Model, control.FenceResponse, control.SharedCostResponse, budget.RootAggregate) {
	t.Helper()
	m, request, closed := cleanupClosureModel(t, kind, opening, settled)
	var shared control.SharedCostResponse
	var root budget.RootAggregate
	switch kind {
	case "actual":
		_, _, receipt := partialCapacityModel(t)
		shared, root = receipt.Shared, receipt.PGAggregate
	case "committed":
		_, f, aggregate := committedAbortModel(t)
		shared, root = f.Shared, aggregate
	case "unchanged":
		_, _, receipt := unchangedAbortModel(t)
		shared, root = receipt.Shared, receipt.PGAggregate
	}
	m, err := m.StartCleanupServiceClosure(m.Owner, request)
	require.NoError(t, err)
	if m.ServiceClosure.Original == nil {
		m, err = m.RecordServiceClosure(m.Owner, closed, false)
	} else {
		fact := control.AdmissionClosure{ProtocolVersion: 1, Original: *m.ServiceClosure.Original, ClosedBy: closed, PriorMayHaveApplied: true, VMShrinkPermit: false}
		m, err = m.recordOriginalServiceClosure(m.Owner, fact, false, "cleanup_source_unconfirmed")
	}
	require.NoError(t, err)
	if takeover {
		m, err = m.Takeover(m.Owner, "cleanup-recovery-owner")
		require.NoError(t, err)
	}
	shared.Observed = closed.Observed
	shared.Observed.IssuerFloorEpoch = m.Owner.Epoch
	fence := control.FenceResponse{ProtocolVersion: 1, RequestedInstance: *m.PGInstance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: shared.Observed, VMShrinkPermit: false}
	return m, fence, shared, root
}

func TestServiceCleanupRecoveryKeepsThreeOriginalCancellationBoundaries(t *testing.T) {
	for _, kind := range []string{"committed", "actual", "unchanged"} {
		for _, opening := range []bool{false, true} {
			for _, settled := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/opening-%t/settled-%t", kind, opening, settled), func(t *testing.T) {
					m, fence, current, root := cleanupRecoveryModel(t, kind, opening, settled, true)
					before := m.Clone()
					_, _, _, selector := m.ServiceClosure.Selection()
					m, err := m.RecordServiceCleanupRecovery(m.Owner, selector, fence, current, root)
					require.NoError(t, err)
					require.Equal(t, before.Active, m.Active)
					require.Equal(t, before.BudgetHeld, m.BudgetHeld)
					require.Equal(t, before.CapacityHeld, m.CapacityHeld)
					require.Equal(t, before.InitialHeld, m.InitialHeld)
					require.Equal(t, kind, m.ServiceClosure.CleanupRecovery.Kind)
					require.Nil(t, m.ServiceClosure.Reopen)
					_, wire, err := encode(m, MaxEncodedBytes)
					require.NoError(t, err)
					decoded, err := decode(wire, m.VMUID, m.Incarnation)
					require.NoError(t, err)
					require.Equal(t, m, decoded)
					_, err = m.Finish(m.Owner, m.Active.Request.Key)
					require.Error(t, err, "只读Proof不能当原Cleanup成功或Host结清")
					_, err = m.RecordServiceCleanupRecovery(m.Owner, selector, fence, current, root)
					require.Error(t, err, "已选证明不因较新观察改写")
					cloned := m.Clone()
					cloned.ServiceClosure.CleanupRecovery.Current.Observed.Arrays[0].CommittedUpperBytes++
					require.NotEqual(t, cloned.ServiceClosure.CleanupRecovery.Current, m.ServiceClosure.CleanupRecovery.Current)
				})
			}
		}
	}
}

func TestServiceCleanupRecoveryRejectsChangedBudgetClosureAndOwner(t *testing.T) {
	for _, kind := range []string{"committed", "actual", "unchanged"} {
		t.Run(kind, func(t *testing.T) {
			m, fence, current, root := cleanupRecoveryModel(t, kind, true, false, true)
			_, _, _, selector := m.ServiceClosure.Selection()
			m, err := m.RecordServiceCleanupRecovery(m.Owner, selector, fence, current, root)
			require.NoError(t, err)
			for _, change := range []string{"kind", "original", "floor", "generation", "open", "close", "source", "owner", "sequence", "budget"} {
				t.Run(change, func(t *testing.T) {
					n := m.Clone()
					p := n.ServiceClosure.CleanupRecovery
					switch change {
					case "kind":
						p.Kind = "different"
					case "original":
						p.OperationSHA256 += "x"
					case "floor":
						p.Current.Observed.IssuerFloorEpoch--
					case "generation":
						p.Current.Observed.CapacityGeneration++
					case "open":
						p.Current.Observed.AdmissionOpen = true
					case "close":
						p.Closed.Revision++
					case "source":
						p.PGAggregate.Bound.SourceID += "-replacement"
					case "owner":
						p.Origin.Epoch++
					case "sequence":
						p.RecordedSequence = p.ClosedSequence
					case "budget":
						if p.Finalization != nil {
							p.Finalization.CommandSHA256 += "x"
						} else {
							var empty Step
							p.Finalization = &empty
						}
					}
					require.Error(t, n.Validate())
				})
			}
		})
	}
}

func TestStoreServiceCleanupRecoveryRequiresConfirmedOriginalCAS(t *testing.T) {
	for _, mode := range []string{"normal", "lost-record", "stale-rv", "recreated-vm"} {
		t.Run(mode, func(t *testing.T) {
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, fence, current, root := cleanupRecoveryModel(t, "actual", true, false, true)
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
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "replacement-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			_, _, _, selector := m.ServiceClosure.Selection()
			result, err := store.RecordServiceCleanupRecovery(context.Background(), s, m.Owner, selector, fence, current, root)
			if mode == "stale-rv" || mode == "recreated-vm" {
				require.Error(t, err)
				require.Nil(t, result)
				return
			}
			if mode == "lost-record" {
				var lost *UnconfirmedWrite
				require.ErrorAs(t, err, &lost)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
			}
			loaded, err := store.Load(context.Background())
			require.NoError(t, err)
			require.Equal(t, m.Active, loaded.State().Active)
			require.Nil(t, loaded.State().CapacityHeld)
			require.NotNil(t, loaded.State().ServiceClosure.CleanupRecovery)
			raw, err := json.Marshal(loaded.State())
			require.NoError(t, err)
			var decoded Model
			require.NoError(t, json.Unmarshal(raw, &decoded))
		})
	}
}

func reopenedCleanupRecovery(t *testing.T, kind string, opening, settled, takeover bool) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	m, fence, current, root := cleanupRecoveryModel(t, kind, opening, settled, takeover)
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err := m.RecordServiceCleanupRecovery(m.Owner, selector, fence, current, root)
	require.NoError(t, err)
	require.NoError(t, m.CheckStableServiceRecovery())
	before := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: current.Observed.CapacityGeneration, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Renewed: false, Armed: true, Expired: false, DeadlineNS: 200, LeaseEpoch: m.Owner.Epoch, TimeoutMS: 30000, Observed: current.Observed}
	m, err = m.StartServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
	require.NoError(t, err)
	before.Observed = m.ServiceClosure.Reopen.Receipt.Observed
	return m, before
}

func TestServiceCleanupRecoveryCompletesWithoutRewritingOriginalCancellation(t *testing.T) {
	for _, kind := range []string{"committed", "actual", "unchanged"} {
		for _, settled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/settled-%t", kind, settled), func(t *testing.T) {
				m, current := reopenedCleanupRecovery(t, kind, true, settled, true)
				original := m.Clone()
				done, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
				require.NoError(t, err)
				require.Nil(t, done.Active)
				require.Nil(t, done.ServiceClosure)
				record := done.History[len(done.History)-1]
				require.Equal(t, RecoveredAfterCancellation, record.Outcome)
				require.Equal(t, *original.Active, record.Operation)
				require.NotNil(t, record.ServiceCoverage.CleanupRecovery)
				if kind == "actual" {
					require.Nil(t, done.InitialHeld)
					require.Nil(t, done.BudgetHeld)
					require.EqualValues(t, 2, done.CapacityHeld.ProtocolVersion)
					require.Equal(t, *original.Active.Cleanup, done.CapacityHeld.Cleanup)
					require.Equal(t, *original.Active, done.CapacityHeld.Recovery.Original)
					require.NoError(t, done.CheckClosedCapacity(original.Active.Request))
				} else {
					require.Equal(t, original.BudgetHeld, done.BudgetHeld)
					var f budget.FinalizationCommand
					require.NoError(t, json.Unmarshal(done.BudgetHeld.Finalization.Command, &f))
					require.NoError(t, done.CheckClosedBudget(f.Finalized.Request))
				}
				_, raw, err := encode(done, MaxEncodedBytes)
				require.NoError(t, err)
				decoded, err := decode(raw, done.VMUID, done.Incarnation)
				require.NoError(t, err)
				require.Equal(t, done, decoded)
				for _, change := range []string{"coverage", "unknown", "lifecycle", "boundary", "outcome", "current"} {
					t.Run(change, func(t *testing.T) {
						n := done.Clone()
						r := &n.History[len(n.History)-1]
						switch change {
						case "coverage":
							r.ServiceCoverage.CleanupRecovery = nil
						case "unknown":
							r.Operation.Cleanup.Command[0] = '['
						case "lifecycle":
							n.ServiceLifecycle = nil
						case "boundary":
							n.BudgetBoundary = nil
						case "outcome":
							r.Outcome = Succeeded
						case "current":
							r.ServiceCoverage.Current.Expired = true
						}
						require.Error(t, n.Validate())
					})
				}
			})
		}
	}
}

func TestServiceCleanupRecoveryCompletedBudgetSurvivesHistoryEviction(t *testing.T) {
	for _, kind := range []string{"actual", "unchanged"} {
		t.Run(kind, func(t *testing.T) {
			m, current := reopenedCleanupRecovery(t, kind, false, false, true)
			original := cloneOperation(*m.Active)
			m, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
			require.NoError(t, err)
			for revision := uint64(3); revision <= HistoryLimit+5; revision++ {
				request := testRequest(m, revision, control.Grow)
				request.ExpectedCapacityGeneration = current.Observed.CapacityGeneration
				m, _, err = m.Begin(m.Owner, request, 1)
				require.NoError(t, err)
				m, err = m.RequestAbort(m.Owner, request.Key)
				require.NoError(t, err)
				m, err = m.Finish(m.Owner, request.Key)
				require.NoError(t, err)
			}
			require.Positive(t, uint64(m.ExpiredResults))
			if kind == "actual" {
				require.Equal(t, original, m.CapacityHeld.Recovery.Original)
				require.NoError(t, m.CheckClosedCapacity(original.Request))
			}
			_, raw, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			_, err = decode(raw, m.VMUID, m.Incarnation)
			require.NoError(t, err)
		})
	}
}

func TestStoreServiceCleanupRecoveryCompletionIsAtomic(t *testing.T) {
	for _, mode := range []string{"normal", "lost-record", "stale-rv", "recreated-vm"} {
		t.Run(mode, func(t *testing.T) {
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			// 同owner与前述更高owner组合互补；恢复不能偷偷要求换epoch。
			m, current := reopenedCleanupRecovery(t, "actual", true, false, false)
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(context.Background())
			require.NoError(t, err)
			if mode == "stale-rv" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"new": "rv"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-record"
			if mode == "recreated-vm" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "replacement-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			result, err := store.CompleteServiceReopen(context.Background(), s, m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
			if mode == "stale-rv" || mode == "recreated-vm" {
				require.Error(t, err)
				require.Nil(t, result)
				api.mu.Lock()
				unchanged := api.vm.Annotations[control.JournalAnnotation]
				api.mu.Unlock()
				require.Equal(t, wire, unchanged, "失败不能只撤销Active或替换一半预算")
				return
			}
			if mode == "lost-record" {
				var lost *UnconfirmedWrite
				require.ErrorAs(t, err, &lost)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
			}
			loaded, err := store.Load(context.Background())
			require.NoError(t, err)
			done := loaded.State()
			require.Nil(t, done.Active)
			require.Nil(t, done.ServiceClosure)
			require.Equal(t, *m.Active.Cleanup, done.CapacityHeld.Cleanup)
			require.Equal(t, *m.Active, done.CapacityHeld.Recovery.Original)
			require.Equal(t, RecoveredAfterCancellation, done.History[len(done.History)-1].Outcome)
			require.NoError(t, done.CheckClosedCapacity(m.Active.Request))
		})
	}
}

func TestServiceCleanupRecoveryWireAndOriginalDigestAreStrict(t *testing.T) {
	m, fence, current, root := cleanupRecoveryModel(t, "unchanged", true, false, true)
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err := m.RecordServiceCleanupRecovery(m.Owner, selector, fence, current, root)
	require.NoError(t, err)
	p := m.ServiceClosure.CleanupRecovery
	raw, err := json.Marshal(p)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	for field := range fields {
		if field == "finalization" {
			continue // 可选字段的必需性由取消类别校验，不能放宽其余必填项。
		}
		var missing map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &missing))
		delete(missing, field)
		body, err := json.Marshal(missing)
		require.NoError(t, err)
		var decoded ServiceCleanupRecovery
		require.Error(t, json.Unmarshal(body, &decoded), field)
	}
	var decoded ServiceCleanupRecovery
	require.Error(t, json.Unmarshal(append([]byte(`{"kind":"unchanged",`), raw[1:]...), &decoded))
	require.Error(t, json.Unmarshal(append([]byte(`{"unexpected":true,`), raw[1:]...), &decoded))
	changed := cloneServiceCleanupRecovery(p)
	var final budget.FinalizationCommand
	require.NoError(t, json.Unmarshal(changed.Finalization.Command, &final))
	final.Finalized.GuestUsableMemoryBytes++
	changed.Finalization.Command, err = json.Marshal(final)
	require.NoError(t, err)
	changed.Finalization.Observation.Data, err = json.Marshal(final.Finalized)
	require.NoError(t, err)
	require.Error(t, changed.validate(*m.Active), "同时修改命令/回执仍不能沿用原预算Step摘要")
}

func TestServiceCleanupRecoveryRearmAndCompletionTakeover(t *testing.T) {
	for _, kind := range []string{"actual", "committed", "unchanged"} {
		t.Run(kind, func(t *testing.T) {
			m, fence, current, root := cleanupRecoveryModel(t, kind, true, false, true)
			_, _, _, selector := m.ServiceClosure.Selection()
			m, err := m.RecordServiceCleanupRecovery(m.Owner, selector, fence, current, root)
			require.NoError(t, err)
			original := cloneOperation(*m.Active)
			before := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: current.Observed.CapacityGeneration, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Renewed: false, Armed: true, Expired: true, DeadlineNS: 100, LeaseEpoch: m.Owner.Epoch - 1, TimeoutMS: 30000, Observed: current.Observed}
			m, err = m.StartServiceRearm(m.Owner, selector, before)
			require.NoError(t, err)
			m, err = m.RecordServiceRearm(m.Owner, rearmFact(t, m))
			require.NoError(t, err)
			before.Expired, before.DeadlineNS, before.LeaseEpoch = false, 200, m.Owner.Epoch
			m, err = m.StartServiceReopen(m.Owner, selector, before)
			require.NoError(t, err)
			m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
			require.NoError(t, err)
			opening := m.ServiceClosure.Reopen.Request
			before.Observed = m.ServiceClosure.Reopen.Receipt.Observed
			m, err = m.Takeover(m.Owner, "cleanup-completion-owner")
			require.NoError(t, err)
			before.Request.IssuerEpoch, before.Observed.IssuerFloorEpoch = m.Owner.Epoch, m.Owner.Epoch
			m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, before)
			require.NoError(t, err)
			r := m.History[len(m.History)-1]
			require.Equal(t, original, r.Operation)
			require.Equal(t, opening, r.ServiceCoverage.Request)
			require.Greater(t, r.ServiceCoverage.Origin.Epoch, opening.IssuerEpoch)
			require.Equal(t, RecoveredAfterCancellation, r.Outcome)
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			_, err = decode(wire, m.VMUID, m.Incarnation)
			require.NoError(t, err)
		})
	}
}
