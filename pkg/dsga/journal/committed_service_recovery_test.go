package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func committedServiceModel(t *testing.T, mode string) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	var m Model
	var observed control.Snapshot
	var err error
	if mode == "planned" {
		original, request, fact := plannedResumeClosureModel(t)
		m, err = original.StartServiceClosure(original.Owner, request)
		require.NoError(t, err)
		m, err = m.RecordServiceClosure(m.Owner, fact, false)
		observed = fact.Observed
	} else {
		original, request, fact := resumeClosureModel(t, mode == "settled")
		m, err = original.StartResumeServiceClosure(original.Owner, request)
		require.NoError(t, err)
		m, err = m.RecordResumeServiceClosure(m.Owner, fact, false)
		observed = fact.ClosedBy.Observed
	}
	require.NoError(t, err)
	before := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: observed.CapacityGeneration, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Renewed: false, Armed: true, Expired: false, DeadlineNS: 200, LeaseEpoch: m.Owner.Epoch, TimeoutMS: 30000, Observed: observed}
	require.NoError(t, before.ValidateFor(before.Request, *m.PGInstance))
	return m, before
}

func reopenedCommittedService(t *testing.T, mode string) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	m, before := committedServiceModel(t, mode)
	_, _, _, selected := m.ServiceClosure.Selection()
	m, err := m.StartServiceReopen(m.Owner, selected, before)
	require.NoError(t, err)
	m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
	require.NoError(t, err)
	current := cloneLeaseResponse(before)
	current.Observed = m.ServiceClosure.Reopen.Receipt.Observed
	return m, current
}

func TestCommittedServiceRecoveryPreservesOriginalAndClosesBudgetAtomically(t *testing.T) {
	for _, mode := range []string{"planned", "unknown", "settled"} {
		t.Run(mode, func(t *testing.T) {
			m, current := reopenedCommittedService(t, mode)
			original := m.Clone()
			_, err := m.Finish(m.Owner, m.Active.Request.Key)
			require.Error(t, err, "普通Finish不能代替独立业务完成")
			done, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
			require.NoError(t, err)
			require.Equal(t, original, m)
			require.Nil(t, done.Active)
			require.Nil(t, done.ServiceClosure)
			require.Equal(t, original.BudgetHeld, done.BudgetHeld)
			record := done.History[len(done.History)-1]
			require.Equal(t, RecoveredAfterResourceCommit, record.Outcome)
			require.Equal(t, *original.Active, record.Operation, "不改原Resume未知或未派发事实")
			require.NotNil(t, record.ServiceCoverage)
			require.NoError(t, done.CheckClosedBudget(record.Operation.Request))
			frozen, err := recoveryContext(done.ServiceLifecycle.Context)
			require.NoError(t, err)
			require.Equal(t, original, frozen)
			_, wire, err := encode(done, MaxEncodedBytes)
			require.NoError(t, err)
			loaded, err := decode(wire, m.VMUID, m.Incarnation)
			require.NoError(t, err)
			require.Equal(t, done, loaded)
			// 后继零派发取消必须继续同一永久水位；超过历史上限后原
			// 覆盖可从结果缓存淘汰，完整原Active仍在最近生命周期原文。
			for revision := uint64(2); revision <= HistoryLimit+3; revision++ {
				r := testRequest(done, revision, control.Grow)
				r.ExpectedCapacityGeneration = current.Observed.CapacityGeneration
				var disposition Disposition
				done, disposition, err = done.Begin(done.Owner, r, 1)
				require.NoError(t, err)
				require.Equal(t, Accepted, disposition)
				done, err = done.RequestAbort(done.Owner, r.Key)
				require.NoError(t, err)
				done, err = done.Finish(done.Owner, r.Key)
				require.NoError(t, err)
			}
			require.Positive(t, uint64(done.ExpiredResults))
			require.NoError(t, done.CheckClosedBudget(record.Operation.Request))
			require.Equal(t, original.Active, frozen.Active)
		})
	}
}

func TestCommittedServiceRecoveryRejectsCoverageProjection(t *testing.T) {
	m, current := reopenedCommittedService(t, "unknown")
	done, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
	require.NoError(t, err)
	for name, change := range map[string]func(*Model){
		"lost-record":   func(m *Model) { m.History = []Record{} },
		"lost-proof":    func(m *Model) { m.History[len(m.History)-1].ServiceCoverage = nil },
		"false-success": func(m *Model) { m.History[len(m.History)-1].Outcome = Succeeded },
		"false-original": func(m *Model) {
			op := &m.History[len(m.History)-1].Operation
			op.Steps[len(op.Steps)-1].Phase = Planned
		},
		"wrong-finalization": func(m *Model) {
			m.History[len(m.History)-1].ServiceCoverage.FinalizationSHA256 = strings.Repeat("a", 64)
		},
		"future-owner":         func(m *Model) { m.History[len(m.History)-1].ServiceCoverage.Origin.Epoch++ },
		"expired":              func(m *Model) { m.History[len(m.History)-1].ServiceCoverage.Current.Expired = true },
		"newer-open":           func(m *Model) { m.History[len(m.History)-1].ServiceCoverage.Current.Observed.AdmissionRevision++ },
		"lost-lifecycle":       func(m *Model) { m.ServiceLifecycle = nil },
		"lost-budget-boundary": func(m *Model) { m.BudgetBoundary = nil },
	} {
		t.Run(name, func(t *testing.T) {
			bad := done.Clone()
			change(&bad)
			require.Error(t, bad.Validate())
		})
	}
	clone := done.Clone()
	clone.History[len(clone.History)-1].ServiceCoverage.Receipt.Observed.Arrays[0].CommittedUpperBytes++
	clone.History[len(clone.History)-1].ServiceCoverage.Current.Observed.Arrays[0].CommittedUpperBytes++
	require.NotEqual(t, clone, done)
}

func TestCommittedServiceRecoveryTakeoverCompletesOriginalConfirmedReopen(t *testing.T) {
	m, current := reopenedCommittedService(t, "unknown")
	original := m.Clone()
	m, err := m.Takeover(m.Owner, "completion-successor")
	require.NoError(t, err)
	current.Request.IssuerEpoch, current.Observed.IssuerFloorEpoch = m.Owner.Epoch, m.Owner.Epoch
	done, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
	require.NoError(t, err)
	record := done.History[len(done.History)-1]
	require.Equal(t, *original.Active, record.Operation)
	require.Equal(t, original.ServiceClosure.Reopen.Request, record.ServiceCoverage.Request)
	require.Equal(t, m.Owner, record.ServiceCoverage.Origin)
	require.Less(t, record.ServiceCoverage.Request.IssuerEpoch, record.ServiceCoverage.Origin.Epoch)
	require.NoError(t, done.CheckClosedBudget(original.Active.Request))
	_, wire, err := encode(done, MaxEncodedBytes)
	require.NoError(t, err)
	_, err = decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
}

func TestCommittedServiceRecoveryRejectsUnfinishedResourceClosures(t *testing.T) {
	for _, planned := range []bool{false, true} {
		m, request, fact := pendingFinalizeClosureModel(t, planned)
		m, err := m.StartServiceClosure(m.Owner, request)
		require.NoError(t, err)
		m, err = m.RecordServiceClosure(m.Owner, fact, false)
		require.NoError(t, err)
		require.NoError(t, m.Validate())
		require.Error(t, m.CheckStableServiceRecovery(), "完整资源效果仍缺原Finalize结算，不能借准入恢复补造")
	}
	for _, action := range []Action{PrepareBudget, ResizeVM, GuestReady, ResizePG} {
		t.Run(string(action), func(t *testing.T) {
			m, request, fact := activeClosureModel(t, control.Grow, action, true)
			m, err := m.StartActiveServiceClosure(m.Owner, request)
			require.NoError(t, err)
			m, err = m.RecordServiceClosure(m.Owner, fact, false)
			require.NoError(t, err)
			require.Error(t, m.CheckStableServiceRecovery(), "资源Unknown必须走其完整恢复协议")
		})
	}
	for _, kind := range []string{"committed", "actual", "unchanged"} {
		t.Run("cleanup-"+kind, func(t *testing.T) {
			m, request, fact := cleanupClosureModel(t, kind, true, false)
			m, err := m.StartCleanupServiceClosure(m.Owner, request)
			require.NoError(t, err)
			proof := control.AdmissionClosure{ProtocolVersion: 1, VMShrinkPermit: false, Original: *m.ServiceClosure.Original, ClosedBy: fact, PriorMayHaveApplied: true}
			m, err = m.recordOriginalServiceClosure(m.Owner, proof, false, "cleanup_source_unconfirmed")
			require.NoError(t, err)
			require.Error(t, m.CheckStableServiceRecovery(), "不能把取消Cleanup未知退休成资源成功")
		})
	}
}

func TestCommittedServiceRecoveryAcrossRearmReclosureAndRepeatedExpiry(t *testing.T) {
	for _, mode := range []string{"planned", "unknown", "settled"} {
		t.Run(mode, func(t *testing.T) {
			m, b := committedServiceModel(t, mode)
			original := m.Clone()
			var err error
			m, err = m.Takeover(m.Owner, "committed-first-rearm")
			require.NoError(t, err)
			b.Expired = true
			b.DeadlineNS = 100
			b.Request.IssuerEpoch, b.Observed.IssuerFloorEpoch = m.Owner.Epoch, m.Owner.Epoch
			_, _, _, selector := m.ServiceClosure.Selection()
			m, err = m.StartServiceRearm(m.Owner, selector, b)
			require.NoError(t, err)
			m, err = m.RecordServiceRearm(m.Owner, rearmFact(t, m))
			require.NoError(t, err)
			b = cloneLeaseResponse(m.ServiceClosure.Rearm.Receipt.Current)
			b.Request.Action, b.Expired = "observe", false
			m, err = m.StartServiceReopen(m.Owner, selector, b)
			require.NoError(t, err)
			// 新开放仍Unknown时独立再关闭，不回填原Resume或新开放成功。
			b.Observed = reopenFact(t, m).Observed
			m, err = m.StartServiceReopenClosure(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, b)
			require.NoError(t, err)
			closeFact := reopenClosureFact(t, m)
			m, err = m.RecordServiceReopenClosure(m.Owner, closeFact, false)
			require.NoError(t, err)
			b.Observed, b.Expired = closeFact.ClosedBy.Observed, true
			_, _, _, selector = m.ServiceClosure.Reopen.Closing.Close.Selection()
			for cycle := 1; cycle <= 3; cycle++ {
				m, err = m.Takeover(m.Owner, fmt.Sprintf("committed-expiry-%d", cycle))
				require.NoError(t, err)
				b.Request.Action, b.Request.IssuerEpoch, b.Observed.IssuerFloorEpoch = "observe", m.Owner.Epoch, m.Owner.Epoch
				m, err = m.StartReopenLeaseRecovery(m.Owner, selector, b)
				require.NoError(t, err)
				fact := reopenRearmFact(t, m)
				m, err = m.RecordReopenLeaseRecovery(m.Owner, fact)
				require.NoError(t, err)
				b = cloneLeaseResponse(fact.Current)
				selector = m.ServiceClosure.Reopen.Rearm.CommandSHA256
				require.Equal(t, original.Active, m.Active)
				require.Equal(t, original.BudgetHeld, m.BudgetHeld)
				_, wire, encErr := encode(m, MaxEncodedBytes)
				require.NoError(t, encErr)
				m, err = decode(wire, m.VMUID, m.Incarnation)
				require.NoError(t, err)
			}
			b.Request.Action, b.Expired = "observe", false
			_, _, _, selector = m.ServiceClosure.Reopen.Closing.Close.Selection()
			m, err = m.RetryServiceReopen(m.Owner, selector, b)
			require.NoError(t, err)
			m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
			require.NoError(t, err)
			b.Observed = m.ServiceClosure.Reopen.Receipt.Observed
			m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, b)
			require.NoError(t, err)
			require.Equal(t, *original.Active, m.History[len(m.History)-1].Operation)
			require.NoError(t, m.CheckClosedBudget(original.Active.Request))
			// 后续完整服务周期不能丢弃旧covered结果或递归嵌套上轮上下文。
			for cycle := 0; cycle < 3; cycle++ {
				q := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: b.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: b.Observed.CapacityGeneration, Open: false, BudgetCommitmentID: original.Active.Request.BudgetCommitmentID}
				m, err = m.StartServiceClosure(m.Owner, q)
				require.NoError(t, err)
				f := serviceReopenFactForRequest(t, q, b.Observed)
				m, err = m.RecordServiceClosure(m.Owner, f, false)
				require.NoError(t, err)
				b.Observed = f.Observed
				m, err = m.StartServiceReopen(m.Owner, m.ServiceClosure.CommandSHA256, b)
				require.NoError(t, err)
				m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
				require.NoError(t, err)
				b.Observed = m.ServiceClosure.Reopen.Receipt.Observed
				m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, b)
				require.NoError(t, err)
				require.Equal(t, *original.Active, m.History[len(m.History)-1].Operation)
				_, _, encErr := encode(m, MaxEncodedBytes)
				require.NoError(t, encErr)
			}
		})
	}
}

func serviceReopenFactForRequest(t *testing.T, q control.AdmissionRequest, snapshot control.Snapshot) control.AdmissionResponse {
	t.Helper()
	digest, err := q.ContextSHA256()
	require.NoError(t, err)
	snapshot.AdmissionOpen, snapshot.AdmissionRevision = q.Open, q.Revision
	return control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: q, ContextSHA256: digest, Disposition: control.AdmissionApplied, Observed: snapshot}
}

func TestStoreCommittedServiceRecoveryCoverageCacheIsolation(t *testing.T) {
	_, store := newStoreAPI(t)
	m, current := reopenedCommittedService(t, "unknown")
	m, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
	require.NoError(t, err)
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	var group sync.WaitGroup
	for range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			loaded, err := store.decodeModel(wire)
			if !assert.NoError(t, err) {
				return
			}
			p := loaded.History[len(loaded.History)-1].ServiceCoverage
			p.Receipt.Observed.Arrays[0].CommittedUpperBytes++
			p.Current.Observed.Arrays[0].CommittedUpperBytes++
			loaded.ServiceLifecycle.Context[0] ^= 1
		}()
	}
	group.Wait()
	loaded, err := store.decodeModel(wire)
	require.NoError(t, err)
	require.Equal(t, m, loaded)
}

func TestCommittedServiceRecoveryReservesRawAtomicCompletionBeforeDispatch(t *testing.T) {
	// 显式模型夹具：三个普通历史操作带有各领域的大但合法审计对象。
	// 压缩后很小仍不能代表原始JSON/base64归档容量足够；当前原预算和
	// Active不得被裁掉来换派发票据。这里不把模型Completed称为生产执行。
	m := heldBoundModel(t, testModel(t))
	for revision := uint64(1); revision <= 3; revision++ {
		var obs Observation
		m, obs = heldStart(t, m, revision, runnerSources())
		var err error
		m, err = m.Record(m.Owner, obs.Key, obs)
		require.NoError(t, err)
		m = heldFinish(t, m)
	}
	m, obs := heldStart(t, m, 4, runnerSources())
	m, err := m.Record(m.Owner, obs.Key, obs)
	require.NoError(t, err)
	for i := range m.History {
		for j := 0; j < 3; j++ {
			m.History[i].Operation.Steps[j].Observation.Data = []byte(`{"audit":"` + strings.Repeat("a", 30*1024) + `"}`)
		}
	}
	require.NoError(t, m.Validate())
	f, err := m.PlannedResumeBudgetForClosure()
	require.NoError(t, err)
	q := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: f.Shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: f.Shared.Observed.CapacityGeneration, Open: false, BudgetCommitmentID: f.Request.BudgetCommitmentID}
	m, err = m.StartServiceClosure(m.Owner, q)
	require.NoError(t, err)
	closed := serviceReopenFactForRequest(t, q, f.Shared.Observed)
	m, err = m.RecordServiceClosure(m.Owner, closed, false)
	require.NoError(t, err)
	original := m.Clone()
	before := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: q.ExpectedCapacityGeneration, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Renewed: false, Armed: true, Expired: false, DeadlineNS: 200, LeaseEpoch: m.Owner.Epoch, TimeoutMS: 30000, Observed: closed.Observed}
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	require.Less(t, len(raw)+MaxObservationBytes+4096, MaxRawBytes/2, "旧单上下文预留会放行本例")
	_, err = m.StartServiceReopen(m.Owner, m.ServiceClosure.CommandSHA256, before)
	require.ErrorContains(t, err, "atomic operation coverage")
	require.Equal(t, original, m)
}

func TestCommittedServiceRecoveryCompletionCASIsAtomic(t *testing.T) {
	for _, mode := range []string{"normal", "lost-record", "stale-rv", "recreated-vm"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, current := reopenedCommittedService(t, "unknown")
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(ctx)
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
			result, err := store.CompleteServiceReopen(ctx, s, m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
			if mode == "stale-rv" || mode == "recreated-vm" {
				require.Error(t, err)
				require.Nil(t, result)
				return
			}
			if mode == "lost-record" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
			}
			loaded, err := store.Load(ctx)
			require.NoError(t, err)
			require.Nil(t, loaded.State().Active)
			require.Nil(t, loaded.State().ServiceClosure)
			require.Equal(t, *m.Active, loaded.State().History[0].Operation)
			require.NoError(t, loaded.State().CheckClosedBudget(m.Active.Request))
		})
	}
}

func TestCommittedServiceRecoveryStrictCoverageDecode(t *testing.T) {
	m, current := reopenedCommittedService(t, "planned")
	done, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
	require.NoError(t, err)
	raw, err := json.Marshal(done.History[0].ServiceCoverage)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	for key := range fields {
		t.Run(key, func(t *testing.T) {
			var cloned map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &cloned))
			delete(cloned, key)
			bad, err := json.Marshal(cloned)
			require.NoError(t, err)
			var p CommittedServiceCoverage
			require.Error(t, json.Unmarshal(bad, &p))
		})
	}
	for _, extra := range []string{`,"query":false}`, `,"unknown":0}`} {
		var p CommittedServiceCoverage
		require.Error(t, json.Unmarshal(append(append([]byte(nil), raw[:len(raw)-1]...), []byte(extra)...), &p))
	}
}
