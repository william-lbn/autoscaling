package journal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func rearmModel(t *testing.T) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	m, request, fact := serviceClosureModel(t)
	m, err := m.StartServiceClosure(m.Owner, request)
	require.NoError(t, err)
	m, err = m.RecordServiceClosure(m.Owner, fact, false)
	require.NoError(t, err)
	m, err = m.Takeover(m.Owner, "rearm-owner")
	require.NoError(t, err)
	s := fact.Observed
	s.IssuerFloorEpoch = m.Owner.Epoch
	q := control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: 1, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}
	before := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: q, Renewed: false, Armed: true, Expired: true, DeadlineNS: 100, LeaseEpoch: m.Owner.Epoch - 1, TimeoutMS: 30000, Observed: s}
	return m, before
}

func rearmFact(t *testing.T, m Model) control.AdmissionRearmStatus {
	t.Helper()
	r := m.ServiceClosure.Rearm
	current := r.Before
	current.Request, current.LeaseEpoch, current.DeadlineNS = r.Request, r.Origin.Epoch, 200
	// 历史已应用但查询时又过期，仍能准确保存原应用事实，不能开放服务。
	fact := control.AdmissionRearmStatus{ProtocolVersion: 1, VMShrinkPermit: false, Request: r.Request, Retained: true, AppliedDeadlineNS: 200, Current: current}
	require.NoError(t, fact.ValidateFor(r.Request, *m.PGInstance))
	return fact
}

func TestServiceRearmRetainsClosureBudgetUnknownAndExactHistoricalReceipt(t *testing.T) {
	for _, mode := range []string{"idle", "initial-unknown", "continuation"} {
		t.Run(mode, func(t *testing.T) {
			m, before := rearmModel(t)
			switch mode {
			case "initial-unknown":
				original, selection, proof := initialClosureModel(t)
				var err error
				m, err = original.StartInitialServiceClosure(original.Owner, selection)
				require.NoError(t, err)
				m, err = m.RecordInitialServiceClosure(m.Owner, proof, false)
				require.NoError(t, err)
				m, err = m.Takeover(m.Owner, "rearm-initial-owner")
				require.NoError(t, err)
				before.Observed = proof.ClosedBy.Observed
				before.Observed.IssuerFloorEpoch = m.Owner.Epoch
				before.Request.Instance = *m.PGInstance
				before.Request.IssuerEpoch = m.Owner.Epoch
			case "continuation":
				original, request, proof := closureContinuationModel(t)
				var err error
				m, err = original.StartServiceClosureContinuation(original.Owner, original.ServiceClosure.CommandSHA256, request)
				require.NoError(t, err)
				m, err = m.RecordServiceClosureContinuation(m.Owner, proof, false)
				require.NoError(t, err)
				before.Observed = proof.ClosedBy.Observed
			}
			retained := m.Clone()
			_, _, _, selector := m.ServiceClosure.Selection()
			started, err := m.StartServiceRearm(m.Owner, selector, before)
			require.NoError(t, err)
			require.EqualValues(t, 19, started.ProtocolVersion)
			_, wire, err := encode(started, MaxEncodedBytes)
			require.NoError(t, err)
			_, _, err = encode(started, len(wire))
			require.Error(t, err, "必须保留原生回执空间")
			writeBudget := len(wire) + MaxObservationBytes + 4096
			_, _, err = encode(started, writeBudget)
			require.NoError(t, err)
			loaded, err := decode(wire, m.VMUID, m.Incarnation)
			require.NoError(t, err)
			require.Equal(t, started, loaded)
			loaded, err = loaded.Takeover(loaded.Owner, "query-rearm-owner")
			require.NoError(t, err)
			completed, err := loaded.RecordServiceRearm(loaded.Owner, rearmFact(t, loaded))
			require.NoError(t, err)
			_, _, err = encode(completed, writeBudget)
			require.NoError(t, err, "同一注解预算足以完整落账")
			closed := completed.Clone().ServiceClosure
			closed.Rearm = nil
			require.Equal(t, retained.ServiceClosure, closed)
			require.Equal(t, retained.InitialHeld, completed.InitialHeld)
			require.Nil(t, completed.Active)
			require.True(t, completed.ServiceClosure.Rearm.Receipt.Current.Expired)
			cloned := completed.Clone()
			cloned.ServiceClosure.Rearm.Receipt.Current.Observed.Arrays[0].CommittedUpperBytes++
			require.NotEqual(t, cloned, completed)
		})
	}
}

func TestServiceRearmRejectsChangedClosedSourcesAndFalseApplication(t *testing.T) {
	m, before := rearmModel(t)
	for name, change := range map[string]func(*control.AdmissionLeaseResponse){
		"live":         func(b *control.AdmissionLeaseResponse) { b.Expired = false },
		"same-epoch":   func(b *control.AdmissionLeaseResponse) { b.LeaseEpoch = m.Owner.Epoch },
		"future-floor": func(b *control.AdmissionLeaseResponse) { b.Observed.IssuerFloorEpoch++ },
		"old-revision": func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionRevision = 0 },
		"capacity":     func(b *control.AdmissionLeaseResponse) { b.Observed.CapacityGeneration++ },
	} {
		t.Run(name, func(t *testing.T) {
			bad := before
			change(&bad)
			_, err := m.StartServiceRearm(m.Owner, m.ServiceClosure.CommandSHA256, bad)
			require.Error(t, err)
		})
	}
	started, err := m.StartServiceRearm(m.Owner, m.ServiceClosure.CommandSHA256, before)
	require.NoError(t, err)
	for name, change := range map[string]func(*Model){
		"erase":     func(m *Model) { m.ServiceClosure.Rearm = nil },
		"downgrade": func(m *Model) { m.ProtocolVersion = 18 },
		"format":    func(m *Model) { m.ServiceClosure.Rearm.ClosureVersion = 13 },
		"closure":   func(m *Model) { m.ServiceClosure.Reason = "changed" },
		"held":      func(m *Model) { m.InitialHeld.BudgetSHA256 = "changed" },
		"deadline":  func(m *Model) { m.ServiceClosure.Rearm.Request.ExpectedDeadlineNS++ },
		"origin":    func(m *Model) { m.ServiceClosure.Rearm.Origin.HolderID = "other-process" },
	} {
		t.Run(name, func(t *testing.T) { bad := started.Clone(); change(&bad); require.Error(t, bad.Validate()) })
	}
	_, err = started.StartServiceRearm(started.Owner, m.ServiceClosure.CommandSHA256, before)
	require.Error(t, err)
	unknown := rearmFact(t, started)
	unknown.Retained, unknown.AppliedDeadlineNS = false, 0
	_, err = started.RecordServiceRearm(started.Owner, unknown)
	require.Error(t, err)
	raw, err := json.Marshal(started.ServiceClosure.Rearm)
	require.NoError(t, err)
	var strict ServiceRearm
	require.Error(t, json.Unmarshal(append(raw[:len(raw)-1], []byte(`,"phase":"unconfirmed"}`)...), &strict))
}

func TestServiceRearmStoreNeverDispatchesUnconfirmedCAS(t *testing.T) {
	for _, mode := range []string{"lost-start", "lost-record", "stale-rv", "recreated-vm", "normal"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, before := rearmModel(t)
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(ctx)
			require.NoError(t, err)
			if mode == "stale-rv" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"update": "concurrent"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-start"
			if mode == "recreated-vm" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "replacement-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			started, ticket, err := store.StartServiceRearm(ctx, s, m.Owner, m.ServiceClosure.CommandSHA256, before)
			if mode == "stale-rv" || mode == "recreated-vm" {
				require.Error(t, err)
				require.Nil(t, started)
				require.Nil(t, ticket)
				return
			}
			if mode == "lost-start" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				require.Nil(t, ticket)
				loaded, err := store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, Unconfirmed, loaded.State().ServiceClosure.Rearm.Phase)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, ticket)
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-record"
			api.mu.Unlock()
			_, err = store.RecordServiceRearm(ctx, started, m.Owner, rearmFact(t, started.State()))
			if mode == "lost-record" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
			} else {
				require.NoError(t, err)
			}
			loaded, err := store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, Settled, loaded.State().ServiceClosure.Rearm.Phase)
			require.NotNil(t, loaded.State().ServiceClosure)
		})
	}
}
