package journal

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func reopenModel(t *testing.T, mode string) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	m, before := rearmModel(t)
	switch mode {
	case "initial-unknown":
		original, selection, proof := initialClosureModel(t)
		var err error
		m, err = original.StartInitialServiceClosure(original.Owner, selection)
		require.NoError(t, err)
		m, err = m.RecordInitialServiceClosure(m.Owner, proof, false)
		require.NoError(t, err)
		before.Observed = proof.ClosedBy.Observed
	case "continuation":
		original, request, proof := closureContinuationModel(t)
		var err error
		m, err = original.StartServiceClosureContinuation(original.Owner, original.ServiceClosure.CommandSHA256, request)
		require.NoError(t, err)
		m, err = m.RecordServiceClosureContinuation(m.Owner, proof, false)
		require.NoError(t, err)
		before.Observed = proof.ClosedBy.Observed
	case "rearm":
		var err error
		m, err = m.StartServiceRearm(m.Owner, m.ServiceClosure.CommandSHA256, before)
		require.NoError(t, err)
		fact := rearmFact(t, m)
		m, err = m.RecordServiceRearm(m.Owner, fact)
		require.NoError(t, err)
		before = fact.Current
	case "idle":
	default:
		t.Fatalf("unknown reopen fixture: %s", mode)
	}
	before.Request = control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: 1, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}
	before.Renewed, before.Armed, before.Expired = false, true, false
	before.DeadlineNS, before.LeaseEpoch = 200, m.Owner.Epoch
	before.Observed.IssuerFloorEpoch = m.Owner.Epoch
	require.NoError(t, before.ValidateFor(before.Request, *m.PGInstance))
	return m, before
}

func reopenFact(t *testing.T, m Model) control.AdmissionResponse {
	t.Helper()
	r := m.ServiceClosure.Reopen
	digest, err := r.Request.ContextSHA256()
	require.NoError(t, err)
	fact := control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: r.Request, Disposition: control.AdmissionApplied, ContextSHA256: digest, Observed: r.Before.Observed}
	fact.Observed.AdmissionOpen, fact.Observed.AdmissionRevision = true, r.Request.Revision
	require.NoError(t, fact.ValidateFor(r.Request, *m.PGInstance, false))
	return fact
}

// 模型测试必须区分原事实与新开放回执。即便新回执已确认，原关闭仍
// 保留、Begin 仍 Busy；后继生产来源审计/再关闭/归档不能省略。
func TestServiceReopenRetainsOriginalClosureBudgetAndUnknown(t *testing.T) {
	for _, mode := range []string{"idle", "initial-unknown", "continuation", "rearm"} {
		t.Run(mode, func(t *testing.T) {
			m, before := reopenModel(t, mode)
			original := m.Clone()
			_, _, _, selector := m.ServiceClosure.Selection()
			started, err := m.StartServiceReopen(m.Owner, selector, before)
			require.NoError(t, err)
			require.EqualValues(t, 20, started.ProtocolVersion)
			require.Equal(t, original, m, "模型选择不能改写输入")
			_, wire, err := encode(started, MaxEncodedBytes)
			require.NoError(t, err)
			_, _, err = encode(started, len(wire))
			require.Error(t, err, "必须保留完整新开放回执空间")
			writeBudget := len(wire) + 4*(MaxObservationBytes+4096)
			_, _, err = encode(started, writeBudget)
			require.NoError(t, err)
			loaded, err := decode(wire, m.VMUID, m.Incarnation)
			require.NoError(t, err)
			require.Equal(t, started, loaded)
			loaded, err = loaded.Takeover(loaded.Owner, "reopen-query-owner")
			require.NoError(t, err)
			fact := reopenFact(t, loaded)
			_, err = loaded.RecordServiceReopen(loaded.Owner, fact, false)
			require.Error(t, err, "新 owner 不能落旧 setter 回执")
			fact.Disposition = control.AdmissionReplayed
			fact.Observed.IssuerFloorEpoch = loaded.Owner.Epoch
			completed, err := loaded.RecordServiceReopen(loaded.Owner, fact, true)
			require.NoError(t, err)
			_, _, err = encode(completed, writeBudget)
			require.NoError(t, err)
			closed := completed.Clone().ServiceClosure
			closed.Reopen = nil
			require.Equal(t, original.ServiceClosure, closed)
			require.Equal(t, original.InitialHeld, completed.InitialHeld)
			require.Equal(t, original.HighWater, completed.HighWater)
			require.Equal(t, original.BudgetBoundary, completed.BudgetBoundary)
			request := testRequest(completed, 1, control.Grow)
			request.Key.Instance = *completed.PGInstance
			request.Key.IssuerEpoch = completed.Owner.Epoch
			_, disposition, err := completed.Begin(completed.Owner, request, 1)
			require.NoError(t, err)
			require.Equal(t, Busy, disposition)
			cloned := completed.Clone()
			cloned.ServiceClosure.Reopen.Before.Observed.Arrays[0].CommittedUpperBytes++
			cloned.ServiceClosure.Reopen.Receipt.Observed.Arrays[0].CommittedUpperBytes++
			require.NotEqual(t, completed, cloned, "快照内层必须隔离")
			_, err = completed.StartServiceReopen(completed.Owner, selector, before)
			require.Error(t, err, "不能重选或覆盖原新开放")
		})
	}
}

func TestServiceReopenRejectsInvalidLeaseAndChangedOriginals(t *testing.T) {
	m, before := reopenModel(t, "idle")
	_, _, _, selector := m.ServiceClosure.Selection()
	for name, change := range map[string]func(*control.AdmissionLeaseResponse){
		"expired":        func(b *control.AdmissionLeaseResponse) { b.Expired = true },
		"unarmed":        func(b *control.AdmissionLeaseResponse) { b.Armed, b.DeadlineNS, b.LeaseEpoch = false, 0, 0 },
		"open":           func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionOpen = true },
		"capacity":       func(b *control.AdmissionLeaseResponse) { b.Observed.CapacityGeneration++ },
		"old-revision":   func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionRevision-- },
		"other-closed":   func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionRevision++ },
		"max-revision":   func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionRevision = control.Uint64(math.MaxUint64) },
		"future-floor":   func(b *control.AdmissionLeaseResponse) { b.Observed.IssuerFloorEpoch++ },
		"future-lease":   func(b *control.AdmissionLeaseResponse) { b.LeaseEpoch = m.Owner.Epoch + 1 },
		"renew-result":   func(b *control.AdmissionLeaseResponse) { b.Request.Action = "renew" },
		"wrong-observer": func(b *control.AdmissionLeaseResponse) { b.Request.IssuerEpoch++ },
	} {
		t.Run(name, func(t *testing.T) {
			bad := cloneLeaseResponse(before)
			change(&bad)
			_, err := m.StartServiceReopen(m.Owner, selector, bad)
			require.Error(t, err)
		})
	}
	_, err := m.StartServiceReopen(m.Owner, "another-closure", before)
	require.Error(t, err)
	started, err := m.StartServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	for name, change := range map[string]func(*Model){
		"erase":      func(m *Model) { m.ServiceClosure.Reopen = nil },
		"downgrade":  func(m *Model) { m.ProtocolVersion = 10 },
		"format":     func(m *Model) { m.ServiceClosure.Reopen.ClosureVersion = 13 },
		"rearm-lost": func(m *Model) { m.ServiceClosure.Reopen.ClosureVersion = 19 },
		"old-close":  func(m *Model) { m.ServiceClosure.Query = !m.ServiceClosure.Query },
		"held":       func(m *Model) { m.ServiceClosure.Reopen.HeldSHA256 = "changed" },
		"budget":     func(m *Model) { m.ServiceClosure.Reopen.Request.BudgetCommitmentID = "another-budget" },
		"revision":   func(m *Model) { m.ServiceClosure.Reopen.Request.Revision++ },
		"owner":      func(m *Model) { m.ServiceClosure.Reopen.Origin.HolderID = "another-process" },
		"sequence":   func(m *Model) { m.ServiceClosure.Reopen.StartedSequence = m.ServiceClosure.RecordedSequence },
		"false-done": func(m *Model) { m.ServiceClosure.Reopen.Phase = Settled },
	} {
		t.Run(name, func(t *testing.T) { bad := started.Clone(); change(&bad); require.Error(t, bad.Validate()) })
	}
	raw, err := json.Marshal(started.ServiceClosure.Reopen)
	require.NoError(t, err)
	var strict ServiceReopen
	require.Error(t, json.Unmarshal(append(raw[:len(raw)-1], []byte(`,"phase":"unconfirmed"}`)...), &strict))
	require.Error(t, json.Unmarshal(append(raw[:len(raw)-1], []byte(`,"ignored":true}`)...), &strict))
}

func TestServiceReopenCannotAdoptCurrentOpenOrExpiredOriginal(t *testing.T) {
	m, before := reopenModel(t, "rearm")
	_, _, _, selector := m.ServiceClosure.Selection()
	started, err := m.StartServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	for _, mode := range []string{"newer-open", "expired", "wrong-budget", "future-floor", "wrong-capacity", "closed"} {
		t.Run(mode, func(t *testing.T) {
			fact := reopenFact(t, started)
			fact.Disposition = control.AdmissionReplayed
			switch mode {
			case "newer-open":
				fact.Observed.AdmissionRevision++
			case "expired":
				fact.Observed.AdmissionRevision++
				fact.Disposition = control.AdmissionResultExpired
			case "wrong-budget":
				fact.Request.BudgetCommitmentID = "another-budget"
				fact.ContextSHA256, err = fact.Request.ContextSHA256()
				require.NoError(t, err)
			case "future-floor":
				fact.Observed.IssuerFloorEpoch++
			case "wrong-capacity":
				fact.Observed.CapacityGeneration++
			case "closed":
				fact.Observed.AdmissionOpen = false
			}
			_, err := started.RecordServiceReopen(started.Owner, fact, true)
			require.Error(t, err)
		})
	}
}

func TestServiceReopenRejectsUnsettledBoundariesAndOverflow(t *testing.T) {
	m, before := reopenModel(t, "idle")
	_, _, _, selector := m.ServiceClosure.Selection()
	wrong := m.Owner
	wrong.HolderID = "unrelated-process"
	_, err := m.StartServiceReopen(wrong, selector, before)
	require.Error(t, err)
	exhausted := m.Clone()
	exhausted.Sequence = control.Uint64(math.MaxUint64)
	require.NoError(t, exhausted.Validate())
	_, err = exhausted.StartServiceReopen(exhausted.Owner, selector, before)
	require.Error(t, err, "序列不能回绕")

	unclosed, closeRequest, _ := serviceClosureModel(t)
	unclosed, err = unclosed.StartServiceClosure(unclosed.Owner, closeRequest)
	require.NoError(t, err)
	_, err = unclosed.StartServiceReopen(unclosed.Owner, unclosed.ServiceClosure.CommandSHA256, before)
	require.Error(t, err, "关闭未知不能重开")

	unrearmed, expired := rearmModel(t)
	unrearmed, err = unrearmed.StartServiceRearm(unrearmed.Owner, unrearmed.ServiceClosure.CommandSHA256, expired)
	require.NoError(t, err)
	_, err = unrearmed.StartServiceReopen(unrearmed.Owner, unrearmed.ServiceClosure.CommandSHA256, before)
	require.Error(t, err, "原 rearm 未知不能跳过")

	active, activeClose, activeFact := pendingFinalizeClosureModel(t, false)
	active, err = active.StartServiceClosure(active.Owner, activeClose)
	require.NoError(t, err)
	active, err = active.RecordServiceClosure(active.Owner, activeFact, false)
	require.NoError(t, err)
	_, err = active.StartServiceReopen(active.Owner, active.ServiceClosure.CommandSHA256, before)
	require.Error(t, err, "原Finalize尚未知，已确认关闭仍须完整资源对账")
}

func TestServiceReopenStoreNeverDispatchesUnconfirmedCAS(t *testing.T) {
	for _, mode := range []string{"lost-start", "lost-record", "stale-rv", "recreated-vm", "normal"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, before := reopenModel(t, "idle")
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(ctx)
			require.NoError(t, err)
			if mode == "stale-rv" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"concurrent": "update"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-start"
			if mode == "recreated-vm" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "replacement-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			started, ticket, err := store.StartServiceReopen(ctx, s, m.Owner, m.ServiceClosure.CommandSHA256, before)
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
				require.Equal(t, Unconfirmed, loaded.State().ServiceClosure.Reopen.Phase)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, ticket)
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-record"
			api.mu.Unlock()
			_, err = store.RecordServiceReopen(ctx, started, m.Owner, reopenFact(t, started.State()), false)
			if mode == "lost-record" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
			} else {
				require.NoError(t, err)
			}
			loaded, err := store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, Settled, loaded.State().ServiceClosure.Reopen.Phase)
			require.Equal(t, m.InitialHeld, loaded.State().InitialHeld)
			require.NotNil(t, loaded.State().ServiceClosure)
		})
	}
}
