package journal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func initialClosureModel(t *testing.T) (Model, control.AdmissionCloseRequest, control.AdmissionClosure) {
	t.Helper()
	_, command, receipt := initialDomain(t)
	m, err := initialBoundModel(t).StartInitialAdmission(testModel(t).Owner, command)
	require.NoError(t, err)
	closeRequest := command.Request
	closeRequest.Open, closeRequest.Revision = false, command.Request.Revision+1
	selection := control.AdmissionCloseRequest{ProtocolVersion: 1, Original: command.Request, Close: closeRequest}
	fact := receipt.Response
	fact.Request, fact.Observed.AdmissionOpen, fact.Observed.AdmissionRevision = closeRequest, false, closeRequest.Revision
	fact.ContextSHA256, err = closeRequest.ContextSHA256()
	require.NoError(t, err)
	proof := control.AdmissionClosure{ProtocolVersion: 1, Original: command.Request, ClosedBy: fact, PriorMayHaveApplied: true, VMShrinkPermit: false}
	require.NoError(t, proof.ValidateFor(selection, closeRequest.Instance, false))
	return m, selection, proof
}

// 仅模型/codec原事实关系验证；原生门禁由Neon的独立真实PG用例验证，
// 本地组合另测生产Session/SDK，不能将本夹具当Linux物理或排空证明。
func TestInitialServiceClosurePreservesUnknownBudgetAndFullProof(t *testing.T) {
	m, selection, fact := initialClosureModel(t)
	before := m.Clone()
	closed, err := m.StartInitialServiceClosure(m.Owner, selection)
	require.NoError(t, err)
	require.EqualValues(t, 12, closed.ProtocolVersion)
	require.Equal(t, before.InitialHeld, closed.InitialHeld)
	require.Equal(t, selection.Original, closed.ServiceClosure.ProtectedAdmission())
	_, encoded, err := encode(closed, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(encoded, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, closed, loaded)
	_, disposition, err := loaded.Begin(loaded.Owner, testRequest(loaded, 1, control.Grow), 0)
	// 首次开放未知本身也拒绝Begin；关闭不能把它投影成首次完成。
	require.NoError(t, err)
	require.Equal(t, Busy, disposition)
	_, err = loaded.RecordServiceClosure(loaded.Owner, fact.ClosedBy, false)
	require.Error(t, err, "只有完整Original/ClosedBy/PriorMayHaveApplied才可结清关闭")
	loaded, err = loaded.Takeover(loaded.Owner, "new-initial-closure-owner")
	require.NoError(t, err)
	_, err = loaded.RecordInitialServiceClosure(loaded.Owner, fact, false)
	require.Error(t, err)
	fact.ClosedBy.Disposition, fact.ClosedBy.Observed.IssuerFloorEpoch = control.AdmissionReplayed, loaded.Owner.Epoch
	loaded, err = loaded.RecordInitialServiceClosure(loaded.Owner, fact, true)
	require.NoError(t, err)
	require.False(t, loaded.ServiceClosure.Pending())
	require.Equal(t, before.InitialHeld, loaded.InitialHeld)
	require.Equal(t, Unconfirmed, loaded.InitialHeld.Admission.Phase)
	require.Nil(t, loaded.ServiceClosure.Receipt)
	cloned := loaded.Clone()
	cloned.ServiceClosure.Original.Revision++
	cloned.ServiceClosure.ClosureReceipt.ClosedBy.Observed.Arrays[0].CommittedUpperBytes++
	require.NotEqual(t, cloned.ServiceClosure, loaded.ServiceClosure)
	_, encoded, err = encode(loaded, MaxEncodedBytes)
	require.NoError(t, err)
	decoded, err := decode(encoded, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, loaded, decoded)
}

func TestInitialServiceClosureRejectsProjectionAndFalseHistory(t *testing.T) {
	m, selection, fact := initialClosureModel(t)
	closed, err := m.StartInitialServiceClosure(m.Owner, selection)
	require.NoError(t, err)
	for _, mode := range []string{"v11", "v10", "original-missing", "original-boot", "original-revision", "original-closed", "close-open", "close-budget", "close-capacity", "close-revision", "predated", "initial-settled", "false-receipt", "reason"} {
		t.Run(mode, func(t *testing.T) {
			next := closed.Clone()
			c := next.ServiceClosure
			switch mode {
			case "v11":
				next.ProtocolVersion = 11
			case "v10":
				next.ProtocolVersion = 10
			case "original-missing":
				c.Original = nil
			case "original-boot":
				c.Original.Instance.PGBootID++
			case "original-revision":
				c.Original.Revision++
			case "original-closed":
				c.Original.Open = false
			case "close-open":
				c.Request.Open = true
			case "close-budget":
				c.Request.BudgetCommitmentID = "borrowed-budget"
			case "close-capacity":
				c.Request.ExpectedCapacityGeneration++
			case "close-revision":
				c.Request.Revision = c.Original.Revision
			case "predated":
				c.StartedSequence = next.InitialHeld.Admission.StartedSequence
			case "initial-settled":
				next.InitialHeld.Admission.Phase = Settled
			case "false-receipt":
				c.ClosureReceipt = &fact
			case "reason":
				c.Reason = "idle_source_unconfirmed"
			}
			c.CommandSHA256, _ = c.Request.ContextSHA256()
			require.Error(t, next.Validate())
		})
	}
	for _, mode := range []string{"no-prior", "wrong-original", "open-observed", "future-floor", "no-proof"} {
		t.Run(mode, func(t *testing.T) {
			bad := fact
			bad.ClosedBy = *cloneAdmissionResponse(&fact.ClosedBy)
			switch mode {
			case "no-prior":
				bad.PriorMayHaveApplied = false
			case "wrong-original":
				bad.Original.Revision++
			case "open-observed":
				bad.ClosedBy.Observed.AdmissionOpen = true
			case "future-floor":
				bad.ClosedBy.Observed.IssuerFloorEpoch++
			case "no-proof":
				bad.ClosedBy.Disposition = control.AdmissionNotFound
			}
			_, err := closed.RecordInitialServiceClosure(closed.Owner, bad, true)
			require.Error(t, err)
		})
	}
}

func TestInitialServiceClosureContinuationRetainsOpenAndVersion(t *testing.T) {
	m, selection, fact := initialClosureModel(t)
	m, err := m.StartInitialServiceClosure(m.Owner, selection)
	require.NoError(t, err)
	initial := m.InitialHeld
	m, err = m.Takeover(m.Owner, "initial-closure-successor")
	require.NoError(t, err)
	request := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: selection.Original, PreviousClose: selection.Close, Close: selection.Close}
	request.Close.IssuerEpoch = m.Owner.Epoch
	m, err = m.StartServiceClosureContinuation(m.Owner, m.ServiceClosure.CommandSHA256, request)
	require.NoError(t, err)
	require.EqualValues(t, 12, m.ProtocolVersion, "接续不得把v12投影回v11")
	proof := control.AdmissionContinuationResponse{ProtocolVersion: 1, Request: request, PriorMayHaveApplied: true, ClosedBy: fact.ClosedBy, VMShrinkPermit: false, ContextSHA256: ""}
	proof.ContextSHA256, err = request.ContextSHA256()
	require.NoError(t, err)
	proof.ClosedBy.Request, proof.ClosedBy.Observed.IssuerFloorEpoch = request.Close, m.Owner.Epoch
	proof.ClosedBy.ContextSHA256, err = request.Close.ContextSHA256()
	require.NoError(t, err)
	m, err = m.RecordServiceClosureContinuation(m.Owner, proof, false)
	require.NoError(t, err)
	require.False(t, m.ServiceClosure.Pending())
	require.Equal(t, initial, m.InitialHeld)
	require.Equal(t, Unconfirmed, m.ServiceClosure.Phase)
	require.Nil(t, m.ServiceClosure.ClosureReceipt)
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m, loaded)
}

func TestInitialServiceClosureStoreRequiresExactCASAndRetainsLostReplies(t *testing.T) {
	for _, mode := range []string{"ok", "lost-start", "lost-record", "stale-rv", "recreated-vm"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, request, fact := initialClosureModel(t)
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
			started, ticket, err := store.StartInitialServiceClosure(ctx, s, m.Owner, request)
			if mode == "stale-rv" || mode == "recreated-vm" {
				require.Error(t, err)
				require.Nil(t, started)
				require.Nil(t, ticket)
				return
			}
			if mode == "lost-start" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				require.Nil(t, started)
				require.Nil(t, ticket)
				loaded, err := store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, request.Original, *loaded.State().ServiceClosure.Original)
				require.Equal(t, request.Close, loaded.State().ServiceClosure.Request)
				return
			}
			require.NoError(t, err)
			require.Equal(t, request.Close, ticket.Request)
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-record"
			api.mu.Unlock()
			completed, err := store.RecordInitialServiceClosure(ctx, started, m.Owner, fact, false)
			if mode == "lost-record" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				require.Nil(t, completed)
			} else {
				require.NoError(t, err)
			}
			loaded, err := store.Load(ctx)
			require.NoError(t, err)
			require.False(t, loaded.State().ServiceClosure.Pending())
			require.Equal(t, m.InitialHeld, loaded.State().InitialHeld)
			raw, err := json.Marshal(loaded.State())
			require.NoError(t, err)
			var roundtrip Model
			require.NoError(t, json.Unmarshal(raw, &roundtrip))
			require.Equal(t, loaded.State(), roundtrip)
		})
	}
}
