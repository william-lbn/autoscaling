package journal

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 只验证Model/WAL/codec，不将领域夹具Completed称为生产资源执行证据。
func activeClosureModel(t *testing.T, kind control.Kind, action Action, unknown bool) (Model, control.AdmissionRequest, control.AdmissionResponse) {
	t.Helper()
	initial, opening, receipt := initialDomain(t)
	m := initialBoundModel(t)
	m, err := m.StartInitialAdmission(m.Owner, opening)
	require.NoError(t, err)
	m, err = m.RecordInitialAdmission(m.Owner, receipt)
	require.NoError(t, err)
	request := testRequest(m, 1, kind)
	request.TargetBuffers = 1024
	m = beginModel(t, m, request)
	for _, step := range m.Active.Steps {
		if step.Action == action && !unknown {
			break
		}
		raw := testCommand
		if step.Action == Quiesce {
			closing := opening.Request
			closing.Open, closing.Revision, closing.BudgetCommitmentID = false, closing.Revision+1, request.BudgetCommitmentID
			command := budget.QuiesceCommand{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, Close: closing, PGAggregate: initial.PGAggregate}
			raw, err = json.Marshal(command)
			require.NoError(t, err)
		}
		m, err = m.Start(m.Owner, request.Key, step.Action, request.Key.Instance.ComputeInstanceID, raw)
		require.NoError(t, err)
		if step.Action == action {
			break
		}
		m, err = m.Record(m.Owner, request.Key, observation(m, m.Active.Steps[int(step.Ordinal)-1], Completed))
		require.NoError(t, err)
	}
	r := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: opening.Request.Revision + 2, ExpectedCapacityGeneration: request.ExpectedCapacityGeneration, Open: false, BudgetCommitmentID: request.BudgetCommitmentID}
	o := receipt.Response.Observed
	o.AdmissionOpen, o.AdmissionRevision = false, r.Revision
	digest, err := r.ContextSHA256()
	require.NoError(t, err)
	fact := control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: r, ContextSHA256: digest, Disposition: control.AdmissionApplied, Observed: o}
	require.NoError(t, fact.ValidateFor(r, r.Instance, false))
	return m, r, fact
}

func TestActiveClosurePreservesEveryUnknownAndOriginalBudget(t *testing.T) {
	for _, action := range []Action{PrepareBudget, ReserveHost, ResizeVM, GuestReady, ResizePG} {
		for _, unknown := range []bool{false, true} {
			if action == ResizePG && unknown {
				continue
			}
			t.Run(string(action)+map[bool]string{false: "-planned", true: "-unknown"}[unknown], func(t *testing.T) {
				m, r, fact := activeClosureModel(t, control.Grow, action, unknown)
				before := m.Clone()
				m, err := m.StartActiveServiceClosure(m.Owner, r)
				require.NoError(t, err)
				require.EqualValues(t, 16, m.ProtocolVersion)
				require.Nil(t, m.ServiceClosure.Original)
				require.Equal(t, before.Active, m.Active)
				require.Equal(t, before.InitialHeld, m.InitialHeld)
				_, err = m.RequestAbort(m.Owner, m.Active.Request.Key)
				require.Error(t, err)
				_, err = m.Finish(m.Owner, m.Active.Request.Key)
				require.Error(t, err)
				m, err = m.RecordServiceClosure(m.Owner, fact, false)
				require.NoError(t, err)
				_, wire, err := encode(m, MaxEncodedBytes)
				require.NoError(t, err)
				loaded, err := decode(wire, m.VMUID, m.Incarnation)
				require.NoError(t, err)
				require.Equal(t, m, loaded)
				require.Equal(t, before.Active, loaded.Active)
			})
		}
	}
}

func TestActiveClosureFencesOriginalQuiesceAndContinuesOnlyItsSelection(t *testing.T) {
	m, r, fact := activeClosureModel(t, control.Shrink, Quiesce, true)
	original, err := m.ActiveAdmissionForClosure()
	require.NoError(t, err)
	require.NotNil(t, original)
	before := m.Clone()
	m, err = m.StartActiveServiceClosure(m.Owner, r)
	require.NoError(t, err)
	require.Equal(t, original, m.ServiceClosure.Original)
	proof := control.AdmissionClosure{ProtocolVersion: 1, VMShrinkPermit: false, Original: *original, ClosedBy: fact, PriorMayHaveApplied: true}
	settled, err := m.recordOriginalServiceClosure(m.Owner, proof, false, "active_source_unconfirmed")
	require.NoError(t, err)
	require.Equal(t, before.Active, settled.Active)
	m, err = m.Takeover(m.Owner, "active-close-successor")
	require.NoError(t, err)
	n := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: *original, PreviousClose: r, Close: r}
	n.Close.IssuerEpoch = m.Owner.Epoch
	m, err = m.StartServiceClosureContinuation(m.Owner, m.ServiceClosure.CommandSHA256, n)
	require.NoError(t, err)
	require.Equal(t, before.Active, m.Active)
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	_, err = decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
}

func TestActiveClosureRejectsCapacityChangesAndWALProjection(t *testing.T) {
	m, r, _ := activeClosureModel(t, control.Grow, ResizeVM, true)
	closed, err := m.StartActiveServiceClosure(m.Owner, r)
	require.NoError(t, err)
	for _, mode := range []string{"old-reader", "no-active", "rewrite-active", "wrong-generation", "wrong-budget", "wrong-reason", "invent-original", "no-held"} {
		t.Run(mode, func(t *testing.T) {
			next := closed.Clone()
			switch mode {
			case "old-reader":
				next.ProtocolVersion = 15
			case "no-active":
				next.Active = nil
			case "rewrite-active":
				next.Active.AbortRequested = true
			case "wrong-generation":
				next.ServiceClosure.Request.ExpectedCapacityGeneration++
			case "wrong-budget":
				next.ServiceClosure.Request.BudgetCommitmentID = "substitute"
			case "wrong-reason":
				next.ServiceClosure.Reason = "finalize_source_unconfirmed"
			case "invent-original":
				value := r
				next.ServiceClosure.Original = &value
			case "no-held":
				next.InitialHeld = nil
			}
			next.ServiceClosure.CommandSHA256, _ = next.ServiceClosure.Request.ContextSHA256()
			require.Error(t, next.Validate())
		})
	}
	m, r, _ = activeClosureModel(t, control.Grow, ResizePG, true)
	_, err = m.StartActiveServiceClosure(m.Owner, r)
	require.Error(t, err, "PG已发送不能套用原容量关闭合同")
}

func TestActiveClosureStoreRequiresConfirmedCAS(t *testing.T) {
	serviceClosureStoreContract(t, func(t *testing.T) (Model, control.AdmissionRequest, control.AdmissionResponse) {
		return activeClosureModel(t, control.Grow, ResizeVM, true)
	}, true)
}
