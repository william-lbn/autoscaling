package journal

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// Model合同夹具仅验证WAL/codec，不把分项Completed当作真实PG/Root/VM证明。
func pendingFinalizeClosureModel(t *testing.T, planned bool) (Model, control.AdmissionRequest, control.AdmissionResponse) {
	t.Helper()
	_, command, receipt := initialDomain(t)
	m := initialBoundModel(t)
	m, err := m.StartInitialAdmission(m.Owner, command)
	require.NoError(t, err)
	m, err = m.RecordInitialAdmission(m.Owner, receipt)
	require.NoError(t, err)
	m, _ = heldStart(t, m, 1, runnerSources())
	if planned {
		// 显式领域夹具：空Planned不含命令/派发者，也不生成任何未知回执。
		step := &m.Active.Steps[len(m.Active.Steps)-2]
		*step = Step{Ordinal: step.Ordinal, Action: FinalizeBudget, TargetID: "", Command: nil, CommandSHA256: "", Phase: Planned, DispatchedBy: nil, Observation: nil}
	}
	generation, err := m.PendingFinalizationGenerationForClosure()
	require.NoError(t, err)
	r := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: receipt.Command.Request.Revision + 1, ExpectedCapacityGeneration: generation, Open: false, BudgetCommitmentID: m.Active.Request.BudgetCommitmentID}
	o := heldCommand(t, m.Active.Request).Finalized.Shared.Observed
	o.AdmissionOpen, o.AdmissionRevision = false, r.Revision
	digest, err := r.ContextSHA256()
	require.NoError(t, err)
	fact := control.AdmissionResponse{ProtocolVersion: 1, Request: r, ContextSHA256: digest, Disposition: control.AdmissionApplied, Observed: o, VMShrinkPermit: false}
	require.NoError(t, fact.ValidateFor(r, r.Instance, false))
	return m, r, fact
}

func TestPendingFinalizeClosureRetainsOldBudgetAndUnsettledFinalization(t *testing.T) {
	for _, planned := range []bool{true, false} {
		t.Run(map[bool]string{true: "planned", false: "unconfirmed"}[planned], func(t *testing.T) {
			m, request, fact := pendingFinalizeClosureModel(t, planned)
			before := m.Clone()
			m, err := m.StartServiceClosure(m.Owner, request)
			require.NoError(t, err)
			require.EqualValues(t, 15, m.ProtocolVersion)
			require.Equal(t, before.InitialHeld, m.InitialHeld)
			require.Nil(t, m.BudgetHeld)
			require.Equal(t, before.Active, m.Active)
			_, err = m.RequestAbort(m.Owner, m.Active.Request.Key)
			require.Error(t, err)
			_, err = m.Finish(m.Owner, m.Active.Request.Key)
			require.Error(t, err)
			if !planned {
				step := m.Active.Steps[len(m.Active.Steps)-2]
				_, err = m.Record(m.Owner, m.Active.Request.Key, observation(m, step, Completed))
				require.Error(t, err, "关闭不能将原Finalize未知补写为成功预算")
			}
			for _, complete := range []bool{false, true} {
				if complete {
					m, err = m.RecordServiceClosure(m.Owner, fact, false)
					require.NoError(t, err)
				}
				_, wire, err := encode(m, MaxEncodedBytes)
				require.NoError(t, err)
				loaded, err := decode(wire, m.VMUID, m.Incarnation)
				require.NoError(t, err)
				require.Equal(t, m, loaded)
				require.Equal(t, before.Active, loaded.Active)
				require.Equal(t, before.InitialHeld, loaded.InitialHeld)
			}
		})
	}
}

func TestPendingFinalizeClosureRejectsProjectionAndUnfinishedResources(t *testing.T) {
	m, request, _ := pendingFinalizeClosureModel(t, false)
	closed, err := m.StartServiceClosure(m.Owner, request)
	require.NoError(t, err)
	for _, mode := range []string{"v14", "v13", "no-active", "no-held", "wrong-hash", "wrong-capacity", "old-budget-id", "original-open", "reason", "prior-no-effect", "rewrite-finalize"} {
		t.Run(mode, func(t *testing.T) {
			next := closed.Clone()
			c := next.ServiceClosure
			switch mode {
			case "v14":
				next.ProtocolVersion = 14
			case "v13":
				next.ProtocolVersion = 13
			case "no-active":
				next.Active = nil
			case "no-held":
				next.InitialHeld = nil
			case "wrong-hash":
				c.OperationSHA256 = "wrong"
			case "wrong-capacity":
				c.Request.ExpectedCapacityGeneration++
			case "old-budget-id":
				initial, err := next.InitialBudgetFor()
				require.NoError(t, err)
				c.Request.BudgetCommitmentID = initial.PGAggregate.Bootstrap.BudgetCommitmentID
			case "original-open":
				original := request
				original.Open = true
				c.Original = &original
			case "reason":
				c.Reason = "planned_resume_source_unconfirmed"
			case "prior-no-effect":
				next.Active.Steps[0].Observation.Result = NoEffect
			case "rewrite-finalize":
				next.Active.Steps[len(next.Active.Steps)-2].Command = []byte(`{}`)
			}
			c.CommandSHA256, _ = c.Request.ContextSHA256()
			require.Error(t, next.Validate())
		})
	}
	m.Active.Request.ExpectedCapacityGeneration = math.MaxUint64
	_, err = m.pendingFinalizationGeneration()
	require.Error(t, err, "容量代次不能回绕")
}

func TestPendingFinalizeClosureContinuationKeepsOriginalUnknown(t *testing.T) {
	m, request, fact := pendingFinalizeClosureModel(t, false)
	m, err := m.StartServiceClosure(m.Owner, request)
	require.NoError(t, err)
	before := m.Clone()
	m, err = m.Takeover(m.Owner, "pending-finalize-successor")
	require.NoError(t, err)
	n := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: request, PreviousClose: request, Close: request}
	n.Close.IssuerEpoch = m.Owner.Epoch
	m, err = m.StartServiceClosureContinuation(m.Owner, m.ServiceClosure.CommandSHA256, n)
	require.NoError(t, err)
	proof := control.AdmissionContinuationResponse{ProtocolVersion: 1, Request: n, ContextSHA256: "", ClosedBy: fact, PriorMayHaveApplied: true, VMShrinkPermit: false}
	proof.ContextSHA256, err = n.ContextSHA256()
	require.NoError(t, err)
	proof.ClosedBy.Request, proof.ClosedBy.Observed.IssuerFloorEpoch = n.Close, m.Owner.Epoch
	proof.ClosedBy.ContextSHA256, err = n.Close.ContextSHA256()
	require.NoError(t, err)
	m, err = m.RecordServiceClosureContinuation(m.Owner, proof, false)
	require.NoError(t, err)
	require.False(t, m.ServiceClosure.Pending())
	require.Equal(t, before.Active, m.Active)
	require.Equal(t, before.InitialHeld, m.InitialHeld)
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	var loaded Model
	require.NoError(t, json.Unmarshal(raw, &loaded))
	require.Equal(t, m, loaded)
}

func TestPendingFinalizeClosureStoreRequiresConfirmedCAS(t *testing.T) {
	serviceClosureStoreContract(t, func(t *testing.T) (Model, control.AdmissionRequest, control.AdmissionResponse) {
		return pendingFinalizeClosureModel(t, false)
	})
}
