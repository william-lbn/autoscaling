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
		if step.Action == ResizePG {
			raw, err = json.Marshal(request)
			require.NoError(t, err)
		}
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

// 在线观察原命令没有 Gate 写入，真实硬预算、原 epoch、PG 和目标都要逐项匹配。
// 夹具沿已有的真实格式写入，分别验证接管和实际安全关门依然能保留原操作。
func TestOnlineQuiesceRestartDoesNotInventAnAdmissionWrite(t *testing.T) {
	m, example := completePrepareFixture(t)
	initial, _, _ := initialDomain(t)
	p := example.Prepared
	p.Request.Kind = control.Shrink
	p.Request.TargetBuffers = p.Shared.Observed.LowBuffers / 2
	p.Request.TargetVMMemoryBytes = p.VMBeforeMemoryBytes
	p.GuestUsableMemoryBytes = initial.GuestUsableMemoryBytes
	p.TargetSharedUpperBytes = p.Shared.CurrentMainAndArraysUpperBytes
	p.Shared.Observed.AdmissionOpen, p.Shared.Observed.AdmissionRevision = true, 2
	p.NormalQuotaFence.Observed = p.Shared.Observed
	p.OnlineShrinkProtocolVersion = 1
	require.NoError(t, p.ValidateFor(p.Request, p.Identity))
	checkpoint := budget.QuiesceCommand{ProtocolVersion: 2, Request: p.Request, ServingRevision: 2, PGAggregate: initial.PGAggregate}
	require.NoError(t, checkpoint.ValidateFor(p, m.Owner.Epoch))
	encode := func(v any) []byte {
		b, err := json.Marshal(v)
		require.NoError(t, err)
		return b
	}
	id := p.Request.Key.Instance.ComputeInstanceID
	op := Operation{Origin: m.Owner, Request: p.Request, Steps: []Step{
		{Action: PrepareBudget, TargetID: id, Ordinal: 1, Phase: Settled, Observation: &Observation{Result: Completed, Data: encode(p)}},
		{Action: Quiesce, TargetID: id, Ordinal: 2, Phase: Unconfirmed, Command: encode(checkpoint), DispatchedBy: &m.Owner},
	}}
	step := op.Steps[1]
	online, err := op.OnlineQuiesce(step)
	require.NoError(t, err)
	require.True(t, online)
	m.Active = &op
	request, err := m.activeClosureBoundary()
	require.NoError(t, err)
	require.Nil(t, request)
	wrong := step
	wrong.TargetID = "another-compute"
	_, err = op.OnlineQuiesce(wrong)
	require.Error(t, err)
	op.Steps[0].Observation = nil
	_, err = op.OnlineQuiesce(step)
	require.Error(t, err)
	_, err = m.activeClosureBoundary()
	require.Error(t, err)
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
	closed, err = m.StartActiveServiceClosure(m.Owner, r)
	require.NoError(t, err)
	require.EqualValues(t, 17, closed.ProtocolVersion, "PG已派发使用完整原命令和有限容量边界")
}

func TestActiveClosureStoreRequiresConfirmedCAS(t *testing.T) {
	serviceClosureStoreContract(t, func(t *testing.T) (Model, control.AdmissionRequest, control.AdmissionResponse) {
		return activeClosureModel(t, control.Grow, ResizeVM, true)
	}, true)
}
