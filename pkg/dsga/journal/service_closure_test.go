package journal

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

func serviceClosureModel(t *testing.T) (Model, control.AdmissionRequest, control.AdmissionResponse) {
	t.Helper()
	_, c, receipt := initialDomain(t)
	m := initialBoundModel(t)
	var err error
	m, err = m.StartInitialAdmission(m.Owner, c)
	require.NoError(t, err)
	m, err = m.RecordInitialAdmission(m.Owner, receipt)
	require.NoError(t, err)
	closeRequest := c.Request
	closeRequest.Open = false
	closeRequest.Revision++
	fact := receipt.Response
	fact.Request = closeRequest
	fact.ContextSHA256, err = closeRequest.ContextSHA256()
	require.NoError(t, err)
	fact.Disposition = control.AdmissionApplied
	fact.Observed.AdmissionOpen = false
	fact.Observed.AdmissionRevision = closeRequest.Revision
	require.NoError(t, fact.ValidateFor(closeRequest, closeRequest.Instance, false))
	return m, closeRequest, fact
}

func TestComputeSuspensionBindsDrainAndPreservesOriginalBudget(t *testing.T) {
	m, closeRequest, fact := serviceClosureModel(t)
	const op = "op_suspend_original"
	closing, err := m.StartComputeSuspension(m.Owner, closeRequest, op)
	require.NoError(t, err)
	require.Equal(t, m.InitialHeld, closing.InitialHeld)
	require.Error(t, closing.CheckComputeSuspensionDrain(op, control.PreparedResponse{}))
	closed, err := closing.RecordServiceClosure(closing.Owner, fact, false)
	require.NoError(t, err)
	_, encoded, err := encode(closed, MaxEncodedBytes)
	require.NoError(t, err)
	stored, err := decode(encoded, closed.VMUID, closed.Incarnation)
	require.NoError(t, err)
	require.Equal(t, closed, stored)
	prepared := control.PreparedResponse{ProtocolVersion: 1, PreparedAPIVersion: 1, RuntimePreparesBlocked: true,
		Connections: control.ConnectionResponse{ProtocolVersion: 1, ConnectionAPIVersion: 1, Enabled: true,
			RequestedSerial: 2, AppliedSerial: 2, Acknowledged: true, PrivateManagementReady: true, PublicConnectionsBlocked: true,
			Work: control.WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 2, ForegroundIdle: true, ObserverExcluded: true, Observed: fact.Observed}}}
	require.NoError(t, stored.CheckComputeSuspensionDrain(op, prepared))
	require.Error(t, stored.CheckComputeSuspensionDrain("op_other", prepared))
	_, err = stored.StartServiceReopen(stored.Owner, stored.ServiceClosure.CommandSHA256, control.AdmissionLeaseResponse{})
	require.Error(t, err)
	_, err = stored.StartServiceRearm(stored.Owner, stored.ServiceClosure.CommandSHA256, control.AdmissionLeaseResponse{})
	require.Error(t, err)
	for _, changed := range []func(*control.PreparedResponse){
		func(p *control.PreparedResponse) { p.PreparedSlots, p.MaximumPreparedSlots = 1, 1 },
		func(p *control.PreparedResponse) {
			p.Connections.Work.ClientProcesses, p.Connections.Work.IdleClients = 1, 1
		},
		func(p *control.PreparedResponse) { p.Connections.Work.ObserverExcluded = false },
		func(p *control.PreparedResponse) { p.Connections.Work.Observed.AdmissionRevision++ },
		func(p *control.PreparedResponse) { p.Connections.Work.Observed.IssuerFloorEpoch++ },
	} {
		copy := prepared
		changed(&copy)
		require.Error(t, stored.CheckComputeSuspensionDrain(op, copy))
	}
}

func TestServiceClosureKeepsNormalAndReconciledPermanentBudgets(t *testing.T) {
	for _, mode := range []string{"normal", "actual-capacity"} {
		t.Run(mode, func(t *testing.T) {
			var m Model
			var snapshot control.Snapshot
			var budgetID string
			if mode == "normal" {
				// v10保留既有Runner来源要求，不能将无Runner的历史v3
				// 预算静默补成新格式。采用实际生产的Runner-backed原预算。
				started, obs := heldStart(t, heldBoundModel(t, testModel(t)), 1, runnerSources())
				var err error
				m, err = started.Record(started.Owner, obs.Key, obs)
				require.NoError(t, err)
				m = heldFinish(t, m)
				var finalized budget.Finalized
				require.NoError(t, json.Unmarshal(m.BudgetHeld.Finalization.Observation.Data, &finalized))
				snapshot, budgetID = finalized.Shared.Observed, finalized.Request.BudgetCommitmentID
			} else {
				started, command, receipt := partialCapacityModel(t)
				m = recordPartialCapacity(t, started, command, receipt)
				var err error
				m, err = m.Finish(m.Owner, command.Capacity.Request.Key)
				require.NoError(t, err)
				snapshot, budgetID = receipt.Shared.Observed, command.Capacity.Request.BudgetCommitmentID
			}
			before := m.Clone()
			request := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: snapshot.AdmissionRevision + 1, ExpectedCapacityGeneration: snapshot.CapacityGeneration, Open: false, BudgetCommitmentID: budgetID}
			closed, err := m.StartServiceClosure(m.Owner, request)
			require.NoError(t, err)
			predated := closed.Clone()
			predated.ServiceClosure.StartedSequence = before.History[len(before.History)-1].ClosedSequence
			require.Error(t, predated.Validate(), "正常/实际预算关闭WAL均须晚于上一操作结案")
			snapshot.AdmissionRevision, snapshot.AdmissionOpen = request.Revision, false
			digest, err := request.ContextSHA256()
			require.NoError(t, err)
			fact := control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, Disposition: control.AdmissionApplied, ContextSHA256: digest, Observed: snapshot}
			closed, err = closed.RecordServiceClosure(closed.Owner, fact, false)
			require.NoError(t, err)
			require.Equal(t, before.BudgetHeld, closed.BudgetHeld)
			require.Equal(t, before.CapacityHeld, closed.CapacityHeld)
			require.Equal(t, before.History, closed.History)
			require.Equal(t, before.BudgetBoundary, closed.BudgetBoundary)
			_, encoded, err := encode(closed, MaxEncodedBytes)
			require.NoError(t, err)
			loaded, err := decode(encoded, closed.VMUID, closed.Incarnation)
			require.NoError(t, err)
			require.Equal(t, closed, loaded)
		})
	}
}

// 这里只认证模型/codec和原命令关系，不把协议fixture当真实PG关门证明。
func TestServiceClosureRetainsBudgetUnknownAndOriginalOwnerAcrossCodec(t *testing.T) {
	m, request, fact := serviceClosureModel(t)
	before := m.Clone()
	closed, err := m.StartServiceClosure(m.Owner, request)
	require.NoError(t, err)
	require.EqualValues(t, 10, closed.ProtocolVersion)
	require.Equal(t, before.InitialHeld, closed.InitialHeld)
	require.Nil(t, closed.Active)
	require.Nil(t, closed.HighWater)
	_, encoded, err := encode(closed, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(encoded, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, closed, loaded)
	_, disposition, err := loaded.Begin(loaded.Owner, testRequest(loaded, 1, control.Grow), 0)
	require.NoError(t, err)
	require.Equal(t, Busy, disposition)
	_, err = loaded.StartServiceClosure(loaded.Owner, request)
	require.Error(t, err, "原未知不能换代次覆盖或自动重放")
	loaded, err = loaded.Takeover(loaded.Owner, "new-closure-owner")
	require.NoError(t, err)
	require.Equal(t, closed.ServiceClosure, loaded.ServiceClosure)
	_, err = loaded.RecordServiceClosure(loaded.Owner, fact, false)
	require.Error(t, err, "新owner不能声称直接执行了旧命令")
	fact.Disposition, fact.Observed.IssuerFloorEpoch = control.AdmissionReplayed, loaded.Owner.Epoch
	loaded, err = loaded.RecordServiceClosure(loaded.Owner, fact, true)
	require.NoError(t, err)
	require.Equal(t, Settled, loaded.ServiceClosure.Phase)
	require.Equal(t, before.InitialHeld, loaded.InitialHeld)
	cloned := loaded.Clone()
	cloned.ServiceClosure.Receipt.Observed.Arrays[0].CommittedUpperBytes++
	require.NotEqual(t, cloned.ServiceClosure.Receipt, loaded.ServiceClosure.Receipt)
	_, encoded, err = encode(loaded, MaxEncodedBytes)
	require.NoError(t, err)
	decoded, err := decode(encoded, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, loaded, decoded)
}

func TestServiceClosureRejectsProjectionBorrowedBudgetAndFalseCompletion(t *testing.T) {
	m, request, fact := serviceClosureModel(t)
	closed, err := m.StartServiceClosure(m.Owner, request)
	require.NoError(t, err)
	for _, mode := range []string{"old-version", "missing", "open", "boot", "budget", "owner", "sequence", "digest", "reason", "false-receipt", "initial-unknown", "active"} {
		t.Run(mode, func(t *testing.T) {
			next := closed.Clone()
			switch mode {
			case "old-version":
				next.ProtocolVersion = 9
			case "missing":
				next.ServiceClosure = nil
			case "open":
				next.ServiceClosure.Request.Open = true
			case "boot":
				next.ServiceClosure.Request.Instance.PGBootID++
			case "budget":
				next.ServiceClosure.Request.BudgetCommitmentID = "borrowed-budget"
			case "owner":
				next.ServiceClosure.Origin.HolderID = "different-holder"
			case "sequence":
				next.ServiceClosure.StartedSequence = next.Sequence + 1
			case "digest":
				next.ServiceClosure.CommandSHA256 = "wrong"
			case "reason":
				next.ServiceClosure.Reason = "arbitrary-sensitive-error"
			case "false-receipt":
				next.ServiceClosure.Receipt = &fact
			case "initial-unknown":
				next.InitialHeld.Admission.Phase = Unconfirmed
				next.InitialHeld.Admission.RecordedSequence = 0
				next.InitialHeld.Admission.Receipt = nil
			case "active":
				active, disposition, err := m.Begin(m.Owner, testRequest(m, 1, control.Grow), 0)
				require.NoError(t, err)
				require.Equal(t, Accepted, disposition)
				next.Active, next.HighWater = active.Active, active.HighWater
			}
			require.Error(t, next.Validate())
		})
	}
	for _, mode := range []string{"wrong-request", "open-observed", "not-found"} {
		t.Run(mode, func(t *testing.T) {
			bad := *cloneAdmissionResponse(&fact)
			switch mode {
			case "wrong-request":
				bad.Request.BudgetCommitmentID = "other-budget"
			case "open-observed":
				bad.Observed.AdmissionOpen = true
			case "not-found":
				bad.Disposition = control.AdmissionNotFound
			}
			_, err := closed.RecordServiceClosure(closed.Owner, bad, false)
			require.Error(t, err)
		})
	}
	raw, err := json.Marshal(closed.ServiceClosure)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	for key := range fields {
		projected := make(map[string]json.RawMessage)
		for field, value := range fields {
			if field != key {
				projected[field] = value
			}
		}
		body, err := json.Marshal(projected)
		require.NoError(t, err)
		var decoded ServiceClosure
		require.Error(t, json.Unmarshal(body, &decoded), key+"字段不能缺失")
	}
}

// 关闭WAL必须晚于它所保护的原预算/首次开放，查询回执也不能来自外层
// 尚未确认的未来owner。否则单独看每个字段合法，拼接后却伪造了恢复顺序。
func TestServiceClosureRejectsPredatedBoundaryAndFutureOwnerReceipt(t *testing.T) {
	m, request, fact := serviceClosureModel(t)
	closed, err := m.StartServiceClosure(m.Owner, request)
	require.NoError(t, err)
	for _, mode := range []string{"before-budget", "before-first-open", "future-owner-receipt"} {
		t.Run(mode, func(t *testing.T) {
			next := closed.Clone()
			switch mode {
			case "before-budget":
				next.ServiceClosure.StartedSequence = next.InitialHeld.RecordedSequence
				require.Error(t, next.Validate(), "关闭不能早于它保护的永久预算")
			case "before-first-open":
				next.ServiceClosure.StartedSequence = next.InitialHeld.Admission.RecordedSequence
				require.Error(t, next.Validate(), "空闲关闭必须在原首次开放确认之后")
			case "future-owner-receipt":
				borrowed := *cloneAdmissionResponse(&fact)
				borrowed.Disposition = control.AdmissionReplayed
				borrowed.Observed.IssuerFloorEpoch = next.Owner.Epoch + 1
				_, err := next.RecordServiceClosure(next.Owner, borrowed, true)
				require.Error(t, err, "旧owner不能借外层尚未确认的新floor记录关闭")
			}
		})
	}
}

func TestServiceClosureCodecReservesSpaceForNativeReceipt(t *testing.T) {
	m, request, fact := serviceClosureModel(t)
	closed, err := m.StartServiceClosure(m.Owner, request)
	require.NoError(t, err)
	_, encoded, err := encode(closed, MaxEncodedBytes)
	require.NoError(t, err)
	_, _, err = encode(closed, len(encoded))
	require.Error(t, err, "不能只容得下关闭写前WAL，却不给原生回执和接管留空间")
	// 与既有Active写前使用同一4KiB保留量。真实关闭后仍在同一
	// 注解预算下落账并完整解码，不靠裁剪未知、原预算或原生命令。
	writeBudget := len(encoded) + 4096
	_, _, err = encode(closed, writeBudget)
	require.NoError(t, err)
	settled, err := closed.RecordServiceClosure(closed.Owner, fact, false)
	require.NoError(t, err)
	_, completed, err := encode(settled, writeBudget)
	require.NoError(t, err)
	loaded, err := decode(completed, settled.VMUID, settled.Incarnation)
	require.NoError(t, err)
	require.Equal(t, settled, loaded)
}
