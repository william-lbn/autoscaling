package budget

import (
	"encoding/json"
	"os"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

func TestContinuousQuiesceKeepsRetainedCostsAndRejectsUnprovedLowerLimit(t *testing.T) {
	var req control.Request
	raw, err := os.ReadFile("../../api/dsga/testdata/request-v1.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &req))
	req.Kind, req.TargetBuffers = control.Shrink, 512
	view := aggregateView()
	view.Identity.VMUID, view.Identity.ComputeInstanceID = req.Key.Instance.VMUID, req.Key.Instance.ComputeInstanceID
	view.Bootstrap.Identity, view.Birth.Identity, view.Actual.Identity = view.Identity, view.Identity, view.Identity
	view.Bootstrap.Limits.MemoryMaxBytes, view.Actual.MemoryMaxBytes = 512<<20, 512<<20
	view.Actual.GuestMemTotalBytes = 768 << 20
	a, err := RootAggregateFromView(view)
	require.NoError(t, err)
	var fact control.ControlResponse
	raw, err = os.ReadFile("../../api/dsga/testdata/response-v1.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &fact))
	s := fact.Observed
	s.Operation = nil
	s.LowBuffers, s.HighBuffers = 1024, 1024
	s.CapacityGeneration = req.ExpectedCapacityGeneration
	s.AdmissionRevision = 1
	shared := control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: 16 << 20, StartupArraysUpperBytes: 16 << 20, CurrentMainAndArraysUpperBytes: 80 << 20, Observed: s}
	b := func(id string, n control.Uint64) Bound {
		return Bound{Kind: CommittedUpper, SourceID: id, UpperBytes: n}
	}
	p := Prepared{ProtocolVersion: 1, Request: req, Identity: view.Identity, RunnerPodName: "runner", VMBeforeMemoryBytes: 2 << 30, GuestUsableMemoryBytes: 768 << 20, Shared: shared, TargetSharedUpperBytes: 80 << 20, Limits: view.Bootstrap.Limits, SystemOutsidePG: b("system", 24<<20), Transition: b("transition", 4<<20), Safety: b("safety", 4<<20)}
	require.NoError(t, p.ValidateFor(req, view.Identity))
	closeIntent := control.AdmissionRequest{ProtocolVersion: 1, Instance: req.Key.Instance, IssuerEpoch: req.Key.IssuerEpoch, Revision: 1, ExpectedCapacityGeneration: req.ExpectedCapacityGeneration, BudgetCommitmentID: req.BudgetCommitmentID}
	q := Quiesced{ProtocolVersion: 1, Request: req, Close: closeIntent, PGAggregate: a, DSM: control.DSMResponse{ProtocolVersion: 1, DSMAPIVersion: 1, CurrentBootOnly: true, AccountingValid: true, PageBytes: 4096, ControlUpperBytes: 4096, ExternalSegments: 1, UnknownSegments: 1, ReservedUpperBytes: 4096, Prepared: control.PreparedResponse{ProtocolVersion: 1, PreparedAPIVersion: 1, PreparedSlots: 1, MaximumPreparedSlots: 4, RuntimePreparesBlocked: true, Connections: control.ConnectionResponse{ProtocolVersion: 1, ConnectionAPIVersion: 1, Enabled: true, RequestedSerial: 2, AppliedSerial: 2, Acknowledged: true, PrivateManagementReady: true, PublicConnectionsBlocked: true, Work: control.WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 2, ForegroundIdle: true, ObserverExcluded: true, BackgroundProcesses: 3, AuxiliaryProcesses: 2, Observed: s}}}}}
	require.NoError(t, q.ValidateFor(p, req.Key.IssuerEpoch), "非零后台/2PC/未知DSM持续保留，不伪造清空")
	command := QuiesceCommand{ProtocolVersion: 1, Request: req, Close: closeIntent, PGAggregate: a}
	require.NoError(t, command.ValidateFor(p, req.Key.IssuerEpoch))
	changedTasks := p
	changedTasks.Limits.PidsMaxTasks = 64
	require.ErrorContains(t, command.ValidateFor(changedTasks, req.Key.IssuerEpoch), "immutable task limit")
	changedSystem := p
	changedSystem.Limits.SystemMemoryMaxBytes = 16 << 20
	require.ErrorContains(t, command.ValidateFor(changedSystem, req.Key.IssuerEpoch), "immutable task limit")

	changedAS := p
	changedAS.Limits.PidsMaxTasks = 64
	changedAS.Limits.AddressSpaceMaxBytes = 128 << 20
	require.ErrorContains(t, command.ValidateFor(changedAS, req.Key.IssuerEpoch), "immutable task limit")

	commandJSON, err := json.Marshal(command)
	require.NoError(t, err)
	var commandFields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(commandJSON, &commandFields))
	for key := range commandFields {
		var copy map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(commandJSON, &copy))
		delete(copy, key)
		missing, e := json.Marshal(copy)
		require.NoError(t, e)
		var decoded QuiesceCommand
		require.Error(t, json.Unmarshal(missing, &decoded), key)
	}
	raw, err = json.Marshal(q)
	require.NoError(t, err)
	for _, change := range []func(*Quiesced){
		func(q *Quiesced) { q.VMShrinkPermit = true }, func(q *Quiesced) { q.Close.Open = true }, func(q *Quiesced) { q.Close.Revision++ },
		func(q *Quiesced) { q.Close.BudgetCommitmentID = "other" }, func(q *Quiesced) { q.DSM.Prepared.Connections.PrivateManagementReady = false },
		func(q *Quiesced) { q.DSM.Prepared.Connections.Work.ObserverExcluded = false }, func(q *Quiesced) { q.DSM.Prepared.Connections.Work.Observed.CapacityGeneration++ },
		func(q *Quiesced) { q.PGAggregate.Bound.UpperBytes++ }, func(q *Quiesced) { q.DSM.AccountingValid = false },
	} {
		var next Quiesced
		require.NoError(t, json.Unmarshal(raw, &next))
		change(&next)
		require.Error(t, next.ValidateFor(p, req.Key.IssuerEpoch))
	}
	lower := p
	lower.Limits.MemoryMaxBytes = 256 << 20
	require.NoError(t, lower.ValidateFor(req, view.Identity), "总量/当前RSS足够不代表后台未来不会分配")
	require.ErrorContains(t, q.ValidateFor(lower, req.Key.IssuerEpoch), "memory.max decrease")
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	for key := range fields {
		var copy map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &copy))
		delete(copy, key)
		b, err := json.Marshal(copy)
		require.NoError(t, err)
		var next Quiesced
		require.Error(t, json.Unmarshal(b, &next), key)
	}
}
