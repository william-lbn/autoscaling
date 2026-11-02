package budget

import (
	"encoding/json"
	"os"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

// 候选只验证关闭准入及排水合同，不赋予硬限或 VM 下降权限。
func normalQuiesceQualificationFixture(t *testing.T) (Prepared, QuiesceCommand, Quiesced) {
	t.Helper()
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
	command := QuiesceCommand{ProtocolVersion: 1, Request: req, Close: closeIntent, PGAggregate: a}
	return p, command, q
}

func normalQuiesceLowerPrepared(t *testing.T) (Prepared, QuiesceCommand, Quiesced) {
	t.Helper()
	p, command, q := normalQuiesceQualificationFixture(t)
	p.Limits.MemoryMaxBytes = 256 << 20
	p.NormalQuotaProtocolVersion = 1
	p.NormalQuotaFence = &control.FenceResponse{ProtocolVersion: 1, RequestedInstance: p.Request.Key.Instance, RequestedEpoch: p.Request.Key.IssuerEpoch, Disposition: control.Fenced, Observed: p.Shared.Observed}
	require.NoError(t, p.ValidateFor(p.Request, p.Identity))
	return p, command, q
}

func TestNormalQuiesceLowerLimitOnlyPreparesDrain(t *testing.T) {
	p, command, q := normalQuiesceLowerPrepared(t)
	require.False(t, p.VMShrinkPermit)
	require.False(t, command.VMShrinkPermit)
	require.False(t, q.VMShrinkPermit)
	require.Less(t, p.Limits.MemoryMaxBytes, command.PGAggregate.Bootstrap.Limits.MemoryMaxBytes)
	require.NoError(t, command.ValidateFor(p, p.Request.Key.IssuerEpoch))
	require.NoError(t, q.ValidateFor(p, p.Request.Key.IssuerEpoch))
	legacy := p
	legacy.NormalQuotaProtocolVersion, legacy.NormalQuotaFence = 0, nil
	require.NoError(t, legacy.ValidateFor(legacy.Request, legacy.Identity))
	require.ErrorContains(t, command.ValidateFor(legacy, legacy.Request.Key.IssuerEpoch), "memory.max decrease")
	require.ErrorContains(t, q.ValidateFor(legacy, legacy.Request.Key.IssuerEpoch), "memory.max decrease")
}

func TestNormalQuiesceRequiresCompleteOriginalQualification(t *testing.T) {
	for _, name := range []string{"missing_fence", "unknown_protocol", "wrong_fence_generation", "command_vm_permit", "prepared_vm_permit", "wrong_budget", "wrong_close_generation", "wrong_task_limit", "wrong_root_identity"} {
		t.Run(name, func(t *testing.T) {
			p, command, _ := normalQuiesceLowerPrepared(t)
			switch name {
			case "missing_fence":
				p.NormalQuotaFence = nil
			case "unknown_protocol":
				p.NormalQuotaProtocolVersion = 2
			case "wrong_fence_generation":
				p.NormalQuotaFence.Observed.CapacityGeneration++
			case "command_vm_permit":
				command.VMShrinkPermit = true
			case "prepared_vm_permit":
				p.VMShrinkPermit = true
			case "wrong_budget":
				command.Close.BudgetCommitmentID = "other"
			case "wrong_close_generation":
				command.Close.ExpectedCapacityGeneration++
			case "wrong_task_limit":
				p.Limits.PidsMaxTasks++
			case "wrong_root_identity":
				command.PGAggregate.Identity.CgroupInode++
			}
			require.Error(t, command.ValidateFor(p, p.Request.Key.IssuerEpoch))
		})
	}
}
