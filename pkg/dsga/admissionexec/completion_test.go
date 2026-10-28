package admissionexec

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 仅验证完整历史审计的结构/跨字段合同。来源和最终结案另由owner的
// 正式SDK/Lease/CAS组合覆盖；此静态领域夹具不能证明任何当前物理效果。
func completionAuditFixture(t *testing.T) (audit, control.AdmissionRequest, budget.Finalized) {
	t.Helper()
	p, root, _ := recoveryDomain(t)
	r := p.Request
	shared := p.Shared
	s := &shared.Observed
	s.CapacityGeneration = r.ExpectedCapacityGeneration + 1
	s.LowBuffers, s.HighBuffers = r.TargetBuffers, r.TargetBuffers
	s.AdmissionOpen, s.AdmissionRevision = true, 1
	s.IssuerFloorEpoch = r.Key.IssuerEpoch
	digest, err := r.ContextSHA256()
	require.NoError(t, err)
	s.Operation = &control.OperationFact{Key: r.Key, Kind: r.Kind, Phase: control.Committed, Reason: control.ReasonNone, TargetBuffers: r.TargetBuffers, ExpectedCapacityGeneration: r.ExpectedCapacityGeneration, ContextSHA256: digest, CancelRequested: false, CancelClosed: true, Replayed: false}
	require.NoError(t, shared.ValidateFor(r.Key.Instance))
	f := budget.Finalized{ProtocolVersion: 1, VMShrinkPermit: false, Request: r, IssuerEpoch: r.Key.IssuerEpoch, RunnerPodName: p.RunnerPodName, RunnerRestartCount: p.RunnerRestartCount, HostReservationID: "original-host-completion-fixture", GuestUsableMemoryBytes: p.GuestUsableMemoryBytes, Shared: shared, SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, Aggregate: &root.Bound, PrivateAndDSM: nil, LFCOutsideShared: nil, HugePoolOutsideShared: nil, Sources: nil}
	require.NoError(t, f.ValidateFor(r, shared, r.Key.IssuerEpoch, f.RunnerPodName, f.RunnerRestartCount, uint64(r.TargetVMMemoryBytes)))
	request := control.AdmissionRequest{ProtocolVersion: 1, Instance: r.Key.Instance, IssuerEpoch: r.Key.IssuerEpoch, Revision: 1, ExpectedCapacityGeneration: s.CapacityGeneration, Open: true, BudgetCommitmentID: r.BudgetCommitmentID}
	hash, err := request.ContextSHA256()
	require.NoError(t, err)
	response := control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, Disposition: control.AdmissionApplied, ContextSHA256: hash, Observed: *s}
	fence := control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: r.Key.Instance, RequestedEpoch: r.Key.IssuerEpoch, Disposition: control.Fenced, Observed: *s}
	return audit{ProtocolVersion: 2, Query: false, Fence: fence, Shared: shared, Response: response, PGAggregate: root, ResourceEpoch: r.Key.IssuerEpoch}, request, f
}

func TestCompletionAuditRequiresOriginalCommandBudgetAndIssuer(t *testing.T) {
	for _, mode := range []string{"applied", "queried-after-takeover", "generic-completed", "old-format", "different-command", "wrong-root-bound", "missing-floor", "different-floor", "unconfirmed", "pending-fence", "new-generation", "unknown-field", "missing-query", "null-query", "duplicate-query"} {
		t.Run(mode, func(t *testing.T) {
			saved, request, full := completionAuditFixture(t)
			switch mode {
			case "queried-after-takeover":
				// 原命令epoch保持不变；仅历史原查询发生于更高已确认floor。
				saved.Query = true
				saved.ResourceEpoch++
				saved.Fence.RequestedEpoch = saved.ResourceEpoch
				saved.Fence.Observed.IssuerFloorEpoch = saved.ResourceEpoch
				saved.Shared.Observed.IssuerFloorEpoch = saved.ResourceEpoch
				saved.Response.Observed.IssuerFloorEpoch = saved.ResourceEpoch
				saved.Response.Disposition = control.AdmissionReplayed
			case "old-format":
				saved.ProtocolVersion = 1
			case "different-command":
				saved.Response.Request.BudgetCommitmentID = "different-original-budget"
			case "wrong-root-bound":
				saved.PGAggregate.Bound.UpperBytes++
			case "missing-floor":
				saved.ResourceEpoch = 0
			case "different-floor":
				saved.Response.Observed.IssuerFloorEpoch++
			case "unconfirmed":
				saved.Response.Disposition = control.AdmissionBusy
			case "pending-fence":
				saved.Fence.Disposition = control.FencePending
			case "new-generation":
				saved.Shared.Observed.CapacityGeneration++
			}
			raw, err := json.Marshal(saved)
			require.NoError(t, err)
			if mode == "generic-completed" {
				raw = []byte(`{"test_only_prior_domain":true}`)
			}
			if mode == "unknown-field" {
				raw = append(raw[:len(raw)-1], []byte(`,"future_permission":true}`)...)
			}
			switch mode {
			case "missing-query":
				raw = bytes.Replace(raw, []byte(`"query":false,`), nil, 1)
			case "null-query":
				raw = bytes.Replace(raw, []byte(`"query":false`), []byte(`"query":null`), 1)
			case "duplicate-query":
				raw = append(raw[:len(raw)-1], []byte(`,"query":false}`)...)
			}
			var step journal.Step
			var observation journal.Observation
			observation.Data = raw
			step.Observation = &observation
			err = validateCompletionAudit(step, request, full)
			if mode == "applied" || mode == "queried-after-takeover" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	_, request, full := completionAuditFixture(t)
	var missing journal.Step
	require.Error(t, validateCompletionAudit(missing, request, full))
}
