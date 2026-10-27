package budget

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func committedAbortFixture(t *testing.T) (Finalized, CommittedAbortCommand, CommittedAbortReceipt) {
	t.Helper()
	raw, err := os.ReadFile("../../api/dsga/testdata/request-v1.json")
	require.NoError(t, err)
	var request control.Request
	require.NoError(t, json.Unmarshal(raw, &request))
	raw, err = os.ReadFile("../../api/dsga/testdata/response-v1.json")
	require.NoError(t, err)
	var response control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &response))
	view := aggregateView()
	view.Identity.VMUID, view.Identity.ComputeInstanceID = request.Key.Instance.VMUID, request.Key.Instance.ComputeInstanceID
	view.Bootstrap.Identity, view.Birth.Identity = view.Identity, view.Identity
	view.Bootstrap.IssuerEpoch = request.Key.IssuerEpoch
	view.IssuerFloorEpoch, view.Actual.Identity = request.Key.IssuerEpoch, view.Identity
	aggregate, err := RootAggregateFromView(view)
	require.NoError(t, err)
	f := Finalized{
		ProtocolVersion: 1, Request: request, IssuerEpoch: request.Key.IssuerEpoch, RunnerPodName: "runner", HostReservationID: "host-original", GuestUsableMemoryBytes: 768 << 20,
		Shared: control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: 16 << 20, StartupArraysUpperBytes: 16 << 20, CurrentMainAndArraysUpperBytes: 80 << 20, Observed: response.Observed, VMShrinkPermit: false}, Aggregate: &aggregate.Bound,
		SystemOutsidePG: Bound{Kind: CommittedUpper, UpperBytes: 24 << 20, SourceID: "system-original"}, Transition: Bound{Kind: CommittedUpper, UpperBytes: 4 << 20, SourceID: "transition-original"}, Safety: Bound{Kind: CommittedUpper, UpperBytes: 4 << 20, SourceID: "safety-original"}, VMShrinkPermit: false, RunnerRestartCount: 0, PrivateAndDSM: nil, LFCOutsideShared: nil, HugePoolOutsideShared: nil, Sources: nil,
	}
	command := CommittedAbortCommand{
		ProtocolVersion: 1, Request: request, FinalizationSHA256: strings.Repeat("a", 64), MaySend: true,
		Admission: control.AdmissionRequest{ProtocolVersion: 1, Instance: request.Key.Instance, IssuerEpoch: request.Key.IssuerEpoch, Revision: response.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: response.Observed.CapacityGeneration, Open: true, BudgetCommitmentID: request.BudgetCommitmentID},
	}
	shared := f.Shared
	shared.Observed.AdmissionOpen, shared.Observed.AdmissionRevision = true, command.Admission.Revision
	hash, err := command.Admission.ContextSHA256()
	require.NoError(t, err)
	receipt := CommittedAbortReceipt{
		ProtocolVersion: 1, Command: command, Query: false,
		Fence: control.FenceResponse{ProtocolVersion: 1, RequestedInstance: request.Key.Instance, RequestedEpoch: request.Key.IssuerEpoch, Disposition: control.Fenced, Observed: shared.Observed, VMShrinkPermit: false}, Shared: shared,
		Response: control.AdmissionResponse{ProtocolVersion: 1, Request: command.Admission, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: shared.Observed, VMShrinkPermit: false}, PGAggregate: aggregate, ResourceEpoch: request.Key.IssuerEpoch,
	}
	require.NoError(t, receipt.ValidateFor(command, f, request.Key.IssuerEpoch, request.Key.IssuerEpoch))
	return f, command, receipt
}

func TestCommittedAbortWireRequiresCompleteOriginalAndStrictFields(t *testing.T) {
	f, command, receipt := committedAbortFixture(t)
	for _, value := range []any{command, receipt} {
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		decode := func(body []byte) error {
			if _, ok := value.(CommittedAbortCommand); ok {
				var decoded CommittedAbortCommand
				if err := json.Unmarshal(body, &decoded); err != nil {
					return err
				}
				return decoded.ValidateFor(f, f.IssuerEpoch)
			}
			var decoded CommittedAbortReceipt
			if err := json.Unmarshal(body, &decoded); err != nil {
				return err
			}
			return decoded.ValidateFor(command, f, f.IssuerEpoch, f.IssuerEpoch)
		}
		require.NoError(t, decode(raw))
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &fields))
		for field := range fields {
			var missing map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &missing))
			delete(missing, field)
			body, err := json.Marshal(missing)
			require.NoError(t, err)
			require.Error(t, decode(body), field)
		}
		require.Error(t, decode(append([]byte(`{"protocol_version":1,`), raw[1:]...)))
		require.Error(t, decode(append([]byte(`{"unexpected":true,`), raw[1:]...)))
	}
}

func TestCommittedAbortCommandRejectsChangedContextOrReboundEpoch(t *testing.T) {
	f, original, _ := committedAbortFixture(t)
	for _, mode := range []string{"target", "hash", "budget", "generation", "closed", "old-epoch", "new-epoch", "readonly-new-setter"} {
		t.Run(mode, func(t *testing.T) {
			command := original
			switch mode {
			case "target":
				command.Request.TargetVMMemoryBytes++
			case "hash":
				command.FinalizationSHA256 = strings.Repeat("A", 64)
			case "budget":
				command.Admission.BudgetCommitmentID += "-different"
			case "generation":
				command.Admission.ExpectedCapacityGeneration++
			case "closed":
				command.Admission.Open = false
			case "old-epoch":
				command.Admission.IssuerEpoch--
			case "new-epoch":
				command.Admission.IssuerEpoch++
			case "readonly-new-setter":
				command.MaySend = false
			}
			if mode == "readonly-new-setter" {
				_, _, receipt := committedAbortFixture(t)
				receipt.Command = command
				require.Error(t, receipt.ValidateFor(command, f, f.IssuerEpoch, f.IssuerEpoch))
			} else {
				require.Error(t, command.ValidateFor(f, f.IssuerEpoch))
			}
		})
	}
}
