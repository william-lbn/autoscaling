package budget

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func unchangedAbortFixture(t *testing.T, reopen bool) (Finalized, UnchangedAbortCommand, UnchangedAbortReceipt) {
	t.Helper()
	f, prior, previous := committedAbortFixture(t)
	oldSources := RunnerSources{BirthClaim: ObjectRef{Name: "original-birth", UID: "birth-uid"}, StateClaim: ObjectRef{Name: "original-state", UID: "state-uid"}, ReceiptClaim: ObjectRef{Name: "original-receipt", UID: "receipt-uid"}, CommandSHA256: strings.Repeat("a", 64), JournalSHA256: strings.Repeat("b", 64)}
	f.Sources = &SettlementSources{OutsideClaim: ObjectRef{Name: "original-outside", UID: "outside-uid"}, HostClaim: ObjectRef{Name: "original-host", UID: "host-uid"}, VMReceiptSHA256: strings.Repeat("c", 64), Runner: &oldSources}
	next := f.Request
	next.Key.Revision++
	next.Key.OperationID.Low++
	next.TargetVMMemoryBytes += 256 << 20
	next.ExpectedCapacityGeneration = f.Shared.Observed.CapacityGeneration
	next.BudgetCommitmentID += "-cancelled"
	current := oldSources
	current.ReceiptClaim = ObjectRef{Name: "cancelled-receipt", UID: "cancelled-uid"}
	current.JournalSHA256 = strings.Repeat("d", 64)
	c := UnchangedAbortCommand{ProtocolVersion: 1, Request: next, FinalizationSHA256: prior.FinalizationSHA256, CancelledVMSHA256: strings.Repeat("e", 64), CancellationSources: current, Admission: nil}
	r := UnchangedAbortReceipt{ProtocolVersion: 1, Command: c, Query: true, Fence: previous.Fence, Shared: previous.Shared, Response: nil, PGAggregate: previous.PGAggregate, ResourceEpoch: f.IssuerEpoch}
	if reopen {
		admission := prior.Admission
		c.Admission = &admission
		r.Command, r.Query, r.Response = c, false, &previous.Response
	}
	require.NoError(t, c.ValidateFor(f, f.IssuerEpoch))
	require.NoError(t, r.ValidateFor(c, f, f.IssuerEpoch, f.IssuerEpoch))
	return f, c, r
}

func TestUnchangedAbortRequiresOldBudgetAndOriginalCancellationSources(t *testing.T) {
	for _, mode := range []string{"read-only", "reopen"} {
		t.Run(mode, func(t *testing.T) {
			f, c, r := unchangedAbortFixture(t, mode == "reopen")
			raw, err := json.Marshal(r)
			require.NoError(t, err)
			var restored UnchangedAbortReceipt
			require.NoError(t, json.Unmarshal(raw, &restored))
			require.NoError(t, restored.ValidateFor(c, f, f.IssuerEpoch, f.IssuerEpoch))
			for _, what := range []string{"new-target-budget", "same-key", "different-source", "closed-gate", "new-pg-generation", "different-epoch", "invented-mutation"} {
				t.Run(what, func(t *testing.T) {
					var bad UnchangedAbortReceipt
					require.NoError(t, json.Unmarshal(raw, &bad))
					switch what {
					case "new-target-budget":
						bad.Command.FinalizationSHA256 = "new-target-is-not-completed"
					case "same-key":
						bad.Command.Request.Key = f.Request.Key
					case "different-source":
						bad.Command.CancellationSources.BirthClaim.UID += "-replacement"
					case "closed-gate":
						bad.Shared.Observed.AdmissionOpen = false
					case "new-pg-generation":
						bad.Shared.Observed.CapacityGeneration++
					case "different-epoch":
						bad.ResourceEpoch++
					case "invented-mutation":
						if bad.Command.Admission == nil {
							bad.Query = false
						} else {
							bad.Command.Admission.BudgetCommitmentID = bad.Command.Request.BudgetCommitmentID
						}
					}
					require.Error(t, bad.ValidateFor(bad.Command, f, f.IssuerEpoch, f.IssuerEpoch))
				})
			}
			for _, value := range []any{c, r} {
				encoded, err := json.Marshal(value)
				require.NoError(t, err)
				decode := func(body []byte) error {
					if _, ok := value.(UnchangedAbortCommand); ok {
						var decoded UnchangedAbortCommand
						return json.Unmarshal(body, &decoded)
					}
					var decoded UnchangedAbortReceipt
					return json.Unmarshal(body, &decoded)
				}
				var fields map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(encoded, &fields))
				for field := range fields {
					var missing map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(encoded, &missing))
					delete(missing, field)
					body, err := json.Marshal(missing)
					require.NoError(t, err)
					require.Error(t, decode(body), field)
				}
				require.Error(t, decode(append([]byte(`{"unexpected":true,`), encoded[1:]...)))
				require.Error(t, decode(append([]byte(`{"protocol_version":1,`), encoded[1:]...)))
			}
		})
	}
}
