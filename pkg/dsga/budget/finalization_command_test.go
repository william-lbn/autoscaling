package budget

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

func TestFinalizationCommandPreservesOriginalWireAndRejectsPartialSources(t *testing.T) {
	raw, err := os.ReadFile("../../api/dsga/testdata/request-v1.json")
	require.NoError(t, err)
	var req control.Request
	require.NoError(t, json.Unmarshal(raw, &req))
	raw, err = os.ReadFile("../../api/dsga/testdata/response-v1.json")
	require.NoError(t, err)
	var response control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &response))
	f := Finalized{ProtocolVersion: 1, Request: req, IssuerEpoch: req.Key.IssuerEpoch, RunnerPodName: "runner", HostReservationID: "host-original", GuestUsableMemoryBytes: 768 << 20, Shared: control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: 16 << 20, StartupArraysUpperBytes: 16 << 20, CurrentMainAndArraysUpperBytes: 80 << 20, Observed: response.Observed}, Aggregate: &Bound{Kind: HardLimit, UpperBytes: 512 << 20, SourceID: "root-original"}, SystemOutsidePG: Bound{Kind: CommittedUpper, UpperBytes: 24 << 20, SourceID: "system"}, Transition: Bound{Kind: CommittedUpper, UpperBytes: 4 << 20, SourceID: "transition"}, Safety: Bound{Kind: CommittedUpper, UpperBytes: 4 << 20, SourceID: "safety"}, Sources: &SettlementSources{OutsideClaim: ObjectRef{Name: "outside-original", UID: "outside-uid"}, HostClaim: ObjectRef{Name: "host-original", UID: "host-uid"}, VMReceiptSHA256: strings.Repeat("c", 64)}}
	c := FinalizationCommand{ProtocolVersion: 1, Finalized: f, PreparedSHA256: strings.Repeat("a", 64), GuestSHA256: strings.Repeat("b", 64), VMSHA256: strings.Repeat("c", 64)}
	require.NoError(t, c.Validate())
	raw, err = json.Marshal(c)
	require.NoError(t, err)
	var restored FinalizationCommand
	require.NoError(t, json.Unmarshal(raw, &restored))
	require.Equal(t, c, restored)
	for _, mode := range []string{"version", "missing-sources", "uppercase-digest", "short-digest", "different-vm", "unenforced-aggregate", "missing-host-uid", "incomplete-pg"} {
		t.Run(mode, func(t *testing.T) {
			var copy FinalizationCommand
			require.NoError(t, json.Unmarshal(raw, &copy))
			switch mode {
			case "version":
				copy.ProtocolVersion = 0
			case "missing-sources":
				copy.Finalized.Sources = nil
			case "uppercase-digest":
				copy.PreparedSHA256 = strings.Repeat("A", 64)
			case "short-digest":
				copy.GuestSHA256 = strings.Repeat("b", 63)
			case "different-vm":
				copy.VMSHA256 = strings.Repeat("d", 64)
			case "unenforced-aggregate":
				copy.Finalized.Aggregate.Kind = CommittedUpper
			case "missing-host-uid":
				copy.Finalized.Sources.HostClaim.UID = ""
			case "incomplete-pg":
				copy.Finalized.Shared.Observed.Operation = nil
			}
			require.Error(t, copy.Validate())
		})
	}
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	for field := range fields {
		t.Run("missing-"+field, func(t *testing.T) {
			var missing map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &missing))
			delete(missing, field)
			body, err := json.Marshal(missing)
			require.NoError(t, err)
			var copy FinalizationCommand
			require.Error(t, json.Unmarshal(body, &copy))
		})
	}
	var duplicate FinalizationCommand
	require.Error(t, json.Unmarshal(append([]byte(`{"protocol_version":1,`), raw[1:]...), &duplicate))
}
