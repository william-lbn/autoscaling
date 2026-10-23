package dsga

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"strings"
	"testing"
)

func TestSharedCostCheckedDedupAndStrictSchema(t *testing.T) {
	raw, err := os.ReadFile("testdata/response-v1.json")
	require.NoError(t, err)
	var baseline ControlResponse
	require.NoError(t, json.Unmarshal(raw, &baseline))
	startup := baseline.Observed.StartupSharedUpperBytes
	fact := SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: startup / 2, StartupArraysUpperBytes: startup - startup/2, Observed: baseline.Observed}
	fact.CurrentMainAndArraysUpperBytes = fact.StartupMainUpperBytes
	for _, a := range fact.Observed.Arrays {
		fact.CurrentMainAndArraysUpperBytes += a.CommittedUpperBytes
	}
	require.NoError(t, fact.ValidateFor(fact.Observed.Instance))
	encoded, err := json.Marshal(fact)
	require.NoError(t, err)
	var decoded SharedCostResponse
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.NoError(t, decoded.ValidateFor(fact.Observed.Instance))
	for _, bad := range []string{strings.Replace(string(encoded), `"vm_shrink_permit":false`, `"vm_shrink_permit":null`, 1), strings.Replace(string(encoded), `"vm_shrink_permit":false`, `"vm_shrink_permit":false,"vm_shrink_permit":true`, 1)} {
		require.Error(t, json.Unmarshal([]byte(bad), &decoded))
	}
	decoded = fact
	decoded.CurrentMainAndArraysUpperBytes += fact.StartupArraysUpperBytes
	require.Error(t, decoded.ValidateFor(fact.Observed.Instance))
	decoded = fact
	decoded.Observed.Arrays = append([]ArrayFact(nil), fact.Observed.Arrays...)
	decoded.Observed.Arrays[0].CommittedUpperBytes = Uint64(math.MaxUint64)
	require.Error(t, decoded.ValidateFor(fact.Observed.Instance))
}
