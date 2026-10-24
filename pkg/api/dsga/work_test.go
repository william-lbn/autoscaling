package dsga

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"strings"
	"testing"
)

func TestWorkNativeCountsBindingAndStrictSchema(t *testing.T) {
	raw, err := os.ReadFile("testdata/response-v1.json")
	require.NoError(t, err)
	var base ControlResponse
	require.NoError(t, json.Unmarshal(raw, &base))
	base.Observed.AdmissionOpen = false
	fact := WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 1, ClientProcesses: 2, IdleClients: 1, BoundedInputs: 1, ObserverExcluded: true, ForegroundIdle: true, Observed: base.Observed}
	require.NoError(t, fact.ValidateFor(base.Observed.Instance))
	for _, mutate := range []func(*WorkResponse){
		func(r *WorkResponse) { r.VMShrinkPermit = true },
		func(r *WorkResponse) { r.WorkAPIVersion = 2 },
		func(r *WorkResponse) { r.ClientProcesses = 0 },
		func(r *WorkResponse) { r.FrontendReads = math.MaxUint32 },
		func(r *WorkResponse) { r.RetainedTransactions = 3 },
		func(r *WorkResponse) { r.RetainedTransactions = 1 },
		func(r *WorkResponse) { r.Observed.AdmissionOpen = true },
		func(r *WorkResponse) { r.Observed.Instance.ComputeInstanceID = "other" },
	} {
		bad := fact
		mutate(&bad)
		require.Error(t, bad.ValidateFor(base.Observed.Instance))
	}
	encoded, err := json.Marshal(fact)
	require.NoError(t, err)
	for _, key := range []string{"work_api_version", "foreground_idle", "retained_transactions", "observer_excluded"} {
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(encoded, &fields))
		delete(fields, key)
		missing, err := json.Marshal(fields)
		require.NoError(t, err)
		var decoded WorkResponse
		require.Error(t, json.Unmarshal(missing, &decoded))
		fields[key] = json.RawMessage("null")
		null, err := json.Marshal(fields)
		require.NoError(t, err)
		require.Error(t, json.Unmarshal(null, &decoded))
	}
	var decoded WorkResponse
	duplicate := strings.Replace(string(encoded), `"work_api_version":1`, `"work_api_version":1,"work_api_version":1`, 1)
	require.Error(t, json.Unmarshal([]byte(duplicate), &decoded))
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.NoError(t, decoded.ValidateFor(base.Observed.Instance))
}
