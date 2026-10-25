package dsga

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

func TestPreparedSlotsRetainIndependentMeaningAndStrictNativeEnvelope(t *testing.T) {
	raw, err := os.ReadFile("testdata/response-v1.json")
	require.NoError(t, err)
	var base ControlResponse
	require.NoError(t, json.Unmarshal(raw, &base))
	base.Observed.AdmissionOpen = false
	fact := PreparedResponse{ProtocolVersion: 1, PreparedAPIVersion: 1, PreparedSlots: 1, MaximumPreparedSlots: 4, RuntimePreparesBlocked: true,
		Connections: ConnectionResponse{ProtocolVersion: 1, ConnectionAPIVersion: 1, Enabled: true, RequestedSerial: 3, AppliedSerial: 3, Acknowledged: true,
			PrivateManagementReady: true, PublicConnectionsBlocked: true, Work: WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 2, ObserverExcluded: true, ForegroundIdle: true, Observed: base.Observed}}}
	bound := base.Observed.Instance
	require.NoError(t, fact.ValidateFor(bound), "前台idle与非零prepared槽可共存，不能丢掉保留事实")
	for _, change := range []func(*PreparedResponse){
		func(r *PreparedResponse) { r.VMShrinkPermit = true }, func(r *PreparedResponse) { r.PreparedAPIVersion = 0 },
		func(r *PreparedResponse) { r.PreparedSlots = 5 }, func(r *PreparedResponse) { r.MaximumPreparedSlots = 0 },
		func(r *PreparedResponse) { r.RuntimePreparesBlocked = false }, func(r *PreparedResponse) { r.Connections.Work.Observed.Instance.VMUID = "foreign" },
	} {
		bad := fact
		change(&bad)
		require.Error(t, bad.ValidateFor(bound))
	}
	empty := fact
	empty.PreparedSlots = 0
	empty.MaximumPreparedSlots = 0
	require.NoError(t, empty.ValidateFor(bound))
	raw, err = json.Marshal(fact)
	require.NoError(t, err)
	for _, key := range []string{"prepared_slots", "maximum_prepared_slots", "runtime_prepares_blocked", "connections"} {
		var obj map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &obj))
		delete(obj, key)
		missing, _ := json.Marshal(obj)
		var decoded PreparedResponse
		require.Error(t, json.Unmarshal(missing, &decoded))
		obj[key] = json.RawMessage("null")
		null, _ := json.Marshal(obj)
		require.Error(t, json.Unmarshal(null, &decoded))
	}
	var decoded PreparedResponse
	require.Error(t, json.Unmarshal([]byte(strings.Replace(string(raw), `"prepared_slots":1`, `"prepared_slots":1,"prepared_slots":1`, 1)), &decoded))
}
