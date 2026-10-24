package dsga

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

func TestConnectionAcknowledgementBindingSaturationAndStrictSchema(t *testing.T) {
	raw, err := os.ReadFile("testdata/response-v1.json")
	require.NoError(t, err)
	var base ControlResponse
	require.NoError(t, json.Unmarshal(raw, &base))
	base.Observed.AdmissionOpen = false
	fact := ConnectionResponse{ProtocolVersion: 1, ConnectionAPIVersion: 1, Enabled: true, RequestedSerial: 3, AppliedSerial: 3, Acknowledged: true, PrivateManagementReady: true, PublicConnectionsBlocked: true, Work: WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 2, ObserverExcluded: true, ForegroundIdle: true, Observed: base.Observed}}
	bound := base.Observed.Instance
	require.NoError(t, fact.ValidateFor(bound))
	for _, mutate := range []func(*ConnectionResponse){
		func(r *ConnectionResponse) { r.VMShrinkPermit = true },
		func(r *ConnectionResponse) { r.AppliedSerial = 4 },
		func(r *ConnectionResponse) { r.AppliedSerial = 2 },
		func(r *ConnectionResponse) { r.AppliedOpen = true },
		func(r *ConnectionResponse) { r.RequestedOpen = true },
		func(r *ConnectionResponse) { r.RequestedSerial = ^uint32(0) },
		func(r *ConnectionResponse) { r.Enabled = false },
		func(r *ConnectionResponse) { r.Work.Observed.Instance.VMUID = "other" },
	} {
		bad := fact
		mutate(&bad)
		require.Error(t, bad.ValidateFor(bound))
	}
	pending := fact
	pending.AppliedSerial = 2
	pending.AppliedOpen = true
	pending.Acknowledged = false
	pending.PublicConnectionsBlocked = false
	require.NoError(t, pending.ValidateFor(bound))
	saturated := pending
	saturated.RequestedSerial = ^uint32(0) >> 1
	saturated.AppliedSerial = saturated.RequestedSerial
	require.NoError(t, saturated.ValidateFor(bound))
	saturated.Acknowledged = true
	require.Error(t, saturated.ValidateFor(bound), "相同饱和serial不能忽略开闭位")
	disabled := ConnectionResponse{ProtocolVersion: 1, ConnectionAPIVersion: 1, Work: fact.Work}
	require.NoError(t, disabled.ValidateFor(bound))
	encoded, err := json.Marshal(fact)
	require.NoError(t, err)
	var decoded ConnectionResponse
	for _, key := range []string{"acknowledged", "enabled", "applied_serial", "private_management_ready", "work"} {
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(encoded, &fields))
		delete(fields, key)
		missing, err := json.Marshal(fields)
		require.NoError(t, err)
		require.Error(t, json.Unmarshal(missing, &decoded))
		fields[key] = json.RawMessage("null")
		null, err := json.Marshal(fields)
		require.NoError(t, err)
		require.Error(t, json.Unmarshal(null, &decoded))
	}
	duplicate := strings.Replace(string(encoded), `"acknowledged":true`, `"acknowledged":true,"acknowledged":true`, 1)
	require.Error(t, json.Unmarshal([]byte(duplicate), &decoded))
}
