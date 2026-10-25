package dsga

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

func dsmFixture(t *testing.T) DSMResponse {
	raw, err := os.ReadFile("testdata/response-v1.json")
	require.NoError(t, err)
	var base ControlResponse
	require.NoError(t, json.Unmarshal(raw, &base))
	base.Observed.AdmissionOpen = false
	return DSMResponse{ProtocolVersion: 1, DSMAPIVersion: 1, CurrentBootOnly: true, ReservedUpperBytes: 32768, ControlUpperBytes: 16384,
		ExternalBudgetBytes: 65536, ExternalSegments: 2, PendingCreations: 1, UnknownSegments: 1, PageBytes: 16384, LimitEnforced: true, AccountingValid: true,
		Prepared: PreparedResponse{ProtocolVersion: 1, PreparedAPIVersion: 1, PreparedSlots: 1, MaximumPreparedSlots: 4, RuntimePreparesBlocked: true,
			Connections: ConnectionResponse{ProtocolVersion: 1, ConnectionAPIVersion: 1, Enabled: true, RequestedSerial: 3, AppliedSerial: 3, Acknowledged: true,
				PrivateManagementReady: true, PublicConnectionsBlocked: true, Work: WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 2, ObserverExcluded: true,
					ForegroundIdle: true, BackgroundProcesses: 1, Observed: base.Observed}}}}
}

func TestDSMReservationsKeepUnknownAndUnenforcedMeaningWithStrictBinding(t *testing.T) {
	fact := dsmFixture(t)
	bound := fact.Prepared.Connections.Work.Observed.Instance
	require.NoError(t, fact.ValidateFor(bound))
	for _, change := range []func(*DSMResponse){
		func(r *DSMResponse) { r.VMShrinkPermit = true }, func(r *DSMResponse) { r.DSMAPIVersion = 0 }, func(r *DSMResponse) { r.CurrentBootOnly = false },
		func(r *DSMResponse) { r.AccountingValid = false }, func(r *DSMResponse) { r.PageBytes = 0 }, func(r *DSMResponse) { r.PageBytes = 12288 },
		func(r *DSMResponse) { r.ControlUpperBytes = 0 }, func(r *DSMResponse) { r.ReservedUpperBytes = 16384 }, func(r *DSMResponse) { r.PendingCreations = ^uint32(0) },
		func(r *DSMResponse) { r.UnknownSegments = 2 }, func(r *DSMResponse) { r.LimitEnforced = false }, func(r *DSMResponse) { r.ExternalBudgetBytes = 16384 },
		func(r *DSMResponse) { r.Prepared.Connections.Work.Observed.Instance.VMUID = "foreign" },
	} {
		bad := fact
		change(&bad)
		require.Error(t, bad.ValidateFor(bound))
	}
	defaultFact := fact
	defaultFact.LimitEnforced = false
	defaultFact.ExternalBudgetBytes = 0
	require.NoError(t, defaultFact.ValidateFor(bound))
	empty := defaultFact
	empty.ExternalSegments = 0
	empty.PendingCreations = 0
	empty.UnknownSegments = 0
	empty.ReservedUpperBytes = 0
	require.NoError(t, empty.ValidateFor(bound))
	raw, err := json.Marshal(fact)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	for key := range fields {
		var obj map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &obj))
		delete(obj, key)
		missing, _ := json.Marshal(obj)
		var decoded DSMResponse
		require.Error(t, json.Unmarshal(missing, &decoded), key)
		obj[key] = json.RawMessage("null")
		null, _ := json.Marshal(obj)
		require.Error(t, json.Unmarshal(null, &decoded), key)
	}
	for _, bad := range []string{strings.Replace(string(raw), `"page_bytes":16384`, `"page_bytes":16384,"page_bytes":16384`, 1), strings.Replace(string(raw), `"reserved_upper_bytes":"32768"`, `"reserved_upper_bytes":32768`, 1), strings.Replace(string(raw), `"reserved_upper_bytes":"32768"`, `"reserved_upper_bytes":"032768"`, 1)} {
		var decoded DSMResponse
		require.Error(t, json.Unmarshal([]byte(bad), &decoded))
	}
}
