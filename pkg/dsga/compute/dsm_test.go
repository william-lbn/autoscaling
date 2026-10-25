package compute

import (
	"context"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestDSMStateKeepsOriginalPendingAndRejectsInvalidPayloadEnvelope(t *testing.T) {
	intent, base := fixtures(t)
	base.Observed.AdmissionOpen = false
	prepared := control.PreparedResponse{ProtocolVersion: 1, PreparedAPIVersion: 1, PreparedSlots: 1, MaximumPreparedSlots: 4, RuntimePreparesBlocked: true,
		Connections: control.ConnectionResponse{ProtocolVersion: 1, ConnectionAPIVersion: 1, Enabled: true, RequestedSerial: 3, AppliedSerial: 3,
			Acknowledged: true, PrivateManagementReady: true, PublicConnectionsBlocked: true,
			Work: control.WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 2, ObserverExcluded: true, ForegroundIdle: true, Observed: base.Observed}}}
	fact := control.DSMResponse{ProtocolVersion: 1, DSMAPIVersion: 1, CurrentBootOnly: true, ReservedUpperBytes: 32768, ControlUpperBytes: 16384, ExternalBudgetBytes: 65536, ExternalSegments: 2, PendingCreations: 1, UnknownSegments: 1, PageBytes: 16384, LimitEnforced: true, AccountingValid: true, Prepared: prepared}
	var tamper atomic.Bool
	_, cfg := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/dsg/v1/work/dsm", r.URL.Path)
		response := fact
		if tamper.Load() {
			response.ReservedUpperBytes = 0
		}
		reply(w, response)
	})
	cfg.Pending = &intent
	client, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	r, err := client.DSMState(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 32768, r.ReservedUpperBytes)
	require.True(t, r.Prepared.Connections.Work.ForegroundIdle, "前台空闲不应掩盖未知DSM预留")
	require.Equal(t, &intent, client.Pending())
	tamper.Store(true)
	_, err = client.DSMState(context.Background())
	require.Error(t, err)
	require.Equal(t, &intent, client.Pending())
}
