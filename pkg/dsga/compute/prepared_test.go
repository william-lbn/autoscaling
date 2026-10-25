package compute

import (
	"context"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestPreparedStateKeepsOriginalPendingAndRejectsFalseEmptyEnvelope(t *testing.T) {
	intent, base := fixtures(t)
	base.Observed.AdmissionOpen = false
	fact := control.PreparedResponse{ProtocolVersion: 1, PreparedAPIVersion: 1, PreparedSlots: 1, MaximumPreparedSlots: 4, RuntimePreparesBlocked: true,
		Connections: control.ConnectionResponse{ProtocolVersion: 1, ConnectionAPIVersion: 1, Enabled: true, RequestedSerial: 3, AppliedSerial: 3,
			Acknowledged: true, PrivateManagementReady: true, PublicConnectionsBlocked: true,
			Work: control.WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 2, ObserverExcluded: true, ForegroundIdle: true, Observed: base.Observed}}}
	var tamper atomic.Bool
	_, cfg := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/dsg/v1/work/prepared", r.URL.Path)
		response := fact
		if tamper.Load() {
			response.MaximumPreparedSlots = 0
		}
		reply(w, response)
	})
	cfg.Pending = &intent
	client, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	r, err := client.PreparedState(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 1, r.PreparedSlots)
	require.True(t, r.Connections.Work.ForegroundIdle, "前台空闲不应掩盖保留的prepared槽")
	require.Equal(t, &intent, client.Pending())
	tamper.Store(true)
	_, err = client.PreparedState(context.Background())
	require.Error(t, err)
	require.Equal(t, &intent, client.Pending())
}
