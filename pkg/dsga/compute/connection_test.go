package compute

import (
	"context"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestConnectionStatePreservesOriginalPendingAndRejectsFalseAck(t *testing.T) {
	intent, base := fixtures(t)
	base.Observed.AdmissionOpen = false
	fact := control.ConnectionResponse{ProtocolVersion: 1, ConnectionAPIVersion: 1, Enabled: true, RequestedSerial: 3, AppliedSerial: 3, Acknowledged: true, PrivateManagementReady: true, PublicConnectionsBlocked: true, Work: control.WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 2, ObserverExcluded: true, ForegroundIdle: true, Observed: base.Observed}}
	var tamper atomic.Bool
	_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/dsg/v1/connections/state", r.URL.Path)
		require.Equal(t, http.MethodGet, r.Method)
		response := fact
		if tamper.Load() {
			response.AppliedSerial = 2
		}
		reply(w, response)
	})
	config.Pending = &intent
	client, err := New(context.Background(), config)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	_, err = client.ConnectionState(context.Background())
	require.NoError(t, err)
	require.Equal(t, &intent, client.Pending())
	tamper.Store(true)
	_, err = client.ConnectionState(context.Background())
	require.Error(t, err)
	require.Equal(t, &intent, client.Pending())
}
