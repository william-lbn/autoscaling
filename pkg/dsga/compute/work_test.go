package compute

import (
	"context"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestWorkStatePreservesRestoredUnknownAndRejectsContradictions(t *testing.T) {
	intent, base := fixtures(t)
	base.Observed.AdmissionOpen = false
	fact := control.WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 1, ClientProcesses: 1, BoundedInputs: 1, ObserverExcluded: true, ForegroundIdle: true, Observed: base.Observed}
	var tamper atomic.Bool
	_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/dsg/v1/work/state", r.URL.Path)
		require.Equal(t, http.MethodGet, r.Method)
		response := fact
		if tamper.Load() {
			response.RetainedTransactions = 1
		}
		reply(w, response)
	})
	config.Pending = &intent
	client, err := New(context.Background(), config)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	_, err = client.WorkState(context.Background())
	require.NoError(t, err)
	require.Equal(t, &intent, client.Pending())
	tamper.Store(true)
	_, err = client.WorkState(context.Background())
	require.Error(t, err)
	require.Equal(t, &intent, client.Pending())
}
