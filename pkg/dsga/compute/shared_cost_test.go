package compute

import (
	"context"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestSharedCostNativeContractDoesNotClearRestoredUnknown(t *testing.T) {
	intent, baseline := fixtures(t)
	startup := baseline.Observed.StartupSharedUpperBytes
	fact := control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: startup / 2, StartupArraysUpperBytes: startup - startup/2, Observed: baseline.Observed}
	fact.CurrentMainAndArraysUpperBytes = fact.StartupMainUpperBytes
	for _, a := range fact.Observed.Arrays {
		fact.CurrentMainAndArraysUpperBytes += a.CommittedUpperBytes
	}
	var tamper atomic.Bool
	_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/dsg/v1/budget/shared", r.URL.Path)
		require.Equal(t, http.MethodGet, r.Method)
		response := fact
		if tamper.Load() {
			response.CurrentMainAndArraysUpperBytes += response.StartupArraysUpperBytes
		}
		reply(w, response)
	})
	config.Pending = &intent
	client, err := New(context.Background(), config)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	_, err = client.SharedCost(context.Background())
	require.NoError(t, err)
	require.Equal(t, &intent, client.Pending())
	tamper.Store(true)
	_, err = client.SharedCost(context.Background())
	require.Error(t, err)
	require.Equal(t, &intent, client.Pending())
}
