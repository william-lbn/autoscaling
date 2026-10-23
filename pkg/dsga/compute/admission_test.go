package compute

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

// 此TLS服务只提供协议故障夹具，不声称是真实PG/预算/集群。原生路由/PG行为
// 由Neon严格用例覆盖；这里验证Go丢回复后保留原意图、持久恢复和关闭证明校验。
func TestAdmissionUnknownRetainedAcrossQueriesRestoreAndExplicitClose(t *testing.T) {
	resize, baseline := fixtures(t)
	original := control.AdmissionRequest{ProtocolVersion: 1, Instance: resize.Key.Instance, IssuerEpoch: resize.Key.IssuerEpoch, Revision: 1, ExpectedCapacityGeneration: baseline.Observed.CapacityGeneration, Open: true, BudgetCommitmentID: "admission-original"}
	closeIntent := original
	closeIntent.Open = false
	closeIntent.Revision = 2
	closing := control.AdmissionCloseRequest{ProtocolVersion: 1, Original: original, Close: closeIntent}
	var tamper atomic.Bool
	tamper.Store(true)
	fact := func(intent control.AdmissionRequest, disposition control.AdmissionDisposition) control.AdmissionResponse {
		observed := baseline.Observed
		observed.Operation = nil
		observed.IssuerFloorEpoch = intent.IssuerEpoch
		observed.AdmissionRevision = intent.Revision
		observed.AdmissionOpen = intent.Open
		digest, err := intent.ContextSHA256()
		require.NoError(t, err)
		return control.AdmissionResponse{ProtocolVersion: 1, Request: intent, Disposition: disposition, ContextSHA256: digest, Observed: observed}
	}
	client, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dsg/v1/admission":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/dsg/v1/admission/status":
			response := fact(original, control.AdmissionNotFound)
			response.Observed.AdmissionRevision = 0
			response.Observed.AdmissionOpen = false
			reply(w, response)
		case "/dsg/v1/admission/close-original", "/dsg/v1/admission/close-original/status":
			var input control.AdmissionCloseRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&input))
			require.Equal(t, closing, input)
			disposition := control.AdmissionApplied
			if strings.HasSuffix(r.URL.Path, "/status") {
				disposition = control.AdmissionReplayed
			}
			reply(w, control.AdmissionClosure{ProtocolVersion: 1, Original: original, ClosedBy: fact(closeIntent, disposition), PriorMayHaveApplied: !tamper.Load()})
		default:
			w.WriteHeader(500)
		}
	})
	ctx := context.Background()
	_, err := client.SetAdmission(ctx, original)
	require.Error(t, err)
	pending, _ := client.PendingAdmission()
	require.Equal(t, &original, pending)
	_, err = client.AdmissionStatus(ctx, original)
	require.NoError(t, err)
	pending, _ = client.PendingAdmission()
	require.Equal(t, &original, pending)
	_, err = client.Resize(ctx, resize)
	require.Error(t, err)
	config.AdmissionPending = &original
	recovered, err := New(ctx, config)
	require.NoError(t, err)
	defer recovered.CloseIdleConnections()
	_, err = recovered.CloseAdmissionOriginal(ctx, closing)
	require.Error(t, err, "没有PriorMayHaveApplied事实不能解除未知")
	pending, closeCopy := recovered.PendingAdmission()
	require.Equal(t, &original, pending)
	require.Equal(t, &closeIntent, closeCopy)
	config.AdmissionClose = &closeIntent
	restarted, err := New(ctx, config)
	require.NoError(t, err)
	defer restarted.CloseIdleConnections()
	tamper.Store(false)
	confirmed, err := restarted.AdmissionClosureStatus(ctx, closing)
	require.NoError(t, err)
	require.True(t, confirmed.PriorMayHaveApplied)
	pending, closeCopy = restarted.PendingAdmission()
	require.Nil(t, pending)
	require.Nil(t, closeCopy)
	confirmed, err = client.CloseAdmissionOriginal(ctx, closing)
	require.NoError(t, err)
	require.True(t, confirmed.PriorMayHaveApplied)
	pending, closeCopy = client.PendingAdmission()
	require.Nil(t, pending)
	require.Nil(t, closeCopy)
}
