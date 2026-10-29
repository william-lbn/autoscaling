package compute

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func TestNativeAdmissionLeaseCASRetainsAllOriginalUnknowns(t *testing.T) {
	intent, base := fixtures(t)
	var mode, posts atomic.Int32
	_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/dsg/v1/admission/lease", r.URL.Path)
		posts.Add(1)
		var request control.AdmissionLeaseRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		if mode.Load() == 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		s := base.Observed
		s.Operation, s.AdmissionRevision, s.AdmissionOpen = nil, 0, false
		fact := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Expired: false, Request: request, Armed: true, Renewed: request.Action == "renew", DeadlineNS: 20, LeaseEpoch: intent.Key.IssuerEpoch, TimeoutMS: 30000, Observed: s}
		fact.VMShrinkPermit = mode.Load() == 1
		reply(w, fact)
	})
	admission := control.AdmissionRequest{ProtocolVersion: 1, Instance: intent.Key.Instance, IssuerEpoch: intent.Key.IssuerEpoch, Revision: 1, ExpectedCapacityGeneration: base.Observed.CapacityGeneration, Open: true, BudgetCommitmentID: "retained-original"}
	config.Pending, config.AdmissionPending = &intent, &admission
	client, err := New(context.Background(), config)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	request := control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: intent.Key.Instance, IssuerEpoch: intent.Key.IssuerEpoch, ExpectedCapacityGeneration: base.Observed.CapacityGeneration, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 10, Action: "renew"}
	fact, err := client.AdmissionLease(context.Background(), request)
	require.NoError(t, err)
	require.True(t, fact.Renewed)
	for _, value := range []int32{1, 2} {
		mode.Store(value)
		_, err := client.AdmissionLease(context.Background(), request)
		require.Error(t, err)
		var unknown *CallError
		require.ErrorAs(t, err, &unknown)
		require.True(t, unknown.MayHaveExecuted)
	}
	require.EqualValues(t, 3, posts.Load(), "丢回复不能自动重试")
	wrong := request
	wrong.IssuerEpoch++
	_, err = client.AdmissionLease(context.Background(), wrong)
	require.Error(t, err)
	require.EqualValues(t, 3, posts.Load(), "错误owner必须写前拒绝")
	require.Equal(t, &intent, client.Pending())
	original, closeIntent := client.PendingAdmission()
	require.Equal(t, &admission, original)
	require.Nil(t, closeIntent)
}
