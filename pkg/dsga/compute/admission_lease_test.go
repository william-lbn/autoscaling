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

func TestNativeAdmissionRearmStatusIsReadOnlyAndKeepsOriginalUnknowns(t *testing.T) {
	intent, base := fixtures(t)
	var mode, posts atomic.Int32
	_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/dsg/v1/admission/lease/rearm/status", r.URL.Path)
		posts.Add(1)
		var request control.AdmissionLeaseRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.Equal(t, "rearm", request.Action)
		if mode.Load() == 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		s := base.Observed
		s.Operation, s.AdmissionRevision, s.AdmissionOpen = nil, 0, false
		current := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Expired: true, Request: request, Armed: true, Renewed: false, DeadlineNS: 20, LeaseEpoch: intent.Key.IssuerEpoch, TimeoutMS: 30000, Observed: s}
		fact := control.AdmissionRearmStatus{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, Retained: true, AppliedDeadlineNS: 20, Current: current}
		if mode.Load() == 1 {
			fact.AppliedDeadlineNS = request.ExpectedDeadlineNS
		} else if mode.Load() == 3 {
			fact.Retained, fact.AppliedDeadlineNS = false, 0
		}
		reply(w, fact)
	})
	admission := control.AdmissionRequest{ProtocolVersion: 1, Instance: intent.Key.Instance, IssuerEpoch: intent.Key.IssuerEpoch, Revision: 1, ExpectedCapacityGeneration: base.Observed.CapacityGeneration, Open: true, BudgetCommitmentID: "retained-original"}
	config.Pending, config.AdmissionPending = &intent, &admission
	client, err := New(context.Background(), config)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	request := control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: intent.Key.Instance, IssuerEpoch: intent.Key.IssuerEpoch, ExpectedCapacityGeneration: base.Observed.CapacityGeneration, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 10, Action: "rearm"}
	fact, err := client.AdmissionRearmStatus(context.Background(), request)
	require.NoError(t, err)
	require.True(t, fact.Retained)
	require.True(t, fact.Current.Expired, "历史成功不能被当作当前期限有效")
	for _, value := range []int32{1, 2} {
		mode.Store(value)
		_, err := client.AdmissionRearmStatus(context.Background(), request)
		require.Error(t, err)
	}
	mode.Store(3)
	fact, err = client.AdmissionRearmStatus(context.Background(), request)
	require.NoError(t, err)
	require.False(t, fact.Retained)
	for _, change := range []func(*control.AdmissionLeaseRequest){
		func(r *control.AdmissionLeaseRequest) { r.Action = "renew" },
		func(r *control.AdmissionLeaseRequest) { r.IssuerEpoch++ },
		func(r *control.AdmissionLeaseRequest) { r.Instance.PGBootID++ },
	} {
		bad := request
		change(&bad)
		_, err = client.AdmissionRearmStatus(context.Background(), bad)
		require.Error(t, err)
	}
	require.EqualValues(t, 4, posts.Load(), "查询失败不自动重试/重发setter，坏选择写前拒绝")
	require.Equal(t, &intent, client.Pending())
	original, closing := client.PendingAdmission()
	require.Equal(t, &admission, original)
	require.Nil(t, closing)
}
