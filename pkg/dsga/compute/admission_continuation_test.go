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

// 实际TLS承载的是明确协议peer；只检验SDK生命周期，不宣称真实PG/Lease/VM。
func TestAdmissionContinuationLostRepliesRepeatedOwnersAndExactRestore(t *testing.T) {
	resize, baseline := fixtures(t)
	original := control.AdmissionRequest{
		ProtocolVersion: 1, Instance: resize.Key.Instance,
		IssuerEpoch: resize.Key.IssuerEpoch, Revision: 1, ExpectedCapacityGeneration: baseline.Observed.CapacityGeneration,
		Open: false, BudgetCommitmentID: "continuation-budget",
	}
	first := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: original, PreviousClose: original, Close: original}
	first.Close.IssuerEpoch++
	next := first
	next.PreviousClose = first.Close
	next.Close.IssuerEpoch++
	var writes, reads atomic.Int32
	var applied atomic.Bool
	var tamper atomic.Bool
	proof := func(req control.AdmissionContinuationRequest, query bool) control.AdmissionContinuationResponse {
		s := baseline.Observed
		s.Operation, s.AdmissionOpen = nil, false
		s.IssuerFloorEpoch, s.AdmissionRevision = req.Close.IssuerEpoch, req.Close.Revision
		digest, err := req.ContextSHA256()
		require.NoError(t, err)
		native, err := req.Close.ContextSHA256()
		require.NoError(t, err)
		disposition := control.AdmissionApplied
		if query {
			disposition = control.AdmissionReplayed
		}
		return control.AdmissionContinuationResponse{
			ProtocolVersion: 1, VMShrinkPermit: false, Request: req, ContextSHA256: digest, PriorMayHaveApplied: !tamper.Load(),
			ClosedBy: control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: req.Close, ContextSHA256: native, Disposition: disposition, Observed: s},
		}
	}
	_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		var req control.AdmissionContinuationRequest
		switch r.URL.Path {
		case "/dsg/v1/admission/continue-close":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			writes.Add(1)
			// 两次分别表示未送达PG，以及PG生效后回复丢失；HTTP错误都不能清意图。
			if req == next {
				applied.Store(true)
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/dsg/v1/admission/continue-close/status":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			reads.Add(1)
			if !applied.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			// 旧请求的完整响应也故意返回，验证本地选择匹配，不能借它清新未知。
			reply(w, proof(req, true))
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	ctx := context.Background()
	config.IssuerEpoch = first.Close.IssuerEpoch
	config.AdmissionPending = &original
	a, err := New(ctx, config)
	require.NoError(t, err)
	defer a.CloseIdleConnections()
	_, err = a.ContinueAdmissionClosure(ctx, first)
	require.Error(t, err)
	require.Equal(t, &first, a.PendingAdmissionContinuation())
	_, err = a.AdmissionContinuationStatus(ctx, first)
	require.Error(t, err)
	require.Equal(t, int32(1), writes.Load(), "只读恢复不能自动重发")
	config.AdmissionClose, config.AdmissionContinuation = &first.Close, &first
	config.IssuerEpoch = next.Close.IssuerEpoch
	b, err := New(ctx, config)
	require.NoError(t, err)
	defer b.CloseIdleConnections()
	_, err = b.ContinueAdmissionClosure(ctx, next)
	require.Error(t, err)
	require.Equal(t, &next, b.PendingAdmissionContinuation())
	_, err = b.AdmissionContinuationStatus(ctx, first)
	require.Error(t, err, "旧完整回执不能清新选择")
	require.Equal(t, &next, b.PendingAdmissionContinuation())
	_, err = b.SetAdmission(ctx, next.Close)
	require.Error(t, err, "不能以legacy入口绕过完整选择")
	_, err = b.Resize(ctx, resize)
	require.Error(t, err)
	config.AdmissionClose, config.AdmissionContinuation = &next.Close, &next
	c, err := New(ctx, config)
	require.NoError(t, err)
	defer c.CloseIdleConnections()
	tamper.Store(true)
	_, err = c.AdmissionContinuationStatus(ctx, next)
	require.Error(t, err)
	require.Equal(t, &next, c.PendingAdmissionContinuation())
	tamper.Store(false)
	got, err := c.AdmissionContinuationStatus(ctx, next)
	require.NoError(t, err)
	require.True(t, got.PriorMayHaveApplied)
	require.Nil(t, c.PendingAdmissionContinuation())
	pending, closeIntent := c.PendingAdmission()
	require.Nil(t, pending)
	require.Nil(t, closeIntent)
	require.Equal(t, int32(2), writes.Load())
	require.Equal(t, int32(4), reads.Load())
	// 恢复配置不得缺原始或借用另一份关闭，错误必须在协商/网络之前拒绝。
	config.AdmissionClose = &first.Close
	_, err = New(ctx, config)
	require.Error(t, err)
	config.AdmissionClose, config.AdmissionPending = &next.Close, nil
	_, err = New(ctx, config)
	require.Error(t, err)
}

func TestAdmissionContinuationReservationSerializesAndRetainsPredecessor(t *testing.T) {
	resize, baseline := fixtures(t)
	original := control.AdmissionRequest{
		ProtocolVersion: 1, Instance: resize.Key.Instance, IssuerEpoch: 1, Revision: 1,
		ExpectedCapacityGeneration: baseline.Observed.CapacityGeneration, Open: false, BudgetCommitmentID: "budget",
	}
	req := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: original, PreviousClose: original, Close: original}
	req.Close.IssuerEpoch = 2
	client := new(Client)
	require.NoError(t, client.reserveAdmissionContinuation(req))
	require.Error(t, client.reserveAdmissionContinuation(req))
	client.finishAdmission(false)
	bad := req
	bad.PreviousClose.IssuerEpoch = 3
	bad.Close.IssuerEpoch = 4
	require.Error(t, client.reserveAdmissionContinuation(bad))
	require.Equal(t, &req, client.PendingAdmissionContinuation())
	next := req
	next.PreviousClose = req.Close
	next.Close.IssuerEpoch = 3
	require.NoError(t, client.reserveAdmissionContinuation(next))
	client.finishAdmission(false)
	require.Equal(t, &next, client.PendingAdmissionContinuation())
	retainedValue := client.PendingAdmissionContinuation()
	retainedValue.Original.BudgetCommitmentID = "changed-copy"
	require.Equal(t, &next, client.PendingAdmissionContinuation())
	upgraded := next
	upgraded.ProtocolVersion, upgraded.PreviousClose = 2, next.Close
	upgraded.Close.IssuerEpoch++
	require.NoError(t, client.reserveAdmissionContinuation(upgraded))
	client.finishAdmission(false)
	downgraded := upgraded
	downgraded.ProtocolVersion, downgraded.PreviousClose = 1, upgraded.Close
	downgraded.Close.IssuerEpoch++
	require.NoError(t, downgraded.Validate(), "单次v1选择合法也不能覆盖已持久的v2语义")
	require.Error(t, client.reserveAdmissionContinuation(downgraded))
	require.Equal(t, &upgraded, client.PendingAdmissionContinuation())
}

// TLS peer分别丢失v1旧代次关闭与v2新代次关闭的回复。恢复只查明确原文，
// 包装降级和旧回执都不能清未知；关闭完成后原Resize未知仍独立保留。
func TestAdmissionContinuationCapacityLostReplyAndRestore(t *testing.T) {
	resize, baseline := fixtures(t)
	original := control.AdmissionRequest{
		ProtocolVersion: 1, Instance: resize.Key.Instance,
		IssuerEpoch: resize.Key.IssuerEpoch, Revision: 1, ExpectedCapacityGeneration: baseline.Observed.CapacityGeneration,
		Open: false, BudgetCommitmentID: "capacity-continuation-budget",
	}
	first := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: original, PreviousClose: original, Close: original}
	first.Close.IssuerEpoch++
	next := first
	next.ProtocolVersion, next.PreviousClose = 2, first.Close
	next.Close.IssuerEpoch++
	next.Close.ExpectedCapacityGeneration++
	var writes atomic.Int32
	var downgrade atomic.Bool
	_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		var request control.AdmissionContinuationRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		if r.URL.Path == "/dsg/v1/admission/continue-close" {
			writes.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		require.Equal(t, "/dsg/v1/admission/continue-close/status", r.URL.Path)
		s := baseline.Observed
		s.Operation, s.AdmissionOpen = nil, false
		s.IssuerFloorEpoch, s.AdmissionRevision = next.Close.IssuerEpoch, request.Close.Revision
		s.CapacityGeneration = next.Close.ExpectedCapacityGeneration
		digest, err := request.ContextSHA256()
		require.NoError(t, err)
		native, err := request.Close.ContextSHA256()
		require.NoError(t, err)
		version := request.ProtocolVersion
		if downgrade.Load() {
			version = 1
		}
		reply(w, control.AdmissionContinuationResponse{
			ProtocolVersion: version, VMShrinkPermit: false, Request: request, ContextSHA256: digest,
			PriorMayHaveApplied: true, ClosedBy: control.AdmissionResponse{
				ProtocolVersion: 1, VMShrinkPermit: false, Request: request.Close,
				Disposition: control.AdmissionReplayed, ContextSHA256: native, Observed: s,
			},
		})
	})
	ctx := context.Background()
	config.IssuerEpoch, config.Pending, config.AdmissionPending = first.Close.IssuerEpoch, &resize, &original
	a, err := New(ctx, config)
	require.NoError(t, err)
	defer a.CloseIdleConnections()
	_, err = a.ContinueAdmissionClosure(ctx, first)
	require.Error(t, err)
	config.IssuerEpoch, config.AdmissionClose, config.AdmissionContinuation = next.Close.IssuerEpoch, &first.Close, &first
	b, err := New(ctx, config)
	require.NoError(t, err)
	defer b.CloseIdleConnections()
	_, err = b.ContinueAdmissionClosure(ctx, next)
	require.Error(t, err)
	require.Equal(t, &next, b.PendingAdmissionContinuation())
	config.AdmissionClose, config.AdmissionContinuation = &next.Close, &next
	c, err := New(ctx, config)
	require.NoError(t, err)
	defer c.CloseIdleConnections()
	_, err = c.AdmissionContinuationStatus(ctx, first)
	require.Error(t, err)
	downgrade.Store(true)
	_, err = c.AdmissionContinuationStatus(ctx, next)
	require.Error(t, err)
	require.Equal(t, &next, c.PendingAdmissionContinuation())
	downgrade.Store(false)
	_, err = c.AdmissionContinuationStatus(ctx, next)
	require.NoError(t, err)
	require.Nil(t, c.PendingAdmissionContinuation())
	require.Equal(t, &resize, c.Pending(), "安全关闭不能抹去PG原Resize未知")
	require.Equal(t, int32(2), writes.Load(), "查询不得隐式重放")
}
