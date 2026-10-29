package journal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 本文件只认证持久模型、严格codec及真实typed HTTP/RFC6902 CAS。
// 原生关闭回执来自显式合同夹具，不是PG、选主、NeonVM或Linux认证。
func closureContinuationModel(t *testing.T) (Model, control.AdmissionContinuationRequest, control.AdmissionContinuationResponse) {
	t.Helper()
	m, original, closed := serviceClosureModel(t)
	m, err := m.StartServiceClosure(m.Owner, original)
	require.NoError(t, err)
	m, err = m.Takeover(m.Owner, "continuation-owner")
	require.NoError(t, err)
	request := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: original, PreviousClose: original, Close: original}
	request.Close.IssuerEpoch = m.Owner.Epoch
	// 原命令未投递时复用尚未消费的revision；已投递时由生产者根据PG
	// 实际revision选下一项。本模型不猜测原请求的执行结果。
	require.NoError(t, request.Validate())
	digest, err := request.ContextSHA256()
	require.NoError(t, err)
	closed.Request = request.Close
	closed.ContextSHA256, err = request.Close.ContextSHA256()
	require.NoError(t, err)
	closed.Observed.IssuerFloorEpoch = request.Close.IssuerEpoch
	fact := control.AdmissionContinuationResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, ContextSHA256: digest, ClosedBy: closed, PriorMayHaveApplied: true}
	require.NoError(t, fact.ValidateFor(request, request.Close.Instance, false))
	return m, request, fact
}

func TestServiceContinuationRetainsOriginalAndRepeatedPredecessors(t *testing.T) {
	m, request, fact := closureContinuationModel(t)
	original := *m.ServiceClosure
	before := m.Clone()
	m, err := m.StartServiceClosureContinuation(m.Owner, original.CommandSHA256, request)
	require.NoError(t, err)
	require.EqualValues(t, 11, m.ProtocolVersion)
	require.True(t, m.ServiceClosure.Pending())
	require.Equal(t, before.InitialHeld, m.InitialHeld)
	require.Equal(t, before.History, m.History)
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m, loaded)
	// 同owner不能因NotFound/超时不断生成新选择；重放原选择由独立
	// 执行器控制，不能再次写前覆盖。更高owner也必须选择最近完整摘要。
	_, err = m.StartServiceClosureContinuation(m.Owner, m.ServiceClosure.Continuation.CommandSHA256, request)
	require.Error(t, err)
	m, err = m.Takeover(m.Owner, "third-closure-owner")
	require.NoError(t, err)
	previous := *m.ServiceClosure.Continuation
	nextRequest := request
	nextRequest.PreviousClose = request.Close
	nextRequest.Close.IssuerEpoch = m.Owner.Epoch
	nextRequest.Close.Revision++
	_, err = m.StartServiceClosureContinuation(m.Owner, original.CommandSHA256, nextRequest)
	require.Error(t, err, "不能跳过最近一次未知关闭")
	m, err = m.StartServiceClosureContinuation(m.Owner, previous.CommandSHA256, nextRequest)
	require.NoError(t, err)
	require.Equal(t, previous.Origin, m.ServiceClosure.Continuation.PreviousOrigin)
	require.Equal(t, previous.StartedSequence, m.ServiceClosure.Continuation.PreviousSequence)
	_, err = m.RecordServiceClosureContinuation(m.Owner, fact, true)
	require.Error(t, err, "前驱成功回执不能结清后继未知")
	_, err = m.RecordServiceClosure(m.Owner, fact.ClosedBy, true)
	require.Error(t, err, "旧关闭接口不能覆盖接续")
	// 新owner只读确认后继；不伪造最初请求成功/未执行，不释放预算。
	fact.Request = nextRequest
	fact.ContextSHA256, err = nextRequest.ContextSHA256()
	require.NoError(t, err)
	fact.ClosedBy.Request = nextRequest.Close
	fact.ClosedBy.ContextSHA256, err = nextRequest.Close.ContextSHA256()
	require.NoError(t, err)
	fact.ClosedBy.Observed.AdmissionRevision = nextRequest.Close.Revision
	fact.ClosedBy.Observed.IssuerFloorEpoch = m.Owner.Epoch
	fact.ClosedBy.Disposition = control.AdmissionReplayed
	m, err = m.Takeover(m.Owner, "query-only-owner")
	require.NoError(t, err)
	_, err = m.RecordServiceClosureContinuation(m.Owner, fact, false)
	require.Error(t, err)
	m, err = m.RecordServiceClosureContinuation(m.Owner, fact, true)
	require.NoError(t, err)
	require.False(t, m.ServiceClosure.Pending())
	require.Equal(t, Settled, m.ServiceClosure.Continuation.Phase)
	retained := *m.ServiceClosure
	retained.Continuation = nil
	require.Equal(t, original, retained, "原始命令、owner、序列与未知逐字保留")
	require.Equal(t, before.InitialHeld, m.InitialHeld)
	_, disposition, err := m.Begin(m.Owner, testRequest(m, 1, control.Grow), 0)
	require.NoError(t, err)
	require.Equal(t, Busy, disposition)
	cloned := m.Clone()
	cloned.ServiceClosure.Continuation.Receipt.ClosedBy.Observed.Arrays[0].CommittedUpperBytes++
	require.NotEqual(t, cloned.ServiceClosure, m.ServiceClosure)
	_, wire, err = encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err = decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m, loaded)
}

func TestServiceContinuationRejectsChangedSelectionAndProjection(t *testing.T) {
	m, request, fact := closureContinuationModel(t)
	for _, mode := range []string{"selector", "original", "predecessor", "budget", "boot", "capacity", "epoch", "actor"} {
		t.Run(mode, func(t *testing.T) {
			bad, actor, selector := request, m.Owner, m.ServiceClosure.CommandSHA256
			switch mode {
			case "selector":
				selector = "wrong"
			case "original":
				bad.Original.Revision++
			case "predecessor":
				bad.PreviousClose.Revision++
			case "budget":
				bad.Close.BudgetCommitmentID = "other-budget"
			case "boot":
				bad.Close.Instance.PGBootID++
			case "capacity":
				bad.Close.ExpectedCapacityGeneration++
			case "epoch":
				bad.Close.IssuerEpoch--
			case "actor":
				actor.HolderID = "unselected-owner"
			}
			_, err := m.StartServiceClosureContinuation(actor, selector, bad)
			require.Error(t, err)
		})
	}
	started, err := m.StartServiceClosureContinuation(m.Owner, m.ServiceClosure.CommandSHA256, request)
	require.NoError(t, err)
	for _, mode := range []string{"version", "no-closure", "no-continuation", "original-settled", "previous-holder", "previous-sequence", "started-sequence", "origin", "digest", "unknown-receipt", "future-floor"} {
		t.Run(mode, func(t *testing.T) {
			next := started.Clone()
			c := next.ServiceClosure.Continuation
			switch mode {
			case "version":
				next.ProtocolVersion = 10
			case "no-closure":
				next.ServiceClosure = nil
			case "no-continuation":
				next.ServiceClosure.Continuation = nil
			case "original-settled":
				next.ServiceClosure.Phase = Settled
			case "previous-holder":
				c.PreviousOrigin.HolderID = "other-holder"
			case "previous-sequence":
				c.PreviousSequence++
			case "started-sequence":
				c.StartedSequence = c.PreviousSequence
			case "origin":
				c.Origin.HolderID = "other-holder"
			case "digest":
				c.CommandSHA256 = "wrong"
			case "unknown-receipt":
				c.Receipt = &fact
			case "future-floor":
				proof := fact
				proof.ClosedBy = *cloneAdmissionResponse(&fact.ClosedBy)
				proof.ClosedBy.Disposition = control.AdmissionReplayed
				proof.ClosedBy.Observed.IssuerFloorEpoch = next.Owner.Epoch + 1
				_, err := next.RecordServiceClosureContinuation(next.Owner, proof, true)
				require.Error(t, err)
				return
			}
			require.Error(t, next.Validate())
		})
	}
}

func TestServiceContinuationStrictCodecAndCompletionSpace(t *testing.T) {
	m, request, fact := closureContinuationModel(t)
	started, err := m.StartServiceClosureContinuation(m.Owner, m.ServiceClosure.CommandSHA256, request)
	require.NoError(t, err)
	_, wire, err := encode(started, MaxEncodedBytes)
	require.NoError(t, err)
	_, _, err = encode(started, len(wire))
	require.Error(t, err)
	writeBudget := len(wire) + 4096
	_, _, err = encode(started, writeBudget)
	require.NoError(t, err)
	settled, err := started.RecordServiceClosureContinuation(started.Owner, fact, false)
	require.NoError(t, err)
	_, _, err = encode(settled, writeBudget)
	require.NoError(t, err)
	raw, err := json.Marshal(started.ServiceClosure.Continuation)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	for key := range fields {
		t.Run("missing-"+key, func(t *testing.T) {
			projection := make(map[string]json.RawMessage)
			for field, value := range fields {
				if field != key {
					projection[field] = value
				}
			}
			body, err := json.Marshal(projection)
			require.NoError(t, err)
			var decoded ServiceClosureContinuation
			require.Error(t, json.Unmarshal(body, &decoded))
		})
	}
	for _, raw := range []string{`{"origin":null,"origin":null}`, `{"unexpected":1}`, `null`} {
		var decoded ServiceClosureContinuation
		require.Error(t, json.Unmarshal([]byte(raw), &decoded))
	}
}

func TestServiceContinuationStoreRequiresConfirmedUIDRV(t *testing.T) {
	for _, mode := range []string{"complete", "lost-start", "stale-rv", "recreated-vm", "lost-record"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, request, fact := closureContinuationModel(t)
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(ctx)
			require.NoError(t, err)
			if mode == "stale-rv" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"concurrent": "update"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-start"
			if mode == "recreated-vm" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "replacement-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			started, ticket, err := store.StartServiceClosureContinuation(ctx, s, m.Owner, m.ServiceClosure.CommandSHA256, request)
			if mode == "stale-rv" || mode == "recreated-vm" {
				require.Error(t, err)
				require.Nil(t, started)
				require.Nil(t, ticket)
				return
			}
			if mode == "lost-start" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				require.Nil(t, started)
				require.Nil(t, ticket, "写成功丢回复仍不得派发")
				loaded, err := api.client(t).Load(ctx)
				require.NoError(t, err)
				require.Equal(t, request, loaded.State().ServiceClosure.Continuation.Request)
				require.True(t, loaded.State().ServiceClosure.Pending())
				_, ticket, err = store.StartServiceClosureContinuation(ctx, loaded, m.Owner, m.ServiceClosure.CommandSHA256, request)
				require.Error(t, err)
				require.Nil(t, ticket)
				return
			}
			require.NoError(t, err)
			require.Equal(t, request, ticket.Request)
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-record"
			api.mu.Unlock()
			completed, err := store.RecordServiceClosureContinuation(ctx, started, m.Owner, fact, false)
			if mode == "lost-record" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				require.Nil(t, completed)
			} else {
				require.NoError(t, err)
			}
			loaded, err := api.client(t).Load(ctx)
			require.NoError(t, err)
			require.False(t, loaded.State().ServiceClosure.Pending())
			require.Equal(t, Unconfirmed, loaded.State().ServiceClosure.Phase)
			require.Equal(t, request, loaded.State().ServiceClosure.Continuation.Request)
			require.Equal(t, m.InitialHeld, loaded.State().InitialHeld)
		})
	}
}
