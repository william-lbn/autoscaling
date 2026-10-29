package journal

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func reopenClosureModel(t *testing.T, mode string) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	m, before := reopenModel(t, "initial-unknown")
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err := m.StartServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	if mode == "settled" {
		m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
		require.NoError(t, err)
	}
	if mode != "unknown-before" {
		before.Observed.AdmissionRevision = m.ServiceClosure.Reopen.Request.Revision
		before.Observed.AdmissionOpen = mode != "expired"
	}
	before.Expired = mode == "expired"
	return m, before
}

func reopenClosureFact(t *testing.T, m Model) control.AdmissionClosure {
	t.Helper()
	a := m.ServiceClosure.Reopen.Closing
	c := a.Close
	digest, err := c.Request.ContextSHA256()
	require.NoError(t, err)
	response := control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: c.Request, Disposition: control.AdmissionApplied, ContextSHA256: digest, Observed: a.Before.Observed}
	response.Observed.AdmissionOpen, response.Observed.AdmissionRevision = false, c.Request.Revision
	fact := control.AdmissionClosure{ProtocolVersion: 1, VMShrinkPermit: false, Original: *c.Original, ClosedBy: response, PriorMayHaveApplied: true}
	require.NoError(t, fact.ValidateFor(control.AdmissionCloseRequest{ProtocolVersion: 1, Original: *c.Original, Close: c.Request}, *m.PGInstance, false))
	return fact
}

func reopenContinuationFact(t *testing.T, m Model) control.AdmissionContinuationResponse {
	t.Helper()
	a := m.ServiceClosure.Reopen.Closing
	n := a.Close.Continuation
	digest, err := n.Request.Close.ContextSHA256()
	require.NoError(t, err)
	response := control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: n.Request.Close, Disposition: control.AdmissionApplied, ContextSHA256: digest, Observed: a.LatestBefore.Observed}
	response.Observed.AdmissionOpen, response.Observed.AdmissionRevision = false, n.Request.Close.Revision
	fact := control.AdmissionContinuationResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: n.Request, ContextSHA256: n.CommandSHA256, ClosedBy: response, PriorMayHaveApplied: true}
	require.NoError(t, fact.ValidateFor(n.Request, *m.PGInstance, false))
	return fact
}

func TestServiceReopenClosureRetainsUnknownAndCompletionSpace(t *testing.T) {
	for _, mode := range []string{"unknown-before", "unknown-after", "settled", "expired"} {
		t.Run(mode, func(t *testing.T) {
			m, before := reopenClosureModel(t, mode)
			original := m.Clone()
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			windows := 4
			if mode == "settled" {
				windows = 3
			}
			byteBudget := len(wire) + windows*(MaxObservationBytes+4096)
			_, _, err = encode(m, byteBudget)
			require.NoError(t, err)
			closed, err := m.StartServiceReopenClosure(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, before)
			require.NoError(t, err)
			_, _, err = encode(closed, byteBudget)
			require.NoError(t, err, "原开放写前预留必须允许后继安全关闭")
			_, err = closed.RecordServiceReopen(closed.Owner, reopenFact(t, closed), false)
			require.Error(t, err, "再关闭开始后不能回写原开放未知")
			fact := reopenClosureFact(t, closed)
			completed, err := closed.RecordServiceReopenClosure(closed.Owner, fact, false)
			require.NoError(t, err)
			_, wire, err = encode(completed, byteBudget)
			require.NoError(t, err)
			loaded, err := decode(wire, m.VMUID, m.Incarnation)
			require.NoError(t, err)
			require.Equal(t, completed, loaded)
			retained := loaded.Clone()
			retained.ServiceClosure.Reopen.Closing = nil
			retained.ServiceClosure.Reopen.SafetyStage = "opening"
			retained.Sequence = original.Sequence
			require.Equal(t, original, retained)
			require.Equal(t, Unconfirmed, loaded.InitialHeld.Admission.Phase, "首次旧未知不被新开放/再关闭覆盖")
			require.False(t, loaded.ServiceClosure.Reopen.Closing.Close.Pending())
			cloned := loaded.Clone()
			cloned.ServiceClosure.Reopen.Closing.Before.Observed.Arrays[0].CommittedUpperBytes++
			cloned.ServiceClosure.Reopen.Closing.Close.ClosureReceipt.ClosedBy.Observed.Arrays[0].CommittedUpperBytes++
			require.NotEqual(t, loaded, cloned)
		})
	}
}

func TestServiceReopenClosureRepeatedTakeoverPreservesOriginalAndPredecessor(t *testing.T) {
	m, before := reopenClosureModel(t, "unknown-after")
	m, err := m.StartServiceReopenClosure(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, before)
	require.NoError(t, err)
	first := m.Clone().ServiceClosure.Reopen.Closing.Close
	for _, holder := range []string{"reopen-close-second", "reopen-close-third"} {
		origin, sequence, previous, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
		m, err = m.Takeover(m.Owner, holder)
		require.NoError(t, err)
		before.Request.IssuerEpoch, before.Observed.IssuerFloorEpoch = m.Owner.Epoch, m.Owner.Epoch
		before.Observed.AdmissionRevision, before.Observed.AdmissionOpen = previous.Revision, false
		m, err = m.ContinueServiceReopenClosure(m.Owner, selector, before)
		require.NoError(t, err)
		a := m.ServiceClosure.Reopen.Closing
		require.Equal(t, origin, a.Close.Continuation.PreviousOrigin)
		require.Equal(t, sequence, a.Close.Continuation.PreviousSequence)
		require.Equal(t, previous, a.Close.Continuation.Request.PreviousClose)
		_, err = m.RecordServiceReopenClosure(m.Owner, reopenClosureFact(t, m), true)
		require.Error(t, err, "前驱回执不能结清后继")
		_, err = m.ContinueServiceReopenClosure(m.Owner, a.Close.Continuation.CommandSHA256, before)
		require.Error(t, err, "同 owner 超时不能反复新选")
	}
	fact := reopenContinuationFact(t, m)
	m, err = m.Takeover(m.Owner, "reopen-close-query-only")
	require.NoError(t, err)
	_, err = m.RecordServiceReopenContinuation(m.Owner, fact, false)
	require.Error(t, err)
	fact.ClosedBy.Disposition = control.AdmissionReplayed
	completed, err := m.RecordServiceReopenContinuation(m.Owner, fact, true)
	require.NoError(t, err)
	retained := completed.Clone().ServiceClosure.Reopen.Closing.Close
	retained.Continuation = nil
	require.Equal(t, first, retained)
	require.Equal(t, Unconfirmed, completed.ServiceClosure.Reopen.Phase)
	projected := completed.Clone()
	projected.ServiceClosure.Reopen.Closing.Close.Continuation = nil
	projected.ServiceClosure.Reopen.Closing.LatestBefore = nil
	require.Error(t, projected.Validate(), "接续不能投影为原关闭未知")
	_, wire, err := encode(completed, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(wire, completed.VMUID, completed.Incarnation)
	require.NoError(t, err)
	require.Equal(t, completed, loaded)
	cloned := loaded.Clone()
	cloned.ServiceClosure.Reopen.Closing.LatestBefore.Observed.Arrays[0].CommittedUpperBytes++
	require.NotEqual(t, loaded, cloned)
}

func TestServiceReopenClosureRejectsBorrowedFactsAndRecursiveRecovery(t *testing.T) {
	m, before := reopenClosureModel(t, "unknown-after")
	selector := m.ServiceClosure.Reopen.CommandSHA256
	for _, mode := range []string{"floor", "capacity", "max-revision", "observer", "selector"} {
		t.Run(mode, func(t *testing.T) {
			bad := cloneLeaseResponse(before)
			selected := selector
			switch mode {
			case "floor":
				bad.Observed.IssuerFloorEpoch++
			case "capacity":
				bad.Observed.CapacityGeneration++
			case "max-revision":
				bad.Observed.AdmissionRevision = control.Uint64(math.MaxUint64)
			case "observer":
				bad.Request.IssuerEpoch++
			case "selector":
				selected = "unrelated-open"
			}
			_, err := m.StartServiceReopenClosure(m.Owner, selected, bad)
			require.Error(t, err)
		})
	}
	closed, err := m.StartServiceReopenClosure(m.Owner, selector, before)
	require.NoError(t, err)
	projected := closed.Clone()
	projected.ServiceClosure.Reopen.Closing = nil
	require.Error(t, projected.Validate(), "不能删除再关闭而保留原开放派发资格")
	for name, change := range map[string]func(*ServiceReopenClosure){
		"missing":        func(c *ServiceReopenClosure) { c.Close = nil },
		"borrowed-open":  func(c *ServiceReopenClosure) { c.Close.Original.Revision++ },
		"budget":         func(c *ServiceReopenClosure) { c.Close.Request.BudgetCommitmentID = "another-budget" },
		"revision":       func(c *ServiceReopenClosure) { c.Close.Request.Revision++ },
		"rearm":          func(c *ServiceReopenClosure) { var invalid ServiceRearm; c.Close.Rearm = &invalid },
		"reopen":         func(c *ServiceReopenClosure) { var invalid ServiceReopen; c.Close.Reopen = &invalid },
		"operation":      func(c *ServiceReopenClosure) { c.Close.OperationSHA256 = "another-active" },
		"false-complete": func(c *ServiceReopenClosure) { c.Close.Phase = Settled },
		"predated":       func(c *ServiceReopenClosure) { c.Close.StartedSequence = 1 },
		"phantom-latest": func(c *ServiceReopenClosure) { b := c.Before; c.LatestBefore = &b },
	} {
		t.Run(name, func(t *testing.T) {
			bad := closed.Clone()
			change(bad.ServiceClosure.Reopen.Closing)
			require.Error(t, bad.Validate())
			if name == "missing" {
				require.NotPanics(t, func() { _, _, err := encode(bad, MaxEncodedBytes); require.Error(t, err) })
			}
		})
	}
	fact := reopenClosureFact(t, closed)
	fact.PriorMayHaveApplied = false
	_, err = closed.RecordServiceReopenClosure(closed.Owner, fact, false)
	require.Error(t, err)
	raw, err := json.Marshal(closed.ServiceClosure.Reopen.Closing)
	require.NoError(t, err)
	var strict ServiceReopenClosure
	require.Error(t, json.Unmarshal(append(raw[:len(raw)-1], []byte(`,"latest_before":null}`)...), &strict))
}

func TestServiceReopenClosureStoreNeverDispatchesUnconfirmedCAS(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		for _, mode := range []string{"lost-start", "lost-record", "stale-rv", "recreated-vm", "normal"} {
			name := mode
			if continuation {
				name = "continuation-" + name
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				api, store := newStoreAPI(t)
				_ = initializeStore(t, api, store)
				m, before := reopenClosureModel(t, "unknown-after")
				selector := m.ServiceClosure.Reopen.CommandSHA256
				var err error
				if continuation {
					m, err = m.StartServiceReopenClosure(m.Owner, selector, before)
					require.NoError(t, err)
					selector = m.ServiceClosure.Reopen.Closing.Close.CommandSHA256
					m, err = m.Takeover(m.Owner, "store-reopen-close-successor")
					require.NoError(t, err)
					before.Request.IssuerEpoch, before.Observed.IssuerFloorEpoch = m.Owner.Epoch, m.Owner.Epoch
				}
				_, wire, err := encode(m, MaxEncodedBytes)
				require.NoError(t, err)
				api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
				s, err := store.Load(ctx)
				require.NoError(t, err)
				if mode == "stale-rv" {
					api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"concurrent": "update"} })
					api.mu.Lock()
					api.beforePatch = nil
					api.mu.Unlock()
				}
				api.mu.Lock()
				api.failAfterCommit = mode == "lost-start"
				if mode == "recreated-vm" {
					api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "replacement-vm"; api.mu.Unlock() }
				}
				api.mu.Unlock()
				var started *Snapshot
				var ticket *ServiceReopenClosure
				if continuation {
					started, ticket, err = store.ContinueServiceReopenClosure(ctx, s, m.Owner, selector, before)
				} else {
					started, ticket, err = store.StartServiceReopenClosure(ctx, s, m.Owner, selector, before)
				}
				if mode == "stale-rv" || mode == "recreated-vm" {
					require.Error(t, err)
					require.Nil(t, started)
					require.Nil(t, ticket)
					return
				}
				if mode == "lost-start" {
					var unknown *UnconfirmedWrite
					require.ErrorAs(t, err, &unknown)
					require.Nil(t, ticket)
					loaded, err := store.Load(ctx)
					require.NoError(t, err)
					require.True(t, loaded.State().ServiceClosure.Reopen.Closing.Close.Pending())
					return
				}
				require.NoError(t, err)
				require.NotNil(t, ticket)
				api.mu.Lock()
				api.failAfterCommit = mode == "lost-record"
				api.mu.Unlock()
				if continuation {
					_, err = store.RecordServiceReopenContinuation(ctx, started, m.Owner, reopenContinuationFact(t, started.State()), false)
				} else {
					_, err = store.RecordServiceReopenClosure(ctx, started, m.Owner, reopenClosureFact(t, started.State()), false)
				}
				if mode == "lost-record" {
					var unknown *UnconfirmedWrite
					require.ErrorAs(t, err, &unknown)
				} else {
					require.NoError(t, err)
				}
				loaded, err := store.Load(ctx)
				require.NoError(t, err)
				require.False(t, loaded.State().ServiceClosure.Reopen.Closing.Close.Pending())
				require.Equal(t, Unconfirmed, loaded.State().ServiceClosure.Reopen.Phase)
			})
		}
	}
}
