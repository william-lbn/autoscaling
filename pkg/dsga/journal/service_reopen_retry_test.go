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

func retryReopenModel(t *testing.T, mode string) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	m, before := reopenClosureModel(t, mode)
	m, err := m.StartServiceReopenClosure(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, before)
	require.NoError(t, err)
	fact := reopenClosureFact(t, m)
	m, err = m.RecordServiceReopenClosure(m.Owner, fact, false)
	require.NoError(t, err)
	before.Observed = fact.ClosedBy.Observed
	return m, before
}

func TestServiceReopenRetryPreservesCompleteUnknownAndAllowsCompletion(t *testing.T) {
	for _, mode := range []string{"unknown-before", "unknown-after", "settled", "continuation"} {
		t.Run(mode, func(t *testing.T) {
			fixture := mode
			if fixture == "continuation" {
				fixture = "unknown-after"
			}
			m, before := retryReopenModel(t, fixture)
			if mode == "continuation" {
				// 真正未知关闭接管：原关闭保持未知，只完成更高owner的
				// 最近接续；旧关闭/原开放都不能被补成成功。
				m, before = reopenClosureModel(t, fixture)
				var err error
				m, err = m.StartServiceReopenClosure(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, before)
				require.NoError(t, err)
				selector := m.ServiceClosure.Reopen.Closing.Close.CommandSHA256
				m, err = m.Takeover(m.Owner, "retry-higher-owner")
				require.NoError(t, err)
				before.Request.IssuerEpoch, before.Observed.IssuerFloorEpoch = m.Owner.Epoch, m.Owner.Epoch
				m, err = m.ContinueServiceReopenClosure(m.Owner, selector, before)
				require.NoError(t, err)
				fact := reopenContinuationFact(t, m)
				m, err = m.RecordServiceReopenContinuation(m.Owner, fact, false)
				require.NoError(t, err)
				before.Observed = fact.ClosedBy.Observed
				verifyRetryCompletion(t, m, before)
				return
			}
			verifyRetryCompletion(t, m, before)
		})
	}
}

func verifyRetryCompletion(t *testing.T, m Model, before control.AdmissionLeaseResponse) {
	t.Helper()
	original := m.Clone()
	_, _, closed, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
	next, err := m.RetryServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	require.Equal(t, original, m)
	require.Equal(t, m.InitialHeld, next.InitialHeld)
	require.Equal(t, closed.Revision+1, next.ServiceClosure.Reopen.Request.Revision)
	var prior ServiceReopen
	require.NoError(t, json.Unmarshal(next.ServiceClosure.Reopen.Retired.Context, &prior))
	require.Equal(t, *original.ServiceClosure.Reopen, prior)
	cloned := next.Clone()
	cloned.ServiceClosure.Reopen.Retired.Context[0] ^= 1
	cloned.ServiceClosure.Reopen.PriorClose.Revision++
	require.NotEqual(t, cloned, next)
	_, wire, err := encode(next, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(wire, next.VMUID, next.Incarnation)
	require.NoError(t, err)
	require.Equal(t, next, loaded)
	next, err = loaded.RecordServiceReopen(loaded.Owner, reopenFact(t, loaded), false)
	require.NoError(t, err)
	before.Observed = next.ServiceClosure.Reopen.Receipt.Observed
	done, err := next.CompleteServiceReopen(next.Owner, next.ServiceClosure.Reopen.CommandSHA256, before)
	require.NoError(t, err)
	require.Nil(t, done.ServiceClosure)
	require.Equal(t, original.InitialHeld.Admission, done.InitialHeld.Admission)
	require.Equal(t, Unconfirmed, done.InitialHeld.Admission.Phase)
	require.NotNil(t, done.InitialHeld.Covered)
}

func TestServiceReopenRetryRejectsStaleSelectionAndProjectedEvidence(t *testing.T) {
	m, before := retryReopenModel(t, "unknown-after")
	_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
	for name, change := range map[string]func(*control.AdmissionLeaseResponse){
		"expired":     func(b *control.AdmissionLeaseResponse) { b.Expired = true },
		"older-close": func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionRevision-- },
		"newer-close": func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionRevision++ },
		"open":        func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionOpen = true },
		"deadline":    func(b *control.AdmissionLeaseResponse) { b.DeadlineNS-- },
		"capacity":    func(b *control.AdmissionLeaseResponse) { b.Observed.CapacityGeneration++ },
		"floor":       func(b *control.AdmissionLeaseResponse) { b.Observed.IssuerFloorEpoch++ },
	} {
		t.Run(name, func(t *testing.T) {
			bad := cloneLeaseResponse(before)
			change(&bad)
			_, err := m.RetryServiceReopen(m.Owner, selector, bad)
			require.Error(t, err)
		})
	}
	_, err := m.RetryServiceReopen(m.Owner, m.ServiceClosure.CommandSHA256, before)
	require.Error(t, err)
	next, err := m.RetryServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	for name, change := range map[string]func(*ServiceReopen){
		"lost-history":   func(r *ServiceReopen) { r.Retired = nil },
		"lost-counter":   func(r *ServiceReopen) { r.Attempt = 0 },
		"project-first":  func(r *ServiceReopen) { r.Attempt, r.PriorClosedAt, r.PriorClose, r.Retired = 0, 0, nil, nil },
		"prior-close":    func(r *ServiceReopen) { r.PriorClose.Revision-- },
		"prior-sequence": func(r *ServiceReopen) { r.PriorClosedAt-- },
		"context":        func(r *ServiceReopen) { r.Retired.Context[0] ^= 1 },
		"chain":          func(r *ServiceReopen) { r.Retired.PreviousSHA256 = r.Retired.ContextSHA256 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := next.Clone()
			change(bad.ServiceClosure.Reopen)
			require.Error(t, bad.Validate())
		})
	}
	m.Sequence = control.Uint64(math.MaxUint64)
	_, err = m.RetryServiceReopen(m.Owner, selector, before)
	require.Error(t, err)
}

func TestServiceReopenRetryRepeatedUnknownClosuresRemainBounded(t *testing.T) {
	m, before := retryReopenModel(t, "unknown-after")
	initial := m.Clone().InitialHeld
	for attempt := 1; attempt <= 24; attempt++ {
		_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
		old := m.Clone().ServiceClosure.Reopen
		var err error
		m, err = m.RetryServiceReopen(m.Owner, selector, before)
		require.NoError(t, err)
		r := m.ServiceClosure.Reopen
		require.EqualValues(t, attempt, r.Attempt)
		var prior ServiceReopen
		require.NoError(t, json.Unmarshal(r.Retired.Context, &prior))
		require.Nil(t, prior.Retired)
		old.Retired = nil
		require.Equal(t, *old, prior)
		require.Equal(t, Unconfirmed, prior.Phase)
		require.Less(t, len(r.Retired.Context), 18000, "完整直接前驱不随轮数递归增长")
		before.Observed.AdmissionOpen, before.Observed.AdmissionRevision = true, r.Request.Revision
		m, err = m.StartServiceReopenClosure(m.Owner, r.CommandSHA256, before)
		require.NoError(t, err)
		fact := reopenClosureFact(t, m)
		m, err = m.RecordServiceReopenClosure(m.Owner, fact, false)
		require.NoError(t, err)
		before.Observed = fact.ClosedBy.Observed
		_, wire, err := encode(m, MaxEncodedBytes)
		require.NoError(t, err)
		m, err = decode(wire, m.VMUID, m.Incarnation)
		require.NoError(t, err)
		require.Equal(t, initial, m.InitialHeld)
	}
}

func TestServiceReopenRetryStoreUnknownReplyNeverReturnsDispatch(t *testing.T) {
	for _, mode := range []string{"normal", "lost-record", "stale-rv", "recreated-vm"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, before := retryReopenModel(t, "unknown-after")
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(ctx)
			require.NoError(t, err)
			if mode == "stale-rv" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"other": "change"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-record"
			if mode == "recreated-vm" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "other-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
			result, dispatch, err := store.RetryServiceReopen(ctx, s, m.Owner, selector, before)
			if mode != "normal" {
				require.Error(t, err)
				require.Nil(t, result)
				require.Nil(t, dispatch)
			} else {
				require.NoError(t, err)
				require.NotNil(t, dispatch)
			}
			if mode == "lost-record" {
				loaded, err := store.Load(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, loaded.State().ServiceClosure.Reopen.Attempt)
				require.Equal(t, Unconfirmed, loaded.State().ServiceClosure.Reopen.Phase)
				require.Equal(t, m.InitialHeld, loaded.State().InitialHeld)
			}
		})
	}
}

func TestServiceReopenRetryAfterCompletedLifecycleKeepsIndependentSafety(t *testing.T) {
	m, current := completedReopenModel(t, "initial-unknown")
	m, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
	require.NoError(t, err)
	previous := *m.ServiceLifecycle
	closed := m.InitialHeld.Covered.Request
	closed.Open, closed.Revision = false, current.Observed.AdmissionRevision+1
	m, err = m.StartServiceClosure(m.Owner, closed)
	require.NoError(t, err)
	digest, err := closed.ContextSHA256()
	require.NoError(t, err)
	fact := control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: closed, Disposition: control.AdmissionApplied, ContextSHA256: digest, Observed: current.Observed}
	fact.Observed.AdmissionOpen, fact.Observed.AdmissionRevision = false, closed.Revision
	m, err = m.RecordServiceClosure(m.Owner, fact, false)
	require.NoError(t, err)
	current.Observed = fact.Observed
	m, err = m.StartServiceReopen(m.Owner, m.ServiceClosure.CommandSHA256, current)
	require.NoError(t, err)
	current.Observed.AdmissionOpen, current.Observed.AdmissionRevision = true, m.ServiceClosure.Reopen.Request.Revision
	m, err = m.StartServiceReopenClosure(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
	require.NoError(t, err)
	proof := reopenClosureFact(t, m)
	m, err = m.RecordServiceReopenClosure(m.Owner, proof, false)
	require.NoError(t, err)
	current.Observed = proof.ClosedBy.Observed
	m, err = m.RetryServiceReopen(m.Owner, m.ServiceClosure.Reopen.Closing.Close.CommandSHA256, current)
	require.NoError(t, err)
	require.EqualValues(t, 21, m.ProtocolVersion)
	require.EqualValues(t, 20, m.ClosureFormatVersion())
	require.Equal(t, "closing", m.ServiceLifecycle.SafetyStage)
	projected := m.Clone()
	projected.ServiceClosure = nil
	require.Error(t, projected.Validate(), "删除本轮重试WAL不能投影为前轮服务仍开放")
	m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
	require.NoError(t, err)
	current.Observed = m.ServiceClosure.Reopen.Receipt.Observed
	m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
	require.NoError(t, err)
	require.Equal(t, previous.Cycle+1, m.ServiceLifecycle.Cycle)
	require.Equal(t, previous.Current, m.ServiceLifecycle.Previous.Current)
	_, _, err = encode(m, MaxEncodedBytes)
	require.NoError(t, err)
}
