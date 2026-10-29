package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func expiredReopenModel(t *testing.T) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	m, b := retryReopenModel(t, "unknown-after")
	m, err := m.Takeover(m.Owner, "reopen-expired-owner")
	require.NoError(t, err)
	b.Expired = true
	b.Request.IssuerEpoch, b.Observed.IssuerFloorEpoch = m.Owner.Epoch, m.Owner.Epoch
	return m, b
}

func reopenRearmFact(t *testing.T, m Model) control.AdmissionRearmStatus {
	t.Helper()
	r := m.ServiceClosure.Reopen.Rearm
	b := cloneLeaseResponse(r.Before)
	b.Request, b.LeaseEpoch, b.DeadlineNS = r.Request, r.Origin.Epoch, r.Before.DeadlineNS+100
	// 历史命中可在当前已再次过期时保存，不能由此推导可开放业务。
	f := control.AdmissionRearmStatus{ProtocolVersion: 1, VMShrinkPermit: false, Request: r.Request, Retained: true, AppliedDeadlineNS: b.DeadlineNS, Current: b}
	require.NoError(t, f.ValidateFor(r.Request, *m.PGInstance))
	return f
}

func TestReopenLeaseRecoveryPreservesUnknownAndRequiresExactConfirmedLease(t *testing.T) {
	m, b := expiredReopenModel(t)
	_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
	original := m.Clone()
	n, err := m.StartReopenLeaseRecovery(m.Owner, selector, b)
	require.NoError(t, err)
	require.Equal(t, original, m)
	r := n.ServiceClosure.Reopen.Rearm
	require.Equal(t, "rearming", n.ServiceClosure.Reopen.SafetyStage)
	_, wire, err := encode(n, MaxEncodedBytes)
	require.NoError(t, err)
	_, _, err = encode(n, len(wire)+4096)
	require.Error(t, err, "完整原生回执不能只预留关闭默认4KiB")
	writeBudget := len(wire) + MaxObservationBytes + 4096
	_, _, err = encode(n, writeBudget)
	require.NoError(t, err)
	n, err = decode(wire, n.VMUID, n.Incarnation)
	require.NoError(t, err)
	live := cloneLeaseResponse(b)
	live.Expired = false
	_, err = n.RetryServiceReopen(n.Owner, selector, live)
	require.Error(t, err, "有效布尔不能代替原rearm确认")
	fact := reopenRearmFact(t, n)
	n, err = n.RecordReopenLeaseRecovery(n.Owner, fact)
	require.NoError(t, err)
	_, _, err = encode(n, writeBudget)
	require.NoError(t, err)
	retained := n.Clone().ServiceClosure.Reopen
	retained.Rearm, retained.SafetyStage = nil, "closing"
	require.Equal(t, *original.ServiceClosure.Reopen, *retained)
	require.Equal(t, original.InitialHeld, n.InitialHeld)
	cloned := n.Clone()
	cloned.ServiceClosure.Reopen.Rearm.Receipt.Current.Observed.Arrays[0].CommittedUpperBytes++
	require.NotEqual(t, n, cloned)
	live = cloneLeaseResponse(fact.Current)
	live.Request, live.Expired = b.Request, false
	bad := cloneLeaseResponse(live)
	bad.DeadlineNS = r.Before.DeadlineNS
	_, err = n.RetryServiceReopen(n.Owner, selector, bad)
	require.Error(t, err)
	bad = cloneLeaseResponse(live)
	bad.LeaseEpoch--
	_, err = n.RetryServiceReopen(n.Owner, selector, bad)
	require.Error(t, err)
	verifyRetryCompletion(t, n, live)
}

func TestReopenLeaseRecoveryRejectsProjectedHistoryAndWrongNativeSelection(t *testing.T) {
	m, b := expiredReopenModel(t)
	_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
	for name, change := range map[string]func(*control.AdmissionLeaseResponse){
		"live":       func(b *control.AdmissionLeaseResponse) { b.Expired = false },
		"unarmed":    func(b *control.AdmissionLeaseResponse) { b.Armed = false },
		"same-owner": func(b *control.AdmissionLeaseResponse) { b.LeaseEpoch = m.Owner.Epoch },
		"boot":       func(b *control.AdmissionLeaseResponse) { b.Observed.Instance.PGBootID++ },
		"capacity":   func(b *control.AdmissionLeaseResponse) { b.Observed.CapacityGeneration++ },
		"old-close":  func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionRevision-- },
		"new-close":  func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionRevision++ },
		"floor":      func(b *control.AdmissionLeaseResponse) { b.Observed.IssuerFloorEpoch++ },
		"deadline":   func(b *control.AdmissionLeaseResponse) { b.DeadlineNS-- },
	} {
		t.Run(name, func(t *testing.T) {
			bad := cloneLeaseResponse(b)
			change(&bad)
			_, err := m.StartReopenLeaseRecovery(m.Owner, selector, bad)
			require.Error(t, err)
		})
	}
	_, err := m.StartReopenLeaseRecovery(m.Owner, m.ServiceClosure.CommandSHA256, b)
	require.Error(t, err)
	n, err := m.StartReopenLeaseRecovery(m.Owner, selector, b)
	require.NoError(t, err)
	for name, change := range map[string]func(*Model){
		"erase":        func(m *Model) { m.ServiceClosure.Reopen.Rearm = nil },
		"stage":        func(m *Model) { m.ServiceClosure.Reopen.SafetyStage = "closing" },
		"close":        func(m *Model) { m.ServiceClosure.Reopen.Closing = nil },
		"budget":       func(m *Model) { m.InitialHeld.BudgetSHA256 = "different" },
		"command":      func(m *Model) { m.ServiceClosure.Reopen.Rearm.Request.ExpectedDeadlineNS++ },
		"cycle":        func(m *Model) { m.ServiceClosure.Reopen.Rearm.Cycle++ },
		"origin":       func(m *Model) { m.ServiceClosure.Reopen.Rearm.Origin.HolderID = "other" },
		"closing-hash": func(m *Model) { m.ServiceClosure.Reopen.Rearm.ClosingSHA256 = selector },
	} {
		t.Run(name, func(t *testing.T) { bad := n.Clone(); change(&bad); require.Error(t, bad.Validate()) })
	}
	unknown := reopenRearmFact(t, n)
	unknown.Retained, unknown.AppliedDeadlineNS = false, 0
	_, err = n.RecordReopenLeaseRecovery(n.Owner, unknown)
	require.Error(t, err)
	m.Sequence = control.Uint64(math.MaxUint64)
	_, err = m.StartReopenLeaseRecovery(m.Owner, selector, b)
	require.Error(t, err)
}

func TestReopenLeaseRecoveryRepeatedExpiryRetainsBoundedCompletePredecessor(t *testing.T) {
	m, b := expiredReopenModel(t)
	initial := m.Clone().InitialHeld
	_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
	for cycle := 1; cycle <= 24; cycle++ {
		previous := cloneReopenLeaseRecovery(m.ServiceClosure.Reopen.Rearm)
		var err error
		m, err = m.StartReopenLeaseRecovery(m.Owner, selector, b)
		require.NoError(t, err)
		r := m.ServiceClosure.Reopen.Rearm
		require.EqualValues(t, cycle, r.Cycle)
		if previous != nil {
			previous.Retired = nil
			var retired ReopenLeaseRecovery
			require.NoError(t, json.Unmarshal(r.Retired.Context, &retired))
			require.Equal(t, *previous, retired)
			require.Nil(t, retired.Retired)
			require.Less(t, len(r.Retired.Context), 16000)
			cloned := m.Clone()
			cloned.ServiceClosure.Reopen.Rearm.Retired.Context[0] ^= 1
			require.Error(t, cloned.Validate())
		}
		fact := reopenRearmFact(t, m)
		if cycle%2 == 0 {
			// 一半前驱保持写后未知，不凭当前较新期限补旧成功。
			m, err = m.RecordReopenLeaseRecovery(m.Owner, fact)
			require.NoError(t, err)
		}
		_, wire, err := encode(m, MaxEncodedBytes)
		require.NoError(t, err)
		m, err = decode(wire, m.VMUID, m.Incarnation)
		require.NoError(t, err)
		require.Equal(t, initial, m.InitialHeld)
		_, err = m.StartReopenLeaseRecovery(m.Owner, r.CommandSHA256, b)
		require.Error(t, err, "同owner不能覆盖原未知或已应用选择")
		selector = r.CommandSHA256
		m, err = m.Takeover(m.Owner, fmt.Sprintf("next-expiry-owner-%d", cycle))
		require.NoError(t, err)
		b = cloneLeaseResponse(fact.Current)
		b.Request.Action, b.Request.IssuerEpoch, b.Observed.IssuerFloorEpoch = "observe", m.Owner.Epoch, m.Owner.Epoch
		_, err = m.StartReopenLeaseRecovery(m.Owner, m.ServiceClosure.Reopen.Closing.Close.CommandSHA256, b)
		require.Error(t, err, "最近选择已变为恢复摘要，旧关闭摘要不能驱动替换")
	}
}

func TestReopenLeaseRecoveryStrictDecode(t *testing.T) {
	m, b := expiredReopenModel(t)
	_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
	m, err := m.StartReopenLeaseRecovery(m.Owner, selector, b)
	require.NoError(t, err)
	raw, err := json.Marshal(m.ServiceClosure.Reopen.Rearm)
	require.NoError(t, err)
	var object map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &object))
	for field := range object {
		t.Run(field, func(t *testing.T) {
			var bad map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &bad))
			delete(bad, field)
			encoded, err := json.Marshal(bad)
			require.NoError(t, err)
			var r ReopenLeaseRecovery
			require.Error(t, json.Unmarshal(encoded, &r))
		})
	}
	for _, extra := range []string{`,"phase":"unconfirmed"}`, `,"unexpected":0}`} {
		var r ReopenLeaseRecovery
		require.Error(t, json.Unmarshal(append(append([]byte(nil), raw[:len(raw)-1]...), []byte(extra)...), &r))
	}
}

func TestReopenLeaseRecoveryCannotRollBackLatestCloseLease(t *testing.T) {
	m, b := reopenClosureModel(t, "unknown-after")
	b.DeadlineNS += 100
	m, err := m.StartServiceReopenClosure(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, b)
	require.NoError(t, err)
	fact := reopenClosureFact(t, m)
	m, err = m.RecordServiceReopenClosure(m.Owner, fact, false)
	require.NoError(t, err)
	m, err = m.Takeover(m.Owner, "later-close-lease-owner")
	require.NoError(t, err)
	b.Expired, b.Observed = true, fact.ClosedBy.Observed
	b.Request.IssuerEpoch, b.Observed.IssuerFloorEpoch = m.Owner.Epoch, m.Owner.Epoch
	_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
	old := cloneLeaseResponse(b)
	old.DeadlineNS--
	_, err = m.StartReopenLeaseRecovery(m.Owner, selector, old)
	require.Error(t, err, "高于原开放但低于最近关闭的deadline同样拒绝")
	_, err = m.StartReopenLeaseRecovery(m.Owner, selector, b)
	require.NoError(t, err)
}

func TestStoreValidatedDecodeCacheReopenLeaseRecoveryIsolation(t *testing.T) {
	_, store := newStoreAPI(t)
	m, b := expiredReopenModel(t)
	_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
	m, err := m.StartReopenLeaseRecovery(m.Owner, selector, b)
	require.NoError(t, err)
	fact := reopenRearmFact(t, m)
	m, err = m.RecordReopenLeaseRecovery(m.Owner, fact)
	require.NoError(t, err)
	m, err = m.Takeover(m.Owner, "cache-rearm-second-owner")
	require.NoError(t, err)
	b = cloneLeaseResponse(fact.Current)
	b.Request.Action, b.Request.IssuerEpoch, b.Observed.IssuerFloorEpoch = "observe", m.Owner.Epoch, m.Owner.Epoch
	m, err = m.StartReopenLeaseRecovery(m.Owner, m.ServiceClosure.Reopen.Rearm.CommandSHA256, b)
	require.NoError(t, err)
	m, err = m.RecordReopenLeaseRecovery(m.Owner, reopenRearmFact(t, m))
	require.NoError(t, err)
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	// 并发命中同一原文后，只修改各自返回值。race同时检查Before、
	// 原回执及完整直接前驱不与缓存或其他读者共享可变内存。
	var group sync.WaitGroup
	for range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			loaded, err := store.decodeModel(wire)
			if !assert.NoError(t, err) {
				return
			}
			r := loaded.ServiceClosure.Reopen.Rearm
			r.Before.Observed.Arrays[0].CommittedUpperBytes++
			r.Receipt.Current.Observed.Arrays[0].CommittedUpperBytes++
			r.Retired.Context[0] ^= 1
		}()
	}
	group.Wait()
	loaded, err := store.decodeModel(wire)
	require.NoError(t, err)
	require.Equal(t, m, loaded)
}

func TestReopenLeaseRecoveryKeepsOuterRearmAndLatestCloseContinuation(t *testing.T) {
	for _, mode := range []string{"outer-rearm", "close-continuation"} {
		t.Run(mode, func(t *testing.T) {
			fixture := "initial-unknown"
			if mode == "outer-rearm" {
				fixture = "rearm"
			}
			m, b := reopenModel(t, fixture)
			m, err := m.StartServiceReopen(m.Owner, m.ServiceClosure.CommandSHA256, b)
			require.NoError(t, err)
			b.Observed = reopenFact(t, m).Observed
			m, err = m.StartServiceReopenClosure(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, b)
			require.NoError(t, err)
			if mode == "close-continuation" {
				m, err = m.Takeover(m.Owner, "close-continuation-owner")
				require.NoError(t, err)
				b.Request.IssuerEpoch, b.Observed.IssuerFloorEpoch = m.Owner.Epoch, m.Owner.Epoch
				m, err = m.ContinueServiceReopenClosure(m.Owner, m.ServiceClosure.Reopen.Closing.Close.CommandSHA256, b)
				require.NoError(t, err)
				f := reopenContinuationFact(t, m)
				m, err = m.RecordServiceReopenContinuation(m.Owner, f, false)
				require.NoError(t, err)
				b.Observed = f.ClosedBy.Observed
			} else {
				f := reopenClosureFact(t, m)
				m, err = m.RecordServiceReopenClosure(m.Owner, f, false)
				require.NoError(t, err)
				b.Observed = f.ClosedBy.Observed
			}
			original := m.Clone()
			m, err = m.Takeover(m.Owner, "latest-close-expiry-owner")
			require.NoError(t, err)
			b.Expired, b.Request.IssuerEpoch, b.Observed.IssuerFloorEpoch = true, m.Owner.Epoch, m.Owner.Epoch
			_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
			m, err = m.StartReopenLeaseRecovery(m.Owner, selector, b)
			require.NoError(t, err)
			f := reopenRearmFact(t, m)
			m, err = m.RecordReopenLeaseRecovery(m.Owner, f)
			require.NoError(t, err)
			require.Equal(t, original.ServiceClosure.Rearm, m.ServiceClosure.Rearm)
			require.Equal(t, original.ServiceClosure.Reopen.Closing, m.ServiceClosure.Reopen.Closing)
			b = cloneLeaseResponse(f.Current)
			b.Request.Action, b.Expired = "observe", false
			m, err = m.RetryServiceReopen(m.Owner, selector, b)
			require.NoError(t, err)
			m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
			require.NoError(t, err)
			b.Observed = m.ServiceClosure.Reopen.Receipt.Observed
			m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, b)
			require.NoError(t, err)
			require.Nil(t, m.ServiceClosure)
			require.NotNil(t, m.ServiceLifecycle)
		})
	}
}

func TestReopenLeaseRecoveryStoreNeverDispatchesUnknownCAS(t *testing.T) {
	for _, mode := range []string{"normal", "lost-start", "lost-record", "stale-rv", "recreated-vm"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, b := expiredReopenModel(t)
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(ctx)
			require.NoError(t, err)
			if mode == "stale-rv" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"changed": "true"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-start"
			if mode == "recreated-vm" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "other-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
			n, ticket, err := store.StartReopenLeaseRecovery(ctx, s, m.Owner, selector, b)
			if mode == "stale-rv" || mode == "recreated-vm" || mode == "lost-start" {
				require.Error(t, err)
				require.Nil(t, n)
				require.Nil(t, ticket)
				if mode == "lost-start" {
					s, err := store.Load(ctx)
					require.NoError(t, err)
					require.Equal(t, Unconfirmed, s.State().ServiceClosure.Reopen.Rearm.Phase)
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, ticket)
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-record"
			api.mu.Unlock()
			_, err = store.RecordReopenLeaseRecovery(ctx, n, m.Owner, reopenRearmFact(t, n.State()))
			if mode == "lost-record" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			s, err = store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, Settled, s.State().ServiceClosure.Reopen.Rearm.Phase)
		})
	}
}
