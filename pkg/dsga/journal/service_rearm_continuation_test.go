package journal

import (
	"context"
	"encoding/json"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

// 旧生产模型没有闭合续接方法时，精确尝试其唯一首恢复入口，必须真实暴露
// 原已存在 Rearm 的死端。GREEN 选择新增正式模型方法，夹具和断言保持相同。
func continueClosedRearmForTest(m Model, actor Owner, selector string, b control.AdmissionLeaseResponse) (Model, error) {
	if next, ok := any(m).(interface {
		ContinueServiceRearm(Owner, string, control.AdmissionLeaseResponse) (Model, error)
	}); ok {
		return next.ContinueServiceRearm(actor, selector, b)
	}
	return m.StartServiceRearm(actor, selector, b)
}
func expiredClosedRearmModel(t *testing.T) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	m, b := rearmModel(t)
	_, _, _, selected := m.ServiceClosure.Selection()
	m, err := m.StartServiceRearm(m.Owner, selected, b)
	require.NoError(t, err)
	m, err = m.RecordServiceRearm(m.Owner, rearmFact(t, m))
	require.NoError(t, err)
	m, err = m.Takeover(m.Owner, "closed-rearm-higher-owner")
	require.NoError(t, err)
	b = cloneLeaseResponse(m.ServiceClosure.Rearm.Receipt.Current)
	b.Request = control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: 1, Action: "observe"}
	b.Observed.IssuerFloorEpoch = m.Owner.Epoch
	b.Renewed, b.Expired = false, true
	return m, b
}
func closedRearmFact(t *testing.T, m Model) control.AdmissionRearmStatus {
	t.Helper()
	q := m.ServiceClosure.LatestRearm()
	b := cloneLeaseResponse(q.Before)
	b.Request, b.LeaseEpoch, b.DeadlineNS = q.Request, q.Origin.Epoch, q.Before.DeadlineNS+100
	f := control.AdmissionRearmStatus{ProtocolVersion: 1, Request: q.Request, Retained: true, AppliedDeadlineNS: b.DeadlineNS, Current: b}
	require.NoError(t, f.ValidateFor(q.Request, *m.PGInstance))
	return f
}
func TestClosedRearmContinuationRestoresExpiredSettledWithoutInventingReopen(t *testing.T) {
	m, b := expiredClosedRearmModel(t)
	before := m.Clone()
	n, err := continueClosedRearmForTest(m, m.Owner, m.ServiceClosure.Rearm.CommandSHA256, b)
	require.NoError(t, err, "真实已成功后再过期必须有显式闭合续接")
	require.Equal(t, before, m)
	require.Equal(t, before.ServiceClosure.Rearm, n.ServiceClosure.Rearm)
	require.Nil(t, n.ServiceClosure.Reopen)
	require.Equal(t, before.InitialHeld, n.InitialHeld)
	require.Equal(t, before.Active, n.Active)
	n, err = n.RecordServiceRearm(n.Owner, closedRearmFact(t, n))
	require.NoError(t, err)
	require.Equal(t, before.ServiceClosure.Rearm, n.ServiceClosure.Rearm)
	require.Equal(t, Settled, n.ServiceClosure.LatestRearm().Phase)
	require.Equal(t, before.HighWater, n.HighWater)
}
func TestClosedRearmContinuationRejectsStaleUnknownAndNonFreshLease(t *testing.T) {
	m, b := expiredClosedRearmModel(t)
	for name, change := range map[string]func(*control.AdmissionLeaseResponse){
		"live":                func(b *control.AdmissionLeaseResponse) { b.Expired = false },
		"unarmed":             func(b *control.AdmissionLeaseResponse) { b.Armed = false },
		"open":                func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionOpen = true },
		"same-native-owner":   func(b *control.AdmissionLeaseResponse) { b.LeaseEpoch = m.Owner.Epoch },
		"historical-deadline": func(b *control.AdmissionLeaseResponse) { b.DeadlineNS-- },
		"revision":            func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionRevision++ },
		"capacity":            func(b *control.AdmissionLeaseResponse) { b.Observed.CapacityGeneration++ },
		"wrong-floor":         func(b *control.AdmissionLeaseResponse) { b.Observed.IssuerFloorEpoch-- },
	} {
		t.Run(name, func(t *testing.T) {
			bad := cloneLeaseResponse(b)
			change(&bad)
			_, err := m.ContinueServiceRearm(m.Owner, m.ServiceClosure.Rearm.CommandSHA256, bad)
			require.Error(t, err)
		})
	}
	_, err := m.ContinueServiceRearm(m.Owner, m.ServiceClosure.CommandSHA256, b)
	require.Error(t, err)
	n, err := m.ContinueServiceRearm(m.Owner, m.ServiceClosure.Rearm.CommandSHA256, b)
	require.NoError(t, err)
	frozen := n.Clone()
	_, err = n.ContinueServiceRearm(n.Owner, m.ServiceClosure.Rearm.CommandSHA256, b)
	require.Error(t, err)
	require.Equal(t, frozen, n, "重复选择不能创建第二 CAS 或替换 Unknown")
	fact := closedRearmFact(t, n)
	fact.Retained = false
	fact.AppliedDeadlineNS = 0
	_, err = n.RecordServiceRearm(n.Owner, fact)
	require.Error(t, err)
	_, err = n.StartServiceReopen(n.Owner, m.ServiceClosure.CommandSHA256, b)
	require.Error(t, err)
}
func TestClosedRearmContinuationKeepsBoundedExactHistoryAndTamperRejection(t *testing.T) {
	m, b := expiredClosedRearmModel(t)
	original, _ := json.Marshal(m.ServiceClosure.Rearm)
	for cycle := 1; cycle <= 4; cycle++ {
		selected := m.ServiceClosure.LatestRearm().CommandSHA256
		n, err := m.ContinueServiceRearm(m.Owner, selected, b)
		require.NoError(t, err)
		require.EqualValues(t, cycle, n.ServiceClosure.RearmContinuation.Cycle)
		require.Equal(t, original, mustClosedJSON(t, n.ServiceClosure.Rearm))
		_, wire, err := encode(n, MaxEncodedBytes)
		require.NoError(t, err)
		_, _, err = encode(n, len(wire)+4096)
		require.Error(t, err, "必须预留完整 native 回执")
		n, err = decode(wire, n.VMUID, n.Incarnation)
		require.NoError(t, err)
		clone := n.Clone()
		clone.ServiceClosure.RearmContinuation.Rearm.Before.Observed.Arrays[0].CommittedUpperBytes++
		require.NotEqual(t, clone, n)
		n, err = n.RecordServiceRearm(n.Owner, closedRearmFact(t, n))
		require.NoError(t, err)
		if cycle > 1 {
			h := n.ServiceClosure.RearmContinuation.Retired
			require.NotNil(t, h)
			var old ServiceRearmContinuation
			require.NoError(t, json.Unmarshal(h.Context, &old))
			require.Nil(t, old.Retired)
			clone = n.Clone()
			clone.ServiceClosure.RearmContinuation.Retired.Context[0] = '!'
			require.Error(t, clone.Validate())
		}
		clone = n.Clone()
		clone.ServiceClosure.RearmContinuation.ProtocolVersion = 2
		require.Error(t, clone.Validate())
		clone = n.Clone()
		clone.ServiceClosure.RearmContinuation.Rearm.HeldSHA256 = strings.Repeat("0", 64)
		require.Error(t, clone.Validate())
		m, err = n.Takeover(n.Owner, "closed-owner-"+string(rune('a'+cycle)))
		require.NoError(t, err)
		b = cloneLeaseResponse(n.ServiceClosure.LatestRearm().Receipt.Current)
		b.Request = control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: 1, Action: "observe"}
		b.Observed.IssuerFloorEpoch = m.Owner.Epoch
	}
}
func mustClosedJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return raw
}
func TestClosedRearmContinuationOldRawAndStrictNestedMembers(t *testing.T) {
	m, b := expiredClosedRearmModel(t)
	raw := mustClosedJSON(t, m)
	require.NotContains(t, string(raw), "rearm_continuation")
	var old Model
	require.NoError(t, json.Unmarshal(raw, &old))
	require.Equal(t, raw, mustClosedJSON(t, old))
	n, err := m.ContinueServiceRearm(m.Owner, m.ServiceClosure.Rearm.CommandSHA256, b)
	require.NoError(t, err)
	raw = mustClosedJSON(t, n.ServiceClosure.RearmContinuation)
	for _, extra := range []string{`,"unknown":1}`, `,"cycle":"1"}`} {
		var strict ServiceRearmContinuation
		require.Error(t, json.Unmarshal(append(append([]byte(nil), raw[:len(raw)-1]...), []byte(extra)...), &strict))
	}
}
func TestClosedRearmContinuationStoreLostCASAndIdentityDrift(t *testing.T) {
	for _, mode := range []string{"lost-start", "stale-rv", "recreated-vm", "normal"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, b := expiredClosedRearmModel(t)
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(ctx)
			require.NoError(t, err)
			if mode == "stale-rv" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"update": "concurrent"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-start"
			if mode == "recreated-vm" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "replacement-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			next, ticket, err := store.ContinueServiceRearm(ctx, s, m.Owner, m.ServiceClosure.Rearm.CommandSHA256, b)
			if mode != "normal" {
				require.Error(t, err)
				require.Nil(t, ticket)
			} else {
				require.NoError(t, err)
				require.NotNil(t, next)
				require.NotNil(t, ticket)
			}
			if mode == "lost-start" {
				actual, err := store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, Unconfirmed, actual.State().ServiceClosure.LatestRearm().Phase)
			}
		})
	}
}

// 新字段不可放进再关闭的内部载体后被旧专用校验忽略。
func TestClosedRearmContinuationCannotHideInsideReopenClosing(t *testing.T) {
	m, _ := expiredReopenModel(t)
	require.NoError(t, m.Validate())
	bad := m.Clone()
	bad.ServiceClosure.Reopen.Closing.Close.RearmContinuation = &ServiceRearmContinuation{ProtocolVersion: 1}
	require.Error(t, bad.Validate())
}
