package journal

import (
	"encoding/json"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

// 复用已有的独立关闭和原始首次开放 Unknown。硬预算档案只允许同 boot、
// 同写者及同预算读回，不借用有限租期的 deadline 或退休原未知命令。
func TestEnforcedBudgetRecoveryUsesOriginalClosureAndBoundPG(t *testing.T) {
	m, legacy := reopenModel(t, "initial-unknown")
	bound := *m.PGInstance
	capacity, release := uint32(16), false
	caps := control.Capabilities{ProtocolVersion: 1, Supported: true, Enabled: true, InstanceBound: true,
		AdmissionPolicy: control.AdmissionPolicyEnforcedBudget, BufferAPIVersion: 6, HistoryCapacity: &capacity, ProviderCanRelease: &release,
		Instance: &control.DiscoveryIdentity{VMUID: bound.VMUID, ComputeInstanceID: bound.ComputeInstanceID, PGBootID: &bound.PGBootID}}
	read := func(pg control.Snapshot) control.ServiceAdmission {
		return control.ServiceAdmission{Budget: &control.BudgetAdmissionRead{Capabilities: caps, Memory: control.MemoryResponse{ProtocolVersion: 1, Observed: pg}}}
	}
	_, _, _, selector := m.ServiceClosure.Selection()
	before := read(legacy.Observed)
	started, err := m.StartServiceReopenObserved(m.Owner, selector, before)
	require.NoError(t, err)
	require.Nil(t, started.ServiceClosure.Rearm)
	require.Nil(t, started.ServiceClosure.Reopen.Before.Timed)
	_, wire, err := encode(started, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, started, loaded)
	raw, err := json.Marshal(loaded.ServiceClosure.Reopen.Before)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "deadline_ns")
	require.Contains(t, string(raw), "enforced_budget_v1")
	var oldReader control.AdmissionLeaseResponse
	require.Error(t, json.Unmarshal(raw, &oldReader))

	// 实际状态一旦改变，不能把旧关闭记录用于同一个新开放。
	for _, field := range []string{"boot", "policy", "floor", "generation", "revision", "open"} {
		t.Run(field, func(t *testing.T) {
			bad := cloneServiceAdmission(before)
			switch field {
			case "boot":
				*bad.Budget.Capabilities.Instance.PGBootID++
			case "policy":
				bad.Budget.Capabilities.AdmissionPolicy = control.AdmissionPolicyLease
			case "floor":
				bad.Budget.Memory.Observed.IssuerFloorEpoch++
			case "generation":
				bad.Budget.Memory.Observed.CapacityGeneration++
			case "revision":
				bad.Budget.Memory.Observed.AdmissionRevision++
			case "open":
				bad.Budget.Memory.Observed.AdmissionOpen = true
			}
			_, err := m.StartServiceReopenObserved(m.Owner, selector, bad)
			require.Error(t, err)
		})
	}
	applied := reopenFact(t, loaded)
	settled, err := loaded.RecordServiceReopen(m.Owner, applied, false)
	require.NoError(t, err)
	selector = settled.ServiceClosure.Reopen.CommandSHA256
	oldLive := legacy
	oldLive.Observed = applied.Observed
	_, err = settled.CompleteServiceReopen(m.Owner, selector, oldLive)
	require.Error(t, err, "同 boot 不能混入另一策略的服务完成记录")
	completed, err := settled.CompleteServiceReopenObserved(m.Owner, selector, read(applied.Observed))
	require.NoError(t, err)
	require.Equal(t, "serving", completed.ServiceLifecycle.SafetyStage)
	require.Nil(t, completed.ServiceClosure)
	require.Equal(t, m.InitialHeld.Admission, completed.InitialHeld.Admission)
	require.Equal(t, m.HighWater, completed.HighWater)
	_, wire, err = encode(completed, MaxEncodedBytes)
	require.NoError(t, err)
	again, err := decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, completed, again)

	// 再次发生真实漂移并已保护关闭时，上一轮的策略仍属于该 boot。
	nextClose := m.ServiceClosure.Request
	nextClose.IssuerEpoch = completed.Owner.Epoch
	nextClose.Revision = applied.Observed.AdmissionRevision + 1
	closing, err := completed.StartServiceClosure(completed.Owner, nextClose)
	require.NoError(t, err)
	digest, err := nextClose.ContextSHA256()
	require.NoError(t, err)
	closedPG := applied.Observed
	closedPG.AdmissionOpen, closedPG.AdmissionRevision = false, nextClose.Revision
	closed, err := closing.RecordServiceClosure(closing.Owner, control.AdmissionResponse{
		ProtocolVersion: 1, Disposition: control.AdmissionApplied, ContextSHA256: digest, Request: nextClose, Observed: closedPG,
	}, false)
	require.NoError(t, err)
	oldLive.Observed = closedPG
	_, err = closed.StartServiceReopen(closed.Owner, closed.ServiceClosure.CommandSHA256, oldLive)
	require.Error(t, err, "首次写前就应拒绝同 boot 混用策略")
	_, err = closed.StartServiceReopenObserved(closed.Owner, closed.ServiceClosure.CommandSHA256, read(closedPG))
	require.NoError(t, err)
}
