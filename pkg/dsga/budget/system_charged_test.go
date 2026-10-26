package budget

import (
	"encoding/json"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
)

func systemView() resourceguard.View {
	v := aggregateView()
	v.Identity.SystemCgroupDevice = v.Identity.CgroupDevice
	v.Identity.SystemCgroupInode = 99
	v.Bootstrap.Identity = v.Identity
	v.Birth.Identity = v.Identity
	v.Actual.Identity = v.Identity
	v.Bootstrap.Limits.SystemMemoryMaxBytes = 16 << 20
	v.SystemPlacementObserved = true
	v.Actual.System = &resourceguard.SystemEvidence{Scope: resourceguard.SystemChargedScope, MemoryMaxBytes: 16 << 20, MemoryCurrentBytes: 2 << 20, PID1Member: true, ProcessCount: 3}
	return v
}
func TestSystemChargedUsesOwnDomainOnceAndSurvivesPGResourceChange(t *testing.T) {
	v := systemView()
	s, err := SystemChargedFromView(v)
	require.NoError(t, err)
	require.Equal(t, control.Uint64(16<<20), s.Bound.UpperBytes)
	pg, err := RootAggregateFromView(v)
	require.NoError(t, err)
	require.Equal(t, control.Uint64(80<<20), pg.Bound.UpperBytes, "system cap is within excluded Outside, never added to PG aggregate")
	total, err := v.Bootstrap.Limits.TotalUpperBytes()
	require.NoError(t, err)
	require.EqualValues(t, 112<<20, total, "normal64+wholepool16+outside32, not plus system16 again")
	v.IssuerFloorEpoch = 2
	v.Actual.System.MemoryCurrentBytes++
	held := resourceguard.Command{ProtocolVersion: 1, Identity: v.Identity, IssuerEpoch: 2, Revision: 1, BudgetCommitmentID: "runtime", PostmasterPID: 123, PostmasterStartTicks: 456, MemoryMaxBytes: 96 << 20, SystemMemoryMaxBytes: 16 << 20, OutsideUpperBytes: 32 << 20, HugePools: v.Bootstrap.Limits.HugePools}
	v.Original = &held
	v.Held = &held
	v.Applied = true
	v.Actual.MemoryMaxBytes = held.MemoryMaxBytes
	require.NoError(t, s.Match(v), "PG cap/owner changed, system source remained original")
	latest, err := RootAggregateFromView(v)
	require.NoError(t, err)
	require.EqualValues(t, 112<<20, latest.Bound.UpperBytes)
	held.SystemMemoryMaxBytes = 8 << 20
	_, err = RootAggregateFromView(v)
	require.Error(t, err)
	require.Error(t, s.Match(v))
}
func TestSystemChargedRejectsUnknownPlacementDriftAndScopeTampering(t *testing.T) {
	v := systemView()
	s, err := SystemChargedFromView(v)
	require.NoError(t, err)
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	for _, change := range []func(*resourceguard.View){func(v *resourceguard.View) { v.SystemPlacementObserved = false; v.Birth = nil }, func(v *resourceguard.View) { v.Actual.System.RootUserspaceCount = 1 }, func(v *resourceguard.View) { v.Actual.System.PID1Member = false }, func(v *resourceguard.View) { v.Actual.System.MemoryMaxBytes++ }, func(v *resourceguard.View) { v.Identity.SystemCgroupInode++ }} {
		var copy resourceguard.View
		require.NoError(t, json.Unmarshal(raw, &copy))
		change(&copy)
		require.Error(t, s.Match(copy))
	}
	bad := s
	bad.Bound.UpperBytes += 16 << 20
	require.Error(t, bad.Validate(), "not whole OutsideUpper")
	encoded, err := json.Marshal(s)
	require.NoError(t, err)
	var decoded SystemCharged
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.NoError(t, decoded.Match(v))
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	for key := range fields {
		var copy map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(encoded, &copy))
		delete(copy, key)
		missing, _ := json.Marshal(copy)
		require.Error(t, json.Unmarshal(missing, &decoded), key)
	}
	_, err = SystemChargedFromView(aggregateView())
	require.Error(t, err, "legacy has no finite system source")
}
