package budget

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// 预算/原生结果均为明确领域夹具；这里只证明原初始来源的合同和算术，
// 不证明真实资格、cgroup或VM效果。生产组合另外核完整UID/WAL/Runner。
func reconciledInitialInputs(t *testing.T) (ReconcileInputs, Initial) {
	t.Helper()
	i, _ := initialFixture(t)
	p, _ := settlementPrepared(t)
	r := p.Request
	r.Key.Instance, r.Key.IssuerEpoch = i.Instance, i.IssuerEpoch
	r.ExpectedCapacityGeneration, r.TargetBuffers, r.TargetVMMemoryBytes = 1, 1024, 2<<30
	shared := i.Shared
	shared.Observed.AdmissionOpen, shared.Observed.AdmissionRevision = true, 1
	upper, err := GrowSharedUpper(shared, r.TargetBuffers)
	require.NoError(t, err)
	p = Prepared{ProtocolVersion: 1, VMShrinkPermit: false, Request: r, Identity: i.PGAggregate.Identity, RunnerPodName: i.RunnerPodName, RunnerRestartCount: i.RunnerRestartCount, VMBeforeMemoryBytes: i.VMMemoryBytes, GuestUsableMemoryBytes: (2 << 30) - (256 << 20), Shared: shared, TargetSharedUpperBytes: upper, Limits: i.PGAggregate.Bootstrap.Limits, SystemOutsidePG: i.SystemOutsidePG, Transition: i.Transition, Safety: i.Safety, OutsideClaim: nil}
	hostRef := ObjectRef{Name: "real-cancelled-host", UID: "real-cancelled-host-uid"}
	host := HostReservation{ProtocolVersion: 2, VMShrinkPermit: false, Request: r, Identity: p.Identity, Incarnation: i.Incarnation, SourceID: "original-host-peak", RunnerPodName: i.RunnerPodName, RunnerPodUID: i.RunnerPodUID, NodeName: i.NodeName, NodeUID: i.NodeUID, ReservedMemoryBytes: r.TargetVMMemoryBytes + i.HostOverheadBytes, HostOverheadBytes: i.HostOverheadBytes, HoldUntilExplicitSettlement: true}
	native := NativeCapacity{Sources: RunnerSources{BirthClaim: i.RunnerBirth, StateClaim: ObjectRef{Name: "real-state", UID: "real-state-uid"}, ReceiptClaim: ObjectRef{Name: "real-cancelled-receipt", UID: "real-cancelled-receipt-uid"}, CommandSHA256: strings.Repeat("a", 64), JournalSHA256: strings.Repeat("b", 64)}, Request: r, BeforeBytes: i.VMMemoryBytes, Cancelled: true, PreparedSHA256: strings.Repeat("c", 64), GuestSHA256: "", HostClaim: hostRef}
	var noOutside OutsideReservation
	var noRef ObjectRef
	return ReconcileInputs{Prepared: p, Shared: shared, Root: i.PGAggregate, Outside: noOutside, OutsideClaim: noRef, Host: host, HostClaim: hostRef, VM: native, GuestUsableMemoryBytes: i.GuestUsableMemoryBytes, IssuerEpoch: i.IssuerEpoch}, i
}

func TestReconciledInitialOutsidePreservesOriginalWithoutSyntheticRequest(t *testing.T) {
	inputs, initial := reconciledInitialInputs(t)
	c, err := ReconcileInitial(inputs, initial)
	require.NoError(t, err)
	require.EqualValues(t, 2, c.ProtocolVersion)
	require.Equal(t, inputs.Prepared.Request, c.Request)
	require.Less(t, c.VMMemoryBytes, c.Request.TargetVMMemoryBytes)
	require.Empty(t, c.OutsideClaim)
	require.Empty(t, c.OutsideRequest)
	require.Equal(t, initial, *c.InitialOutside)
	require.NoError(t, c.ValidateCurrent(inputs.Shared, inputs.IssuerEpoch, initial.VMMemoryBytes))
	raw, err := json.Marshal(c)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "outside_request")
	require.NotContains(t, string(raw), "outside_claim")
	var restored ReconciledCapacity
	require.NoError(t, json.Unmarshal(raw, &restored))
	require.Equal(t, c, restored)
	initial.PGAggregate.Bootstrap.Limits.HugePools[0].Pages++
	require.NotEqual(t, initial, *c.InitialOutside, "原Initial的slice不能由调用方随后修改")
	// 原v1线格式仍省略新字段，不将历史Outside投影为初始来源。
	legacy, err := Reconcile(reconciledInputs(t, false))
	require.NoError(t, err)
	raw, err = json.Marshal(legacy)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "initial_outside")
}

func TestReconciledInitialOutsideRejectsProjectionAndChangedOriginal(t *testing.T) {
	inputs, initial := reconciledInitialInputs(t)
	c, err := ReconcileInitial(inputs, initial)
	require.NoError(t, err)
	raw, err := json.Marshal(c)
	require.NoError(t, err)
	for _, what := range []string{"legacy-protocol", "missing-initial", "mixed-outside", "VM-drift", "Guest-too-small", "changed-root", "different-birth", "changed-generation", "invented-before"} {
		t.Run(what, func(t *testing.T) {
			var next ReconciledCapacity
			require.NoError(t, json.Unmarshal(raw, &next))
			switch what {
			case "legacy-protocol":
				next.ProtocolVersion = 1
			case "missing-initial":
				next.InitialOutside = nil
			case "mixed-outside":
				next.OutsideRequest = next.Request
			case "VM-drift":
				next.VMMemoryBytes = next.Request.TargetVMMemoryBytes
			case "Guest-too-small":
				next.GuestUsableMemoryBytes--
			case "changed-root":
				next.RootSHA256 = strings.Repeat("f", 64)
			case "different-birth":
				next.VM.Sources.BirthClaim.UID = "replacement-uid"
			case "changed-generation":
				next.CapacityGeneration++
			case "invented-before":
				next.VMBeforeMemoryBytes--
			}
			require.Error(t, next.Validate())
		})
	}
	inputs.OutsideClaim = ObjectRef{Name: "unrelated", UID: "unrelated-uid"}
	_, err = ReconcileInitial(inputs, initial)
	require.Error(t, err)
}
