package budget

import (
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
)

func settlementPrepared(t *testing.T) (Prepared, RootAggregate) {
	t.Helper()
	raw, err := os.ReadFile("../../api/dsga/testdata/request-v1.json")
	require.NoError(t, err)
	var req control.Request
	require.NoError(t, json.Unmarshal(raw, &req))
	raw, err = os.ReadFile("../../api/dsga/testdata/response-v1.json")
	require.NoError(t, err)
	var fact control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &fact))
	s := fact.Observed
	s.Operation = nil
	s.CapacityGeneration = req.ExpectedCapacityGeneration
	s.LowBuffers = 512
	s.HighBuffers = 512
	shared := control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: 16 << 20, StartupArraysUpperBytes: 16 << 20, CurrentMainAndArraysUpperBytes: 80 << 20, Observed: s}
	id := resourceguard.Identity{VMUID: req.Key.Instance.VMUID, ComputeInstanceID: req.Key.Instance.ComputeInstanceID, GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
	b := func(id string, n control.Uint64) Bound {
		return Bound{Kind: CommittedUpper, SourceID: id, UpperBytes: n}
	}
	p := Prepared{ProtocolVersion: 1, Request: req, Identity: id, RunnerPodName: "runner", VMBeforeMemoryBytes: 512 << 20, GuestUsableMemoryBytes: 768 << 20, Shared: shared, TargetSharedUpperBytes: 144 << 20, Limits: resourceguard.Limits{MemoryMaxBytes: 512 << 20, OutsideUpperBytes: 32 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 8}, {PageSizeKB: 1048576, Pages: 0}}}, SystemOutsidePG: b("system", 24<<20), Transition: b("transition", 4<<20), Safety: b("safety", 4<<20)}
	require.NoError(t, p.ValidateFor(req, id))
	v := aggregateView()
	v.Identity = id
	v.Bootstrap.Identity = id
	v.Birth.Identity = id
	v.Actual.Identity = id
	v.Bootstrap.Limits = p.Limits
	v.Actual.MemoryMaxBytes = p.Limits.MemoryMaxBytes
	v.Actual.GuestMemTotalBytes = 768 << 20
	a, err := RootAggregateFromView(v)
	require.NoError(t, err)
	return p, a
}
func TestSettlementClaimsKeepLifetimeIdentityAndHostOverhead(t *testing.T) {
	p, a := settlementPrepared(t)
	inc := "1234567890abcdef1234567890abcdef"
	outside := OutsideReservation{ProtocolVersion: 1, Request: p.Request, Identity: p.Identity, Incarnation: inc, ResidualProfileID: "qualified-profile", SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true}
	require.NoError(t, outside.ValidateFor(p, inc))
	badOutside := outside
	badOutside.HoldUntilExplicitSettlement = false
	require.Error(t, badOutside.ValidateFor(p, inc))
	badOutside = outside
	badOutside.SystemOutsidePG.UpperBytes++
	require.Error(t, badOutside.ValidateFor(p, inc))
	host := HostReservation{ProtocolVersion: 1, Request: p.Request, Identity: p.Identity, Incarnation: inc, SourceID: "host-original", RunnerPodName: p.RunnerPodName, RunnerPodUID: "runner-uid", NodeName: "node", NodeUID: "node-uid", HostOverheadBytes: 4 << 20, ReservedMemoryBytes: p.Request.TargetVMMemoryBytes + (4 << 20), HoldUntilExplicitSettlement: true}
	require.NoError(t, host.ValidateFor(p, inc))
	for name, change := range map[string]func(*HostReservation){"no-overhead": func(h *HostReservation) { h.HostOverheadBytes = 0 }, "guest-only": func(h *HostReservation) { h.ReservedMemoryBytes = p.Request.TargetVMMemoryBytes }, "overflow": func(h *HostReservation) { h.HostOverheadBytes = math.MaxUint64 }, "new-runner": func(h *HostReservation) { h.RunnerPodName = "replacement" }, "release-on-expiry": func(h *HostReservation) { h.HoldUntilExplicitSettlement = false }} {
		t.Run(name, func(t *testing.T) { bad := host; change(&bad); require.Error(t, bad.ValidateFor(p, inc)) })
	}
	vm := VMSettlement{ProtocolVersion: 1, Request: p.Request, Identity: p.Identity, HostClaim: ObjectRef{Name: "host-original", UID: "host-uid"}, Host: host, RunnerFenceEpoch: p.Request.Key.IssuerEpoch, RunnerCommandID: "original-command", GuestUsableMemoryBytes: p.GuestUsableMemoryBytes, ActualMemoryBytes: p.Request.TargetVMMemoryBytes, QMPObservedMemoryBytes: p.Request.TargetVMMemoryBytes, NoPendingCommands: true, MigrationBlocked: true}
	require.NoError(t, vm.ValidateFor(p, a, inc), "grow VM occurs before future GuestReady source exists")
	bad := vm
	bad.PGResourceSourceID = a.Bound.SourceID
	require.Error(t, bad.ValidateFor(p, a, inc), "grow cannot manufacture future resource source")
	p.Request.Kind = control.Shrink
	p.Request.TargetBuffers = 512
	p.Shared.Observed.LowBuffers = 1024
	p.Shared.Observed.HighBuffers = 1024
	p.VMBeforeMemoryBytes = 2 << 30
	require.NoError(t, p.ValidateFor(p.Request, p.Identity))
	vm.Request = p.Request
	vm.Host.Request = p.Request
	require.Error(t, vm.ValidateFor(p, a, inc), "shrink requires completed pre-VM resource source")
	vm.PGResourceSourceID = a.Bound.SourceID
	require.NoError(t, vm.ValidateFor(p, a, inc))
	vm.QMPObservedMemoryBytes--
	require.Error(t, vm.ValidateFor(p, a, inc), "status equality alone cannot replace QMP result")
}
func TestSettlementStrictSourcesPreserveLegacyAndRejectPartialClaims(t *testing.T) {
	p, _ := settlementPrepared(t)
	raw, err := json.Marshal(p)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "outside_claim")
	p.OutsideClaim = &ObjectRef{Name: "outside-original", UID: ""}
	require.Error(t, p.ValidateFor(p.Request, p.Identity))
	p.OutsideClaim.UID = "outside-uid"
	require.NoError(t, p.ValidateFor(p.Request, p.Identity))
	refs := SettlementSources{OutsideClaim: *p.OutsideClaim, HostClaim: ObjectRef{Name: "host-original", UID: "host-uid"}, VMReceiptSHA256: strings.Repeat("a", 64)}
	require.NoError(t, refs.Validate())
	refs.VMReceiptSHA256 = strings.Repeat("a", 63)
	require.Error(t, refs.Validate())
	out := OutsideReservation{ProtocolVersion: 1, Request: p.Request, Identity: p.Identity, Incarnation: "1234567890abcdef1234567890abcdef", ResidualProfileID: "qualified-profile", SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true}
	encoded, err := json.Marshal(out)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	for key := range fields {
		var copy map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(encoded, &copy))
		delete(copy, key)
		missing, _ := json.Marshal(copy)
		var decoded OutsideReservation
		require.Error(t, json.Unmarshal(missing, &decoded), key)
	}
	duplicate := append([]byte(`{"protocol_version":1,`), encoded[1:]...)
	var decoded OutsideReservation
	require.Error(t, json.Unmarshal(duplicate, &decoded))
}
