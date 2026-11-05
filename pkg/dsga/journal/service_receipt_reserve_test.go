package journal

import (
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"math"
	"reflect"
	"strings"
	"testing"
)

// DTO新增字段不能静默使用不完整上界；本锁属于codec测试，不是wire版本。
func TestServiceReceiptReserveCoversCompleteCanonicalTypes(t *testing.T) {
	shapes := map[reflect.Type]string{
		reflect.TypeOf(control.Instance{}):                      "VMUID ComputeInstanceID PGBootID",
		reflect.TypeOf(control.Key{}):                           "Instance IssuerEpoch Revision OperationID",
		reflect.TypeOf(control.OperationID{}):                   "High Low",
		reflect.TypeOf(control.ProviderFact{}):                  "HasResult Status SystemErrno RequestedOffset RequestedBytes AffectedOffset AffectedBytes PhysicalReleaseVerified AllocationMayBePartial ReleaseMayBePartial",
		reflect.TypeOf(control.ArrayFact{}):                     "Array CommittedUpperBytes AllocationUnknown ReleaseState ReleaseKey ReleaseCapacityGeneration Provider",
		reflect.TypeOf(control.OperationFact{}):                 "Key Kind Phase Reason TargetBuffers ExpectedCapacityGeneration ContextSHA256 CancelRequested CancelClosed Replayed",
		reflect.TypeOf(control.Snapshot{}):                      "Instance IssuerFloorEpoch CapacityGeneration LowBuffers HighBuffers MaximumBuffers AdmissionOpen AdmissionRevision StartupSharedUpperBytes Operation Arrays",
		reflect.TypeOf(control.AdmissionRequest{}):              "ProtocolVersion Instance IssuerEpoch Revision ExpectedCapacityGeneration Open BudgetCommitmentID",
		reflect.TypeOf(control.AdmissionResponse{}):             "ProtocolVersion VMShrinkPermit Request Disposition ContextSHA256 Observed",
		reflect.TypeOf(control.AdmissionLeaseRequest{}):         "ProtocolVersion Instance IssuerEpoch ExpectedCapacityGeneration ExpectedAdmissionRevision ExpectedDeadlineNS Action",
		reflect.TypeOf(control.AdmissionLeaseResponse{}):        "ProtocolVersion VMShrinkPermit Request Renewed Armed Expired DeadlineNS LeaseEpoch TimeoutMS Observed",
		reflect.TypeOf(control.AdmissionRearmStatus{}):          "ProtocolVersion VMShrinkPermit Request Retained AppliedDeadlineNS Current",
		reflect.TypeOf(control.AdmissionClosure{}):              "ProtocolVersion VMShrinkPermit Original ClosedBy PriorMayHaveApplied",
		reflect.TypeOf(control.AdmissionContinuationRequest{}):  "ProtocolVersion Original PreviousClose Close",
		reflect.TypeOf(control.AdmissionContinuationResponse{}): "ProtocolVersion VMShrinkPermit Request ContextSHA256 ClosedBy PriorMayHaveApplied",
	}
	for typ, expected := range shapes {
		var names []string
		for i := 0; i < typ.NumField(); i++ {
			names = append(names, typ.Field(i).Name)
		}
		require.Equal(t, expected, strings.Join(names, " "), typ.Name())
	}
	instance := control.Instance{VMUID: strings.Repeat("&", 128), ComputeInstanceID: strings.Repeat("<", 128), PGBootID: control.Uint64(math.MaxUint64)}
	require.NoError(t, instance.Validate())
	open, lease, close, cont, rearm := maximumServiceReceipts(instance, strings.Repeat(">", 128))
	require.Len(t, open.Observed.Arrays, 4)
	require.NotNil(t, open.Observed.Operation)
	for _, a := range open.Observed.Arrays {
		require.NotNil(t, a.ReleaseKey)
		require.Equal(t, instance, a.ReleaseKey.Instance)
	}
	for _, v := range []any{open, lease, close, cont, rearm} {
		raw, err := json.Marshal(v)
		require.NoError(t, err)
		window, err := serviceReceiptWindow(v)
		require.NoError(t, err)
		require.Greater(t, window, len(raw)+4096, "base64/zlib膨胀不能漏算")
	}
	raw, err := json.Marshal(cont)
	require.NoError(t, err)
	require.Contains(t, string(raw), strings.Repeat("\\u0026", 128))
	rawOpen, err := json.Marshal(open)
	require.NoError(t, err)
	require.Greater(t, len(raw), len(rawOpen))
}

// 在已绑定capacity/floor/revision下填满仍可独立达到上界的合法Native字段。
func maximalLegalServiceSnapshot(t *testing.T, s control.Snapshot) control.Snapshot {
	t.Helper()
	u := control.Uint64(math.MaxUint64)
	key := control.Key{Instance: s.Instance, IssuerEpoch: s.IssuerFloorEpoch, Revision: u, OperationID: control.OperationID{High: u, Low: u}}
	s.MaximumBuffers = uint32(math.MaxInt32 / 2)
	s.LowBuffers = s.HighBuffers
	s.StartupSharedUpperBytes = u
	s.Operation = &control.OperationFact{Key: key, Kind: control.Shrink, Phase: control.Denied, Reason: control.ReasonPopulateFailed,
		TargetBuffers: s.HighBuffers, ExpectedCapacityGeneration: s.CapacityGeneration, ContextSHA256: strings.Repeat("f", 64)}
	for i := range s.Arrays {
		k := key
		s.Arrays[i] = control.ArrayFact{Array: s.Arrays[i].Array, CommittedUpperBytes: u, AllocationUnknown: true,
			ReleaseState: control.ReleaseWaitCheckpoint, ReleaseKey: &k, ReleaseCapacityGeneration: s.CapacityGeneration,
			Provider: control.ProviderFact{Status: control.ProviderUnknown, SystemErrno: math.MaxInt32,
				RequestedOffset: u / 2, RequestedBytes: u / 2, AffectedOffset: u / 2, AffectedBytes: u / 2,
				AllocationMayBePartial: true, ReleaseMayBePartial: true}}
	}
	require.NoError(t, s.Validate())
	return s
}

func TestServiceReceiptReserveMaximumLegalTailKeepsAllFourSlots(t *testing.T) {
	m, before := reopenModel(t, "initial-unknown")
	before.Observed = maximalLegalServiceSnapshot(t, before.Observed)
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err := m.StartServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	windows, err := serviceReceiptWindows(m)
	require.NoError(t, err)
	require.Positive(t, windows.open)
	require.Positive(t, windows.before)
	require.Positive(t, windows.close)
	check := func(m Model) {
		t.Helper()
		_, wire, err := encode(m, MaxEncodedBytes)
		require.NoError(t, err)
		require.LessOrEqual(t, len(wire), MaxEncodedBytes)
		loaded, err := decode(wire, m.VMUID, m.Incarnation)
		require.NoError(t, err)
		require.Equal(t, m, loaded)
	}
	check(m)
	fact := reopenFact(t, m)
	fact.Observed = maximalLegalServiceSnapshot(t, fact.Observed)
	m, err = m.RecordServiceReopen(m.Owner, fact, false)
	require.NoError(t, err)
	check(m)
	before.Observed = fact.Observed
	m, err = m.StartServiceReopenClosure(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, before)
	require.NoError(t, err)
	check(m)
	// 更高Owner有界接续；原首次关闭未知和开放回执始终保留。
	m, err = m.Takeover(m.Owner, "receipt-reserve-higher-owner")
	require.NoError(t, err)
	before.Request.IssuerEpoch = m.Owner.Epoch
	before.Observed.IssuerFloorEpoch = m.Owner.Epoch
	before.Observed.AdmissionOpen = false
	before.Observed.AdmissionRevision = m.ServiceClosure.Reopen.Closing.Close.Request.Revision
	before.Observed = maximalLegalServiceSnapshot(t, before.Observed)
	m, err = m.ContinueServiceReopenClosure(m.Owner, m.ServiceClosure.Reopen.Closing.Close.CommandSHA256, before)
	require.NoError(t, err)
	check(m)
	final := reopenContinuationFact(t, m)
	final.ClosedBy.Observed = maximalLegalServiceSnapshot(t, final.ClosedBy.Observed)
	m, err = m.RecordServiceReopenContinuation(m.Owner, final, false)
	require.NoError(t, err)
	check(m)
	require.NotNil(t, m.ServiceClosure.Reopen.Receipt)
	require.NotNil(t, m.ServiceClosure.Reopen.Closing.LatestBefore)
	require.NotNil(t, m.ServiceClosure.Reopen.Closing.Close.Continuation.Receipt)
	require.Equal(t, Unconfirmed, m.ServiceClosure.Reopen.Closing.Close.Phase)
	require.Equal(t, Unconfirmed, m.InitialHeld.Admission.Phase)
}

func TestServiceReceiptReserveBoundariesRemainFailClosed(t *testing.T) {
	m, b := reopenModel(t, "initial-unknown")
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err := m.StartServiceReopen(m.Owner, selector, b)
	require.NoError(t, err)
	original := m.Clone()
	w, err := serviceReceiptWindows(m)
	require.NoError(t, err)
	for _, budget := range []int{0, 1, w.open + 2*w.before + w.close, MaxEncodedBytes + 1} {
		_, _, err := encode(m, budget)
		require.Error(t, err)
		require.Equal(t, original, m, "拒绝不能清Unknown或原预算")
	}
	bad := m.Clone()
	bad.ServiceClosure.Reopen.Request.BudgetCommitmentID = "another-context"
	_, _, err = encode(bad, MaxEncodedBytes)
	require.Error(t, err)
	require.NotPanics(t, func() {
		bad := m.Clone()
		bad.PGInstance = nil
		_, _, err := encode(bad, MaxEncodedBytes)
		require.Error(t, err)
	})
}
