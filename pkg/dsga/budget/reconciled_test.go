package budget

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 只验证预算合同/去重，平台/原生动作均为明确领域输入。完整生产者还须认证
// 原UID/RV与当前PG/Root/Runner；本fixture不是已完成Linux或Agent组合的证据。
func reconciledInputs(t *testing.T, shrink bool) ReconcileInputs {
	t.Helper()
	f, _, old := committedAbortFixture(t)
	r := f.Request
	if shrink {
		r.Kind, r.TargetBuffers = control.Shrink, 256
	}
	before := f.Shared
	before.Observed.Operation = nil
	before.Observed.CapacityGeneration = r.ExpectedCapacityGeneration
	before.Observed.LowBuffers, before.Observed.HighBuffers = 512, 512
	current := before
	if shrink {
		op := *f.Shared.Observed.Operation
		op.Kind, op.TargetBuffers = r.Kind, r.TargetBuffers
		var err error
		op.ContextSHA256, err = r.ContextSHA256()
		require.NoError(t, err)
		current.Observed.Operation = &op
		current.Observed.CapacityGeneration++
		current.Observed.LowBuffers, current.Observed.HighBuffers = r.TargetBuffers, r.TargetBuffers
	}
	v := aggregateView()
	v.Identity = old.PGAggregate.Identity
	v.Bootstrap.Identity, v.Actual.Identity, v.Birth.Identity = v.Identity, v.Identity, v.Identity
	v.Bootstrap.IssuerEpoch, v.IssuerFloorEpoch = r.Key.IssuerEpoch, r.Key.IssuerEpoch
	v.Bootstrap.Limits.MemoryMaxBytes, v.Actual.MemoryMaxBytes = 256<<20, 256<<20
	v.Actual.GuestMemTotalBytes = 768 << 20
	if shrink {
		v.Actual.GuestMemTotalBytes = (2 << 30) - (256 << 20)
	}
	root, err := RootAggregateFromView(v)
	require.NoError(t, err)
	outsideRef := ObjectRef{Name: "outside-selected", UID: "outside-uid"}
	p := Prepared{ProtocolVersion: 1, VMShrinkPermit: false, Request: r, Identity: root.Identity, RunnerPodName: "runner", RunnerRestartCount: 0, VMBeforeMemoryBytes: 512 << 20, GuestUsableMemoryBytes: 768 << 20, Shared: before, TargetSharedUpperBytes: 144 << 20, Limits: v.Bootstrap.Limits, SystemOutsidePG: f.SystemOutsidePG, Transition: f.Transition, Safety: f.Safety, OutsideClaim: &outsideRef}
	if shrink {
		p.VMBeforeMemoryBytes, p.TargetSharedUpperBytes = 2<<30, 80<<20
	}
	out := OutsideReservation{ProtocolVersion: 1, VMShrinkPermit: false, Request: r, Identity: p.Identity, Incarnation: "0123456789abcdef0123456789abcdef", ResidualProfileID: "explicit-qualified-fixture", SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true}
	if shrink {
		out.Request.Key.Revision--
		out.Request.Key.OperationID.Low--
		out.Request.Kind, out.Request.TargetVMMemoryBytes = control.Grow, p.VMBeforeMemoryBytes
	}
	host := HostReservation{ProtocolVersion: 2, VMShrinkPermit: false, Request: r, Identity: p.Identity, Incarnation: out.Incarnation, SourceID: "host-original", RunnerPodName: p.RunnerPodName, RunnerPodUID: "runner-uid", NodeName: "node", NodeUID: "node-uid", ReservedMemoryBytes: max(p.VMBeforeMemoryBytes, r.TargetVMMemoryBytes) + (64 << 20), HostOverheadBytes: 64 << 20, HoldUntilExplicitSettlement: true}
	sources := RunnerSources{BirthClaim: ObjectRef{Name: "birth", UID: "birth-uid"}, StateClaim: ObjectRef{Name: "state", UID: "state-uid"}, ReceiptClaim: ObjectRef{Name: "receipt", UID: "receipt-uid"}, CommandSHA256: strings.Repeat("a", 64), JournalSHA256: strings.Repeat("b", 64)}
	vm := NativeCapacity{Sources: sources, Request: r, BeforeBytes: p.VMBeforeMemoryBytes, Cancelled: shrink, PreparedSHA256: strings.Repeat("c", 64), GuestSHA256: "", HostClaim: ObjectRef{Name: "host", UID: "host-uid"}}
	if shrink {
		vm.GuestSHA256 = strings.Repeat("d", 64)
	}
	return ReconcileInputs{Prepared: p, Shared: current, Root: root, Outside: out, OutsideClaim: outsideRef, Host: host, HostClaim: vm.HostClaim, VM: vm, GuestUsableMemoryBytes: vm.MemoryBytes() - (256 << 20), IssuerEpoch: r.Key.IssuerEpoch}
}

func TestReconciledBudgetRetainsActualCapacityInsteadOfOriginalTarget(t *testing.T) {
	for _, shrink := range []bool{false, true} {
		name := "VM-grown-PG-unchanged"
		if shrink {
			name = "PG-shrunk-VM-cancelled"
		}
		t.Run(name, func(t *testing.T) {
			i := reconciledInputs(t, shrink)
			c, err := Reconcile(i)
			require.NoError(t, err)
			require.Equal(t, i.Prepared.Request, c.Request, "原用户目标不可被实际容量覆盖")
			if shrink {
				require.Greater(t, c.VMMemoryBytes, c.Request.TargetVMMemoryBytes)
				require.Equal(t, c.Request.TargetBuffers, c.Buffers)
			} else {
				require.Equal(t, c.Request.TargetVMMemoryBytes, c.VMMemoryBytes)
				require.Less(t, c.Buffers, c.Request.TargetBuffers)
			}
			require.NoError(t, c.ValidateCurrent(i.Shared, i.IssuerEpoch, c.VMMemoryBytes))
			raw, err := json.Marshal(c)
			require.NoError(t, err)
			var restored ReconciledCapacity
			require.NoError(t, json.Unmarshal(raw, &restored))
			require.Equal(t, c, restored)
			var ordinary Finalized
			require.Error(t, json.Unmarshal(raw, &ordinary), "部分容量合同不能被普通目标成功读者误读")
			if c.PGOperation != nil {
				i.Shared.Observed.Operation.TargetBuffers++
				require.NotEqual(t, i.Shared.Observed.Operation, c.PGOperation, "纯函数不可保留调用方可变指针")
			}
			for _, what := range []string{"VM-drift", "new-generation", "new-PG-operation", "unknown-allocation", "floor"} {
				t.Run(what, func(t *testing.T) {
					original := reconciledInputs(t, shrink)
					native := original.Shared
					memory, epoch := c.VMMemoryBytes, c.IssuerEpoch
					switch what {
					case "VM-drift":
						memory += 256 << 20
					case "new-generation":
						native.Observed.CapacityGeneration++
					case "new-PG-operation":
						if native.Observed.Operation == nil {
							native.Observed.Operation = new(control.OperationFact)
						} else {
							native.Observed.Operation.Key.OperationID.Low++
						}
					case "unknown-allocation":
						native.Observed.Arrays[0].AllocationUnknown = true
					case "floor":
						epoch++
					}
					require.Error(t, c.ValidateCurrent(native, epoch, memory))
				})
			}
		})
	}
}

func TestReconciledBudgetRejectsUnqualifiedOrIncompleteInputs(t *testing.T) {
	for _, what := range []string{"small-VM-outside", "short-Host-peak", "unknown-array", "partial-Root", "different-birth", "overflow", "same-source", "oversized-PG", "wrong-generation", "prepare-direction", "PG-prefix-without-generation", "future-VM", "future-outside"} {
		t.Run(what, func(t *testing.T) {
			i := reconciledInputs(t, true)
			switch what {
			case "small-VM-outside":
				i.Outside.Request = i.Prepared.Request
			case "short-Host-peak":
				i.Host.ReservedMemoryBytes = i.Prepared.Request.TargetVMMemoryBytes + i.Host.HostOverheadBytes
			case "unknown-array":
				i.Shared.Observed.Arrays[0].AllocationUnknown = true
			case "partial-Root":
				i.Root.Bound.UpperBytes--
			case "different-birth":
				i.Root.Birth.PostmasterStartTicks++
			case "overflow":
				i.Outside.Safety.UpperBytes = control.Uint64(math.MaxUint64)
			case "same-source":
				i.Outside.Safety.SourceID = i.Outside.SystemOutsidePG.SourceID
			case "oversized-PG":
				i.GuestUsableMemoryBytes = 64 << 20
			case "wrong-generation":
				i.Shared.Observed.CapacityGeneration++
			case "prepare-direction":
				i.Prepared.Request.TargetVMMemoryBytes = i.Prepared.VMBeforeMemoryBytes + (256 << 20)
			case "PG-prefix-without-generation":
				i.Shared.Observed.CapacityGeneration = i.Prepared.Request.ExpectedCapacityGeneration
			case "future-VM":
				i.VM.Request.Key.Revision++
			case "future-outside":
				i.Outside.Request.Key = i.Prepared.Request.Key
				i.Outside.Request.Key.OperationID.Low++
			}
			_, err := Reconcile(i)
			require.Error(t, err)
		})
	}
}

func TestReconciledAggregateDoesNotChargeHugePoolToNormalAgain(t *testing.T) {
	i := reconciledInputs(t, false)
	v := aggregateView()
	v.Identity = i.Root.Identity
	v.Bootstrap.Identity, v.Actual.Identity, v.Birth.Identity = v.Identity, v.Identity, v.Identity
	v.Bootstrap.IssuerEpoch, v.IssuerFloorEpoch = i.IssuerEpoch, i.IssuerEpoch
	v.Actual.GuestMemTotalBytes = i.GuestUsableMemoryBytes
	var err error
	i.Root, err = RootAggregateFromView(v)
	require.NoError(t, err)
	require.Greater(t, i.Shared.CurrentMainAndArraysUpperBytes, v.Bootstrap.Limits.MemoryMaxBytes)
	c, err := Reconcile(i)
	require.NoError(t, err, "normal64+整池16已覆盖Shared80，不能重复计费")
	require.Equal(t, control.Uint64(80<<20), c.PGAggregate.UpperBytes)
}

func reconciledAudit(t *testing.T, reopen bool) (ReconciledAbortCommand, ReconciledAbortReceipt) {
	t.Helper()
	i := reconciledInputs(t, false)
	c, err := Reconcile(i)
	require.NoError(t, err)
	command := ReconciledAbortCommand{ProtocolVersion: 1, Capacity: c, PreparedSHA256: c.VM.PreparedSHA256, Admission: nil}
	shared := i.Shared
	shared.Observed.AdmissionOpen = true
	shared.Observed.AdmissionRevision++
	r := ReconciledAbortReceipt{ProtocolVersion: 1, Command: command, Query: true, Fence: control.FenceResponse{ProtocolVersion: 1, RequestedInstance: c.Request.Key.Instance, RequestedEpoch: c.IssuerEpoch, Disposition: control.Fenced, Observed: shared.Observed, VMShrinkPermit: false}, Shared: shared, Response: nil, PGAggregate: i.Root, ResourceEpoch: c.IssuerEpoch}
	if reopen {
		a := control.AdmissionRequest{ProtocolVersion: 1, Instance: c.Request.Key.Instance, IssuerEpoch: c.IssuerEpoch, Revision: shared.Observed.AdmissionRevision, ExpectedCapacityGeneration: c.CapacityGeneration, Open: true, BudgetCommitmentID: c.Request.BudgetCommitmentID}
		command.Admission = &a
		r.Command, r.Query = command, false
		hash, err := a.ContextSHA256()
		require.NoError(t, err)
		r.Response = &control.AdmissionResponse{ProtocolVersion: 1, Request: a, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: shared.Observed, VMShrinkPermit: false}
	}
	require.NoError(t, r.ValidateFor(command, c.IssuerEpoch, c.IssuerEpoch))
	return command, r
}

func TestReconciledAbortRequiresOriginalActualBudgetAndStrictWire(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		c, r := reconciledAudit(t, reopen)
		for _, value := range []any{c, r, c.Capacity, c.Capacity.VM} {
			raw, err := json.Marshal(value)
			require.NoError(t, err)
			decode := func(body []byte) error {
				switch value.(type) {
				case ReconciledAbortCommand:
					var v ReconciledAbortCommand
					return json.Unmarshal(body, &v)
				case ReconciledAbortReceipt:
					var v ReconciledAbortReceipt
					return json.Unmarshal(body, &v)
				case ReconciledCapacity:
					var v ReconciledCapacity
					return json.Unmarshal(body, &v)
				default:
					var v NativeCapacity
					return json.Unmarshal(body, &v)
				}
			}
			require.NoError(t, decode(raw))
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &fields))
			for field := range fields {
				var missing map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(raw, &missing))
				delete(missing, field)
				body, err := json.Marshal(missing)
				require.NoError(t, err)
				require.Error(t, decode(body), field)
			}
			require.Error(t, decode(append([]byte(`{"unexpected":true,`), raw[1:]...)))
			for field, body := range fields {
				name, err := json.Marshal(field)
				require.NoError(t, err)
				prefix := append([]byte{'{'}, name...)
				prefix = append(prefix, ':')
				prefix = append(prefix, body...)
				prefix = append(prefix, ',')
				require.Error(t, decode(append(prefix, raw[1:]...)), "重复字段也必须拒绝")
				break
			}
		}
		wire, err := json.Marshal(r)
		require.NoError(t, err)
		for _, what := range []string{"closed", "new-actual-generation", "different-root", "unknown-opening", "superseded-opening", "invented-write"} {
			var bad ReconciledAbortReceipt
			require.NoError(t, json.Unmarshal(wire, &bad))
			switch what {
			case "closed":
				bad.Shared.Observed.AdmissionOpen = false
			case "new-actual-generation":
				bad.Shared.Observed.CapacityGeneration++
			case "different-root":
				bad.PGAggregate.Bound.SourceID += "-replacement"
			case "unknown-opening":
				if bad.Response != nil {
					bad.Response.Disposition = control.AdmissionNotFound
				} else {
					bad.Query = false
				}
			case "superseded-opening":
				bad.Shared.Observed.AdmissionRevision++
			case "invented-write":
				if bad.Command.Admission != nil {
					bad.Command.Admission.ExpectedCapacityGeneration++
				} else {
					bad.Response = new(control.AdmissionResponse)
				}
			}
			if what == "superseded-opening" && !reopen {
				continue
			}
			require.Error(t, bad.ValidateFor(bad.Command, c.Capacity.IssuerEpoch, c.Capacity.IssuerEpoch), what)
		}
	}
}
