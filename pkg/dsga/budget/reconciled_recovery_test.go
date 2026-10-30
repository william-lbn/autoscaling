package budget

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

func recoverySharedCopy(t *testing.T, s control.SharedCostResponse) control.SharedCostResponse {
	t.Helper()
	raw, err := json.Marshal(s)
	require.NoError(t, err)
	var owned control.SharedCostResponse
	require.NoError(t, json.Unmarshal(raw, &owned))
	return owned
}

func recoveryCapacityCopy(t *testing.T, c ReconciledCapacity) ReconciledCapacity {
	t.Helper()
	raw, err := json.Marshal(c)
	require.NoError(t, err)
	var owned ReconciledCapacity
	require.NoError(t, json.Unmarshal(raw, &owned))
	return owned
}

func recoveryOperation(t *testing.T, request control.Request, phase control.OperationPhase) *control.OperationFact {
	t.Helper()
	hash, err := request.ContextSHA256()
	require.NoError(t, err)
	return &control.OperationFact{Key: request.Key, Kind: request.Kind, Phase: phase, TargetBuffers: request.TargetBuffers, ExpectedCapacityGeneration: request.ExpectedCapacityGeneration, ContextSHA256: hash, Reason: control.ReasonNone, CancelRequested: false, CancelClosed: phase == control.Committed, Replayed: false}
}

func serviceActualInputs(t *testing.T, shrink bool) ReconcileInputs {
	t.Helper()
	i := reconciledInputs(t, shrink)
	// 旧通用aggregate夹具没有有限system上限；新的正式实际预算必须具备
	// 这项来源。只补新领域夹具，不放宽生产守卫或修改未受影响旧测试。
	i.Prepared.Limits.SystemMemoryMaxBytes = 16 << 20
	i.Root.Bootstrap.Limits.SystemMemoryMaxBytes = 16 << 20
	i.Root.Identity.SystemCgroupDevice, i.Root.Identity.SystemCgroupInode = i.Root.Identity.CgroupDevice, 31
	i.Root.Bootstrap.Identity, i.Root.Birth.Identity = i.Root.Identity, i.Root.Identity
	i.Prepared.Identity, i.Outside.Identity, i.Host.Identity = i.Root.Identity, i.Root.Identity, i.Root.Identity
	var err error
	i.Root.Bound, err = i.Root.derive()
	require.NoError(t, err)
	return i
}

// 原生来源是显式领域夹具；这里只验完整预算、串行证明和不变量，不将
// 自报Fence/Shared称为真实PG、Root或物理释放。认证Reader组合另行验证。
func serviceRecoveryInputs(t *testing.T, shrink bool, mode string) (ReconcileInputs, RecoveryPG) {
	t.Helper()
	i := serviceActualInputs(t, shrink)
	r := i.Prepared.Request
	i.Shared = recoverySharedCopy(t, i.Prepared.Shared)
	if mode != "unreceived" && mode != "denied" {
		i.Shared.Observed.CapacityGeneration++
		i.Shared.Observed.LowBuffers, i.Shared.Observed.HighBuffers = r.TargetBuffers, r.TargetBuffers
	}
	i.Shared.Observed.Operation = nil
	switch mode {
	case "committed", "denied":
		phase := control.Committed
		if mode == "denied" {
			phase = control.Denied
		}
		i.Shared.Observed.Operation = recoveryOperation(t, r, phase)
		if mode == "denied" {
			i.Shared.Observed.Operation.Reason = control.ReasonTimeout
		}
	default:
		i.IssuerEpoch++
		i.Shared.Observed.IssuerFloorEpoch = i.IssuerEpoch
		if mode == "expired" {
			next := r
			next.Key.IssuerEpoch, next.Key.Revision = i.IssuerEpoch, r.Key.Revision+1
			next.Key.OperationID.Low++
			next.Kind, next.ExpectedCapacityGeneration, next.TargetBuffers = control.Grow, i.Shared.Observed.CapacityGeneration, i.Shared.Observed.HighBuffers
			i.Shared.Observed.Operation = recoveryOperation(t, next, control.Committed)
		}
	}
	i.Shared.Observed.AdmissionOpen = false
	i.Shared.Observed.AdmissionRevision++
	require.NoError(t, i.Shared.ValidateFor(r.Key.Instance))
	status := control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: r.Key, Disposition: control.ReceiptNotFound, HasResult: false, Result: nil}, Observed: recoverySharedCopy(t, i.Shared).Observed}
	switch mode {
	case "committed", "denied":
		result := recoverySharedCopy(t, i.Shared).Observed
		status.Receipt.Disposition, status.Receipt.HasResult, status.Receipt.Result = control.ReceiptReplayed, true, &result
	case "expired":
		status.Receipt.Disposition = control.ReceiptResultExpired
	}
	proof := RecoveryPG{Fence: control.FenceResponse{ProtocolVersion: 1, RequestedInstance: r.Key.Instance, RequestedEpoch: i.IssuerEpoch, Disposition: control.Fenced, Observed: recoverySharedCopy(t, i.Shared).Observed, VMShrinkPermit: false}, Status: status}
	_, err := control.ValidateResizeRecovery(r, proof.Fence, proof.Status)
	require.NoError(t, err)
	return i, proof
}

func TestServiceActualBudgetSeparatesPGRecoveryFromOrdinaryCancellation(t *testing.T) {
	for _, shrink := range []bool{false, true} {
		for _, mode := range []string{"unreceived", "committed", "denied", "expired"} {
			t.Run(map[bool]string{false: "grow/", true: "shrink/"}[shrink]+mode, func(t *testing.T) {
				i, proof := serviceRecoveryInputs(t, shrink, mode)
				c, err := ReconcileRecovery(i, proof)
				require.NoError(t, err)
				require.EqualValues(t, 4, c.ProtocolVersion)
				require.Equal(t, i.Prepared.Request, c.Request)
				require.Nil(t, c.PGOperation)
				require.Nil(t, c.UnchangedPG)
				require.Equal(t, proof, *c.RecoveryPG)
				require.NoError(t, c.ValidateCurrent(i.Shared, i.IssuerEpoch, c.VMMemoryBytes))
				current := recoverySharedCopy(t, i.Shared)
				current.Observed.IssuerFloorEpoch++
				current.Observed.AdmissionRevision++
				current.Observed.AdmissionOpen = true
				require.NoError(t, c.ValidateCurrent(current, i.IssuerEpoch+1, c.VMMemoryBytes), "合法接管/新门禁不覆盖历史证明")
				owned := recoveryCapacityCopy(t, c)
				require.Equal(t, c, owned)
				proof.Fence.Observed.Arrays[0].CommittedUpperBytes++
				proof.Status.Observed.Arrays[0].CommittedUpperBytes++
				if proof.Status.Receipt.Result != nil {
					proof.Status.Receipt.Result.Operation.ContextSHA256 = strings.Repeat("f", 64)
				}
				require.Equal(t, owned, c, "构造器必须拥有全部原生数组/Result/Operation")
				command := ReconciledAbortCommand{ProtocolVersion: 1, Capacity: c, PreparedSHA256: strings.Repeat("c", 64), Admission: nil}
				require.Error(t, command.ValidateFor(c.IssuerEpoch), "独立服务预算不得进入普通取消合同")
				var ordinary Finalized
				raw, err := json.Marshal(c)
				require.NoError(t, err)
				require.Error(t, json.Unmarshal(raw, &ordinary))
			})
		}
	}
}

func TestServiceActualUnchangedPGPreservesUndispatchedHeadAtSameOwner(t *testing.T) {
	for _, takeover := range []bool{false, true} {
		for _, priorHead := range []bool{false, true} {
			t.Run(map[bool]string{false: "same-owner/", true: "takeover/"}[takeover]+map[bool]string{false: "no-head", true: "old-terminal"}[priorHead], func(t *testing.T) {
				i := serviceActualInputs(t, false)
				original := recoverySharedCopy(t, i.Prepared.Shared)
				if priorHead {
					prior := i.Prepared.Request
					prior.Key.IssuerEpoch--
					prior.Kind, prior.TargetBuffers = control.Grow, original.Observed.HighBuffers
					original.Observed.Operation = recoveryOperation(t, prior, control.Committed)
					i.Prepared.Shared = recoverySharedCopy(t, original)
				}
				i.Shared = recoverySharedCopy(t, original)
				if takeover {
					i.IssuerEpoch++
					i.Shared.Observed.IssuerFloorEpoch = i.IssuerEpoch
				}
				c, err := ReconcileServiceUnchanged(i, original)
				require.NoError(t, err)
				require.EqualValues(t, 4, c.ProtocolVersion)
				require.Nil(t, c.RecoveryPG)
				require.Equal(t, original, *c.UnchangedPG)
				require.Equal(t, original.Observed.Operation, c.PGOperation)
				require.NoError(t, c.ValidateCurrent(i.Shared, i.IssuerEpoch, c.VMMemoryBytes))
				owned := recoveryCapacityCopy(t, c)
				require.Equal(t, c, owned)
				original.Observed.Arrays[0].CommittedUpperBytes++
				require.Equal(t, owned, c)
				changed := recoverySharedCopy(t, i.Shared)
				next := c.Request
				next.Key.Revision++
				next.Key.OperationID.Low++
				next.ExpectedCapacityGeneration, next.TargetBuffers = c.CapacityGeneration, c.Buffers
				changed.Observed.Operation = recoveryOperation(t, next, control.Committed)
				require.Error(t, c.ValidateCurrent(changed, i.IssuerEpoch, c.VMMemoryBytes), "Planned来源不接受另一个PG head")
			})
		}
	}
}

func TestServiceActualRecoveryCurrentRequiresMonotonicCompletePGHead(t *testing.T) {
	i, proof := serviceRecoveryInputs(t, false, "expired")
	c, err := ReconcileRecovery(i, proof)
	require.NoError(t, err)
	for _, mode := range []string{"newer-terminal", "replayed", "alias", "original-alias", "old-head", "lost-head", "original-resurrected", "same-key-context", "preparing", "unknown-array", "cost-growth", "capacity", "floor"} {
		t.Run(mode, func(t *testing.T) {
			current := recoverySharedCopy(t, i.Shared)
			s := &current.Observed
			switch mode {
			case "newer-terminal":
				next := c.Request
				next.Key = s.Operation.Key
				next.Key.Revision++
				next.Key.OperationID.Low++
				next.Kind, next.ExpectedCapacityGeneration, next.TargetBuffers = control.Grow, s.CapacityGeneration, s.HighBuffers
				s.Operation = recoveryOperation(t, next, control.Committed)
			case "replayed":
				s.Operation.Replayed = !s.Operation.Replayed
			case "alias":
				s.Operation.Key.OperationID.Low++
			case "original-alias":
				s.Operation.Key = c.Request.Key
				s.Operation.Key.OperationID.Low++
			case "old-head":
				s.Operation.Key.Revision--
			case "lost-head":
				s.Operation = nil
			case "original-resurrected":
				s.Operation = recoveryOperation(t, c.Request, control.Committed)
			case "same-key-context":
				s.Operation.ContextSHA256 = strings.Repeat("f", 64)
			case "preparing":
				s.Operation.Phase, s.Operation.CancelClosed = control.Preparing, false
			case "unknown-array":
				s.Arrays[0].AllocationUnknown = true
			case "cost-growth":
				s.Arrays[0].CommittedUpperBytes++
				current.CurrentMainAndArraysUpperBytes++
			case "capacity":
				s.CapacityGeneration++
			case "floor":
				s.IssuerFloorEpoch--
			}
			err := c.ValidateCurrent(current, i.IssuerEpoch, c.VMMemoryBytes)
			if mode == "newer-terminal" || mode == "replayed" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	terminalInputs, terminal := serviceRecoveryInputs(t, false, "denied")
	known, err := ReconcileRecovery(terminalInputs, terminal)
	require.NoError(t, err)
	current := recoverySharedCopy(t, terminalInputs.Shared)
	current.Observed.Operation.Reason = control.ReasonInterrupted
	require.Error(t, known.ValidateCurrent(current, known.IssuerEpoch, known.VMMemoryBytes), "同一原Key终态不可替换")
}

func TestServiceActualBudgetRejectsProjectedPGProofAndCapacity(t *testing.T) {
	i, proof := serviceRecoveryInputs(t, true, "expired")
	c, err := ReconcileRecovery(i, proof)
	require.NoError(t, err)
	for name, change := range map[string]func(*ReconciledCapacity){
		"old-version": func(v *ReconciledCapacity) { v.ProtocolVersion = 3 },
		"no-source":   func(v *ReconciledCapacity) { v.RecoveryPG = nil },
		"mixed-source": func(v *ReconciledCapacity) {
			original := recoverySharedCopy(t, i.Prepared.Shared)
			v.UnchangedPG = &original
		},
		"mixed-head": func(v *ReconciledCapacity) { v.PGOperation = recoveryOperation(t, v.Request, control.Committed) },
		"same-owner-isolation": func(v *ReconciledCapacity) {
			v.RecoveryPG.Fence.RequestedEpoch = v.Request.Key.IssuerEpoch
			v.RecoveryPG.Fence.Observed.IssuerFloorEpoch = v.Request.Key.IssuerEpoch
			v.RecoveryPG.Status.Observed.IssuerFloorEpoch = v.Request.Key.IssuerEpoch
		},
		"future-proof":     func(v *ReconciledCapacity) { v.IssuerEpoch-- },
		"wrong-result-key": func(v *ReconciledCapacity) { v.RecoveryPG.Status.Receipt.Key.OperationID.Low++ },
		"capacity":         func(v *ReconciledCapacity) { v.CapacityGeneration++ },
		"buffers":          func(v *ReconciledCapacity) { v.Buffers++ },
		"maximum":          func(v *ReconciledCapacity) { v.MaximumBuffers++ },
		"startup":          func(v *ReconciledCapacity) { v.StartupArraysUpperBytes++ },
		"startup-overflow": func(v *ReconciledCapacity) { v.StartupArraysUpperBytes = control.Uint64(math.MaxUint64) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := recoveryCapacityCopy(t, c)
			change(&bad)
			require.Error(t, bad.Validate())
		})
	}
	unchanged := serviceActualInputs(t, false)
	original := recoverySharedCopy(t, unchanged.Prepared.Shared)
	original.Observed.Operation = recoveryOperation(t, unchanged.Prepared.Request, control.Denied)
	original.Observed.Operation.Reason = control.ReasonTimeout
	_, err = ReconcileServiceUnchanged(unchanged, original)
	require.Error(t, err, "未派发来源不得含本次原Key")
}

func TestServiceActualBudgetSeparatesOutsideFromCurrentRootDeclaration(t *testing.T) {
	i, proof := serviceRecoveryInputs(t, true, "expired")
	// Root持有小规格声明，实际VM仍大；真实Outside来源保证更大系统成本。
	// 只改变领域输入的排除预留并重建摘要，不调整任何真实资源/限额。
	i.Root.Bootstrap.Limits.OutsideUpperBytes += 1 << 20
	var err error
	i.Root.Bound, err = i.Root.derive()
	require.NoError(t, err)
	c, err := ReconcileRecovery(i, proof)
	require.NoError(t, err)
	require.Equal(t, i.Outside.SystemOutsidePG, c.SystemOutsidePG)
	_, err = Reconcile(i)
	require.Error(t, err, "旧取消仍要求原声明完全匹配")
	bad := i
	bad.Outside.SystemOutsidePG.UpperBytes = i.Root.Bootstrap.Limits.SystemMemoryMaxBytes - 1
	_, err = ReconcileRecovery(bad, proof)
	require.Error(t, err, "认证Outside仍必须覆盖当前有限system硬限")
}

func TestServiceActualInitialOutsideAndVMRemainIndependentOfPGAndRoot(t *testing.T) {
	for _, initialVM := range []bool{false, true} {
		t.Run(map[bool]string{false: "native-cancelled", true: "initial-vm"}[initialVM], func(t *testing.T) {
			i, initial := reconciledInitialInputs(t)
			original := recoverySharedCopy(t, i.Prepared.Shared)
			if initialVM {
				sources := i.VM.Sources
				var noNative NativeCapacity
				i.VM = noNative
				i.InitialVM = &InitialVMSources{Instance: initial.Instance, BirthClaim: initial.RunnerBirth, StateClaim: sources.StateClaim, IdentitySHA256: initial.RunnerIdentitySHA256, MemoryBytes: initial.VMMemoryBytes}
			}
			i.Root.Bootstrap.Limits.OutsideUpperBytes++
			var err error
			i.Root.Bound, err = i.Root.derive()
			require.NoError(t, err)
			c, err := ReconcileInitialServiceUnchanged(i, initial, original)
			require.NoError(t, err)
			require.EqualValues(t, 4, c.ProtocolVersion)
			require.Equal(t, initial, *c.InitialOutside)
			require.NotEqual(t, initial.PGAggregate, i.Root)
			raw, err := json.Marshal(c)
			require.NoError(t, err)
			require.NotContains(t, string(raw), `"outside_request"`)
			require.NotContains(t, string(raw), `"outside_claim"`)
			if initialVM {
				require.NotContains(t, string(raw), `"native_vm"`)
			}
			require.Equal(t, c, recoveryCapacityCopy(t, c))
			initial.SystemOutsidePG.UpperBytes++
			require.NotEqual(t, initial, *c.InitialOutside)
			_, err = ReconcileInitial(i, *c.InitialOutside)
			require.Error(t, err, "v2/v3不能复用已变Root")
		})
	}
}

func TestServiceActualInitialOutsideKeepsQualificationAfterPGAndGuestChanged(t *testing.T) {
	i, initial := reconciledInitialInputs(t)
	r := i.Prepared.Request
	r.Kind, r.TargetBuffers, r.TargetVMMemoryBytes = control.Shrink, initial.Shared.Observed.HighBuffers/2, initial.VMMemoryBytes/2
	i.Prepared.Request, i.Prepared.GuestUsableMemoryBytes, i.Prepared.TargetSharedUpperBytes = r, r.TargetVMMemoryBytes-(256<<20), initial.Shared.StartupMainUpperBytes
	i.Prepared.Limits.MemoryMaxBytes = 128 << 20
	i.Host.Request, i.Host.ReservedMemoryBytes = r, initial.VMMemoryBytes+i.Host.HostOverheadBytes
	i.VM.Request, i.VM.GuestSHA256 = r, strings.Repeat("d", 64)
	i.Shared = recoverySharedCopy(t, initial.Shared)
	i.Shared.Observed.AdmissionRevision++
	i.Shared.Observed.CapacityGeneration++
	i.Shared.Observed.LowBuffers, i.Shared.Observed.HighBuffers = r.TargetBuffers, r.TargetBuffers
	i.Shared.Observed.Operation = recoveryOperation(t, r, control.Committed)
	root := i.Root
	l := root.Bootstrap.Limits
	root.Held = &resourceguard.Command{ProtocolVersion: 1, Identity: root.Identity, IssuerEpoch: i.IssuerEpoch, Revision: 1, BudgetCommitmentID: r.BudgetCommitmentID, PostmasterPID: root.Birth.PostmasterPID, PostmasterStartTicks: root.Birth.PostmasterStartTicks, MemoryMaxBytes: i.Prepared.Limits.MemoryMaxBytes, PidsMaxTasks: l.PidsMaxTasks, AddressSpaceMaxBytes: l.AddressSpaceMaxBytes, SystemMemoryMaxBytes: l.SystemMemoryMaxBytes, OutsideUpperBytes: l.OutsideUpperBytes, HugePools: append([]resourceguard.HugePool(nil), l.HugePools...)}
	var err error
	root.Bound, err = root.derive()
	require.NoError(t, err)
	i.Root = root
	result := recoverySharedCopy(t, i.Shared).Observed
	proof := RecoveryPG{Fence: control.FenceResponse{ProtocolVersion: 1, RequestedInstance: r.Key.Instance, RequestedEpoch: i.IssuerEpoch, Disposition: control.Fenced, Observed: recoverySharedCopy(t, i.Shared).Observed, VMShrinkPermit: false}, Status: control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: r.Key, Disposition: control.ReceiptReplayed, HasResult: true, Result: &result}, Observed: recoverySharedCopy(t, i.Shared).Observed}}
	c, err := ReconcileInitialRecovery(i, initial, proof)
	require.NoError(t, err)
	require.EqualValues(t, 4, c.ProtocolVersion)
	require.Equal(t, initial, *c.InitialOutside)
	require.Equal(t, initial.VMMemoryBytes, c.VMMemoryBytes)
	require.Greater(t, c.VMMemoryBytes, c.Request.TargetVMMemoryBytes)
	require.Greater(t, c.CapacityGeneration, initial.Shared.Observed.CapacityGeneration)
	digest, err := RootDigest(initial.PGAggregate)
	require.NoError(t, err)
	require.NotEqual(t, c.RootSHA256, digest)
	require.Equal(t, c, recoveryCapacityCopy(t, c))
	_, err = ReconcileInitial(i, initial)
	require.Error(t, err, "旧完整初始来源不得被新PG/Guest状态冒用")
}

func TestServiceActualBudgetStrictWirePreservesLegacyAndBounds(t *testing.T) {
	i, proof := serviceRecoveryInputs(t, false, "expired")
	c, err := ReconcileRecovery(i, proof)
	require.NoError(t, err)
	raw, err := json.Marshal(c)
	require.NoError(t, err)
	for _, bad := range []string{
		string(raw[:len(raw)-1]) + `,"recovery_pg":null}`,
		string(raw[:len(raw)-1]) + `,"implicit_recovery":true}`,
		strings.Replace(string(raw), `"recovery_pg":`, `"Recovery_PG":`, 1),
		strings.Replace(string(raw), c.ResidualProfileID, strings.Repeat("x", maxRecoveryCapacityBytes), 1),
	} {
		var rejected ReconciledCapacity
		require.Error(t, json.Unmarshal([]byte(bad), &rejected))
	}
	legacy, err := Reconcile(reconciledInputs(t, false))
	require.NoError(t, err)
	raw, err = json.Marshal(legacy)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "recovery_pg")
	require.NotContains(t, string(raw), "unchanged_pg")
}
