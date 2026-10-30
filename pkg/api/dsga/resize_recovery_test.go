package dsga

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func resizeRecoveryFixture(t *testing.T, mode string) (Request, FenceResponse, ControlResponse) {
	t.Helper()
	_, request := goldenRequest(t)
	_, status := goldenResponse(t)
	if mode == "denied" {
		for _, s := range []*Snapshot{&status.Observed, status.Receipt.Result} {
			s.CapacityGeneration = request.ExpectedCapacityGeneration
			s.Operation.Phase, s.Operation.Reason = Denied, ReasonTimeout
		}
	}
	if mode == "isolated-old" || mode == "isolated-new" || mode == "expired" {
		status.Receipt.Disposition, status.Receipt.HasResult, status.Receipt.Result = ReceiptStale, false, nil
		status.Observed.Operation = nil
		status.Observed.IssuerFloorEpoch++
		if mode != "isolated-new" {
			status.Observed.CapacityGeneration = request.ExpectedCapacityGeneration
		}
		if mode == "expired" {
			status.Receipt.Disposition = ReceiptResultExpired
		}
	}
	fence := FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: request.Key.Instance, RequestedEpoch: status.Observed.IssuerFloorEpoch, Disposition: Fenced, Observed: status.Observed}
	return request, fence, status
}

func TestResizeRecoveryMaximumGenerationCannotWrapToNextCapacity(t *testing.T) {
	request, fence, status := resizeRecoveryFixture(t, "isolated-old")
	request.ExpectedCapacityGeneration = Uint64(math.MaxUint64)
	fence.Observed.CapacityGeneration, status.Observed.CapacityGeneration = request.ExpectedCapacityGeneration, request.ExpectedCapacityGeneration
	effect, err := ValidateResizeRecovery(request, fence, status)
	require.NoError(t, err)
	require.Equal(t, ResizeRecoveredIsolated, effect)
	for _, generation := range []Uint64{0, 1} {
		fence.Observed.CapacityGeneration, status.Observed.CapacityGeneration = generation, generation
		_, err = ValidateResizeRecovery(request, fence, status)
		require.Error(t, err, "原代次耗尽不能回绕为下一代")
	}
}

func TestResizeRecoverySeparatesOriginalTerminalFromUnknownIsolation(t *testing.T) {
	for mode, expected := range map[string]ResizeRecoveryEffect{"committed": ResizeRecoveredCommitted, "denied": ResizeRecoveredDenied, "isolated-old": ResizeRecoveredIsolated, "isolated-new": ResizeRecoveredIsolated, "expired": ResizeRecoveredIsolated} {
		t.Run(mode, func(t *testing.T) {
			request, fence, status := resizeRecoveryFixture(t, mode)
			effect, err := ValidateResizeRecovery(request, fence, status)
			require.NoError(t, err)
			require.Equal(t, expected, effect)
			if expected == ResizeRecoveredIsolated {
				require.Nil(t, status.Receipt.Result, "独立隔离不能制造原生Denied/Committed")
			}
		})
	}
}

func TestResizeRecoveryRejectsUnserializedOrContradictoryCapacity(t *testing.T) {
	for _, mode := range []string{"same-owner", "pending", "floor", "boot", "capacity-race", "capacity-jump", "prefix-race", "unstable", "wrong-target", "original-present", "preparing", "conflict", "request-context", "terminal-preparing", "terminal-capacity"} {
		t.Run(mode, func(t *testing.T) {
			request, fence, status := resizeRecoveryFixture(t, "isolated-new")
			switch mode {
			case "same-owner":
				fence.RequestedEpoch, fence.Observed.IssuerFloorEpoch, status.Observed.IssuerFloorEpoch = request.Key.IssuerEpoch, request.Key.IssuerEpoch, request.Key.IssuerEpoch
			case "pending":
				fence.Disposition = FencePending
			case "floor":
				status.Observed.IssuerFloorEpoch++
			case "boot":
				status.Observed.Instance.PGBootID++
			case "capacity-race":
				fence.Observed.CapacityGeneration--
			case "capacity-jump":
				fence.Observed.CapacityGeneration++
				status.Observed.CapacityGeneration++
			case "prefix-race":
				fence.Observed.LowBuffers--
				fence.Observed.HighBuffers--
			case "unstable":
				fence.Observed.LowBuffers--
				status.Observed.LowBuffers--
			case "wrong-target":
				request.TargetBuffers++
			case "original-present", "preparing":
				_, native := goldenResponse(t)
				status.Observed.Operation = native.Observed.Operation
				fence.Observed.Operation = native.Observed.Operation
				if mode == "preparing" {
					status.Observed.Operation.Phase = Preparing
					status.Observed.Operation.CancelClosed = false
					status.Observed.CapacityGeneration = request.ExpectedCapacityGeneration
					fence.Observed.CapacityGeneration = request.ExpectedCapacityGeneration
				}
			case "conflict":
				status.Receipt.Disposition = ReceiptConflict
			case "request-context":
				request, fence, status = resizeRecoveryFixture(t, "committed")
				request.BudgetCommitmentID += "-other"
			case "terminal-preparing":
				request, fence, status = resizeRecoveryFixture(t, "committed")
				status.Receipt.Result.Operation.Phase, status.Receipt.Result.Operation.CancelClosed = Preparing, false
				status.Receipt.Result.CapacityGeneration = request.ExpectedCapacityGeneration
			case "terminal-capacity":
				request, fence, status = resizeRecoveryFixture(t, "denied")
				status.Observed.CapacityGeneration++
				fence.Observed.CapacityGeneration++
				status.Observed.Operation, fence.Observed.Operation = nil, nil
			}
			_, err := ValidateResizeRecovery(request, fence, status)
			require.Error(t, err)
		})
	}
}
