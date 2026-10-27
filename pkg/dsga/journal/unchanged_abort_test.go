package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 原容量和NoEffect来源在这里是显式领域夹具，只验真实Model/codec的持久
// 不变量。认证旧完成+新取消及完整生产闭合另由owner组合承担。
func unchangedAbortModel(t *testing.T) (Model, budget.Finalized, budget.UnchangedAbortReceipt) {
	t.Helper()
	m, f, aggregate := committedAbortModel(t)
	m = heldFinish(t, m)
	prior := *m.BudgetBoundary
	req := testRequest(m, 2, control.Grow)
	req.TargetVMMemoryBytes = 2 << 30
	req.ExpectedCapacityGeneration = f.Shared.Observed.CapacityGeneration
	req.BudgetCommitmentID += "-new-cancel"
	m = beginModel(t, m, req)
	for _, action := range []Action{PrepareBudget, ReserveHost, ResizeVM} {
		var err error
		m, err = m.Start(m.Owner, req.Key, action, "test-domain", testCommand)
		require.NoError(t, err)
		step := m.Active.Steps[0]
		for _, s := range m.Active.Steps {
			if s.Action == action {
				step = s
			}
		}
		result := Completed
		if action == ResizeVM {
			result = NoEffect
		}
		m, err = m.Record(m.Owner, req.Key, observation(m, step, result))
		require.NoError(t, err)
	}
	require.EqualValues(t, 7, m.ProtocolVersion)
	require.Equal(t, &prior, m.Active.PriorBudgetBoundary)
	require.Nil(t, m.BudgetBoundary)
	var err error
	m, err = m.RequestAbort(m.Owner, req.Key)
	require.NoError(t, err)
	_, err = m.UnchangedAbortBudget()
	require.NoError(t, err)
	vm := m.Active.Steps[2]
	sources := *f.Sources.Runner
	sources.ReceiptClaim = budget.ObjectRef{Name: "cancelled-receipt", UID: "cancelled-uid"}
	sources.JournalSHA256 = vm.CommandSHA256
	sum := sha256.Sum256(vm.Observation.Data)
	command := budget.UnchangedAbortCommand{ProtocolVersion: 1, Request: req, FinalizationSHA256: m.BudgetHeld.Finalization.CommandSHA256, CancelledVMSHA256: hex.EncodeToString(sum[:]), CancellationSources: sources, Admission: nil}
	raw, err := json.Marshal(command)
	require.NoError(t, err)
	m, err = m.StartCleanup(m.Owner, req.Key, req.Key.Instance.ComputeInstanceID, raw)
	require.NoError(t, err)
	shared := f.Shared
	shared.Observed.AdmissionOpen = true
	shared.Observed.AdmissionRevision++
	receipt := budget.UnchangedAbortReceipt{ProtocolVersion: 1, Command: command, Query: true, Fence: control.FenceResponse{ProtocolVersion: 1, RequestedInstance: f.Request.Key.Instance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: shared.Observed, VMShrinkPermit: false}, Shared: shared, Response: nil, PGAggregate: aggregate, ResourceEpoch: m.Owner.Epoch}
	require.NoError(t, receipt.ValidateFor(command, f, m.Owner.Epoch, m.Owner.Epoch))
	return m, f, receipt
}

func TestUnchangedAbortRestoresCapturedBudgetThroughTakeoverAndHistoryExpiry(t *testing.T) {
	m, f, receipt := unchangedAbortModel(t)
	held := m.Clone().BudgetHeld
	prior := *m.Active.PriorBudgetBoundary
	cloned := m.Clone()
	cloned.Active.PriorBudgetBoundary.ThroughSequence++
	require.Equal(t, prior, *m.Active.PriorBudgetBoundary, "操作副本不能修改捕获的原关闭链")
	obs := observation(m, *m.Active.Cleanup, Completed)
	var err error
	obs.Data, err = json.Marshal(receipt)
	require.NoError(t, err)
	m, err = m.Record(m.Owner, m.Active.Request.Key, obs)
	require.NoError(t, err)
	require.NoError(t, m.CheckUnchangedAbort())
	request := m.Active.Request
	m, err = m.Finish(m.Owner, request.Key)
	require.NoError(t, err)
	require.NoError(t, m.CheckClosedBudget(f.Request))
	require.Equal(t, prior.SucceededSequence, m.BudgetBoundary.SucceededSequence)
	require.Equal(t, request.Key, m.BudgetBoundary.ThroughKey)
	require.Equal(t, held, m.BudgetHeld)
	require.Equal(t, AbortedAfterReconcile, m.History[len(m.History)-1].Outcome)
	m, err = m.Takeover(m.Owner, "next-budget-owner")
	require.NoError(t, err)
	badHistory := m.Clone()
	last := &badHistory.History[len(badHistory.History)-1]
	last.Operation.PriorBudgetBoundary.ThroughSequence = last.ClosedSequence
	require.Error(t, badHistory.Validate(), "后续序列增长不能让旧操作引用晚于自身关闭的前驱证明")
	for i := 1; i <= HistoryLimit+1; i++ {
		m = unstartedAbort(t, m, uint64(i))
	}
	require.NotZero(t, m.ExpiredResults)
	require.NoError(t, m.CheckClosedBudget(f.Request))
	_, raw, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	restored, err := decode(raw, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m, restored)
	if dir := os.Getenv("DSGA_VM_WIRE_DIR"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		wire, err := json.Marshal(restored)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "model-v7.json"), wire, 0o600))
	}
}

func TestUnchangedAbortCannotRestoreFromMissingProofOrInvalidCleanup(t *testing.T) {
	base, _, original := unchangedAbortModel(t)
	for _, mode := range []string{"missing-prior", "old-version", "wrong-prior-budget", "future-prior", "wrong-vm-receipt", "closed-gate", "wrong-source", "dummy-cleanup"} {
		t.Run(mode, func(t *testing.T) {
			m := base.Clone()
			raw, err := json.Marshal(original)
			require.NoError(t, err)
			var receipt budget.UnchangedAbortReceipt
			require.NoError(t, json.Unmarshal(raw, &receipt))
			switch mode {
			case "missing-prior":
				m.Active.PriorBudgetBoundary = nil
			case "old-version":
				m.ProtocolVersion = 6
			case "wrong-prior-budget":
				m.Active.PriorBudgetBoundary.FinalizationSHA256 = original.Command.CancelledVMSHA256
			case "future-prior":
				m.Active.PriorBudgetBoundary.ThroughSequence = m.Sequence + 1
			case "wrong-vm-receipt":
				receipt.Command.CancelledVMSHA256 = original.Command.FinalizationSHA256
			case "closed-gate":
				receipt.Shared.Observed.AdmissionOpen = false
			case "wrong-source":
				receipt.Command.CancellationSources.StateClaim.UID = "replacement-state-uid"
			}
			if mode == "old-version" || mode == "future-prior" {
				require.Error(t, m.Validate())
				return
			}
			obs := observation(m, *m.Active.Cleanup, Completed)
			obs.Data, err = json.Marshal(receipt)
			require.NoError(t, err)
			if mode == "dummy-cleanup" {
				obs.Data = testCommand
			}
			m, err = m.Record(m.Owner, m.Active.Request.Key, obs)
			require.NoError(t, err)
			require.Error(t, m.CheckUnchangedAbort())
			m, err = m.Finish(m.Owner, m.Active.Request.Key)
			require.NoError(t, err)
			require.Nil(t, m.BudgetBoundary, "通用Completed或不完整清理不能恢复旧关闭资格")
		})
	}
}
