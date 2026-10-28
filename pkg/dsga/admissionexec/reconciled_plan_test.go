package admissionexec

import (
	"encoding/json"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 固定纯领域夹具源自d99e4cb6的真实Model/codec用例，不含凭据或网络。
// 验证路径/准入授权的边界，不将静态字段称为认证PG/Root/Host或Linux效果。
func recoveryDomain(t *testing.T) (budget.Prepared, budget.RootAggregate, budget.ReconciledCapacity) {
	t.Helper()
	raw, err := os.ReadFile("testdata/reconciled-domain.json")
	require.NoError(t, err)
	var fixture struct {
		Prepared budget.Prepared           `json:"prepared"`
		Root     budget.RootAggregate      `json:"root"`
		Capacity budget.ReconciledCapacity `json:"capacity"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.NoError(t, fixture.Prepared.ValidateFor(fixture.Prepared.Request, fixture.Prepared.Identity))
	require.NoError(t, fixture.Root.Validate())
	require.NoError(t, fixture.Capacity.Validate())
	return fixture.Prepared, fixture.Root, fixture.Capacity
}

func TestReconciledEligibilityNeverCrossesAnyOriginalUnknownOrFinalization(t *testing.T) {
	var original journal.Operation
	original.AbortRequested = true
	for _, action := range []journal.Action{journal.PrepareBudget, journal.ReserveHost, journal.ResizeVM, journal.GuestReady, journal.ResizePG, journal.FinalizeBudget, journal.ResumeAdmission} {
		var step journal.Step
		step.Action, step.Phase = action, journal.Planned
		if action == journal.PrepareBudget {
			step.Phase = journal.Settled
			var observation journal.Observation
			observation.Result = journal.Completed
			step.Observation = &observation
		}
		original.Steps = append(original.Steps, step)
	}
	require.True(t, reconciledEligible(&original))
	require.False(t, reconciledEligible(nil))
	for i, step := range original.Steps {
		t.Run(string(step.Action), func(t *testing.T) {
			op := original
			op.Steps = append([]journal.Step(nil), original.Steps...)
			op.Steps[i].Phase = journal.Unconfirmed
			require.False(t, reconciledEligible(&op), "任何领域未知均须先查原命令")
			if step.Action == journal.FinalizeBudget || step.Action == journal.ResumeAdmission {
				op.Steps[i].Phase = journal.Settled
				require.False(t, reconciledEligible(&op), "已派发原最终预算/开放有独立恢复路径")
			}
		})
	}
	for _, command := range []string{`{"capacity":null}`, `{"capacity":{}}`} {
		var cleanup journal.Step
		cleanup.Command = []byte(command)
		original.Cleanup = &cleanup
		require.True(t, reconciledCleanup(&original), "只选严格解析器，不把null当有效预算")
	}
	original.Cleanup.Command = []byte(`{"request":{}}`)
	require.False(t, reconciledCleanup(&original))
}

func TestReconciledOutsideSelectsExactActualVMAndRetainsOriginalReference(t *testing.T) {
	p, _, c := recoveryDomain(t)
	var model journal.Model
	ref, err := reconciledOutside(model, p, p.Request.TargetVMMemoryBytes)
	require.NoError(t, err)
	require.Equal(t, *p.OutsideClaim, ref)
	// 后继目标更大，当前VM仍是上次实际值；只能采用永久实际预算的原资格。
	p.VMBeforeMemoryBytes = c.VMMemoryBytes
	p.Request.TargetVMMemoryBytes += 256 << 20
	p.Request.Key.Revision++
	p.Request.Key.OperationID.Low++
	var cleanup journal.Step
	cleanup.DispatchedBy = &journal.Owner{HolderID: "fixture", Epoch: c.IssuerEpoch}
	cleanup.Command, err = json.Marshal(budget.ReconciledAbortCommand{ProtocolVersion: 1, Capacity: c, PreparedSHA256: c.VM.PreparedSHA256, Admission: nil})
	require.NoError(t, err)
	var held journal.HeldCapacity
	held.Cleanup = cleanup
	model.CapacityHeld = &held
	ref, err = reconciledOutside(model, p, c.VMMemoryBytes)
	require.NoError(t, err)
	require.Equal(t, c.OutsideClaim, ref)
	_, err = reconciledOutside(model, p, c.VMMemoryBytes+1)
	require.Error(t, err, "同一资格不能按相近容量外推")
	model.CapacityHeld.Cleanup.DispatchedBy = nil
	_, err = reconciledOutside(model, p, c.VMMemoryBytes)
	require.Error(t, err, "不完整原头不能变为新资格")
}

func TestReconciledOpeningRequiresServiceSelectionAndUsesActualGeneration(t *testing.T) {
	p, _, c := recoveryDomain(t)
	var op journal.Operation
	op.Request, op.AbortRequested = p.Request, true
	shared := p.Shared
	shared.Observed.AdmissionOpen = true
	opening, err := reconciledOpening(&op, p, c, shared, c.IssuerEpoch, false)
	require.NoError(t, err)
	require.Nil(t, opening, "已经开放只能只读确认")
	shared.Observed.AdmissionOpen = false
	_, err = reconciledOpening(&op, p, c, shared, c.IssuerEpoch, false)
	require.ErrorIs(t, err, ErrRecoverySelectionRequired, "Grow没有原Quiesce，不能覆盖外部关闭")
	opening, err = reconciledOpening(&op, p, c, shared, c.IssuerEpoch, true)
	require.NoError(t, err)
	require.Equal(t, c.CapacityGeneration, opening.ExpectedCapacityGeneration)
	require.Equal(t, shared.Observed.AdmissionRevision+1, opening.Revision)
	require.Equal(t, op.Request.BudgetCommitmentID, opening.BudgetCommitmentID)
	require.Equal(t, c.IssuerEpoch, opening.IssuerEpoch)
	require.True(t, opening.Open)
	shared.Observed.AdmissionRevision = control.Uint64(math.MaxUint64)
	_, err = reconciledOpening(&op, p, c, shared, c.IssuerEpoch, true)
	require.Error(t, err, "不能回绕准入revision")
}

func TestReconciledOwnQuiesceDoesNotOverrideNewerOrChangedClosure(t *testing.T) {
	p, root, c := recoveryDomain(t)
	p.Request.Kind, p.Request.TargetBuffers = control.Shrink, p.Shared.Observed.HighBuffers/2
	p.Request.Key.Revision++
	p.Request.Key.OperationID.Low++
	p.Request.TargetVMMemoryBytes = p.VMBeforeMemoryBytes
	p.VMBeforeMemoryBytes = c.VMMemoryBytes
	p.GuestUsableMemoryBytes = 768 << 20
	p.Shared.Observed.Operation = nil
	require.NoError(t, p.ValidateFor(p.Request, p.Identity))
	// 本次Shrink尚未改PG/VM，实际容量仍由较早完成来源持有。原目标
	// 不改成实际值；完整实际合同也须有效，避免只测试一个能拼出的开门DTO。
	c.Request, c.VMBeforeMemoryBytes, c.PGOperation = p.Request, p.VMBeforeMemoryBytes, nil
	require.NoError(t, c.Validate())
	shared := p.Shared
	shared.Observed.AdmissionOpen, shared.Observed.AdmissionRevision = false, 5
	closeIntent := control.AdmissionRequest{ProtocolVersion: 1, Instance: p.Request.Key.Instance, IssuerEpoch: p.Request.Key.IssuerEpoch, Revision: 5, ExpectedCapacityGeneration: p.Request.ExpectedCapacityGeneration, Open: false, BudgetCommitmentID: p.Request.BudgetCommitmentID}
	var work control.WorkResponse
	work.ProtocolVersion, work.WorkAPIVersion = 1, 2
	work.ForegroundIdle, work.ObserverExcluded, work.Observed = true, true, shared.Observed
	var connections control.ConnectionResponse
	connections.ProtocolVersion, connections.ConnectionAPIVersion = 1, 1
	connections.Enabled, connections.Acknowledged, connections.PrivateManagementReady, connections.PublicConnectionsBlocked = true, true, true, true
	connections.RequestedSerial, connections.AppliedSerial, connections.Work = 5, 5, work
	var prepared control.PreparedResponse
	prepared.ProtocolVersion, prepared.PreparedAPIVersion = 1, 1
	prepared.PreparedSlots, prepared.MaximumPreparedSlots, prepared.RuntimePreparesBlocked, prepared.Connections = 1, 4, true, connections
	var dsm control.DSMResponse
	dsm.ProtocolVersion, dsm.DSMAPIVersion = 1, 1
	dsm.CurrentBootOnly, dsm.AccountingValid = true, true
	dsm.PageBytes, dsm.ControlUpperBytes, dsm.ReservedUpperBytes, dsm.Prepared = 4096, 4096, 4096, prepared
	dsm.ExternalSegments, dsm.UnknownSegments = 1, 1
	command := budget.QuiesceCommand{ProtocolVersion: 1, VMShrinkPermit: false, Request: p.Request, Close: closeIntent, PGAggregate: root}
	receipt := budget.Quiesced{ProtocolVersion: 1, VMShrinkPermit: false, Request: p.Request, Close: closeIntent, PGAggregate: root, DSM: dsm}
	require.NoError(t, command.ValidateFor(p, p.Request.Key.IssuerEpoch))
	require.NoError(t, receipt.ValidateFor(p, p.Request.Key.IssuerEpoch))
	var step journal.Step
	step.Action, step.Phase, step.TargetID = journal.Quiesce, journal.Settled, p.Request.Key.Instance.ComputeInstanceID
	step.DispatchedBy = &journal.Owner{HolderID: "fixture", Epoch: p.Request.Key.IssuerEpoch}
	var err error
	step.Command, err = json.Marshal(command)
	require.NoError(t, err)
	var observation journal.Observation
	observation.Result = journal.Completed
	step.Observation = &observation
	step.Observation.Data, err = json.Marshal(receipt)
	require.NoError(t, err)
	var op journal.Operation
	op.Request, op.AbortRequested, op.Steps = p.Request, true, []journal.Step{step}
	opening, err := reconciledOpening(&op, p, c, shared, c.IssuerEpoch, false)
	require.NoError(t, err, "自己的有效关闭可以恢复，不需要覆盖外部意图")
	require.NotNil(t, opening)
	for _, mode := range []string{"newer-close", "changed-budget", "changed-root", "unconfirmed", "wrong-target"} {
		t.Run(mode, func(t *testing.T) {
			current := shared
			candidate := step
			saved := command
			switch mode {
			case "newer-close":
				current.Observed.AdmissionRevision++
			case "changed-budget":
				saved.Close.BudgetCommitmentID += "-other"
			case "changed-root":
				saved.PGAggregate.Bound.SourceID += "-other"
			case "unconfirmed":
				candidate.Phase = journal.Unconfirmed
			case "wrong-target":
				candidate.TargetID += "-other"
			}
			candidate.Command, err = json.Marshal(saved)
			require.NoError(t, err)
			op.Steps = []journal.Step{candidate}
			_, err = reconciledOpening(&op, p, c, current, c.IssuerEpoch, false)
			require.ErrorIs(t, err, ErrRecoverySelectionRequired)
		})
	}
}
