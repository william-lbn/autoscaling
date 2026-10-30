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

// 现有领域数据仍作为显式输入，但永久头通过完整公开Model流程生成。
// 避免用零Model+一段Cleanup绕过CurrentCapacity的整模校验，也不复制一份
// 大型静态账本。这里只验证原引用选择，不将领域回执称为网络来源认证。
func retainedRecoveryModel(t *testing.T, p budget.Prepared, root budget.RootAggregate, capacity budget.ReconciledCapacity) journal.Model {
	t.Helper()
	// 原文件来自该原owner的完整命令摘要，不能为复用数据换holder后
	// 仍沿用旧VM来源SHA；这里逐字复现领域夹具的原派发身份。
	m, err := journal.New(p.Identity.VMUID, "1234567890abcdef1234567890abcdef", "process-a")
	require.NoError(t, err)
	m, err = m.BindResource(m.Owner, p.Identity)
	require.NoError(t, err)
	m, err = m.BindPG(m.Owner, p.Request.Key.Instance)
	require.NoError(t, err)
	m, disposition, err := m.Begin(m.Owner, p.Request, 0)
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	host := budget.HostReservation{ProtocolVersion: 2, VMShrinkPermit: false, Request: p.Request, Identity: p.Identity, Incarnation: m.Incarnation, SourceID: capacity.HostReservationID, RunnerPodName: p.RunnerPodName, RunnerPodUID: "runner-uid", NodeName: "node", NodeUID: "node-uid", ReservedMemoryBytes: max(p.VMBeforeMemoryBytes, p.Request.TargetVMMemoryBytes) + (64 << 20), HostOverheadBytes: 64 << 20, HoldUntilExplicitSettlement: true}
	record := func(action journal.Action, target string, body any, cleanup bool) {
		var step journal.Step
		if cleanup {
			raw, err := json.Marshal(body)
			require.NoError(t, err)
			m, err = m.StartCleanup(m.Owner, p.Request.Key, target, raw)
			require.NoError(t, err)
			step = *m.Active.Cleanup
		} else {
			m, err = m.Start(m.Owner, p.Request.Key, action, target, []byte(`{"model_test":true}`))
			require.NoError(t, err)
			for _, candidate := range m.Active.Steps {
				if candidate.Action == action {
					step = candidate
				}
			}
		}
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		if cleanup {
			command, ok := body.(budget.ReconciledAbortCommand)
			require.True(t, ok)
			receipt := budget.ReconciledAbortReceipt{ProtocolVersion: 1, Command: command, Query: true, Fence: control.FenceResponse{ProtocolVersion: 1, RequestedInstance: p.Request.Key.Instance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: p.Shared.Observed, VMShrinkPermit: false}, Shared: p.Shared, Response: nil, PGAggregate: root, ResourceEpoch: m.Owner.Epoch}
			raw, err = json.Marshal(receipt)
			require.NoError(t, err)
		}
		m, err = m.Record(m.Owner, p.Request.Key, journal.Observation{Key: p.Request.Key, RequestSHA256: m.Active.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: step.TargetID, Data: raw})
		require.NoError(t, err)
	}
	record(journal.PrepareBudget, p.Request.Key.Instance.ComputeInstanceID, p, false)
	record(journal.ReserveHost, host.SourceID, host, false)
	record(journal.ResizeVM, host.RunnerPodUID, map[string]bool{"explicit_native_capacity_domain_fixture": true}, false)
	for _, step := range m.Active.Steps {
		if step.Action == journal.ResizeVM {
			require.Equal(t, capacity.VM.Sources.JournalSHA256, step.CommandSHA256)
		}
	}
	m, err = m.RequestAbort(m.Owner, p.Request.Key)
	require.NoError(t, err)
	record(journal.AbortReconcile, p.Request.Key.Instance.ComputeInstanceID, budget.ReconciledAbortCommand{ProtocolVersion: 1, Capacity: capacity, PreparedSHA256: capacity.VM.PreparedSHA256, Admission: nil}, true)
	m, err = m.Finish(m.Owner, p.Request.Key)
	require.NoError(t, err)
	require.NoError(t, m.Validate())
	return m
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
	p, root, c := recoveryDomain(t)
	var model journal.Model
	ref, err := reconciledOutside(model, p, p.Request.TargetVMMemoryBytes)
	require.NoError(t, err)
	require.Equal(t, *p.OutsideClaim, ref)
	complete := retainedRecoveryModel(t, p, root, c)
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
	require.Error(t, err, "只有Cleanup片段而无完整Model不能生成永久来源")
	ref, err = reconciledOutside(complete, p, c.VMMemoryBytes)
	require.NoError(t, err)
	require.Equal(t, c.OutsideClaim, ref)
	_, err = reconciledOutside(complete, p, c.VMMemoryBytes+1)
	require.Error(t, err, "同一资格不能按相近容量外推")
	complete.CapacityHeld.Cleanup.DispatchedBy = nil
	_, err = reconciledOutside(complete, p, c.VMMemoryBytes)
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
