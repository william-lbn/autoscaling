package journal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 本组只验证真正Model/codec的持久协议，不认证平台来源。原容量域仍是显式
// 协议夹具；实际Agent/Runner/来源重读另由owner组合覆盖，不能外推Linux事实。
func committedAbortModel(t *testing.T) (Model, budget.Finalized, budget.RootAggregate) {
	t.Helper()
	m, obs := heldStart(t, heldBoundModel(t, testModel(t)), 1, runnerSources())
	step := &m.Active.Steps[int(obs.Ordinal)-1]
	var command budget.FinalizationCommand
	require.NoError(t, json.Unmarshal(step.Command, &command))
	id := *m.ResourceIdentity
	v := resourceguard.View{
		ProtocolVersion: 1, Identity: id, IssuerFloorEpoch: m.Owner.Epoch,
		Bootstrap: resourceguard.BootstrapCommand{ProtocolVersion: 1, Identity: id, IssuerEpoch: m.Owner.Epoch, BudgetCommitmentID: "fixture-bootstrap", Limits: resourceguard.Limits{MemoryMaxBytes: 512 << 20, OutsideUpperBytes: 32 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 0}}, PidsMaxTasks: 0, SystemMemoryMaxBytes: 0}}, BootstrapApplied: true,
		Birth:          &resourceguard.ProcessIdentity{ProtocolVersion: 1, Identity: id, PostmasterPID: 123, PostmasterStartTicks: 456},
		Actual:         resourceguard.Evidence{ProtocolVersion: 1, Identity: id, MemoryMaxBytes: 512 << 20, GuestMemTotalBytes: 768 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 0}}, HugeOvercommitDisabled: true, CgroupPopulated: true, VMShrinkPermit: false, MemoryCurrentBytes: 0, SwapMaxBytes: 0, SwapCurrentBytes: 0, WholePoolUpperBytes: 0, HugeSurplusPages: 0, TaskLimit: nil, System: nil},
		VMShrinkPermit: false, SystemPlacementObserved: false, Original: nil, Held: nil, Applied: false,
	}
	a, err := budget.RootAggregateFromView(v)
	require.NoError(t, err)
	command.Finalized.Aggregate = &a.Bound
	// 修改的是尚未执行的测试来源并重算真实命令摘要，生产没有改写WAL入口。
	step.Command, err = json.Marshal(command)
	require.NoError(t, err)
	step.CommandSHA256, err = commandDigest(m.Active.RequestSHA256, *step)
	require.NoError(t, err)
	obs.CommandSHA256 = step.CommandSHA256
	obs.Data, err = json.Marshal(command.Finalized)
	require.NoError(t, err)
	m, err = m.Record(m.Owner, obs.Key, obs)
	require.NoError(t, err)
	return m, command.Finalized, a
}

func committedAbortAudit(t *testing.T, m Model, f budget.Finalized, a budget.RootAggregate, query bool) budget.CommittedAbortReceipt {
	t.Helper()
	var command budget.CommittedAbortCommand
	require.NoError(t, json.Unmarshal(m.Active.Cleanup.Command, &command))
	shared := f.Shared
	shared.Observed.IssuerFloorEpoch = m.Owner.Epoch
	shared.Observed.AdmissionOpen = true
	shared.Observed.AdmissionRevision = command.Admission.Revision
	hash, err := command.Admission.ContextSHA256()
	require.NoError(t, err)
	disposition := control.AdmissionApplied
	if query {
		disposition = control.AdmissionReplayed
	}
	return budget.CommittedAbortReceipt{
		ProtocolVersion: 1, Command: command, Query: query,
		Fence:  control.FenceResponse{ProtocolVersion: 1, RequestedInstance: f.Request.Key.Instance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: shared.Observed, VMShrinkPermit: false},
		Shared: shared, Response: control.AdmissionResponse{ProtocolVersion: 1, Request: command.Admission, ContextSHA256: hash, Disposition: disposition, Observed: shared.Observed, VMShrinkPermit: false}, PGAggregate: a, ResourceEpoch: m.Owner.Epoch,
	}
}

func startCommittedAbort(t *testing.T, m Model, f budget.Finalized, priorOpen bool) Model {
	t.Helper()
	open := control.AdmissionRequest{ProtocolVersion: 1, Instance: f.Request.Key.Instance, IssuerEpoch: m.Owner.Epoch, Revision: f.Shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: f.Shared.Observed.CapacityGeneration, Open: true, BudgetCommitmentID: f.Request.BudgetCommitmentID}
	var err error
	if priorOpen {
		raw, marshalErr := json.Marshal(open)
		require.NoError(t, marshalErr)
		m, err = m.Start(m.Owner, f.Request.Key, ResumeAdmission, f.Request.Key.Instance.ComputeInstanceID, raw)
		require.NoError(t, err)
		m, err = m.Record(m.Owner, f.Request.Key, observation(m, m.Active.Steps[len(m.Active.Steps)-1], Completed))
		require.NoError(t, err)
	}
	m, err = m.RequestAbort(m.Owner, f.Request.Key)
	require.NoError(t, err)
	command := budget.CommittedAbortCommand{ProtocolVersion: 1, Request: f.Request, FinalizationSHA256: m.BudgetHeld.Finalization.CommandSHA256, Admission: open, MaySend: !priorOpen}
	raw, err := json.Marshal(command)
	require.NoError(t, err)
	m, err = m.StartCleanup(m.Owner, f.Request.Key, f.Request.Key.Instance.ComputeInstanceID, raw)
	require.NoError(t, err)
	return m
}

func TestCommittedAbortClosesActualCapacityAndSurvivesExpiredHistory(t *testing.T) {
	for _, mode := range []string{"before-open", "already-open", "takeover-query"} {
		t.Run(mode, func(t *testing.T) {
			m, f, a := committedAbortModel(t)
			m = startCommittedAbort(t, m, f, mode == "already-open")
			held := m.Clone().BudgetHeld
			original := cloneStep(*m.Active.Cleanup)
			var err error
			if mode == "takeover-query" {
				m, err = m.Takeover(m.Owner, "next-owner")
				require.NoError(t, err)
			}
			receipt := committedAbortAudit(t, m, f, a, mode != "before-open")
			require.NoError(t, receipt.ValidateFor(receipt.Command, f, original.DispatchedBy.Epoch, m.Owner.Epoch))
			obs := observation(m, *m.Active.Cleanup, Completed)
			obs.Data, err = json.Marshal(receipt)
			require.NoError(t, err)
			m, err = m.Record(m.Owner, f.Request.Key, obs)
			require.NoError(t, err)
			require.NoError(t, m.CheckCommittedAbort())
			m, err = m.Finish(m.Owner, f.Request.Key)
			require.NoError(t, err)
			require.EqualValues(t, 6, m.ProtocolVersion)
			require.Equal(t, AbortedAfterReconcile, m.History[len(m.History)-1].Outcome)
			require.Equal(t, held, m.BudgetHeld, "业务取消保留真实成功目标的原完整预算")
			require.NoError(t, m.CheckClosedBudget(f.Request))
			for i := 1; i <= HistoryLimit+1; i++ {
				m = unstartedAbort(t, m, uint64(i+1))
			}
			require.NotZero(t, m.ExpiredResults)
			require.NoError(t, m.CheckClosedBudget(f.Request))
			_, encoded, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			restored, err := decode(encoded, m.VMUID, m.Incarnation)
			require.NoError(t, err)
			require.Equal(t, m, restored)
			require.NoError(t, restored.CheckClosedBudget(f.Request))
			if dir := os.Getenv("DSGA_VM_WIRE_DIR"); dir != "" && mode == "before-open" {
				require.NoError(t, os.MkdirAll(dir, 0o700))
				raw, err := json.Marshal(restored)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(dir, "model-v6.json"), raw, 0o600))
			}
		})
	}
}

func TestCommittedAbortRejectsBorrowedBudgetPartialAndSupersededAdmission(t *testing.T) {
	base, f, a := committedAbortModel(t)
	base = startCommittedAbort(t, base, f, false)
	for _, mode := range []string{"wrong-budget", "wrong-finalization", "wrong-floor", "old-gate", "closed-gate", "changed-root", "rebound-setter", "partial-capacity", "dummy-cleanup"} {
		t.Run(mode, func(t *testing.T) {
			m := base.Clone()
			receipt := committedAbortAudit(t, m, f, a, false)
			switch mode {
			case "wrong-budget":
				receipt.Command.Admission.BudgetCommitmentID += "-another"
			case "wrong-finalization":
				receipt.Command.FinalizationSHA256 = "not-original"
			case "wrong-floor":
				receipt.ResourceEpoch++
			case "old-gate":
				receipt.Shared.Observed.AdmissionRevision++
			case "closed-gate":
				receipt.Shared.Observed.AdmissionOpen = false
			case "changed-root":
				receipt.PGAggregate.Bound.SourceID += "-other"
			case "rebound-setter":
				var err error
				m, err = m.Takeover(m.Owner, "next-owner")
				require.NoError(t, err)
				receipt = committedAbortAudit(t, m, f, a, false)
			case "partial-capacity":
				m.Active.Steps[0].Observation.Result = NoEffect
			}
			obs := observation(m, *m.Active.Cleanup, Completed)
			var err error
			obs.Data, err = json.Marshal(receipt)
			require.NoError(t, err)
			if mode == "dummy-cleanup" {
				obs.Data = testCommand
			}
			m, err = m.Record(m.Owner, f.Request.Key, obs)
			require.NoError(t, err, "低层通用领域记录不负责网络认证")
			require.Error(t, m.CheckCommittedAbort())
			m, err = m.Finish(m.Owner, f.Request.Key)
			require.NoError(t, err)
			require.Nil(t, m.BudgetBoundary, "通用/错误清理不能取得Host容量结清资格")
			require.Error(t, m.CheckClosedBudget(f.Request))
		})
	}
}
