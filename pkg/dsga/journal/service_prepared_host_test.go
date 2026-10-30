package journal

import (
	"encoding/json"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/stretchr/testify/require"
)

// 本文件仅为Journal领域合同单元：完整Prepared/PG/Root/Runner/Node均为
// 显式输入。Model的真实Start/Record/Close/Reopen/Complete/历史淘汰实际
// 执行；Node网络认证与真实Prepare生产者由owner的协议E2E另外验证。
func completedRetainedFixture(t *testing.T, m Model, shared control.SharedCostResponse, root budget.RootAggregate, continuation ...bool) (Model, ServiceRetainedRecovery) {
	t.Helper()
	source, err := m.CurrentRetainedSource()
	require.NoError(t, err)
	require.NotNil(t, source.Initial)
	initial := *source.Initial
	revision := uint64(1)
	if m.HighWater != nil {
		revision = uint64(m.HighWater.Revision) + 1
	}
	request := testRequest(m, revision, control.Grow)
	request.Key.Instance = *m.PGInstance
	request.TargetBuffers, request.TargetVMMemoryBytes = 1024, 2<<30
	request.ExpectedCapacityGeneration = shared.Observed.CapacityGeneration
	request.BudgetCommitmentID = "completed-prepare-original-target"
	m = beginModel(t, m, request)
	quote, err := budget.GrowSharedUpper(shared, request.TargetBuffers)
	require.NoError(t, err)
	outside := budget.ObjectRef{Name: "prepared-original-outside", UID: "prepared-original-outside-uid"}
	prepared := budget.Prepared{ProtocolVersion: 1, Request: request, Identity: root.Identity,
		RunnerPodName: initial.RunnerPodName, RunnerRestartCount: initial.RunnerRestartCount,
		VMBeforeMemoryBytes: initial.VMMemoryBytes, GuestUsableMemoryBytes: (2 << 30) - (256 << 20),
		Shared: shared, TargetSharedUpperBytes: quote, Limits: root.Bootstrap.Limits,
		SystemOutsidePG: initial.SystemOutsidePG, Transition: initial.Transition, Safety: initial.Safety, OutsideClaim: &outside}
	require.NoError(t, prepared.ValidateFor(request, *m.ResourceIdentity))
	m, err = m.Start(m.Owner, request.Key, PrepareBudget, request.Key.Instance.ComputeInstanceID, testCommand)
	require.NoError(t, err)
	step := m.Active.Steps[0]
	observation := observation(m, step, Completed)
	// 故意使用合法非规范化原文，确保保存的摘要来自原字节而非重编码DTO。
	observation.Data, err = json.MarshalIndent(prepared, "", "  ")
	require.NoError(t, err)
	m, err = m.Record(m.Owner, request.Key, observation)
	require.NoError(t, err)
	m, err = m.RequestAbort(m.Owner, request.Key)
	require.NoError(t, err)
	close := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch,
		Revision: shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: shared.Observed.CapacityGeneration,
		Open: false, BudgetCommitmentID: request.BudgetCommitmentID}
	m, err = m.StartActiveServiceClosure(m.Owner, close)
	require.NoError(t, err)
	closed := shared
	closed.Observed.AdmissionOpen, closed.Observed.AdmissionRevision = false, close.Revision
	digest, err := close.ContextSHA256()
	require.NoError(t, err)
	if len(continuation) > 0 && continuation[0] {
		// 原关闭真实保持Unknown，经更高owner的原生Continuation确认后才
		// 形成可封存边界；绝不把原关闭相位改成成功。
		m, err = m.Takeover(m.Owner, "confirmed-continuation-before-seal")
		require.NoError(t, err)
		request := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: m.ServiceClosure.ProtectedAdmission(), PreviousClose: close, Close: close}
		request.Close.IssuerEpoch, request.Close.Revision = m.Owner.Epoch, close.Revision+1
		require.NoError(t, request.Validate())
		_, _, _, selector := m.ServiceClosure.Selection()
		m, err = m.StartServiceClosureContinuation(m.Owner, selector, request)
		require.NoError(t, err)
		closed.Observed.IssuerFloorEpoch, closed.Observed.AdmissionRevision = m.Owner.Epoch, request.Close.Revision
		requestSHA, err := request.ContextSHA256()
		require.NoError(t, err)
		closeSHA, err := request.Close.ContextSHA256()
		require.NoError(t, err)
		fact := control.AdmissionContinuationResponse{ProtocolVersion: 1, Request: request, ContextSHA256: requestSHA,
			ClosedBy: control.AdmissionResponse{ProtocolVersion: 1, Request: request.Close, ContextSHA256: closeSHA, Disposition: control.AdmissionApplied, Observed: closed.Observed}, PriorMayHaveApplied: true}
		require.NoError(t, fact.ValidateFor(request, *m.PGInstance, false))
		m, err = m.RecordServiceClosureContinuation(m.Owner, fact, false)
		require.NoError(t, err)
		require.Equal(t, Unconfirmed, m.ServiceClosure.Phase)
		require.Equal(t, Settled, m.ServiceClosure.Continuation.Phase)
	} else {
		m, err = m.RecordServiceClosure(m.Owner, control.AdmissionResponse{ProtocolVersion: 1, Request: close,
			ContextSHA256: digest, Disposition: control.AdmissionApplied, Observed: closed.Observed}, false)
		require.NoError(t, err)
	}
	facts, err := m.PreparedHostClosure()
	require.NoError(t, err)
	require.Equal(t, rawDigest(observation.Data), facts.PreparedSHA256)
	canonical, err := json.Marshal(prepared)
	require.NoError(t, err)
	require.NotEqual(t, rawDigest(canonical), facts.PreparedSHA256)
	barrier := budget.HostReservationBarrier{ProtocolVersion: 1, Namespace: "testing", VMName: "vm", Incarnation: m.Incarnation,
		RunnerPodUID: initial.RunnerPodUID, Request: request, BeforeMemoryBytes: initial.VMMemoryBytes, HostOverheadBytes: initial.HostOverheadBytes,
		PreparedSHA256: facts.PreparedSHA256, OperationSHA256: facts.OperationSHA256,
		VMOwner: budget.BarrierOwner{HolderID: m.Owner.HolderID, Epoch: m.Owner.Epoch}, VMSequence: m.Sequence, VMResourceVersion: "original-closed-vm-rv",
		ClosedSHA256: facts.ClosedSHA256, ClosedSequence: facts.ClosedSequence,
		NodeName: initial.NodeName, NodeUID: initial.NodeUID, LeaseNamespace: "host-system", LeaseName: "host-original-lease", LeaseUID: "host-original-lease-uid", NodeOrigin: testIncarnation,
		HostOwner: budget.BarrierOwner{HolderID: "host-original-owner", Epoch: 37}, NodeSequence: 2, NodeResourceVersion: "node-before-seal-rv", Held: nil}
	require.NoError(t, barrier.Validate(), "VM与Host owner属于不同域，不跨域比较epoch")
	proof := ServiceRetainedRecovery{Source: source, Root: root, Current: closed,
		Fence:               control.FenceResponse{ProtocolVersion: 1, RequestedInstance: *m.PGInstance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: closed.Observed},
		InitialVM:           &budget.InitialVMSources{Instance: initial.Instance, BirthClaim: initial.RunnerBirth, StateClaim: runnerSources().StateClaim, IdentitySHA256: initial.RunnerIdentitySHA256, MemoryBytes: initial.VMMemoryBytes},
		PreparedHostBarrier: &barrier}
	return m, proof
}

func TestPG17UP1CompletedPrepareRequiresIndependentBarrier(t *testing.T) {
	t.Log("PG17-U-P1-009: Completed Prepare缺屏障/删除字段/改原摘要均拒绝，原Abort与WAL完整保留")
	m, shared, root := retainedRecoveryBase(t, "initial")
	m, input := completedRetainedFixture(t, m, shared, root)
	original := m.Clone()
	_, _, _, selector := m.ServiceClosure.Selection()
	missing := input
	missing.PreparedHostBarrier = nil
	_, err := m.RecordServiceRetainedRecovery(m.Owner, selector, missing)
	require.Error(t, err)
	forged := input
	forged.ClosedCommandSHA256 = strings.Repeat("f", 64)
	_, err = m.RecordServiceRetainedRecovery(m.Owner, selector, forged)
	require.Error(t, err, "选择摘要只能由原WAL填入")
	saved, err := m.RecordServiceRetainedRecovery(m.Owner, selector, input)
	require.NoError(t, err)
	require.Equal(t, original.Active, saved.Active)
	require.True(t, saved.Active.AbortRequested)
	require.Equal(t, Settled, saved.Active.Steps[0].Phase)
	require.Equal(t, Completed, saved.Active.Steps[0].Observation.Result)
	for _, step := range saved.Active.Steps[1:] {
		require.Equal(t, Planned, step.Phase)
	}
	for _, mode := range []string{"delete", "prepare-digest", "operation-digest", "closed-digest", "closed-sequence", "vm-sequence", "same-position-id", "before-memory"} {
		t.Run(mode, func(t *testing.T) {
			changed := saved.Clone()
			proof := changed.ServiceClosure.RetainedRecovery
			b := proof.PreparedHostBarrier
			switch mode {
			case "delete":
				proof.PreparedHostBarrier = nil
			case "prepare-digest":
				b.PreparedSHA256 = strings.Repeat("e", 64)
			case "operation-digest":
				b.OperationSHA256 = strings.Repeat("e", 64)
			case "closed-digest":
				b.ClosedSHA256 = strings.Repeat("e", 64)
			case "closed-sequence":
				b.ClosedSequence--
			case "vm-sequence":
				b.VMSequence = proof.RecordedSequence
			case "same-position-id":
				b.Request.Key.OperationID.Low++
			case "before-memory":
				b.BeforeMemoryBytes++
			}
			require.Error(t, changed.Validate())
		})
	}
	_, wire, err := encode(saved, MaxEncodedBytes)
	require.NoError(t, err)
	decoded, err := decode(wire, saved.VMUID, saved.Incarnation)
	require.NoError(t, err)
	require.Equal(t, saved, decoded)
	oldM, oldShared, oldRoot := retainedRecoveryBase(t, "initial")
	_, legacy := closeRetainedOperation(t, oldM, oldShared, oldRoot, true, false, control.Grow)
	raw, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "prepared_host_barrier")
	require.NotContains(t, string(raw), "closed_command_sha256", "旧分支必须保持原序列化形状")
}

func TestPG17UP1CompletedBarrierSurvivesRealHistoryExpiryAndBudgetReplacement(t *testing.T) {
	t.Log("PG17-U-P1-010: 真实Model终态淘汰后永久原屏障保留，新普通预算不能借旧slot授权")
	m, shared, root := retainedRecoveryBase(t, "initial")
	m, input := completedRetainedFixture(t, m, shared, root)
	m = selectRetainedRecovery(t, m, input)
	m, live := reopenRetainedRecovery(t, m)
	done, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
	require.NoError(t, err)
	original := cloneRetainedCoverage(done.RetainedCoverage)
	require.NotNil(t, original.Record.ServiceCoverage.RetainedRecovery.PreparedHostBarrier)
	for count := 0; count < HistoryLimit+2; count++ {
		request := testRequest(done, uint64(done.HighWater.Revision)+1, control.Grow)
		done = beginModel(t, done, request)
		done, err = done.RequestAbort(done.Owner, request.Key)
		require.NoError(t, err)
		done, err = done.Finish(done.Owner, request.Key)
		require.NoError(t, err)
	}
	require.Positive(t, done.ExpiredResults)
	require.Equal(t, original, done.RetainedCoverage)
	for _, r := range done.History {
		require.NotEqual(t, original.Record.Operation.Request.Key, r.Operation.Request.Key)
	}
	// 历史已真实淘汰后再次完成同合同恢复：最新完整slot覆盖旧slot，
	// 只保直接前驱摘要，不能递归嵌套旧Node Proof或遗失连续退休边界。
	shared = original.Record.ServiceCoverage.RetainedRecovery.Current
	shared.Observed = live.Observed
	done, input = completedRetainedFixture(t, done, shared, root)
	done = selectRetainedRecovery(t, done, input)
	done, live = reopenRetainedRecovery(t, done)
	done, err = done.CompleteServiceReopen(done.Owner, done.ServiceClosure.Reopen.CommandSHA256, live)
	require.NoError(t, err)
	require.EqualValues(t, 2, done.RetainedCoverage.Cycle)
	require.NotNil(t, done.RetainedCoverage.Previous)
	require.Equal(t, original.Record.Operation.Request.Key, done.RetainedCoverage.Previous.Key)
	require.NotEmpty(t, done.ServiceLifecycle.ContextRetainedSHA256)
	original = cloneRetainedCoverage(done.RetainedCoverage)
	next, observation := heldStart(t, done, uint64(done.HighWater.Revision)+1, runnerSources())
	next, err = next.Record(next.Owner, observation.Key, observation)
	require.NoError(t, err)
	next = heldFinish(t, next)
	require.Nil(t, next.InitialHeld)
	require.Equal(t, original, next.RetainedCoverage)
	require.NoError(t, next.Validate())
	require.Error(t, next.CheckRetainedSource(original.Record.ServiceCoverage.RetainedRecovery.Source), "旧slot仅保审计，新当前永久头独立授权")
	_, wire, err := encode(next, MaxEncodedBytes)
	require.NoError(t, err)
	restored, err := decode(wire, next.VMUID, next.Incarnation)
	require.NoError(t, err)
	require.Equal(t, next, restored)
}

func TestPG17UP1SealBeforeProofTakeoverKeepsConfirmedSelection(t *testing.T) {
	t.Log("PG17-U-P1-012: Seal已成而VM Proof未记时接管，已确认选择不可再Continuation，原准确屏障可保存")
	for _, continuation := range []bool{false, true} {
		name := "confirmed-original"
		if continuation {
			name = "original-unknown-confirmed-continuation"
		}
		t.Run(name, func(t *testing.T) {
			m, shared, root := retainedRecoveryBase(t, "initial")
			m, input := completedRetainedFixture(t, m, shared, root, continuation)
			require.Nil(t, m.ServiceClosure.RetainedRecovery)
			require.False(t, m.ServiceClosure.Pending(), "Node Seal只可来自已确认的当前选择")
			sealed := input.PreparedHostBarrier.Clone()
			before := m.Clone()
			m, err := m.Takeover(m.Owner, "vm-takeover-after-node-seal-before-proof")
			require.NoError(t, err)
			require.Equal(t, before.ServiceClosure, m.ServiceClosure, "接管不重置任何关闭phase或选择")
			require.Equal(t, before.Active, m.Active, "原Unknown/Abort/Completed逐字保持")
			require.NoError(t, m.CheckPreparedHostBarrier(sealed), "合法更高owner/sequence不要求重造同Key封存")
			_, _, previous, selector := m.ServiceClosure.Selection()
			request := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: m.ServiceClosure.ProtectedAdmission(), PreviousClose: previous, Close: previous}
			request.Close.IssuerEpoch, request.Close.Revision = m.Owner.Epoch, previous.Revision+1
			require.NoError(t, request.Validate(), "反例使用结构合法的新关闭，不能只撞坏请求")
			_, err = m.StartServiceClosureContinuation(m.Owner, selector, request)
			require.ErrorContains(t, err, "original service closure is not unknown")
			// 模拟可信collector在新owner下重新完成PG/Runner来源认证；Node
			// 原票据和ClosedSHA/VM旧RV完全不变，仅当前PG floor真实推进。
			input.Current.Observed.IssuerFloorEpoch = m.Owner.Epoch
			input.Fence.RequestedEpoch = m.Owner.Epoch
			input.Fence.Observed = input.Current.Observed
			saved, err := m.RecordServiceRetainedRecovery(m.Owner, selector, input)
			require.NoError(t, err)
			require.Equal(t, &sealed, saved.ServiceClosure.RetainedRecovery.PreparedHostBarrier)
			require.Greater(t, saved.ServiceClosure.RetainedRecovery.Origin.Epoch, sealed.VMOwner.Epoch)
			require.NoError(t, saved.Validate())
		})
	}
}
