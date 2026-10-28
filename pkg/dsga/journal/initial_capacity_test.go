package journal

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 本文件只验证Model/codec的原子性和寿命，不启动HTTP服务或VM，也不把
// 领域夹具当作真实Runner取消证明。真实生产组合单独执行；这里专门覆盖
// InitialHeld被替换后原资格仍永久保留、历史淘汰/接管不丢来源的独立边界。
func initialCapacityModel(t *testing.T) (Model, budget.ReconciledAbortCommand, budget.ReconciledAbortReceipt) {
	return initialCapacityDomain(t, false)
}

func initialCapacityDomain(t *testing.T, beforeVM bool) (Model, budget.ReconciledAbortCommand, budget.ReconciledAbortReceipt) {
	t.Helper()
	i, first, opened := initialDomain(t)
	m := initialBoundModel(t)
	var err error
	m, err = m.StartInitialAdmission(m.Owner, first)
	require.NoError(t, err)
	m, err = m.RecordInitialAdmission(m.Owner, opened)
	require.NoError(t, err)
	r := testRequest(m, 1, control.Grow)
	r.TargetBuffers, r.TargetVMMemoryBytes = 1024, 2<<30
	m = beginModel(t, m, r)
	shared := opened.Shared
	upper, err := budget.GrowSharedUpper(shared, r.TargetBuffers)
	require.NoError(t, err)
	p := budget.Prepared{ProtocolVersion: 1, VMShrinkPermit: false, Request: r, Identity: i.PGAggregate.Identity, RunnerPodName: i.RunnerPodName, RunnerRestartCount: i.RunnerRestartCount, VMBeforeMemoryBytes: i.VMMemoryBytes, GuestUsableMemoryBytes: (2 << 30) - (256 << 20), Shared: shared, TargetSharedUpperBytes: upper, Limits: i.PGAggregate.Bootstrap.Limits, SystemOutsidePG: i.SystemOutsidePG, Transition: i.Transition, Safety: i.Safety, OutsideClaim: nil}
	require.NoError(t, p.ValidateFor(r, p.Identity))
	hostRef := budget.ObjectRef{Name: "initial-capacity-host", UID: "initial-capacity-host-uid"}
	host := budget.HostReservation{ProtocolVersion: 2, VMShrinkPermit: false, Request: r, Identity: p.Identity, Incarnation: m.Incarnation, SourceID: "initial-capacity-host-source", RunnerPodName: i.RunnerPodName, RunnerPodUID: i.RunnerPodUID, NodeName: i.NodeName, NodeUID: i.NodeUID, ReservedMemoryBytes: r.TargetVMMemoryBytes + i.HostOverheadBytes, HostOverheadBytes: i.HostOverheadBytes, HoldUntilExplicitSettlement: true}
	prepareRaw, err := json.Marshal(p)
	require.NoError(t, err)
	actions := []Action{PrepareBudget, ReserveHost}
	if !beforeVM {
		actions = append(actions, ResizeVM)
	}
	for _, action := range actions {
		target, body, result := r.Key.Instance.ComputeInstanceID, any(p), Completed
		switch action {
		case ReserveHost:
			target, body = host.SourceID, host
		case ResizeVM:
			target, body, result = host.RunnerPodUID, map[string]bool{"explicit_cancelled_native_domain_fixture": true}, NoEffect
		}
		m, err = m.Start(m.Owner, r.Key, action, target, testCommand)
		require.NoError(t, err)
		var dispatched Step
		for _, s := range m.Active.Steps {
			if s.Action == action {
				dispatched = s
			}
		}
		fact := observation(m, dispatched, result)
		fact.Data, err = json.Marshal(body)
		require.NoError(t, err)
		m, err = m.Record(m.Owner, r.Key, fact)
		require.NoError(t, err)
	}
	var vm Step
	for _, s := range m.Active.Steps {
		if s.Action == ResizeVM {
			vm = s
		}
	}
	sources := runnerSources()
	sources.BirthClaim, sources.JournalSHA256 = i.RunnerBirth, vm.CommandSHA256
	native := budget.NativeCapacity{Sources: sources, Request: r, BeforeBytes: i.VMMemoryBytes, Cancelled: true, PreparedSHA256: rawDigest(prepareRaw), GuestSHA256: "", HostClaim: hostRef}
	var noOutside budget.OutsideReservation
	var noRef budget.ObjectRef
	inputs := budget.ReconcileInputs{Prepared: p, Shared: shared, Root: i.PGAggregate, Outside: noOutside, OutsideClaim: noRef, Host: host, HostClaim: hostRef, VM: native, InitialVM: nil, GuestUsableMemoryBytes: i.GuestUsableMemoryBytes, IssuerEpoch: m.Owner.Epoch}
	if beforeVM {
		var noNative budget.NativeCapacity
		inputs.VM = noNative
		inputs.InitialVM = &budget.InitialVMSources{Instance: i.Instance, BirthClaim: i.RunnerBirth, StateClaim: sources.StateClaim, IdentitySHA256: i.RunnerIdentitySHA256, MemoryBytes: i.VMMemoryBytes}
	}
	capacity, err := budget.ReconcileInitial(inputs, i)
	require.NoError(t, err)
	m, err = m.RequestAbort(m.Owner, r.Key)
	require.NoError(t, err)
	c := budget.ReconciledAbortCommand{ProtocolVersion: 1, Capacity: capacity, PreparedSHA256: rawDigest(prepareRaw), Admission: nil}
	receipt := budget.ReconciledAbortReceipt{ProtocolVersion: 1, Command: c, Query: true, Fence: control.FenceResponse{ProtocolVersion: 1, RequestedInstance: i.Instance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: shared.Observed, VMShrinkPermit: false}, Shared: shared, Response: nil, PGAggregate: i.PGAggregate, ResourceEpoch: m.Owner.Epoch}
	require.NoError(t, receipt.ValidateFor(c, m.Owner.Epoch, m.Owner.Epoch))
	return m, c, receipt
}

// 领域夹具只证明WAL合同，不认证Runner；原始VM步骤必须一直是Planned，
// 任何Start/NoEffect都不能借相同容量回退成原启动来源。完整Initial与独立
// 启动来源必须同CAS永久保留，较新零派发取消只能推进关闭水位。
func TestInitialVMActualCapacityRejectsDispatchAndSurvivesClosedThrough(t *testing.T) {
	m, command, receipt := initialCapacityDomain(t, true)
	require.NoError(t, m.CheckReconciledPlan(command))
	changed := m.Clone()
	changed.Active.AbortRequested = false
	changed, err := changed.Start(changed.Owner, command.Capacity.Request.Key, ResizeVM, "runner-fixture", testCommand)
	require.NoError(t, err)
	changed, err = changed.RequestAbort(changed.Owner, command.Capacity.Request.Key)
	require.NoError(t, err)
	require.Error(t, changed.CheckReconciledPlan(command), "曾派发的VM不能重新解释为启动来源")
	raw, err := json.Marshal(command)
	require.NoError(t, err)
	if len(raw) <= MaxCommandBytes {
		raw = append(raw, []byte(strings.Repeat(" ", MaxCommandBytes+1-len(raw)))...)
	}
	m, err = m.StartCleanup(m.Owner, command.Capacity.Request.Key, command.Capacity.Request.Key.Instance.ComputeInstanceID, raw)
	require.NoError(t, err)
	fact := observation(m, *m.Active.Cleanup, Completed)
	fact.Data, err = json.Marshal(receipt)
	require.NoError(t, err)
	m, err = m.Record(m.Owner, fact.Key, fact)
	require.NoError(t, err)
	require.Nil(t, m.InitialHeld)
	m, err = m.Finish(m.Owner, fact.Key)
	require.NoError(t, err)
	r := testRequest(m, 2, control.Grow)
	m = beginModel(t, m, r)
	m, err = m.RequestAbort(m.Owner, r.Key)
	require.NoError(t, err)
	m, err = m.Finish(m.Owner, r.Key)
	require.NoError(t, err)
	require.NoError(t, m.CheckClosedCapacity(command.Capacity.Request))
	require.Equal(t, r.Key, m.BudgetBoundary.ThroughKey)
	_, encoded, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	restored, err := decode(encoded, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	capacity, err := restored.CapacityFor(command.Capacity.Request)
	require.NoError(t, err)
	require.Equal(t, command.Capacity, capacity)
}

func TestInitialActualCapacityRetainsOriginalAcrossAtomicReplacementHistoryAndTakeover(t *testing.T) {
	m, c, receipt := initialCapacityModel(t)
	original, err := m.RetainedInitialOutside()
	require.NoError(t, err)
	raw, err := json.Marshal(c)
	require.NoError(t, err)
	// 小夹具可能少于8KiB，补JSON尾空白只触发同一真实typed合同的字节
	// 边界，不增加字段、不改变语义；普通命令不得借这个专用界限扩张。
	if len(raw) <= MaxCommandBytes {
		raw = append(raw, []byte(strings.Repeat(" ", MaxCommandBytes+1-len(raw)))...)
	}
	require.LessOrEqual(t, len(raw), MaxInitialCapacityCommandBytes)
	oversized := append(append([]byte(nil), raw...), []byte(strings.Repeat(" ", MaxInitialCapacityCommandBytes-len(raw)+1))...)
	_, err = m.StartCleanup(m.Owner, c.Capacity.Request.Key, c.Capacity.Request.Key.Instance.ComputeInstanceID, oversized)
	require.Error(t, err)
	_, err = m.StartCleanup(m.Owner, c.Capacity.Request.Key, c.Capacity.Request.Key.Instance.ComputeInstanceID, []byte(`{"padding":"`+strings.Repeat("x", MaxCommandBytes)+`"}`))
	require.Error(t, err, "普通Cleanup仍限8KiB")
	var substituted budget.ReconciledAbortCommand
	require.NoError(t, json.Unmarshal(raw, &substituted))
	substituted.Capacity.InitialOutside.HostProfileID = "same-values-substituted-profile"
	require.NoError(t, substituted.ValidateFor(m.Owner.Epoch))
	require.Error(t, m.CheckReconciledPlan(substituted), "同数值合同不能替换原完整Initial")
	m, err = m.StartCleanup(m.Owner, c.Capacity.Request.Key, c.Capacity.Request.Key.Instance.ComputeInstanceID, raw)
	require.NoError(t, err)
	require.NotNil(t, m.InitialHeld, "仅Start未知不能清初始头")
	require.Nil(t, m.CapacityHeld)
	_, err = m.Finish(m.Owner, c.Capacity.Request.Key)
	require.Error(t, err, "未知Cleanup不能关闭")
	fact := observation(m, *m.Active.Cleanup, Completed)
	fact.Data, err = json.Marshal(receipt)
	require.NoError(t, err)
	m, err = m.Record(m.Owner, fact.Key, fact)
	require.NoError(t, err)
	require.Nil(t, m.InitialHeld)
	require.NotNil(t, m.CapacityHeld)
	m, err = m.Finish(m.Owner, fact.Key)
	require.NoError(t, err)
	require.NoError(t, m.CheckClosedCapacity(c.Capacity.Request))
	m, err = m.Takeover(m.Owner, "initial-capacity-successor-owner")
	require.NoError(t, err)
	// 原取消及首次开放均被淘汰后，原完整资格不能依赖History找回；
	// 只推进零派发水位，不制造任何PG/Guest/VM效果或改写旧请求Key。
	for revision := 2; revision <= HistoryLimit+3; revision++ {
		r := testRequest(m, uint64(revision), control.Grow)
		m = beginModel(t, m, r)
		m, err = m.RequestAbort(m.Owner, r.Key)
		require.NoError(t, err)
		m, err = m.Finish(m.Owner, r.Key)
		require.NoError(t, err)
	}
	require.Positive(t, uint64(m.ExpiredResults))
	require.NoError(t, m.CheckClosedCapacity(c.Capacity.Request))
	_, encoded, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(encoded, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	retained, err := loaded.RetainedInitialOutside()
	require.NoError(t, err)
	require.Equal(t, original, retained)
	var held budget.ReconciledAbortCommand
	require.NoError(t, json.Unmarshal(loaded.CapacityHeld.Cleanup.Command, &held))
	require.Equal(t, c.Capacity.Request, held.Capacity.Request)
	_, err = loaded.InitialBudgetFor()
	require.Error(t, err, "保留原Outside资格不等于恢复初始预算头")
}
