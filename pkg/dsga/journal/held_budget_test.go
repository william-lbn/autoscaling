package journal

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 本文件以真实Model/codec/HTTP clientset和RFC6902 CAS验证持久不变量。
// 上游Prepare/PG/Host/VM数据明确是协议夹具，不冒充内核/来源认证或集群E2E。
func heldBoundModel(t *testing.T, m Model) Model {
	t.Helper()
	id := resourceguard.Identity{VMUID: m.VMUID, ComputeInstanceID: "compute-1", GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
	var err error
	m, err = m.BindResource(m.Owner, id)
	require.NoError(t, err)
	m, err = m.BindPG(m.Owner, testRequest(m, 1, control.Grow).Key.Instance)
	require.NoError(t, err)
	return m
}

func heldCommand(t *testing.T, req control.Request) budget.FinalizationCommand {
	t.Helper()
	raw, err := os.ReadFile("../../api/dsga/testdata/response-v1.json")
	require.NoError(t, err)
	var response control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &response))
	o := response.Observed
	o.Instance, o.IssuerFloorEpoch = req.Key.Instance, req.Key.IssuerEpoch
	o.LowBuffers, o.HighBuffers = req.TargetBuffers, req.TargetBuffers
	o.Operation.Key, o.Operation.Kind, o.Operation.TargetBuffers = req.Key, req.Kind, req.TargetBuffers
	o.Operation.ContextSHA256, err = req.ContextSHA256()
	require.NoError(t, err)
	shared := control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: 16 << 20, StartupArraysUpperBytes: 16 << 20, CurrentMainAndArraysUpperBytes: 80 << 20, Observed: o}
	f := budget.Finalized{
		ProtocolVersion: 1, Request: req, IssuerEpoch: req.Key.IssuerEpoch, RunnerPodName: "runner", HostReservationID: "host-original", GuestUsableMemoryBytes: 768 << 20, Shared: shared,
		Aggregate: &budget.Bound{Kind: budget.HardLimit, UpperBytes: 512 << 20, SourceID: "aggregate-original"}, SystemOutsidePG: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 24 << 20, SourceID: "system-original"}, Transition: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "transition-original"}, Safety: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "safety-original"},
		Sources: &budget.SettlementSources{OutsideClaim: budget.ObjectRef{Name: "outside-original", UID: "outside-uid"}, HostClaim: budget.ObjectRef{Name: "host-original", UID: "host-uid"}, VMReceiptSHA256: strings.Repeat("c", 64), Runner: nil}, VMShrinkPermit: false, RunnerRestartCount: 0, PrivateAndDSM: nil, LFCOutsideShared: nil, HugePoolOutsideShared: nil,
	}
	c := budget.FinalizationCommand{ProtocolVersion: 1, Finalized: f, PreparedSHA256: strings.Repeat("a", 64), GuestSHA256: strings.Repeat("b", 64), VMSHA256: f.Sources.VMReceiptSHA256}
	require.NoError(t, c.Validate())
	return c
}

func heldStart(t *testing.T, m Model, revision uint64, runners ...budget.RunnerSources) (Model, Observation) {
	t.Helper()
	req := testRequest(m, revision, control.Grow)
	req.TargetBuffers = 1024
	m = beginModel(t, m, req)
	for _, original := range m.Active.Steps {
		cmd, target := testCommand, "prior-domain"
		if original.Action == FinalizeBudget {
			var err error
			c := heldCommand(t, req)
			if len(runners) == 1 {
				c.Finalized.Sources.Runner = &runners[0]
			}
			cmd, err = json.Marshal(c)
			require.NoError(t, err)
			target = req.Key.Instance.ComputeInstanceID
			m, err = m.Start(m.Owner, req.Key, original.Action, target, cmd)
			require.NoError(t, err)
			obs := observation(m, m.Active.Steps[int(original.Ordinal)-1], Completed)
			obs.Data, err = json.Marshal(c.Finalized)
			require.NoError(t, err)
			return m, obs
		}
		var err error
		m, err = m.Start(m.Owner, req.Key, original.Action, target, cmd)
		require.NoError(t, err)
		m, err = m.Record(m.Owner, req.Key, observation(m, m.Active.Steps[int(original.Ordinal)-1], Completed))
		require.NoError(t, err)
	}
	t.Fatal("fixture lacks finalization")
	return m, Observation{}
}

func heldRecord(t *testing.T, m Model, obs Observation) Model {
	t.Helper()
	next, err := m.Record(m.Owner, obs.Key, obs)
	require.NoError(t, err)
	require.EqualValues(t, 3, next.ProtocolVersion)
	require.NotNil(t, next.BudgetHeld)
	require.Equal(t, next.Sequence, next.BudgetHeld.RecordedSequence)
	return next
}

func heldFinish(t *testing.T, m Model) Model {
	t.Helper()
	req := m.Active.Request
	var err error
	m, err = m.Start(m.Owner, req.Key, ResumeAdmission, req.Key.Instance.ComputeInstanceID, testCommand)
	require.NoError(t, err)
	step := m.Active.Steps[len(m.Active.Steps)-1]
	m, err = m.Record(m.Owner, req.Key, observation(m, step, Completed))
	require.NoError(t, err)
	m, err = m.Finish(m.Owner, req.Key)
	require.NoError(t, err)
	return m
}

func TestHeldBudgetAtomicRecordSurvivesHistoryAndTakeover(t *testing.T) {
	m, obs := heldStart(t, heldBoundModel(t, testModel(t)), 1)
	require.Nil(t, m.BudgetHeld)
	before := m.Clone()
	m = heldRecord(t, m, obs)
	require.Nil(t, before.BudgetHeld)
	require.Equal(t, Unconfirmed, before.Active.Steps[int(obs.Ordinal)-1].Phase)
	held := m.Clone().BudgetHeld
	// 深拷贝覆盖完整命令、完整回执及派发owner；State/Clone不能让调用方改原预算。
	clone := m.Clone()
	clone.BudgetHeld.Finalization.Command[0] = 'x'
	clone.BudgetHeld.Finalization.Observation.Data[0] = 'x'
	clone.BudgetHeld.Finalization.DispatchedBy.HolderID = "foreign-owner"
	require.Equal(t, held, m.BudgetHeld)
	f, err := m.BudgetFor(m.Active.Request)
	require.NoError(t, err)
	f.Sources.HostClaim.UID = "caller-mutated"
	again, err := m.BudgetFor(m.Active.Request)
	require.NoError(t, err)
	require.Equal(t, "host-uid", again.Sources.HostClaim.UID)
	idempotent, err := m.Record(m.Owner, obs.Key, obs)
	require.NoError(t, err)
	require.Equal(t, m, idempotent)
	m = heldFinish(t, m)
	m, err = m.Takeover(m.Owner, "process-new")
	require.NoError(t, err)
	for i := 1; i <= HistoryLimit+1; i++ {
		req := testRequest(m, uint64(i), control.Grow)
		m = beginModel(t, m, req)
		m, err = m.RequestAbort(m.Owner, req.Key)
		require.NoError(t, err)
		m, err = m.Finish(m.Owner, req.Key)
		require.NoError(t, err)
	}
	require.NotZero(t, m.ExpiredResults)
	require.Nil(t, m.Active)
	require.Equal(t, held, m.BudgetHeld)
	_, encoded, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	restored, err := decode(encoded, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m, restored)
	_, err = restored.BudgetFor(again.Request)
	require.NoError(t, err, "原请求结果过期不等于长期预算丢失")
	wrong := again.Request
	wrong.Key.Instance.PGBootID++
	_, err = restored.BudgetFor(wrong)
	require.Error(t, err)
	_, err = restored.BindPG(restored.Owner, wrong.Key.Instance)
	require.Error(t, err)
}

func TestHeldBudgetRejectsPartialReplacementAndCorruptPersistence(t *testing.T) {
	m, obs := heldStart(t, heldBoundModel(t, testModel(t)), 1)
	for _, mode := range []string{"changed-bound", "missing-sources", "wrong-vm-digest", "wrong-request"} {
		t.Run(mode, func(t *testing.T) {
			var f budget.Finalized
			require.NoError(t, json.Unmarshal(obs.Data, &f))
			switch mode {
			case "changed-bound":
				f.Aggregate.UpperBytes++
			case "missing-sources":
				f.Sources = nil
			case "wrong-vm-digest":
				f.Sources.VMReceiptSHA256 = strings.Repeat("d", 64)
			case "wrong-request":
				f.Request.Key.Revision++
			}
			bad := obs
			var err error
			bad.Data, err = json.Marshal(f)
			require.NoError(t, err)
			unchanged, err := m.Record(m.Owner, obs.Key, bad)
			require.Error(t, err)
			require.Equal(t, m, unchanged)
		})
	}
	m = heldRecord(t, m, obs)
	for _, mode := range []string{"missing-held", "downgrade-v2", "held-version", "sequence-future", "sequence-zero", "different-guest", "wrong-command", "changed-observation", "missing-pg", "wrong-original-owner"} {
		t.Run(mode, func(t *testing.T) {
			bad := m.Clone()
			switch mode {
			case "missing-held":
				bad.BudgetHeld = nil
			case "downgrade-v2":
				bad.ProtocolVersion = 2
			case "held-version":
				bad.BudgetHeld.ProtocolVersion++
			case "sequence-future":
				bad.BudgetHeld.RecordedSequence = bad.Sequence + 1
			case "sequence-zero":
				bad.BudgetHeld.RecordedSequence = 0
			case "different-guest":
				bad.BudgetHeld.Identity.CgroupInode++
			case "wrong-command":
				bad.BudgetHeld.Finalization.Command = testCommand
			case "changed-observation":
				bad.BudgetHeld.Finalization.Observation.Data = []byte(`{"changed":true}`)
			case "missing-pg":
				bad.PGInstance = nil
			case "wrong-original-owner":
				bad.BudgetHeld.Finalization.DispatchedBy.HolderID = "different-owner"
			}
			require.Error(t, bad.Validate())
			raw, err := json.Marshal(bad)
			require.NoError(t, err)
			var decoded Model
			require.Error(t, json.Unmarshal(raw, &decoded))
		})
	}
}

func TestHeldBudgetRetainsOldThroughAbortAndReplacesOnlyCompleteNewBudget(t *testing.T) {
	m, obs := heldStart(t, heldBoundModel(t, testModel(t)), 1)
	m = heldFinish(t, heldRecord(t, m, obs))
	old := m.Clone().BudgetHeld
	m, obs = heldStart(t, m, 2)
	require.Equal(t, old, m.BudgetHeld, "新的写前命令不能提前替换现行预算")
	obs.Result = Rejected
	var err error
	m, err = m.Record(m.Owner, obs.Key, obs)
	require.NoError(t, err)
	require.Equal(t, old, m.BudgetHeld)
	m, err = m.RequestAbort(m.Owner, obs.Key)
	require.NoError(t, err)
	m, err = m.StartCleanup(m.Owner, obs.Key, "cleanup-fixture", testCommand)
	require.NoError(t, err)
	m, err = m.Record(m.Owner, obs.Key, observation(m, *m.Active.Cleanup, Completed))
	require.NoError(t, err)
	m, err = m.Finish(m.Owner, obs.Key)
	require.NoError(t, err)
	require.Equal(t, old, m.BudgetHeld, "通用取消/清理不是释放现行预算证明")
	m, obs = heldStart(t, m, 3)
	partial := m.Clone()
	m = heldRecord(t, m, obs)
	require.Equal(t, old, partial.BudgetHeld)
	require.Equal(t, obs.Key, m.BudgetHeld.Finalization.Observation.Key)
	corrupt := m.Clone()
	corrupt.BudgetHeld = old
	require.Error(t, corrupt.Validate(), "较新Completed不能只保存旧长期预算")
	unchanged, err := m.Record(m.Owner, obs.Key, Observation{Key: obs.Key})
	require.Error(t, err)
	require.Equal(t, m, unchanged)
}

func TestHeldBudgetLegacyV2RequiresExplicitOriginalActiveUpgrade(t *testing.T) {
	m, obs := heldStart(t, heldBoundModel(t, testModel(t)), 1)
	m = heldRecord(t, m, obs)
	legacy := m.Clone()
	legacy.ProtocolVersion, legacy.BudgetHeld = 2, nil
	require.NoError(t, legacy.Validate())
	_, encoded, err := encode(legacy, MaxEncodedBytes)
	require.NoError(t, err)
	legacy, err = decode(encoded, legacy.VMUID, legacy.Incarnation)
	require.NoError(t, err)
	require.Nil(t, legacy.BudgetHeld, "读取旧v2绝不能自动补写")
	legacy, err = legacy.Takeover(legacy.Owner, "takeover-owner")
	require.NoError(t, err)
	upgraded, err := legacy.RetainFinalizedBudget(legacy.Owner, obs.Key)
	require.NoError(t, err)
	require.EqualValues(t, 3, upgraded.ProtocolVersion)
	require.Equal(t, legacy.Active, upgraded.Active, "升级不能修改原命令/回执/旧issuer")
	require.Equal(t, upgraded.Sequence, upgraded.BudgetHeld.RecordedSequence)
	idempotent, err := upgraded.RetainFinalizedBudget(upgraded.Owner, obs.Key)
	require.NoError(t, err)
	require.Equal(t, upgraded, idempotent)
	_, err = legacy.RetainFinalizedBudget(m.Owner, obs.Key)
	require.Error(t, err, "旧owner不能升级")
	oldActive := m.Clone()
	oldActive.ProtocolVersion, oldActive.BudgetHeld = 2, nil
	oldActive, err = oldActive.Start(oldActive.Owner, obs.Key, ResumeAdmission, obs.Key.Instance.ComputeInstanceID, testCommand)
	require.NoError(t, err)
	oldActive, err = oldActive.Record(oldActive.Owner, obs.Key, observation(oldActive, oldActive.Active.Steps[len(oldActive.Active.Steps)-1], Completed))
	require.NoError(t, err)
	unchanged, err := oldActive.Finish(oldActive.Owner, obs.Key)
	require.Error(t, err, "旧Active必须在可重核原来源时先升级，再关闭")
	require.Equal(t, oldActive, unchanged)
	repaired, err := oldActive.RetainFinalizedBudget(oldActive.Owner, obs.Key)
	require.NoError(t, err)
	_, err = repaired.Finish(repaired.Owner, obs.Key)
	require.NoError(t, err)
	// 对尚未升级且全部动作完成的旧Active，Finish必须阻断，不能先关闭后再扫描历史。
	oldComplete := heldFinish(t, m)
	oldComplete.ProtocolVersion, oldComplete.BudgetHeld = 2, nil
	require.NoError(t, oldComplete.Validate())
	_, err = oldComplete.RetainFinalizedBudget(oldComplete.Owner, obs.Key)
	require.Error(t, err, "历史记录不能自动收养为当前持有预算")
	if dir := os.Getenv("DSGA_HELD_WIRE_DIR"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		for name, model := range map[string]Model{"v1": testModel(t), "v2": legacy, "v3": upgraded} {
			raw, err := json.Marshal(model)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dir, name+".json"), raw, 0o600))
		}
		var projection map[string]json.RawMessage
		raw, err := json.Marshal(upgraded)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &projection))
		delete(projection, "budget_held")
		raw, err = json.Marshal(projection)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "v3-projection.json"), raw, 0o600))
	}
}

func TestHeldBudgetStoreOneCASAndUnknownReplyRetainFullState(t *testing.T) {
	for _, mode := range []string{"complete", "lost-record", "stale-rv", "recreated-vm", "legacy-upgrade", "legacy-lost-upgrade"} {
		t.Run(mode, func(t *testing.T) {
			api, store := newStoreAPI(t)
			s := initializeStore(t, api, store)
			m, obs := heldStart(t, heldBoundModel(t, s.State()), 1)
			if strings.HasPrefix(mode, "legacy-") {
				m = heldRecord(t, m, obs)
				m.ProtocolVersion, m.BudgetHeld = 2, nil
			}
			_, encoded, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = encoded })
			s, err = store.Load(context.Background())
			require.NoError(t, err)
			api.mu.Lock()
			before := api.patches
			api.failAfterCommit = strings.Contains(mode, "lost")
			api.mu.Unlock()
			if mode == "stale-rv" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"unrelated": "concurrent"} })
			}
			if mode == "recreated-vm" {
				api.mu.Lock()
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "replacement-vm"; api.mu.Unlock() }
				api.mu.Unlock()
			}
			if strings.HasPrefix(mode, "legacy-") {
				_, err = store.RetainFinalizedBudget(context.Background(), s, m.Owner, obs.Key)
			} else {
				_, err = store.Record(context.Background(), s, m.Owner, obs.Key, obs)
			}
			if mode == "complete" || mode == "legacy-upgrade" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
			}
			api.mu.Lock()
			persisted := api.vm.Annotations[control.JournalAnnotation]
			require.Equal(t, before+1, api.patches, "每次结算/升级只有一个条件写，不另写Held")
			api.mu.Unlock()
			durable, err := decode(persisted, m.VMUID, m.Incarnation)
			require.NoError(t, err)
			if mode == "stale-rv" || mode == "recreated-vm" {
				require.Equal(t, m, durable, "CAS失败不能留下仅Completed或仅Held")
				return
			}
			require.EqualValues(t, 3, durable.ProtocolVersion)
			require.NotNil(t, durable.BudgetHeld)
			require.Equal(t, Settled, durable.Active.Steps[int(obs.Ordinal)-1].Phase)
			require.Equal(t, durable.Active.Steps[int(obs.Ordinal)-1], durable.BudgetHeld.Finalization)
			fresh, err := store.Load(context.Background())
			require.NoError(t, err)
			_, err = fresh.State().BudgetFor(m.Active.Request)
			require.NoError(t, err)
		})
	}
}
