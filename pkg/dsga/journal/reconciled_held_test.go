package journal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 真实Model/codec推进；PG/Root/Host/VM输入是明确领域夹具，网络来源认证由
// Runner/claims组合另测。新Grow只完成VM，PG保留上次容量，不手填永久头。
func partialCapacityModel(t *testing.T) (Model, budget.ReconciledAbortCommand, budget.ReconciledAbortReceipt) {
	t.Helper()
	m, f, root := committedAbortModel(t)
	m = heldFinish(t, m)
	req := testRequest(m, 2, control.Grow)
	req.ExpectedCapacityGeneration = f.Shared.Observed.CapacityGeneration
	req.TargetBuffers, req.TargetVMMemoryBytes = 2048, 2<<30
	m = beginModel(t, m, req)
	outRef := budget.ObjectRef{Name: "outside-partial", UID: "outside-uid"}
	hostRef := budget.ObjectRef{Name: "host-partial", UID: "host-uid"}
	shared := f.Shared
	shared.Observed.AdmissionOpen = true
	shared.Observed.AdmissionRevision++
	quote, err := budget.GrowSharedUpper(shared, req.TargetBuffers)
	require.NoError(t, err)
	p := budget.Prepared{ProtocolVersion: 1, VMShrinkPermit: false, Request: req, Identity: *m.ResourceIdentity, RunnerPodName: f.RunnerPodName, RunnerRestartCount: f.RunnerRestartCount, VMBeforeMemoryBytes: f.Request.TargetVMMemoryBytes, GuestUsableMemoryBytes: req.TargetVMMemoryBytes - (256 << 20), Shared: shared, TargetSharedUpperBytes: quote, Limits: root.Bootstrap.Limits, SystemOutsidePG: f.SystemOutsidePG, Transition: f.Transition, Safety: f.Safety, OutsideClaim: &outRef}
	require.NoError(t, p.ValidateFor(req, p.Identity))
	host := budget.HostReservation{ProtocolVersion: 2, VMShrinkPermit: false, Request: req, Identity: p.Identity, Incarnation: m.Incarnation, SourceID: "host-partial-source", RunnerPodName: p.RunnerPodName, RunnerPodUID: "runner-uid", NodeName: "node", NodeUID: "node-uid", ReservedMemoryBytes: req.TargetVMMemoryBytes + (64 << 20), HostOverheadBytes: 64 << 20, HoldUntilExplicitSettlement: true}
	prepareRaw, err := json.Marshal(p)
	require.NoError(t, err)
	for _, action := range []Action{PrepareBudget, ReserveHost, ResizeVM} {
		target, body := req.Key.Instance.ComputeInstanceID, any(p)
		if action == ReserveHost {
			target, body = host.SourceID, host
		}
		if action == ResizeVM {
			target, body = host.RunnerPodUID, map[string]bool{"explicit_native_capacity_fixture": true}
		}
		m, err = m.Start(m.Owner, req.Key, action, target, testCommand)
		require.NoError(t, err)
		var step Step
		for _, candidate := range m.Active.Steps {
			if candidate.Action == action {
				step = candidate
			}
		}
		obs := observation(m, step, Completed)
		obs.Data, err = json.Marshal(body)
		require.NoError(t, err)
		m, err = m.Record(m.Owner, req.Key, obs)
		require.NoError(t, err)
	}
	var vm Step
	for _, step := range m.Active.Steps {
		if step.Action == ResizeVM {
			vm = step
		}
	}
	sources := runnerSources()
	sources.JournalSHA256 = vm.CommandSHA256
	native := budget.NativeCapacity{Sources: sources, Request: req, BeforeBytes: p.VMBeforeMemoryBytes, Cancelled: false, PreparedSHA256: rawDigest(prepareRaw), GuestSHA256: "", HostClaim: hostRef}
	out := budget.OutsideReservation{ProtocolVersion: 1, VMShrinkPermit: false, Request: req, Identity: p.Identity, Incarnation: m.Incarnation, ResidualProfileID: "explicit-qualified-domain-fixture", SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true}
	capacity, err := budget.Reconcile(budget.ReconcileInputs{Prepared: p, Shared: shared, Root: root, Outside: out, OutsideClaim: outRef, Host: host, HostClaim: hostRef, VM: native, GuestUsableMemoryBytes: p.GuestUsableMemoryBytes, IssuerEpoch: m.Owner.Epoch})
	require.NoError(t, err)
	m, err = m.RequestAbort(m.Owner, req.Key)
	require.NoError(t, err)
	command := budget.ReconciledAbortCommand{ProtocolVersion: 1, Capacity: capacity, PreparedSHA256: rawDigest(prepareRaw), Admission: nil}
	receipt := budget.ReconciledAbortReceipt{ProtocolVersion: 1, Command: command, Query: true, Fence: control.FenceResponse{ProtocolVersion: 1, RequestedInstance: req.Key.Instance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: shared.Observed, VMShrinkPermit: false}, Shared: shared, Response: nil, PGAggregate: root, ResourceEpoch: m.Owner.Epoch}
	require.NoError(t, receipt.ValidateFor(command, m.Owner.Epoch, m.Owner.Epoch))
	return m, command, receipt
}

func recordPartialCapacity(t *testing.T, m Model, c budget.ReconciledAbortCommand, r budget.ReconciledAbortReceipt) Model {
	t.Helper()
	raw, err := json.Marshal(c)
	require.NoError(t, err)
	m, err = m.StartCleanup(m.Owner, c.Capacity.Request.Key, c.Capacity.Request.Key.Instance.ComputeInstanceID, raw)
	require.NoError(t, err)
	obs := observation(m, *m.Active.Cleanup, Completed)
	obs.Data, err = json.Marshal(r)
	require.NoError(t, err)
	m, err = m.Record(m.Owner, c.Capacity.Request.Key, obs)
	require.NoError(t, err)
	return m
}

func TestActualCapacityBudgetIsAtomicPermanentAndNeverOrdinarySuccess(t *testing.T) {
	m, c, r := partialCapacityModel(t)
	if dir := os.Getenv("DSGA_CAPACITY_WIRE_DIR"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		raw, err := json.Marshal(m)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "model-v7.json"), raw, 0o600))
	}
	m = recordPartialCapacity(t, m, c, r)
	require.EqualValues(t, 8, m.ProtocolVersion)
	require.Nil(t, m.BudgetHeld)
	require.NotNil(t, m.CapacityHeld)
	require.Nil(t, m.BudgetBoundary, "落账不等于原操作关闭")
	_, err := m.BudgetFor(c.Capacity.Request)
	require.Error(t, err, "实际预算不能投影为原目标成功")
	actual, err := m.CapacityFor(c.Capacity.Request)
	require.NoError(t, err)
	require.Less(t, actual.Buffers, actual.Request.TargetBuffers)
	m, err = m.Finish(m.Owner, c.Capacity.Request.Key)
	require.NoError(t, err)
	require.Equal(t, AbortedAfterReconcile, m.History[len(m.History)-1].Outcome)
	require.NoError(t, m.CheckClosedCapacity(c.Capacity.Request))
	held := m.Clone().CapacityHeld
	m, err = m.Takeover(m.Owner, "next-owner")
	require.NoError(t, err)
	for revision := uint64(1); revision <= HistoryLimit+1; revision++ {
		next := testRequest(m, revision, control.Grow)
		m = beginModel(t, m, next)
		m, err = m.RequestAbort(m.Owner, next.Key)
		require.NoError(t, err)
		m, err = m.Finish(m.Owner, next.Key)
		require.NoError(t, err)
	}
	require.NotZero(t, m.ExpiredResults)
	require.Equal(t, held, m.CapacityHeld)
	require.NoError(t, m.CheckClosedCapacity(c.Capacity.Request))
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	restored, err := decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, held, restored.CapacityHeld)
	require.NoError(t, restored.CheckClosedCapacity(c.Capacity.Request))
	if dir := os.Getenv("DSGA_CAPACITY_WIRE_DIR"); dir != "" {
		raw, err := json.Marshal(restored)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "model-v8.json"), raw, 0o600))
	}
	actual.PGOperation.TargetBuffers++
	again, err := restored.CapacityFor(c.Capacity.Request)
	require.NoError(t, err)
	require.NotEqual(t, actual.PGOperation, again.PGOperation, "读者不能通过指针修改当前预算")
	bad := restored.Clone()
	bad.CapacityHeld.Cleanup.Command[0] = '['
	require.Error(t, bad.Validate())
	require.NoError(t, restored.Validate(), "Clone必须深拷贝完整Cleanup原文")
}

func TestReconciledCapacityCannotCrossUnknownOrInventAnotherBudget(t *testing.T) {
	base, originalCommand, originalReceipt := partialCapacityModel(t)
	for _, mode := range []string{"unknown-VM", "changed-Prepare", "wrong-VM-outer", "same-generation-other-prefix", "new-finalization", "old-version", "missing-header", "both-headers", "bad-origin", "changed-audit"} {
		t.Run(mode, func(t *testing.T) {
			m, c, r := base.Clone(), originalCommand, originalReceipt
			if mode == "unknown-VM" || mode == "changed-Prepare" || mode == "wrong-VM-outer" || mode == "same-generation-other-prefix" || mode == "new-finalization" {
				switch mode {
				case "unknown-VM":
					for i := range m.Active.Steps {
						if m.Active.Steps[i].Action == ResizeVM {
							m.Active.Steps[i].Phase, m.Active.Steps[i].Observation = Unconfirmed, nil
						}
					}
				case "changed-Prepare":
					c.PreparedSHA256 = c.Capacity.RootSHA256
				case "wrong-VM-outer":
					c.Capacity.VM.Sources.JournalSHA256 = c.Capacity.RootSHA256
				case "same-generation-other-prefix":
					c.Capacity.Buffers++
				case "new-finalization":
					c.Capacity.VM.Request.Key.Revision++
				}
				raw, err := json.Marshal(c)
				require.NoError(t, err)
				_, err = m.StartCleanup(m.Owner, c.Capacity.Request.Key, c.Capacity.Request.Key.Instance.ComputeInstanceID, raw)
				require.Error(t, err)
				return
			}
			m = recordPartialCapacity(t, m, c, r)
			switch mode {
			case "old-version":
				m.ProtocolVersion = 7
			case "missing-header":
				m.CapacityHeld = nil
			case "both-headers":
				m.BudgetHeld = new(HeldBudget)
			case "bad-origin":
				m.CapacityHeld.Origin.HolderID = ""
			case "changed-audit":
				m.CapacityHeld.Cleanup.Observation.Data = []byte(`{"incomplete":true}`)
			}
			require.Error(t, m.Validate())
		})
	}
}

// 仍用真实Model推进正常后继，证明实际预算不是只能进入不能退出的另一套
// 账本。原实际预算在完整新Finalize的同CAS被替换，格式不降级，旧来源不释放。
func TestNormalFinalizationAtomicallyReplacesActualCapacityBudget(t *testing.T) {
	m, c, receipt := partialCapacityModel(t)
	m = recordPartialCapacity(t, m, c, receipt)
	var err error
	m, err = m.Finish(m.Owner, c.Capacity.Request.Key)
	require.NoError(t, err)
	r := testRequest(m, 3, control.Grow)
	r.ExpectedCapacityGeneration = c.Capacity.CapacityGeneration
	r.TargetBuffers, r.TargetVMMemoryBytes = 2048, c.Capacity.VMMemoryBytes
	m = beginModel(t, m, r)
	command := heldCommand(t, r)
	sources := runnerSources()
	command.Finalized.Sources.Runner = &sources
	require.NoError(t, command.Validate())
	for _, original := range m.Active.Steps {
		if original.Action == FinalizeBudget {
			raw, err := json.Marshal(command)
			require.NoError(t, err)
			m, err = m.Start(m.Owner, r.Key, FinalizeBudget, r.Key.Instance.ComputeInstanceID, raw)
			require.NoError(t, err)
			require.NotNil(t, m.CapacityHeld)
			require.Nil(t, m.BudgetHeld)
			step := m.Active.Steps[int(original.Ordinal)-1]
			obs := observation(m, step, Completed)
			obs.Data, err = json.Marshal(command.Finalized)
			require.NoError(t, err)
			m, err = m.Record(m.Owner, r.Key, obs)
			require.NoError(t, err)
			require.Nil(t, m.CapacityHeld)
			require.NotNil(t, m.BudgetHeld)
			require.EqualValues(t, 8, m.ProtocolVersion)
			continue
		}
		m, err = m.Start(m.Owner, r.Key, original.Action, "explicit-domain-fixture", testCommand)
		require.NoError(t, err)
		m, err = m.Record(m.Owner, r.Key, observation(m, m.Active.Steps[int(original.Ordinal)-1], Completed))
		require.NoError(t, err)
	}
	m, err = m.Finish(m.Owner, r.Key)
	require.NoError(t, err)
	require.NoError(t, m.CheckClosedBudget(r))
	_, err = m.CapacityFor(c.Capacity.Request)
	require.Error(t, err)
}
