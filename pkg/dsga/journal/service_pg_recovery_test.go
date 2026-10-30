package journal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 真实Model/Store/codec；PG及非PG效果为明确领域夹具。原Guest/VM即使被
// 夹具标记Completed，也不把本组当作生产预算/资源恢复端到端证据。
func servicePGRecoveryModel(t *testing.T, mode string) (Model, control.FenceResponse, control.ControlResponse) {
	t.Helper()
	m, closing, closed := activeClosureModel(t, control.Grow, ResizePG, true)
	native := heldCommand(t, m.Active.Request).Finalized.Shared.Observed
	if mode == "denied" {
		terminal := native.Operation
		native = closed.Observed
		native.Operation = terminal
		native.Operation.Phase, native.Operation.Reason = control.Denied, control.ReasonTimeout
	}
	if mode == "retained-expired" {
		status := control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: m.Active.Request.Key, Disposition: control.ReceiptReplayed, HasResult: true, Result: &native}, Observed: native}
		audit := struct {
			ProtocolVersion uint32                  `json:"protocol_version"`
			Fence           control.FenceResponse   `json:"issuer_fence"`
			Response        control.ControlResponse `json:"response"`
		}{ProtocolVersion: 1, Fence: control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: *m.PGInstance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: closed.Observed}, Response: status}
		var err error
		for _, step := range m.Active.Steps {
			if step.Action == ResizePG {
				obs := observation(m, step, Completed)
				obs.Data, err = json.Marshal(audit)
				require.NoError(t, err)
				m, err = m.Record(m.Owner, m.Active.Request.Key, obs)
				require.NoError(t, err)
			}
		}
		closing.ExpectedCapacityGeneration = native.CapacityGeneration
		closed.Request, closed.Observed.CapacityGeneration = closing, native.CapacityGeneration
		closed.ContextSHA256, _ = closing.ContextSHA256()
	}
	m, err := m.StartActiveServiceClosure(m.Owner, closing)
	require.NoError(t, err)
	m, err = m.RecordServiceClosure(m.Owner, closed, false)
	require.NoError(t, err)
	if mode == "isolated-old" {
		native = closed.Observed
	}
	if mode == "isolated-old" || mode == "isolated-new" || mode == "retained-expired" {
		m, err = m.Takeover(m.Owner, "PG-independent-recovery")
		require.NoError(t, err)
		native.Operation = nil
	}
	native.IssuerFloorEpoch, native.AdmissionRevision, native.AdmissionOpen = m.Owner.Epoch, closing.Revision, false
	status := control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: m.Active.Request.Key, Disposition: control.ReceiptReplayed, HasResult: true, Result: &native}, Observed: native}
	if native.Operation == nil {
		status.Receipt.Disposition, status.Receipt.HasResult, status.Receipt.Result = control.ReceiptStale, false, nil
	}
	fence := control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: *m.PGInstance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: native}
	return m, fence, status
}

func TestServicePGRecoveryKeepsOriginalUnknownOrRetainedTerminal(t *testing.T) {
	for _, mode := range []string{"committed", "denied", "isolated-old", "isolated-new", "retained-expired"} {
		t.Run(mode, func(t *testing.T) {
			m, fence, status := servicePGRecoveryModel(t, mode)
			before := m.Clone()
			_, _, _, selector := m.ServiceClosure.Selection()
			m, err := m.RecordServicePGRecovery(m.Owner, selector, fence, status)
			require.NoError(t, err)
			require.Equal(t, before.Active, m.Active)
			require.Equal(t, before.InitialHeld, m.InitialHeld)
			require.Equal(t, before.BudgetBoundary, m.BudgetBoundary)
			require.Error(t, m.CheckStableServiceRecovery(), "单PG事实不能退休原VM/Guest及完整预算")
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			decoded, err := decode(wire, m.VMUID, m.Incarnation)
			require.NoError(t, err)
			require.Equal(t, m, decoded)
			cloned := m.Clone()
			cloned.ServiceClosure.PGRecovery.Status.Observed.Arrays[0].CommittedUpperBytes++
			require.NotEqual(t, cloned.ServiceClosure.PGRecovery, m.ServiceClosure.PGRecovery)
			if status.Receipt.Result != nil {
				cloned = m.Clone()
				cloned.ServiceClosure.PGRecovery.Status.Receipt.Result.Operation.ContextSHA256 += "a"
				require.NotEqual(t, cloned.ServiceClosure.PGRecovery, m.ServiceClosure.PGRecovery)
			}
			m, err = m.Takeover(m.Owner, "PG-proof-later-observer")
			require.NoError(t, err, "更高owner保留原证明，不重写原观察者")
			_, err = m.RecordServicePGRecovery(m.Owner, selector, fence, status)
			require.Error(t, err, "已保存证明不能由新观察覆盖")
			_, err = m.Finish(m.Owner, m.Active.Request.Key)
			require.Error(t, err)
		})
	}
}

func TestServicePGRecoveryRejectsChangedOriginAndInventedOutcome(t *testing.T) {
	m, fence, status := servicePGRecoveryModel(t, "isolated-new")
	_, _, _, selector := m.ServiceClosure.Selection()
	_, err := m.RecordServicePGRecovery(m.Owner, selector+"x", fence, status)
	require.Error(t, err)
	m, err = m.RecordServicePGRecovery(m.Owner, selector, fence, status)
	require.NoError(t, err)
	for name, change := range map[string]func(*Model){
		"original": func(n *Model) { n.Active.AbortRequested = true },
		"owner":    func(n *Model) { n.ServiceClosure.PGRecovery.Origin.HolderID += "x" },
		"closed":   func(n *Model) { n.ServiceClosure.PGRecovery.Closed.Revision++ },
		"command":  func(n *Model) { n.ServiceClosure.PGRecovery.CommandSHA256 += "x" },
		"effect":   func(n *Model) { n.ServiceClosure.PGRecovery.Effect = control.ResizeRecoveredCommitted },
		"sequence": func(n *Model) {
			n.ServiceClosure.PGRecovery.RecordedSequence = n.ServiceClosure.PGRecovery.ClosedSequence
		},
		"future":           func(n *Model) { n.ServiceClosure.PGRecovery.RecordedSequence++ },
		"opened":           func(n *Model) { n.ServiceClosure.PGRecovery.Status.Observed.AdmissionOpen = true },
		"floor":            func(n *Model) { n.ServiceClosure.PGRecovery.Fence.RequestedEpoch++ },
		"generation":       func(n *Model) { n.ServiceClosure.PGRecovery.Status.Observed.CapacityGeneration++ },
		"relabel-rejected": func(n *Model) { n.ServiceClosure.PGRecovery.Status.Receipt.Disposition = control.ReceiptRejected },
	} {
		t.Run(name, func(t *testing.T) { n := m.Clone(); change(&n); require.Error(t, n.Validate()) })
	}
	raw, err := json.Marshal(m.ServiceClosure.PGRecovery)
	require.NoError(t, err)
	var decoded ServicePGRecovery
	require.Error(t, json.Unmarshal(append(raw[:len(raw)-1:len(raw)-1], []byte(`,"unknown":true}`)...), &decoded))
	// 删除独立证明只能退回原关闭/未知，仍不能普通Finish或开放。
	n := m.Clone()
	n.ServiceClosure.PGRecovery = nil
	require.NoError(t, n.Validate())
	require.Error(t, n.CheckStableServiceRecovery())
}

func TestStoreServicePGRecoveryRequiresConfirmedOriginalCAS(t *testing.T) {
	for _, mode := range []string{"normal", "lost-reply", "stale-rv", "recreated-vm"} {
		t.Run(mode, func(t *testing.T) {
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, fence, status := servicePGRecoveryModel(t, "isolated-old")
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(context.Background())
			require.NoError(t, err)
			if mode == "stale-rv" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"changed": "true"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-reply"
			if mode == "recreated-vm" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "new-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			_, _, _, selector := m.ServiceClosure.Selection()
			result, err := store.RecordServicePGRecovery(context.Background(), s, m.Owner, selector, fence, status)
			if mode == "normal" {
				require.NoError(t, err)
				require.NotNil(t, result)
			} else {
				require.Error(t, err)
				require.Nil(t, result)
			}
			if mode == "lost-reply" {
				var lost *UnconfirmedWrite
				require.ErrorAs(t, err, &lost)
				loaded, err := store.Load(context.Background())
				require.NoError(t, err)
				require.Equal(t, m.Active, loaded.State().Active)
				require.NotNil(t, loaded.State().ServiceClosure.PGRecovery)
			}
		})
	}
}

func TestServicePGRecoveryShrinkKeepsUnknownAndOriginalLargerBudget(t *testing.T) {
	m, finalized, _ := committedAbortModel(t)
	m = heldFinish(t, m)
	request := testRequest(m, 2, control.Shrink)
	request.ExpectedCapacityGeneration = finalized.Shared.Observed.CapacityGeneration
	request.TargetBuffers = finalized.Request.TargetBuffers / 2
	m = beginModel(t, m, request)
	for _, step := range m.Active.Steps {
		body := testCommand // 非PG前驱为明确领域夹具，不授予预算/物理许可。
		var err error
		if step.Action == ResizePG {
			body, err = json.Marshal(request)
			require.NoError(t, err)
		}
		m, err = m.Start(m.Owner, request.Key, step.Action, request.Key.Instance.ComputeInstanceID, body)
		require.NoError(t, err)
		if step.Action == ResizePG {
			break
		}
		m, err = m.Record(m.Owner, request.Key, observation(m, m.Active.Steps[int(step.Ordinal)-1], Completed))
		require.NoError(t, err)
	}
	closing := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: finalized.Shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: request.ExpectedCapacityGeneration, Open: false, BudgetCommitmentID: request.BudgetCommitmentID}
	m, err := m.StartActiveServiceClosure(m.Owner, closing)
	require.NoError(t, err)
	closed := finalized.Shared.Observed
	closed.AdmissionOpen, closed.AdmissionRevision = false, closing.Revision
	hash, err := closing.ContextSHA256()
	require.NoError(t, err)
	m, err = m.RecordServiceClosure(m.Owner, control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: closing, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: closed}, false)
	require.NoError(t, err)
	original := m.Clone()
	native := heldCommand(t, request).Finalized.Shared.Observed
	native.AdmissionOpen, native.AdmissionRevision = false, closing.Revision
	status := control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: request.Key, Disposition: control.ReceiptReplayed, HasResult: true, Result: &native}, Observed: native}
	fence := control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: *m.PGInstance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: native}
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err = m.RecordServicePGRecovery(m.Owner, selector, fence, status)
	require.NoError(t, err)
	require.Equal(t, original.Active, m.Active)
	require.Equal(t, original.BudgetHeld, m.BudgetHeld, "原较大预算/Host承诺不能被逻辑PG缩小替换")
	require.Equal(t, original.BudgetBoundary, m.BudgetBoundary)
	require.Less(t, native.HighBuffers, finalized.Shared.Observed.HighBuffers)
	require.Error(t, m.CheckStableServiceRecovery(), "Release/Guest/VM仍未完成，不能借逻辑PG提交开放")
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	_, err = decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
}
