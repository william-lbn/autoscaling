package journal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

func recordRejectedPGFixture(t *testing.T, m Model, snapshot control.Snapshot) Model {
	t.Helper()
	r := m.Active.Request
	raw, err := json.Marshal(r)
	require.NoError(t, err)
	m, err = m.Start(m.Owner, r.Key, ResizePG, r.Key.Instance.ComputeInstanceID, raw)
	require.NoError(t, err)
	snapshot.IssuerFloorEpoch = m.Owner.Epoch
	response := control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: r.Key, Disposition: control.ReceiptBusy, HasResult: false, Result: nil}, Observed: snapshot}
	audit := struct {
		ProtocolVersion uint32                  `json:"protocol_version"`
		Fence           control.FenceResponse   `json:"issuer_fence"`
		Response        control.ControlResponse `json:"response"`
	}{ProtocolVersion: 1, Fence: control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: r.Key.Instance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: snapshot}, Response: response}
	for _, step := range m.Active.Steps {
		if step.Action != ResizePG {
			continue
		}
		obs := observation(m, step, Rejected)
		obs.Data, err = json.Marshal(audit)
		require.NoError(t, err)
		m, err = m.Record(m.Owner, r.Key, obs)
		require.NoError(t, err)
	}
	return m
}

// 全部非PG资源为明确合同夹具。测试真实Model/Store/codec的独立屏障与
// 最终预算原子安装，不将夹具或高floor本身当作PG/Guest/VM完成证据。
func rejectedActualRecovery(t *testing.T) (Model, control.FenceResponse, control.ControlResponse, control.SharedCostResponse, budget.RootAggregate) {
	t.Helper()
	m, c, receipt := partialCapacityModel(t)
	// 构造取消前真实Step顺序；不向生产账本提供删除Abort/改原记录的入口。
	m.Active.AbortRequested = false
	r := m.Active.Request
	var err error
	m, err = m.Start(m.Owner, r.Key, GuestReady, r.Key.Instance.ComputeInstanceID, testCommand)
	require.NoError(t, err)
	for _, step := range m.Active.Steps {
		if step.Action == GuestReady {
			m, err = m.Record(m.Owner, r.Key, observation(m, step, Completed))
			require.NoError(t, err)
		}
	}
	m = recordRejectedPGFixture(t, m, receipt.Shared.Observed)
	m, err = m.RequestAbort(m.Owner, r.Key)
	require.NoError(t, err)
	raw, err := json.Marshal(c)
	require.NoError(t, err)
	m, err = m.StartCleanup(m.Owner, r.Key, r.Key.Instance.ComputeInstanceID, raw)
	require.NoError(t, err)
	q := control.AdmissionRequest{ProtocolVersion: 1, Instance: r.Key.Instance, IssuerEpoch: m.Owner.Epoch, Revision: 100, ExpectedCapacityGeneration: r.ExpectedCapacityGeneration, Open: false, BudgetCommitmentID: r.BudgetCommitmentID}
	m, err = m.StartCleanupServiceClosure(m.Owner, q)
	require.NoError(t, err)
	snapshot := receipt.Shared.Observed
	snapshot.AdmissionOpen, snapshot.AdmissionRevision = false, q.Revision
	hash, err := q.ContextSHA256()
	require.NoError(t, err)
	m, err = m.RecordServiceClosure(m.Owner, control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: q, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: snapshot}, false)
	require.NoError(t, err)
	m, err = m.Takeover(m.Owner, "rejection-recovery-owner")
	require.NoError(t, err)
	snapshot.IssuerFloorEpoch = m.Owner.Epoch
	fence := control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: r.Key.Instance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: snapshot}
	status := control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: r.Key, Disposition: control.ReceiptNotFound, HasResult: false, Result: nil}, Observed: snapshot}
	shared := receipt.Shared
	shared.Observed = snapshot
	return m, fence, status, shared, receipt.PGAggregate
}

func TestServicePGRejectionKeepsOriginalAndCompletesActualBudget(t *testing.T) {
	m, fence, status, current, root := rejectedActualRecovery(t)
	original := m.Clone()
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err := m.RecordServicePGRejection(m.Owner, selector, fence, status)
	require.NoError(t, err)
	require.Equal(t, original.Active, m.Active)
	require.Error(t, m.CheckStableServiceRecovery(), "单独PG拒绝屏障不构成完整恢复预算")
	m, err = m.RecordServiceCleanupRecovery(m.Owner, selector, fence, current, root)
	require.NoError(t, err)
	require.NoError(t, m.CheckStableServiceRecovery())
	before := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: current.Observed.CapacityGeneration, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}, Renewed: false, Armed: true, Expired: false, DeadlineNS: 200, LeaseEpoch: m.Owner.Epoch, TimeoutMS: 30000, Observed: current.Observed}
	m, err = m.StartServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
	require.NoError(t, err)
	before.Observed = m.ServiceClosure.Reopen.Receipt.Observed
	m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, before)
	require.NoError(t, err)
	require.Nil(t, m.Active)
	require.Equal(t, *original.Active, m.CapacityHeld.Recovery.Original)
	require.NotNil(t, m.CapacityHeld.Recovery.Proof.PGRejection)
	require.Equal(t, RecoveredAfterCancellation, m.History[len(m.History)-1].Outcome)
	require.NoError(t, m.CheckClosedCapacity(original.Active.Request))
	bad := m.Clone()
	bad.CapacityHeld.Recovery.Proof.PGRejection.Origin.HolderID += "-aliased"
	require.Error(t, bad.CapacityHeld.Recovery.Proof.validate(bad.CapacityHeld.Recovery.Original), "同epoch不能借另一个holder拼接原拒绝证明")
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	decoded, err := decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m, decoded)
}

func TestServicePGRejectionRejectsChangedProofAndProjection(t *testing.T) {
	m, fence, status, current, root := rejectedActualRecovery(t)
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err := m.RecordServicePGRejection(m.Owner, selector, fence, status)
	require.NoError(t, err)
	for _, mode := range []string{"same-owner", "capacity", "closed", "owner", "sequence", "original", "expired", "native-result", "original-rewritten"} {
		t.Run(mode, func(t *testing.T) {
			n := m.Clone()
			p := n.ServiceClosure.PGRejection
			switch mode {
			case "same-owner":
				p.Fence.RequestedEpoch = n.Active.Request.Key.IssuerEpoch
			case "capacity":
				p.Status.Observed.CapacityGeneration++
			case "closed":
				p.Status.Observed.AdmissionOpen = true
			case "owner":
				p.Origin.HolderID += "-different"
			case "sequence":
				p.RecordedSequence = p.ClosedSequence
			case "original":
				p.OperationSHA256 += "x"
			case "expired":
				p.Status.Receipt.Disposition = control.ReceiptResultExpired
			case "native-result":
				p.Status.Receipt.HasResult = true
			case "original-rewritten":
				n.Active.AbortRequested = false
			}
			require.Error(t, n.Validate())
		})
	}
	cloned := m.Clone()
	cloned.ServiceClosure.PGRejection.Status.Observed.Arrays[0].CommittedUpperBytes++
	require.NotEqual(t, cloned.ServiceClosure.PGRejection, m.ServiceClosure.PGRejection)
	m, err = m.RecordServiceCleanupRecovery(m.Owner, selector, fence, current, root)
	require.NoError(t, err)
	n := m.Clone()
	n.ServiceClosure.PGRejection, n.ServiceClosure.CleanupRecovery.PGRejection = nil, nil
	require.Error(t, n.Validate(), "删除全部独立拒绝证明不能投影成普通恢复预算")
	raw, err := json.Marshal(m.ServiceClosure.PGRejection)
	require.NoError(t, err)
	var decoded ServicePGRejection
	require.Error(t, json.Unmarshal(append(raw[:len(raw)-1], []byte(`,"unknown":true}`)...), &decoded))
}

func TestStoreServicePGRejectionCASDoesNotChangeOriginal(t *testing.T) {
	for _, mode := range []string{"normal", "lost-reply", "stale-rv", "recreated-vm"} {
		t.Run(mode, func(t *testing.T) {
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, fence, status, _, _ := rejectedActualRecovery(t)
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
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "replacement-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			_, _, _, selector := m.ServiceClosure.Selection()
			result, err := store.RecordServicePGRejection(context.Background(), s, m.Owner, selector, fence, status)
			if mode == "normal" {
				require.NoError(t, err)
				require.NotNil(t, result.State().ServiceClosure.PGRejection)
			} else {
				require.Error(t, err)
				require.Nil(t, result)
			}
			if mode == "lost-reply" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				loaded, err := store.Load(context.Background())
				require.NoError(t, err)
				require.Equal(t, m.Active, loaded.State().Active)
				require.NotNil(t, loaded.State().ServiceClosure.PGRejection)
			}
		})
	}
}
