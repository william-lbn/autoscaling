package journal

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 只构造严格合同夹具验证Model/codec/CAS。真实Session与各生产领域链
// 在owner组合验证，PG原生关闭在Neon独立验证，不称本夹具为Linux E2E。
func resumeClosureModel(t *testing.T, settled bool) (Model, control.AdmissionCloseRequest, control.AdmissionClosure) {
	t.Helper()
	m, obs := heldStart(t, heldBoundModel(t, testModel(t)), 1, runnerSources())
	m, err := m.Record(m.Owner, obs.Key, obs)
	require.NoError(t, err)
	var f budget.Finalized
	require.NoError(t, json.Unmarshal(m.BudgetHeld.Finalization.Observation.Data, &f))
	original := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: f.Shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: f.Shared.Observed.CapacityGeneration, Open: true, BudgetCommitmentID: f.Request.BudgetCommitmentID}
	raw, err := json.Marshal(original)
	require.NoError(t, err)
	m, err = m.Start(m.Owner, m.Active.Request.Key, ResumeAdmission, original.Instance.ComputeInstanceID, raw)
	require.NoError(t, err)
	if settled {
		m, err = m.Record(m.Owner, m.Active.Request.Key, observation(m, m.Active.Steps[len(m.Active.Steps)-1], Completed))
		require.NoError(t, err)
	}
	closeRequest := original
	closeRequest.Open, closeRequest.Revision = false, original.Revision+1
	selection := control.AdmissionCloseRequest{ProtocolVersion: 1, Original: original, Close: closeRequest}
	digest, err := closeRequest.ContextSHA256()
	require.NoError(t, err)
	o := f.Shared.Observed
	o.AdmissionOpen, o.AdmissionRevision = false, closeRequest.Revision
	fact := control.AdmissionResponse{ProtocolVersion: 1, Request: closeRequest, ContextSHA256: digest, Disposition: control.AdmissionApplied, Observed: o, VMShrinkPermit: false}
	proof := control.AdmissionClosure{ProtocolVersion: 1, Original: original, ClosedBy: fact, PriorMayHaveApplied: true, VMShrinkPermit: false}
	require.NoError(t, proof.ValidateFor(selection, original.Instance, false))
	return m, selection, proof
}

func TestResumeServiceClosureRetainsFullActiveAndUnknownAcrossCodec(t *testing.T) {
	for _, settled := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown", true: "completed-before-finish"}[settled], func(t *testing.T) {
			m, request, fact := resumeClosureModel(t, settled)
			before := m.Clone()
			closed, err := m.StartResumeServiceClosure(m.Owner, request)
			require.NoError(t, err)
			require.EqualValues(t, 13, closed.ProtocolVersion)
			require.Equal(t, before.Active, closed.Active)
			require.Equal(t, before.BudgetHeld, closed.BudgetHeld)
			require.Len(t, closed.ServiceClosure.OperationSHA256, 64)
			_, wire, err := encode(closed, MaxEncodedBytes)
			require.NoError(t, err)
			loaded, err := decode(wire, closed.VMUID, closed.Incarnation)
			require.NoError(t, err)
			require.Equal(t, closed, loaded)
			_, err = loaded.RequestAbort(loaded.Owner, loaded.Active.Request.Key)
			require.Error(t, err, "普通取消不能改写已绑定的原Active")
			_, err = loaded.Finish(loaded.Owner, loaded.Active.Request.Key)
			require.Error(t, err, "关闭不能作为伸缩成功/取消结案证明")
			if !settled {
				_, err = loaded.Record(loaded.Owner, loaded.Active.Request.Key, observation(loaded, loaded.Active.Steps[len(loaded.Active.Steps)-1], Completed))
				require.Error(t, err, "关闭不能将原未知开放补成完成")
			}
			_, err = loaded.RecordInitialServiceClosure(loaded.Owner, fact, false)
			require.Error(t, err, "首次关闭入口不得结清最终开放关闭")
			loaded, err = loaded.RecordResumeServiceClosure(loaded.Owner, fact, false)
			require.NoError(t, err)
			require.False(t, loaded.ServiceClosure.Pending())
			require.Equal(t, before.Active, loaded.Active)
			require.Equal(t, before.BudgetHeld, loaded.BudgetHeld)
			_, wire, err = encode(loaded, MaxEncodedBytes)
			require.NoError(t, err)
			roundtrip, err := decode(wire, m.VMUID, m.Incarnation)
			require.NoError(t, err)
			require.Equal(t, loaded, roundtrip)
		})
	}
}

func TestResumeServiceClosureRejectsProjectionAndRewrittenOperation(t *testing.T) {
	m, request, fact := resumeClosureModel(t, false)
	closed, err := m.StartResumeServiceClosure(m.Owner, request)
	require.NoError(t, err)
	for _, mode := range []string{"v12", "v11", "no-original", "no-active", "no-hash", "wrong-hash", "abort-active", "rewrite-prior", "initial-reason", "wrong-original", "wrong-capacity", "wrong-budget", "predated"} {
		t.Run(mode, func(t *testing.T) {
			next := closed.Clone()
			c := next.ServiceClosure
			switch mode {
			case "v12":
				next.ProtocolVersion = 12
			case "v11":
				next.ProtocolVersion = 11
			case "no-original":
				c.Original = nil
			case "no-active":
				next.Active = nil
			case "no-hash":
				c.OperationSHA256 = ""
			case "wrong-hash":
				c.OperationSHA256 = strings.Repeat("a", 64)
			case "abort-active":
				next.Active.AbortRequested = true
			case "rewrite-prior":
				step := &next.Active.Steps[0]
				step.Observation.Data = []byte(`{"other_individually_valid_original_source":true}`)
			case "initial-reason":
				c.Reason = "initial_source_unconfirmed"
			case "wrong-original":
				c.Original.Revision++
			case "wrong-capacity":
				c.Request.ExpectedCapacityGeneration++
			case "wrong-budget":
				c.Request.BudgetCommitmentID = "another-budget"
			case "predated":
				c.StartedSequence = next.BudgetHeld.RecordedSequence
			}
			c.CommandSHA256, _ = c.Request.ContextSHA256()
			require.Error(t, next.Validate())
		})
	}
	bad := fact
	bad.ClosedBy = *cloneAdmissionResponse(&fact.ClosedBy)
	bad.ClosedBy.Observed.IssuerFloorEpoch++
	_, err = closed.RecordResumeServiceClosure(closed.Owner, bad, true)
	require.Error(t, err, "未来owner不能补结清")
	m.Sequence = math.MaxUint64
	_, err = m.StartResumeServiceClosure(m.Owner, request)
	require.Error(t, err, "写前序列不得回绕")
	initial, initialRequest, _ := initialClosureModel(t)
	_, err = initial.StartResumeServiceClosure(initial.Owner, initialRequest)
	require.Error(t, err, "首次未知不能被投影成最终开放关闭")
}

func TestResumeServiceClosureContinuationRetainsActiveAndVersion(t *testing.T) {
	m, request, fact := resumeClosureModel(t, false)
	m, err := m.StartResumeServiceClosure(m.Owner, request)
	require.NoError(t, err)
	before := m.Clone()
	for _, holder := range []string{"resume-successor-1", "resume-successor-2"} {
		m, err = m.Takeover(m.Owner, holder)
		require.NoError(t, err)
		_, _, previous, selector := m.ServiceClosure.Selection()
		n := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: request.Original, PreviousClose: previous, Close: previous}
		n.Close.IssuerEpoch = m.Owner.Epoch
		m, err = m.StartServiceClosureContinuation(m.Owner, selector, n)
		require.NoError(t, err)
		require.EqualValues(t, 13, m.ProtocolVersion)
		require.Equal(t, before.Active, m.Active)
		require.Equal(t, before.BudgetHeld, m.BudgetHeld)
	}
	n := m.ServiceClosure.Continuation.Request
	proof := control.AdmissionContinuationResponse{ProtocolVersion: 1, Request: n, ContextSHA256: "", ClosedBy: fact.ClosedBy, PriorMayHaveApplied: true, VMShrinkPermit: false}
	proof.ContextSHA256, err = n.ContextSHA256()
	require.NoError(t, err)
	proof.ClosedBy.Request, proof.ClosedBy.Observed.IssuerFloorEpoch = n.Close, m.Owner.Epoch
	proof.ClosedBy.ContextSHA256, err = n.Close.ContextSHA256()
	require.NoError(t, err)
	m, err = m.RecordServiceClosureContinuation(m.Owner, proof, false)
	require.NoError(t, err)
	require.False(t, m.ServiceClosure.Pending())
	require.Equal(t, Unconfirmed, m.ServiceClosure.Phase)
	require.Equal(t, before.Active, m.Active)
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m, loaded)
}

func TestResumeServiceClosureStoreRequiresConfirmedCASAndPreservesLostReplies(t *testing.T) {
	for _, mode := range []string{"ok", "lost-start", "lost-record", "stale-rv", "recreated-vm"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, request, fact := resumeClosureModel(t, false)
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(ctx)
			require.NoError(t, err)
			if mode == "stale-rv" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"concurrent": "update"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-start"
			if mode == "recreated-vm" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "replacement-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			started, ticket, err := store.StartResumeServiceClosure(ctx, s, m.Owner, request)
			if mode == "stale-rv" || mode == "recreated-vm" || mode == "lost-start" {
				require.Error(t, err)
				require.Nil(t, ticket)
				require.Nil(t, started)
				if mode != "lost-start" {
					return
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, request.Close, ticket.Request)
				api.mu.Lock()
				api.failAfterCommit = mode == "lost-record"
				api.mu.Unlock()
				result, err := store.RecordResumeServiceClosure(ctx, started, m.Owner, fact, false)
				if mode == "lost-record" {
					require.Error(t, err)
					require.Nil(t, result)
				} else {
					require.NoError(t, err)
				}
			}
			loaded, err := store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, mode == "lost-start", loaded.State().ServiceClosure.Pending())
			require.Equal(t, m.Active, loaded.State().Active)
			require.Equal(t, m.BudgetHeld, loaded.State().BudgetHeld)
		})
	}
}
