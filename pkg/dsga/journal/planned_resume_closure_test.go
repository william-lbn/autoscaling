package journal

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func plannedResumeClosureModel(t *testing.T) (Model, control.AdmissionRequest, control.AdmissionResponse) {
	t.Helper()
	m, obs := heldStart(t, heldBoundModel(t, testModel(t)), 1, runnerSources())
	m, err := m.Record(m.Owner, obs.Key, obs)
	require.NoError(t, err)
	f, err := m.PlannedResumeBudgetForClosure()
	require.NoError(t, err)
	r := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: f.Shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: f.Shared.Observed.CapacityGeneration, Open: false, BudgetCommitmentID: f.Request.BudgetCommitmentID}
	digest, err := r.ContextSHA256()
	require.NoError(t, err)
	o := f.Shared.Observed
	o.AdmissionOpen, o.AdmissionRevision = false, r.Revision
	fact := control.AdmissionResponse{ProtocolVersion: 1, Request: r, ContextSHA256: digest, Disposition: control.AdmissionApplied, Observed: o, VMShrinkPermit: false}
	require.NoError(t, fact.ValidateFor(r, r.Instance, false))
	return m, r, fact
}

func TestPlannedResumeClosurePreservesUnsentStepAndFullBudget(t *testing.T) {
	m, request, fact := plannedResumeClosureModel(t)
	before := m.Clone()
	m, err := m.StartServiceClosure(m.Owner, request)
	require.NoError(t, err)
	require.EqualValues(t, 14, m.ProtocolVersion)
	require.Nil(t, m.ServiceClosure.Original)
	require.Equal(t, before.Active, m.Active)
	require.Equal(t, before.BudgetHeld, m.BudgetHeld)
	_, err = m.Start(m.Owner, m.Active.Request.Key, ResumeAdmission, request.Instance.ComputeInstanceID, []byte(`{}`))
	require.Error(t, err, "关闭后普通派发不得改写原Planned")
	_, err = m.RequestAbort(m.Owner, m.Active.Request.Key)
	require.Error(t, err)
	_, err = m.Finish(m.Owner, m.Active.Request.Key)
	require.Error(t, err)
	for _, complete := range []bool{false, true} {
		if complete {
			m, err = m.RecordServiceClosure(m.Owner, fact, false)
			require.NoError(t, err)
		}
		_, wire, err := encode(m, MaxEncodedBytes)
		require.NoError(t, err)
		loaded, err := decode(wire, m.VMUID, m.Incarnation)
		require.NoError(t, err)
		require.Equal(t, m, loaded)
		require.Equal(t, before.Active, loaded.Active)
		require.Equal(t, before.BudgetHeld, loaded.BudgetHeld)
	}
}

func TestPlannedResumeClosureRejectsProjectionAndIntermediateEffects(t *testing.T) {
	m, request, _ := plannedResumeClosureModel(t)
	closed, err := m.StartServiceClosure(m.Owner, request)
	require.NoError(t, err)
	for _, mode := range []string{"v13", "v11", "no-active", "no-budget", "no-hash", "wrong-hash", "original-open", "wrong-capacity", "wrong-budget", "reason", "abort", "prior-no-effect", "predated"} {
		t.Run(mode, func(t *testing.T) {
			next := closed.Clone()
			c := next.ServiceClosure
			switch mode {
			case "v13":
				next.ProtocolVersion = 13
			case "v11":
				next.ProtocolVersion = 11
			case "no-active":
				next.Active = nil
			case "no-budget":
				next.BudgetHeld = nil
			case "no-hash":
				c.OperationSHA256 = ""
			case "wrong-hash":
				c.OperationSHA256 = "wrong"
			case "original-open":
				original := request
				original.Open = true
				c.Original = &original
			case "wrong-capacity":
				c.Request.ExpectedCapacityGeneration++
			case "wrong-budget":
				c.Request.BudgetCommitmentID = "another-budget"
			case "reason":
				c.Reason = "idle_source_unconfirmed"
			case "abort":
				next.Active.AbortRequested = true
			case "prior-no-effect":
				next.Active.Steps[0].Observation.Result = NoEffect
			case "predated":
				c.StartedSequence = next.BudgetHeld.RecordedSequence
			}
			c.CommandSHA256, _ = c.Request.ContextSHA256()
			require.Error(t, next.Validate())
		})
	}
	started, _, _ := resumeClosureModel(t, false)
	_, err = started.StartServiceClosure(started.Owner, request)
	require.Error(t, err, "已Start必须使用原开放关闭协议，不能抹掉原未知")
	intermediate, _ := heldStart(t, heldBoundModel(t, testModel(t)), 1, runnerSources())
	_, err = intermediate.StartServiceClosure(intermediate.Owner, request)
	require.Error(t, err, "Finalize未确认不能按旧预算关闭合法中间态")
	m.Sequence = math.MaxUint64
	_, err = m.StartServiceClosure(m.Owner, request)
	require.Error(t, err)
}

func TestPlannedResumeClosureContinuationRetainsUnsentStep(t *testing.T) {
	m, request, fact := plannedResumeClosureModel(t)
	m, err := m.StartServiceClosure(m.Owner, request)
	require.NoError(t, err)
	before := m.Clone()
	for _, holder := range []string{"planned-successor-1", "planned-successor-2"} {
		m, err = m.Takeover(m.Owner, holder)
		require.NoError(t, err)
		_, _, previous, selector := m.ServiceClosure.Selection()
		n := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: request, PreviousClose: previous, Close: previous}
		n.Close.IssuerEpoch = m.Owner.Epoch
		m, err = m.StartServiceClosureContinuation(m.Owner, selector, n)
		require.NoError(t, err)
		require.EqualValues(t, 14, m.ProtocolVersion)
		require.Equal(t, before.Active, m.Active)
	}
	n := m.ServiceClosure.Continuation.Request
	proof := control.AdmissionContinuationResponse{ProtocolVersion: 1, Request: n, ContextSHA256: "", ClosedBy: fact, PriorMayHaveApplied: true, VMShrinkPermit: false}
	proof.ContextSHA256, err = n.ContextSHA256()
	require.NoError(t, err)
	proof.ClosedBy.Request, proof.ClosedBy.Observed.IssuerFloorEpoch = n.Close, m.Owner.Epoch
	proof.ClosedBy.ContextSHA256, err = n.Close.ContextSHA256()
	require.NoError(t, err)
	m, err = m.RecordServiceClosureContinuation(m.Owner, proof, false)
	require.NoError(t, err)
	require.False(t, m.ServiceClosure.Pending())
	require.Equal(t, before.Active, m.Active)
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m, loaded)
}

func TestPlannedResumeClosureStoreRequiresConfirmedCAS(t *testing.T) {
	serviceClosureStoreContract(t, plannedResumeClosureModel)
}

func serviceClosureStoreContract(t *testing.T, factory func(*testing.T) (Model, control.AdmissionRequest, control.AdmissionResponse), active ...bool) {
	t.Helper()
	for _, mode := range []string{"ok", "lost-start", "lost-record", "stale-rv", "recreated-vm"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, request, fact := factory(t)
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
			var started *Snapshot
			var ticket *ServiceClosure
			if len(active) == 1 && active[0] {
				started, ticket, err = store.StartActiveServiceClosure(ctx, s, m.Owner, request)
			} else {
				started, ticket, err = store.StartServiceClosure(ctx, s, m.Owner, request)
			}
			if mode == "stale-rv" || mode == "recreated-vm" || mode == "lost-start" {
				require.Error(t, err)
				require.Nil(t, ticket)
				require.Nil(t, started)
				if mode != "lost-start" {
					return
				}
			} else {
				require.NoError(t, err)
				api.mu.Lock()
				api.failAfterCommit = mode == "lost-record"
				api.mu.Unlock()
				_, err = store.RecordServiceClosure(ctx, started, m.Owner, fact, false)
				if mode == "lost-record" {
					require.Error(t, err)
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
