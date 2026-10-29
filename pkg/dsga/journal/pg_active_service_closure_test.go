package journal

import (
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func TestPGActiveClosureRetainsUnknownAndBoundsCapacity(t *testing.T) {
	for _, kind := range []control.Kind{control.Grow, control.Shrink} {
		for _, advanced := range []bool{false, true} {
			t.Run(string(kind)+map[bool]string{false: "-original", true: "-committed"}[advanced], func(t *testing.T) {
				m, request, fact := activeClosureModel(t, kind, ResizePG, true)
				before := m.Clone()
				if advanced {
					request.ExpectedCapacityGeneration++
					fact.Request = request
					fact.ContextSHA256, _ = request.ContextSHA256()
					fact.Observed.CapacityGeneration++
				}
				closed, err := m.StartActiveServiceClosure(m.Owner, request)
				require.NoError(t, err)
				require.EqualValues(t, 17, closed.ProtocolVersion)
				require.Equal(t, before.Active, closed.Active)
				require.Equal(t, before.InitialHeld, closed.InitialHeld)
				closed, err = closed.RecordServiceClosure(closed.Owner, fact, false)
				require.NoError(t, err)
				_, wire, err := encode(closed, MaxEncodedBytes)
				require.NoError(t, err)
				loaded, err := decode(wire, m.VMUID, m.Incarnation)
				require.NoError(t, err)
				require.Equal(t, closed, loaded)
				old := closed.Clone()
				old.ProtocolVersion = 16
				require.Error(t, old.Validate(), "旧读者不能收养已派发PG的关闭")
				for _, generation := range []control.Uint64{0, request.ExpectedCapacityGeneration + 2} {
					bad := request
					bad.ExpectedCapacityGeneration = generation
					_, err = m.StartActiveServiceClosure(m.Owner, bad)
					require.Error(t, err)
				}
			})
		}
	}
}

func TestPGActiveClosureCapacityContinuationPreservesOriginalWAL(t *testing.T) {
	m, request, fact := activeClosureModel(t, control.Grow, ResizePG, true)
	before := m.Clone()
	m, err := m.StartActiveServiceClosure(m.Owner, request)
	require.NoError(t, err)
	originalWAL := *m.ServiceClosure
	next := control.AdmissionContinuationRequest{ProtocolVersion: 2, Original: request, PreviousClose: request, Close: request}
	next.Close.ExpectedCapacityGeneration++
	next.Close.Revision++
	m, err = m.StartServiceClosureContinuation(m.Owner, m.ServiceClosure.CommandSHA256, next)
	require.NoError(t, err, "原PG提交后同owner自动恢复有界关闭")
	require.Equal(t, before.Active, m.Active)
	saved := m.Clone()
	_, err = m.StartServiceClosureContinuation(m.Owner, m.ServiceClosure.Continuation.CommandSHA256, next)
	require.Error(t, err, "同代次不能重复换选择")
	m, err = m.Takeover(m.Owner, "pg-closure-successor")
	require.NoError(t, err)
	successor := next
	successor.PreviousClose = next.Close
	successor.Close.IssuerEpoch = m.Owner.Epoch
	m, err = m.StartServiceClosureContinuation(m.Owner, m.ServiceClosure.Continuation.CommandSHA256, successor)
	require.NoError(t, err)
	prior := *m.ServiceClosure
	prior.Continuation = nil
	require.Equal(t, originalWAL, prior, "原始关闭未知及序列不能被接续覆盖")
	fact.Request = successor.Close
	fact.ContextSHA256, _ = fact.Request.ContextSHA256()
	fact.Observed.CapacityGeneration = successor.Close.ExpectedCapacityGeneration
	fact.Observed.AdmissionRevision = successor.Close.Revision
	fact.Observed.IssuerFloorEpoch = m.Owner.Epoch
	digest, err := successor.ContextSHA256()
	require.NoError(t, err)
	proof := control.AdmissionContinuationResponse{ProtocolVersion: 2, VMShrinkPermit: false, Request: successor, ContextSHA256: digest, ClosedBy: fact, PriorMayHaveApplied: true}
	m, err = m.RecordServiceClosureContinuation(m.Owner, proof, false)
	require.NoError(t, err)
	require.Equal(t, before.Active, m.Active)
	require.Equal(t, before.InitialHeld, m.InitialHeld)
	for _, mode := range []string{"protocol-downgrade", "extra-generation", "rollback", "future-proof", "rewrite-pg"} {
		t.Run(mode, func(t *testing.T) {
			bad := saved.Clone()
			n := bad.ServiceClosure.Continuation
			switch mode {
			case "protocol-downgrade":
				n.Request.ProtocolVersion = 1
			case "extra-generation":
				n.Request.Close.ExpectedCapacityGeneration++
			case "rollback":
				n.Request.Close.ExpectedCapacityGeneration--
			case "future-proof":
				bad = m.Clone()
				bad.ServiceClosure.Continuation.Receipt.ClosedBy.Observed.CapacityGeneration++
			case "rewrite-pg":
				bad.Active.AbortRequested = true
			}
			n.CommandSHA256, _ = n.Request.ContextSHA256()
			require.Error(t, bad.Validate())
		})
	}
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m, loaded)
}

func TestPGActiveClosureStoreRequiresConfirmedCAS(t *testing.T) {
	serviceClosureStoreContract(t, func(t *testing.T) (Model, control.AdmissionRequest, control.AdmissionResponse) {
		return activeClosureModel(t, control.Grow, ResizePG, true)
	}, true)
}

func TestPGActiveClosureProtectsPostShrinkResourceSteps(t *testing.T) {
	for _, action := range []Action{ReleasePG, GuestReady, ResizeVM, ObserveVM} {
		for _, unknown := range []bool{false, true} {
			t.Run(string(action)+map[bool]string{false: "-planned", true: "-unknown"}[unknown], func(t *testing.T) {
				m, request, fact := activeClosureModel(t, control.Shrink, action, unknown)
				before := m.Clone()
				request.ExpectedCapacityGeneration++
				fact.Request = request
				fact.ContextSHA256, _ = request.ContextSHA256()
				fact.Observed.CapacityGeneration++
				closed, err := m.StartActiveServiceClosure(m.Owner, request)
				require.NoError(t, err)
				require.EqualValues(t, 17, closed.ProtocolVersion)
				closed, err = closed.RecordServiceClosure(closed.Owner, fact, false)
				require.NoError(t, err)
				require.Equal(t, before.Active, closed.Active)
				require.Equal(t, before.InitialHeld, closed.InitialHeld)
			})
		}
	}
}
