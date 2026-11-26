package budget

import (
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"testing"
)

func allocationPlanFixture(t *testing.T) AllocationPlan {
	q, base, source := allocationFixture(t)
	from, err := q.Targets[0].Selector()
	require.NoError(t, err)
	to, err := q.Targets[1].Selector()
	require.NoError(t, err)
	transition, err := q.Transition(source, from, to, base)
	require.NoError(t, err)
	request := control.Request{ProtocolVersion: 1,
		Key: control.Key{Instance: control.Instance{VMUID: base.Identity.VMUID,
			ComputeInstanceID: base.Identity.ComputeInstanceID, PGBootID: 1},
			IssuerEpoch: 1, Revision: 1, OperationID: control.OperationID{High: 1, Low: 2}},
		Kind: control.Shrink, ExpectedCapacityGeneration: 1, TargetBuffers: transition.To.Buffers,
		TargetVMMemoryBytes: transition.To.VMMemoryBytes, BudgetCommitmentID: "original-budget", DeadlineUnixMillis: 123456}
	p := AllocationPlan{ProtocolVersion: 1, Scope: "retained_parent_normal", Request: request, Resource: base.Identity,
		Transition: transition, LFCMaximumBytes: 64 << 20, LFCGranuleBytes: 1 << 20, HoldCompleteParent: true}
	require.NoError(t, p.Validate())
	return p
}

func TestAllocationPlanFreezesFullVectorAndRetainsParentBudget(t *testing.T) {
	p := allocationPlanFixture(t)
	require.Equal(t, control.Uint64(512<<20), p.Transition.Parent.Limits.MemoryMaxBytes)
	require.Equal(t, control.Uint64(272<<20), p.Transition.To.Limits.MemoryMaxBytes)
	hash, err := p.SHA256()
	require.NoError(t, err)
	copy := p.Clone()
	copy.Transition.To.NoncachePrivateUpperBytes -= 1 << 20
	copy.Transition.To.CacheNormalUpperBytes += 1 << 20
	changed, err := copy.SHA256()
	require.NoError(t, err)
	require.NotEqual(t, hash, changed, "同VM同normal总数仍是不同的完整分区")
	require.NotEqual(t, p.Transition.To.NoncachePrivateUpperBytes, copy.Transition.To.NoncachePrivateUpperBytes)
	bad := p.Clone()
	bad.HoldCompleteParent = false
	require.Error(t, bad.Validate(), "较小candidate不授予父预算释放")
	bad = p.Clone()
	bad.Request.TargetBuffers++
	require.Error(t, bad.Validate())
	bad = p.Clone()
	bad.Transition.PeakLimits.MemoryMaxBytes--
	require.Error(t, bad.Validate())
	q, base, source := allocationFixture(t)
	from, _ := q.Targets[0].Selector()
	to, _ := q.Targets[1].Selector()
	_, err = q.Transition(source, to, from, base)
	require.Error(t, err, "正向资格不能借为反向边")
}

func TestAllocationSuccessorRequiresExactStoppedOriginalAndActualPGGeneration(t *testing.T) {
	p := allocationPlanFixture(t)
	first, err := p.LFCRequest(3, false, 1, nil)
	require.NoError(t, err)
	require.Equal(t, p.Transition.From.LFCLimitBytes, first.FromLimitBytes)
	require.Zero(t, first.ToLimitBytes)
	original, requested := first, first
	digest, _ := first.ContextSHA256()
	proof := control.LFCAllocationResponse{ProtocolVersion: 1, Disposition: "replayed", Phase: "applied",
		ContextFormat: "postgres_jsonb_out_ascii_v1", OriginalContextSHA256: &digest, RequestedContextSHA256: &digest,
		Original: &original, Requested: &requested,
		Observed: control.LFCAllocationObservation{PGBootID: 1, CapacityGeneration: 1, IssuerFloorEpoch: 1,
			LFCGeneration: 1, LFCSequence: 1, MaxBytes: 64 << 20, ChunkBytes: 1 << 20, Controlled: true,
			OriginalCurrent: true, DescriptorTrackingComplete: true, SynchronousTrackingComplete: true}}
	next, err := p.LFCRequest(6, true, 2, &proof)
	require.NoError(t, err)
	require.Equal(t, first.Key, next.Key)
	require.Equal(t, first.AllocationPlanSHA256, next.AllocationPlanSHA256)
	require.Equal(t, control.Uint64(2), next.ExpectedCapacityGeneration)
	require.Equal(t, proof.Observed.LFCGeneration, next.ExpectedLFCGeneration)
	require.Equal(t, p.Transition.To.LFCLimitBytes, next.ToLimitBytes)
	require.Zero(t, next.FromLimitBytes)
	for _, mutate := range []func(*control.LFCAllocationResponse){
		func(f *control.LFCAllocationResponse) { f.Observed.SynchronousIOScopes++ },
		func(f *control.LFCAllocationResponse) { f.Observed.DescriptorReferences++ },
		func(f *control.LFCAllocationResponse) { f.Observed.PrewarmActive = true },
		func(f *control.LFCAllocationResponse) { f.Original.BudgetCommitmentID = "other" },
		func(f *control.LFCAllocationResponse) { f.Original.DeadlineUnixMillis++ },
		func(f *control.LFCAllocationResponse) { f.Original.ExpectedLFCSequence++ },
		func(f *control.LFCAllocationResponse) { f.Original.AllocationPlanSHA256 = string(make([]byte, 64)) },
	} {
		raw, e := json.Marshal(proof)
		require.NoError(t, e)
		var changed control.LFCAllocationResponse
		require.NoError(t, json.Unmarshal(raw, &changed))
		mutate(&changed)
		_, e = p.LFCRequest(6, true, 2, &changed)
		require.Error(t, e)
	}
}
