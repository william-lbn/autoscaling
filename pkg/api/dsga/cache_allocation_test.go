package dsga

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func lfcRequestFixture() LFCAllocationRequest {
	return LFCAllocationRequest{ProtocolVersion: 1,
		Key: Key{Instance: Instance{VMUID: "vm", ComputeInstanceID: "compute", PGBootID: 1},
			IssuerEpoch: 1, Revision: 2, OperationID: OperationID{High: 3, Low: 4}},
		StepOrdinal: 5, ExpectedCapacityGeneration: 1, ExpectedLFCGeneration: 0, ExpectedLFCSequence: 0,
		FromLimitBytes: 32 << 20, ToLimitBytes: 0, AllocationPlanSHA256: strings.Repeat("a", 64),
		BudgetCommitmentID: "original-budget", DeadlineUnixMillis: 1234}
}

func lfcResponseFixture(r LFCAllocationRequest) LFCAllocationResponse {
	requested, original := r, r
	digest, _ := r.ContextSHA256()
	return LFCAllocationResponse{ProtocolVersion: 1, Disposition: "applied", Phase: "applied",
		ContextFormat: "postgres_jsonb_out_ascii_v1", OriginalContextSHA256: &digest, RequestedContextSHA256: &digest,
		Original: &original, Requested: &requested,
		Observed: LFCAllocationObservation{PGBootID: 1, CapacityGeneration: 1, IssuerFloorEpoch: 1,
			LFCGeneration: 1, LFCSequence: 1, MaxBytes: 64 << 20, ChunkBytes: 1 << 20, Controlled: true,
			DescriptorTrackingComplete: true, SynchronousTrackingComplete: true, OriginalCurrent: true}}
}

func TestLFCOriginalResponseDoesNotBorrowSuccessorOrPhysicalRelease(t *testing.T) {
	request := lfcRequestFixture()
	fact := lfcResponseFixture(request)
	require.NoError(t, fact.ValidateRequest(request))
	require.True(t, fact.AppliedFor(request))
	require.True(t, fact.SynchronousExitFor(request))
	for name, edit := range map[string]func(*LFCAllocationResponse){
		"old-boot":               func(f *LFCAllocationResponse) { f.Observed.PGBootID++ },
		"physical-release":       func(f *LFCAllocationResponse) { f.Observed.ReleaseAuthorized = true },
		"kernel-release":         func(f *LFCAllocationResponse) { f.Observed.KernelReleaseProven = true },
		"successor":              func(f *LFCAllocationResponse) { f.Original.StepOrdinal++ },
		"missing-original":       func(f *LFCAllocationResponse) { f.Original = nil },
		"different-budget":       func(f *LFCAllocationResponse) { f.Original.BudgetCommitmentID = "other" },
		"response-other-request": func(f *LFCAllocationResponse) { f.Requested.StepOrdinal++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := lfcResponseFixture(request)
			edit(&changed)
			require.False(t, changed.AppliedFor(request))
			require.False(t, changed.SynchronousExitFor(request))
		})
	}
	for name, edit := range map[string]func(*LFCAllocationResponse){
		"live-fd":        func(f *LFCAllocationResponse) { f.Observed.DescriptorReferences = 1 },
		"unknown-fd":     func(f *LFCAllocationResponse) { f.Observed.DescriptorTrackingComplete = false },
		"old-access":     func(f *LFCAllocationResponse) { f.Observed.SynchronousOperations = 1 },
		"old-io":         func(f *LFCAllocationResponse) { f.Observed.SynchronousIOScopes = 1 },
		"unknown-access": func(f *LFCAllocationResponse) { f.Observed.SynchronousTrackingComplete = false },
		"prewarm":        func(f *LFCAllocationResponse) { f.Observed.PrewarmActive = true },
	} {
		t.Run(name, func(t *testing.T) {
			changed := lfcResponseFixture(request)
			edit(&changed)
			require.True(t, changed.AppliedFor(request), "逻辑安装不能被误当成同步退出")
			require.False(t, changed.SynchronousExitFor(request))
		})
	}
}

func TestLFCWireRetainsExplicitZeroAndStrictOriginalFields(t *testing.T) {
	original := lfcRequestFixture()
	raw, err := json.Marshal(original)
	require.NoError(t, err)
	var decoded LFCAllocationRequest
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, original, decoded)
	fields := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal(raw, &fields))
	delete(fields, "expected_lfc_generation")
	missing, _ := json.Marshal(fields)
	require.Error(t, json.Unmarshal(missing, &decoded))
	require.Equal(t, original, decoded, "失败解码不能改掉原意图")
	for _, bad := range []string{
		strings.Replace(string(raw), `"expected_lfc_generation":"0"`, `"expected_lfc_generation":0`, 1),
		strings.Replace(string(raw), `"expected_lfc_sequence":"0"`, `"expected_lfc_sequence":"00"`, 1),
		strings.TrimSuffix(string(raw), "}") + `,"release_authorized":true}`,
		strings.Replace(string(raw), `"step_ordinal":"5"`, `"step_ordinal":"0"`, 1),
	} {
		require.Error(t, json.Unmarshal([]byte(bad), &decoded))
	}
	a, err := original.ContextSHA256()
	require.NoError(t, err)
	original.StepOrdinal++
	b, err := original.ContextSHA256()
	require.NoError(t, err)
	require.NotEqual(t, a, b, "两个原生子步骤不能借用同一完整意图哈希")
}
