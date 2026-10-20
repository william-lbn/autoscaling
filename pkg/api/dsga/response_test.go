package dsga

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// response-v1是人工协议快照，不宣称真实物理动作。context-v1的规范文本/摘要
// 单独由准确486338原生PG的jsonb_out/sha256生成，包含高位和引号/反斜线/HTML字节。
func goldenResponse(t testing.TB) ([]byte, ControlResponse) {
	t.Helper()
	raw, err := os.ReadFile("testdata/response-v1.json")
	require.NoError(t, err)
	var fact ControlResponse
	require.NoError(t, json.Unmarshal(raw, &fact))
	return raw, fact
}

func TestContextDigestMatchesNativePGCanonicalVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/context-v1.json")
	require.NoError(t, err)
	var vectors []struct {
		Request   Request `json:"request"`
		Canonical string  `json:"canonical"`
		SHA256    string  `json:"sha256"`
	}
	require.NoError(t, json.Unmarshal(raw, &vectors))
	require.Len(t, vectors, 2)
	for _, vector := range vectors {
		hash, err := vector.Request.ContextSHA256()
		require.NoError(t, err)
		require.Equal(t, vector.SHA256, hash)
		var value any
		decoder := json.NewDecoder(strings.NewReader(vector.Canonical))
		decoder.UseNumber()
		require.NoError(t, decoder.Decode(&value))
		var canonical bytes.Buffer
		require.NoError(t, writeContext(&canonical, value))
		require.Equal(t, vector.Canonical, canonical.String())
	}
}

func TestReceiptIsIndependentFromLaterObservedOperation(t *testing.T) {
	_, request := goldenRequest(t)
	_, fact := goldenResponse(t)
	fact.Observed.Operation.Key.Revision++
	fact.Observed.Operation.ExpectedCapacityGeneration = 3
	fact.Observed.CapacityGeneration = 4
	fact.Observed.HighBuffers = 2048
	fact.Observed.LowBuffers = 2048
	fact.Observed.Operation.TargetBuffers = 2048
	require.NoError(t, fact.ValidateResize(request, request.Key.Instance))
	require.EqualValues(t, 1024, fact.Receipt.Result.HighBuffers)
	require.EqualValues(t, 2048, fact.Observed.HighBuffers)
	// 仅kind/Buffer/generation相同不足以证明原意图相同：外层VM、预算和期限也在
	// 原生context摘要中。保留同Key改变其中任一字段，不能借用旧成功回执。
	for _, change := range []func(*Request){
		func(r *Request) { r.BudgetCommitmentID += "-changed" },
		func(r *Request) { r.TargetVMMemoryBytes++ },
		func(r *Request) { r.DeadlineUnixMillis-- },
	} {
		changed := request
		change(&changed)
		require.Error(t, fact.ValidateResize(changed, request.Key.Instance))
	}
}

func TestResponseRejectsMixedIdentitiesAndImpossibleProviderFacts(t *testing.T) {
	_, request := goldenRequest(t)
	for _, change := range []func(*ControlResponse){
		func(f *ControlResponse) { f.VMShrinkPermit = true },
		func(f *ControlResponse) { f.Receipt.Key.Revision++ },
		func(f *ControlResponse) { f.Observed.Instance.PGBootID-- },
		func(f *ControlResponse) { f.Receipt.Result.Operation.Key.Revision++ },
		func(f *ControlResponse) { f.Receipt.HasResult = false },
		func(f *ControlResponse) { f.Receipt.Disposition = ReceiptBusy },
		func(f *ControlResponse) { f.Observed.Arrays[1].Array = Descriptors },
		func(f *ControlResponse) { f.Observed.Arrays[0].Provider.PhysicalReleaseVerified = true },
		func(f *ControlResponse) {
			f.Observed.Arrays[0].Provider.RequestedOffset = ^Uint64(0)
			f.Observed.Arrays[0].Provider.RequestedBytes = 1
		},
		func(f *ControlResponse) { f.Observed.Operation.CancelRequested = true },
		func(f *ControlResponse) { f.Observed.Operation.Reason = ReasonTimeout },
		func(f *ControlResponse) { f.Observed.Operation.ContextSHA256 = strings.Repeat("A", 64) },
		func(f *ControlResponse) { f.Receipt.Disposition = "future_unknown" },
	} {
		_, fact := goldenResponse(t)
		change(&fact)
		require.Error(t, fact.ValidateResize(request, request.Key.Instance))
	}
}

func TestOldReleaseAndNewUnknownAllocationRemainDistinct(t *testing.T) {
	_, request := goldenRequest(t)
	_, fact := goldenResponse(t)
	array := &fact.Observed.Arrays[0]
	array.AllocationUnknown = true
	array.ReleaseKey = &request.Key
	array.ReleaseCapacityGeneration = 1
	array.ReleaseState = ReleaseVerified
	array.Provider.HasResult = true
	array.Provider.Status = ProviderOK
	array.Provider.AffectedBytes = 4096
	array.Provider.PhysicalReleaseVerified = true
	require.NoError(t, fact.ValidateResize(request, request.Key.Instance))
	array.Provider.ReleaseMayBePartial = true
	require.Error(t, fact.ValidateResize(request, request.Key.Instance))
}

func TestResponseRequiresExplicitFieldsAndRejectsNullBooleansAtomically(t *testing.T) {
	raw, original := goldenResponse(t)
	for _, body := range []string{
		strings.Replace(string(raw), `"vm_shrink_permit": false`, `"vm_shrink_permit": null`, 1),
		strings.Replace(string(raw), `"cancel_requested": false`, `"cancel_requested": null`, 1),
		strings.Replace(string(raw), `"physical_release_verified": false`, `"physical_release_verified": null`, 1),
		strings.Replace(string(raw), `"release_key": null,`, "", 1),
		strings.Replace(string(raw), "{", `{"protocol_version":1,`, 1),
		strings.Replace(string(raw), "{", `{"unexpected":false,`, 1),
		strings.Replace(string(raw), `"has_result"`, `"Has_Result"`, 1),
	} {
		fact := original
		require.Error(t, json.Unmarshal([]byte(body), &fact), body)
		require.Equal(t, original, fact)
	}
}
