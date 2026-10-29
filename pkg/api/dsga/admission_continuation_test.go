package dsga

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// 两个消费者读取逐字相同的向量。摘要来自独立生成器，不能把Go/Rust
// 互相相等当作规范正确；这里是协议证明，不是真实PG执行或预算释放证明。
func TestAdmissionContinuationVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/admission-continuation-v1.json")
	require.NoError(t, err)
	var vectors []struct {
		Name          string          `json:"name"`
		Valid         bool            `json:"valid"`
		Request       json.RawMessage `json:"request"`
		ContextSHA256 string          `json:"context_sha256"`
	}
	require.NoError(t, json.Unmarshal(raw, &vectors))
	require.Len(t, vectors, 26)
	for _, v := range vectors {
		t.Run(v.Name, func(t *testing.T) {
			var req AdmissionContinuationRequest
			err := json.Unmarshal(v.Request, &req)
			// 历史表是固定v1读者合同。v2的新读者允许旧表中曾被拒绝的2，
			// 但旧读者仍须明确拒绝；新能力使用独立v2向量，不改历史字节。
			if err == nil && req.ProtocolVersion != 1 {
				err = os.ErrInvalid
			}
			if !v.Valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			digest, err := req.ContextSHA256()
			require.NoError(t, err)
			require.Equal(t, v.ContextSHA256, digest)
		})
	}
}

func continuationFixture(t *testing.T) (AdmissionContinuationRequest, AdmissionContinuationResponse) {
	t.Helper()
	raw, err := os.ReadFile("testdata/admission-continuation-v1.json")
	require.NoError(t, err)
	var vectors []struct {
		Request json.RawMessage `json:"request"`
	}
	require.NoError(t, json.Unmarshal(raw, &vectors))
	var req AdmissionContinuationRequest
	require.NoError(t, json.Unmarshal(vectors[0].Request, &req))
	raw, err = os.ReadFile("testdata/response-v1.json")
	require.NoError(t, err)
	var base ControlResponse
	require.NoError(t, json.Unmarshal(raw, &base))
	s := base.Observed
	s.Instance, s.Operation = req.Close.Instance, nil
	s.IssuerFloorEpoch, s.CapacityGeneration = req.Close.IssuerEpoch, req.Close.ExpectedCapacityGeneration
	s.AdmissionRevision, s.AdmissionOpen = req.Close.Revision, false
	digest, err := req.ContextSHA256()
	require.NoError(t, err)
	nativeDigest, err := req.Close.ContextSHA256()
	require.NoError(t, err)
	return req, AdmissionContinuationResponse{
		ProtocolVersion: 1, VMShrinkPermit: false, Request: req, ContextSHA256: digest,
		PriorMayHaveApplied: true, ClosedBy: AdmissionResponse{
			ProtocolVersion: 1, VMShrinkPermit: false, Request: req.Close,
			Disposition: AdmissionApplied, ContextSHA256: nativeDigest, Observed: s,
		},
	}
}

func TestAdmissionContinuationResponseRetainsNativeProof(t *testing.T) {
	req, proof := continuationFixture(t)
	require.NoError(t, proof.ValidateFor(req, req.Close.Instance, false))
	require.Error(t, proof.ValidateFor(req, req.Close.Instance, true))
	query := proof
	query.ClosedBy.Disposition = AdmissionReplayed
	query.ClosedBy.Observed.IssuerFloorEpoch++
	require.NoError(t, query.ValidateFor(req, req.Close.Instance, true))
	require.Error(t, query.ValidateFor(req, req.Close.Instance, false))
	for name, mutate := range map[string]func(*AdmissionContinuationResponse){
		"wrapper-protocol": func(p *AdmissionContinuationResponse) { p.ProtocolVersion = 2 },
		"vm-permit":        func(p *AdmissionContinuationResponse) { p.VMShrinkPermit = true },
		"prior-denied":     func(p *AdmissionContinuationResponse) { p.PriorMayHaveApplied = false },
		"wrapper-digest":   func(p *AdmissionContinuationResponse) { p.ContextSHA256 = strings.Repeat("0", 64) },
		"predecessor":      func(p *AdmissionContinuationResponse) { p.Request.PreviousClose.BudgetCommitmentID = "other" },
		"native-digest":    func(p *AdmissionContinuationResponse) { p.ClosedBy.ContextSHA256 = strings.Repeat("0", 64) },
		"native-open":      func(p *AdmissionContinuationResponse) { p.ClosedBy.Observed.AdmissionOpen = true },
		"native-budget":    func(p *AdmissionContinuationResponse) { p.ClosedBy.Request.BudgetCommitmentID = "other" },
		"wrong-boot":       func(p *AdmissionContinuationResponse) { p.ClosedBy.Observed.Instance.PGBootID-- },
		"wrong-generation": func(p *AdmissionContinuationResponse) { p.ClosedBy.Observed.CapacityGeneration++ },
		"not-found": func(p *AdmissionContinuationResponse) {
			p.ClosedBy.Disposition = AdmissionNotFound
			p.ClosedBy.Observed.AdmissionRevision--
		},
		"expired": func(p *AdmissionContinuationResponse) {
			p.ClosedBy.Disposition = AdmissionResultExpired
			p.ClosedBy.Observed.AdmissionRevision++
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := proof
			mutate(&bad)
			require.Error(t, bad.ValidateFor(req, req.Close.Instance, false))
			require.Error(t, bad.ValidateFor(req, req.Close.Instance, true))
		})
	}
}

func TestAdmissionContinuationStrictJSON(t *testing.T) {
	req, proof := continuationFixture(t)
	for _, value := range []any{req, proof} {
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		for _, invalid := range []string{
			strings.Replace(string(raw), `"protocol_version":1`, `"protocol_version":null`, 1),
			strings.Replace(string(raw), `"protocol_version":1`, `"protocol_version":1,"protocol_version":1`, 1),
			strings.Replace(string(raw), `"protocol_version":1,`, ``, 1),
			strings.Replace(string(raw), `"protocol_version":1`, `"Protocol_Version":1`, 1),
			strings.Replace(string(raw), `"protocol_version":1`, `"protocol_version":1,"extra":false`, 1),
			string(raw) + " {}",
		} {
			if _, ok := value.(AdmissionContinuationRequest); ok {
				var decoded AdmissionContinuationRequest
				require.Error(t, json.Unmarshal([]byte(invalid), &decoded))
			} else {
				var decoded AdmissionContinuationResponse
				require.Error(t, json.Unmarshal([]byte(invalid), &decoded))
			}
		}
	}
}

// 新表由独立Python生成，覆盖容量前进、u64边界、版本协商及不合法降级。
// 这里只验证协议；单个Resize允许的代次和派发权限仍由外层持久模型认证。
func TestAdmissionContinuationCapacityVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/admission-continuation-v2.json")
	require.NoError(t, err)
	var vectors []struct {
		Name          string          `json:"name"`
		Valid         bool            `json:"valid"`
		Request       json.RawMessage `json:"request"`
		ContextSHA256 string          `json:"context_sha256"`
	}
	require.NoError(t, json.Unmarshal(raw, &vectors))
	require.Len(t, vectors, 37)
	for _, v := range vectors {
		t.Run(v.Name, func(t *testing.T) {
			var request AdmissionContinuationRequest
			err := json.Unmarshal(v.Request, &request)
			if !v.Valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			digest, err := request.ContextSHA256()
			require.NoError(t, err)
			require.Equal(t, v.ContextSHA256, digest)
		})
	}
}

func TestAdmissionContinuationCapacityProofRequiresSelectedVersion(t *testing.T) {
	req, proof := continuationFixture(t)
	req.ProtocolVersion = 2
	req.Close.ExpectedCapacityGeneration++
	proof.ProtocolVersion, proof.Request, proof.ClosedBy.Request = 2, req, req.Close
	var err error
	proof.ContextSHA256, err = req.ContextSHA256()
	require.NoError(t, err)
	proof.ClosedBy.ContextSHA256, err = req.Close.ContextSHA256()
	require.NoError(t, err)
	proof.ClosedBy.Observed.CapacityGeneration = req.Close.ExpectedCapacityGeneration
	require.NoError(t, proof.ValidateFor(req, req.Close.Instance, false))
	bad := proof
	bad.ProtocolVersion = 1
	require.Error(t, bad.ValidateFor(req, req.Close.Instance, false), "包装不能降级但保留v2原文")
	proof.ClosedBy.Disposition = AdmissionReplayed
	proof.ClosedBy.Observed.CapacityGeneration++
	require.NoError(t, proof.ValidateFor(req, req.Close.Instance, true), "原关闭后容量前进不能抹去原生回执")
	proof.ClosedBy.Observed.CapacityGeneration = req.Close.ExpectedCapacityGeneration - 1
	require.Error(t, proof.ValidateFor(req, req.Close.Instance, true))
}
