package dsga

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdmissionContextMatchesNativePGVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/admission-context-v1.json")
	require.NoError(t, err)
	var vectors []struct { Request AdmissionRequest `json:"request"`; ContextSHA256 string `json:"context_sha256"` }
	require.NoError(t, json.Unmarshal(raw, &vectors))
	require.Len(t, vectors, 2)
	for _, v := range vectors {
		digest, err := v.Request.ContextSHA256()
		require.NoError(t,err)
		require.Equal(t,v.ContextSHA256,digest)
	}
}

func TestAdmissionStrictContextAndProof(t *testing.T) {
	raw, err := os.ReadFile("testdata/response-v1.json")
	require.NoError(t, err)
	var baseline ControlResponse
	require.NoError(t, json.Unmarshal(raw, &baseline))
	request := AdmissionRequest{ProtocolVersion: 1, Instance: baseline.Observed.Instance, IssuerEpoch: 1, Revision: 1, ExpectedCapacityGeneration: 2, Open: true, BudgetCommitmentID: "budget-admission"}
	encoded, err := json.Marshal(request)
	require.NoError(t, err)
	var decoded AdmissionRequest
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, request, decoded)
	for _, malformed := range []string{
		strings.Replace(string(encoded), `"open":true`, `"open":true,"open":false`, 1),
		strings.Replace(string(encoded), `"open":true`, `"open":null`, 1),
		strings.Replace(string(encoded), `"open":true,`, ``, 1),
		strings.Replace(string(encoded), `"revision":"1"`, `"revision":1`, 1),
		strings.Replace(string(encoded), `"open":true`, `"Open":true`, 1),
	} {
		require.Error(t, json.Unmarshal([]byte(malformed), &decoded), malformed)
	}
	observed := baseline.Observed
	observed.Operation = nil
	observed.IssuerFloorEpoch, observed.AdmissionRevision, observed.AdmissionOpen = 1, 1, true
	digest, err := request.ContextSHA256()
	require.NoError(t, err)
	response := AdmissionResponse{ProtocolVersion: 1, Request: request, Disposition: AdmissionApplied, ContextSHA256: digest, Observed: observed}
	require.NoError(t, response.ValidateFor(request, request.Instance, false))
	require.Error(t, response.ValidateFor(request, request.Instance, true))
	response.Disposition = AdmissionReplayed
	response.Observed.IssuerFloorEpoch = 2
	require.NoError(t, response.ValidateFor(request, request.Instance, true))
	require.Error(t, response.ValidateFor(request, request.Instance, false))
	response.Disposition = AdmissionNotFound
	require.Error(t, response.ValidateFor(request, request.Instance, true))
	closeIntent := request
	closeIntent.Open = false
	closeIntent.Revision = 2
	require.NoError(t, closeIntent.ClosesOver(request))
	closeIntent.Instance.PGBootID++
	require.Error(t, closeIntent.ClosesOver(request))
}
