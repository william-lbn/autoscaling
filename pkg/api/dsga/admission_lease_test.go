package dsga

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdmissionLeaseStrictCASProof(t *testing.T) {
	raw, err := os.ReadFile("testdata/response-v1.json")
	require.NoError(t, err)
	var base ControlResponse
	require.NoError(t, json.Unmarshal(raw, &base))
	s := base.Observed
	s.Operation = nil
	s.IssuerFloorEpoch, s.AdmissionRevision, s.AdmissionOpen = 2, 1, true
	request := AdmissionLeaseRequest{ProtocolVersion: 1, Instance: s.Instance, IssuerEpoch: 2, ExpectedCapacityGeneration: s.CapacityGeneration, ExpectedAdmissionRevision: 1, ExpectedDeadlineNS: 100, Action: "renew"}
	fact := AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Expired: false, Request: request, Renewed: true, Armed: true, DeadlineNS: 200, LeaseEpoch: 2, TimeoutMS: 30000, Observed: s}
	require.NoError(t, fact.ValidateFor(request, s.Instance))
	for name, change := range map[string]func(*AdmissionLeaseResponse){
		"permit":       func(p *AdmissionLeaseResponse) { p.VMShrinkPermit = true },
		"deadline":     func(p *AdmissionLeaseResponse) { p.DeadlineNS = 100 },
		"expired-open": func(p *AdmissionLeaseResponse) { p.Expired = true },
		"unarmed":      func(p *AdmissionLeaseResponse) { p.Armed = false },
		"owner":        func(p *AdmissionLeaseResponse) { p.LeaseEpoch = 3 },
		"capacity":     func(p *AdmissionLeaseResponse) { p.Observed.CapacityGeneration++ },
		"revision":     func(p *AdmissionLeaseResponse) { p.Observed.AdmissionRevision++ },
		"timeout":      func(p *AdmissionLeaseResponse) { p.TimeoutMS = 0 },
		"echo":         func(p *AdmissionLeaseResponse) { p.Request.Action = "rearm" },
	} {
		t.Run(name, func(t *testing.T) { bad := fact; change(&bad); require.Error(t, bad.ValidateFor(request, s.Instance)) })
	}
	encoded, err := json.Marshal(request)
	require.NoError(t, err)
	for _, bad := range []string{strings.Replace(string(encoded), `"action":"renew"`, `"action":"renew","action":"renew"`, 1), strings.Replace(string(encoded), `"renew"`, `"heartbeat"`, 1), strings.Replace(string(encoded), `"expected_deadline_ns":"100"`, `"expected_deadline_ns":100`, 1), strings.Replace(string(encoded), `"expected_deadline_ns":"100",`, ``, 1), strings.Replace(string(encoded), `"issuer_epoch":"2"`, `"issuer_epoch":null`, 1)} {
		var decoded AdmissionLeaseRequest
		require.Error(t, json.Unmarshal([]byte(bad), &decoded))
	}
	request.Action = "rearm"
	fact.Request = request
	require.Error(t, fact.ValidateFor(request, s.Instance), "rearm不能以开放状态发证")
	fact.Observed.AdmissionOpen = false
	require.NoError(t, fact.ValidateFor(request, s.Instance))
}
