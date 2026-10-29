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

func TestAdmissionRearmHistoricalProofDoesNotImplyCurrentService(t *testing.T) {
	raw, err := os.ReadFile("testdata/response-v1.json")
	require.NoError(t, err)
	var base ControlResponse
	require.NoError(t, json.Unmarshal(raw, &base))
	s := base.Observed
	s.Operation = nil
	s.IssuerFloorEpoch, s.AdmissionRevision, s.AdmissionOpen = 3, 1, false
	request := AdmissionLeaseRequest{ProtocolVersion: 1, Instance: s.Instance, IssuerEpoch: 2, ExpectedCapacityGeneration: s.CapacityGeneration, ExpectedAdmissionRevision: 1, ExpectedDeadlineNS: 100, Action: "rearm"}
	current := AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, Renewed: false, Armed: true, Expired: true, DeadlineNS: 300, LeaseEpoch: 2, TimeoutMS: 30000, Observed: s}
	fact := AdmissionRearmStatus{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, Retained: true, AppliedDeadlineNS: 200, Current: current}
	require.NoError(t, fact.ValidateFor(request, s.Instance), "历史成功可与当前过期同时成立")
	for name, change := range map[string]func(*AdmissionRearmStatus){
		"permit":                func(p *AdmissionRearmStatus) { p.VMShrinkPermit = true },
		"applied-before-cas":    func(p *AdmissionRearmStatus) { p.AppliedDeadlineNS = 100 },
		"applied-after-current": func(p *AdmissionRearmStatus) { p.AppliedDeadlineNS = 301 },
		"unknown-with-proof":    func(p *AdmissionRearmStatus) { p.Retained = false },
		"query-renewed":         func(p *AdmissionRearmStatus) { p.Current.Renewed = true },
		"unarmed":               func(p *AdmissionRearmStatus) { p.Current.Armed = false },
		"old-lease":             func(p *AdmissionRearmStatus) { p.Current.LeaseEpoch = 1 },
		"old-floor":             func(p *AdmissionRearmStatus) { p.Current.Observed.IssuerFloorEpoch = 1 },
		"old-capacity": func(p *AdmissionRearmStatus) {
			p.Current.Observed.CapacityGeneration = request.ExpectedCapacityGeneration - 1
		},
		"old-admission": func(p *AdmissionRearmStatus) { p.Current.Observed.AdmissionRevision = 0 },
		"echo":          func(p *AdmissionRearmStatus) { p.Request.ExpectedDeadlineNS++ },
		"nested-echo":   func(p *AdmissionRearmStatus) { p.Current.Request.ExpectedDeadlineNS++ },
		"expired-open":  func(p *AdmissionRearmStatus) { p.Current.Observed.AdmissionOpen = true },
	} {
		t.Run(name, func(t *testing.T) { bad := fact; change(&bad); require.Error(t, bad.ValidateFor(request, s.Instance)) })
	}
	unknown := fact
	unknown.Retained, unknown.AppliedDeadlineNS = false, 0
	require.NoError(t, unknown.ValidateFor(request, s.Instance))
	encoded, err := json.Marshal(fact)
	require.NoError(t, err)
	for _, bad := range []string{
		strings.Replace(string(encoded), `"retained":true`, `"retained":true,"retained":true`, 1),
		strings.Replace(string(encoded), `"applied_deadline_ns":"200"`, `"applied_deadline_ns":200`, 1),
		strings.Replace(string(encoded), `"request":`, `"extra":true,"request":`, 1),
	} {
		var decoded AdmissionRearmStatus
		require.Error(t, json.Unmarshal([]byte(bad), &decoded))
	}
	request.Action = "renew"
	wrong := fact
	wrong.Request, wrong.Current.Request = request, request
	require.Error(t, wrong.ValidateFor(request, s.Instance))
}
