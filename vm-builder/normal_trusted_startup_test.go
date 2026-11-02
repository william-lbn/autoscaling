package main

import (
	"encoding/json"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"strings"
	"testing"
)

func TestNormalTrustedControllerUsesOriginalComputeAndReadyGate(t *testing.T) {
	p := normalTestPlan()
	p.ExecutionModel = resourceguard.TrustedControllerExecutionModel
	raw, e := json.Marshal(p)
	if e != nil {
		t.Fatal(e)
	}
	decoded, e := decodeNormalBuildPlan(raw)
	if e != nil {
		t.Fatal(e)
	}
	s := normalTrustedComputeScript(decoded)
	if !strings.Contains(s, "dsga-resource-guard wait-controller-ready") || !strings.Contains(s, "/neonvm/bin/su-exec") || strings.Contains(s, "exec-system-role") {
		t.Fatal("trusted production Compute path lost actual Root wait or original nonroot exec")
	}
	if strings.Contains(normalComputeScript(normalTestPlan()), "wait-controller-ready") {
		t.Fatal("default script changed")
	}
	p.ExecutionModel = "trusted-controller-v2"
	raw, _ = json.Marshal(p)
	if _, e = decodeNormalBuildPlan(raw); e == nil {
		t.Fatal("unknown execution model accepted")
	}
}
