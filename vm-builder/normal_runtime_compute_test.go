package main

import (
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"strings"
	"testing"
)

func TestNormalTrustedGenericComputeRetainsGuardsAndExactSlot(t *testing.T) {
	p := normalTestPlan()
	p.ExecutionModel = resourceguard.TrustedControllerExecutionModel
	p.ComputeIdentitySource = resourceguard.RuntimeComputeIdentitySource
	p.ComputeArgv = append(p.ComputeArgv, "--compute-id", resourceguard.RuntimeComputeIdentityPlaceholder, "--config", resourceguard.RuntimeComputePublicPath)
	if _, err := decodeNormalBuildPlan(normalTestJSON(p)); err != nil {
		t.Fatal(err)
	}
	script := normalTrustedComputeScript(p)
	for _, part := range []string{"check-guest", "wait-controller-ready", "runtime-compute-id", "flock -x", "su-exec 1000:1000", "env -i", `--compute-id "$compute_identity"`, resourceguard.RuntimeComputePublicPath} {
		if !strings.Contains(script, part) {
			t.Fatalf("missing fixed startup protection %s", part)
		}
	}
	if strings.Contains(script, resourceguard.RuntimeComputeIdentityPlaceholder) || strings.Contains(script, "eval ") {
		t.Fatal("unexpanded identity or eval in launcher")
	}
	old := normalTestPlan()
	old.ExecutionModel = resourceguard.TrustedControllerExecutionModel
	if strings.Contains(normalTrustedComputeScript(old), "runtime-compute-id") {
		t.Fatal("old fixed Guest startup changed")
	}
	p.ComputeArgv = append(p.ComputeArgv, "--compute-id", "other")
	if _, err := decodeNormalBuildPlan(normalTestJSON(p)); err == nil {
		t.Fatal("duplicate identity accepted")
	}
}
