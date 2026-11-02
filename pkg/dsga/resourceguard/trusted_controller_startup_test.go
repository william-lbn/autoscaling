package resourceguard

import (
	"strings"
	"testing"
)

func TestTrustedControllerFixedStartupDoesNotBlockPID1(t *testing.T) {
	rows := TrustedControllerInitEntries()
	old, _ := NormalSystemSourceInitEntriesForVersion(3)
	if strings.Join(rows, "\n") == strings.Join(old, "\n") {
		t.Fatal("trusted contract lost fixed direct services")
	}
	var root, compute bool
	for _, row := range rows {
		if strings.Contains(row, "exec-system-role") {
			t.Fatal("trusted path still requires role broker")
		}
		if strings.HasPrefix(row, "::wait:") {
			t.Fatal("WAIT prevents required respawn startup")
		}
		if row == "::respawn:/neonvm/bin/sh /neonvm/bin/dsga-resource-service resource" {
			root = true
		}
		if row == "::respawn:/neonvm/bin/sh /neonvm/bin/dsga-resource-service compute" {
			compute = true
		}
	}
	if !root || !compute {
		t.Fatal("production Root/Compute lifecycle missing")
	}
	again := TrustedControllerInitEntries()
	rows[0] = "mutated"
	if again[0] != "::sysinit:/neonvm/bin/vminit" {
		t.Fatal("caller changed fixed table")
	}
}
