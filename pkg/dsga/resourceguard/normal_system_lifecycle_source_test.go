package resourceguard

import (
	"strings"
	"testing"
)

func TestNormalSystemLifecycleFixedVectorsAndNoEnvironmentDisclosure(t *testing.T) {
	for _, v := range []struct {
		name string
		env  []string
		ok   bool
	}{
		{"empty", []string{}, true}, {"minimal", []string{"PATH=/neonvm/bin"}, true},
		{"ld", []string{"LD_PRELOAD=sensitive-test-only"}, false},
		{"glibc", []string{"GLIBC_TUNABLES=sensitive-test-only"}, false},
		{"musl", []string{"MUSL_LOCPATH=sensitive-test-only"}, false},
		{"shell", []string{"ENV=sensitive-test-only"}, false},
		{"repeat", []string{"PATH=x", "PATH=y"}, false},
		{"bad-name", []string{"bad_name=sensitive-test-only"}, false},
		{"nul", []string{"TOKEN=sensitive-test-only\x00more"}, false},
	} {
		t.Run(v.name, func(t *testing.T) {
			e := normalSystemCleanEnvironment(v.env)
			if (e == nil) != v.ok {
				t.Fatalf("canonical environment accepted=%t", e == nil)
			}
			if e != nil && strings.Contains(e.Error(), "sensitive-test-only") {
				t.Fatal("environment value leaked")
			}
		})
	}
	a, e := normalSystemVectorSHA([]string{"a", "b"})
	if e != nil {
		t.Fatal(e)
	}
	b, e := normalSystemVectorSHA([]string{"b", "a"})
	if e != nil || a == b {
		t.Fatal("vector order was lost")
	}
	for _, v := range [][]string{nil, {strings.Repeat("a", 4097)}, make([]string, 129)} {
		if _, e := normalSystemVectorSHA(v); e == nil {
			t.Fatal("unbounded vector accepted")
		}
	}
}
func TestNormalSystemLifecycleFixedRoleTableCannotSelectProgram(t *testing.T) {
	for _, role := range []string{"allow-start", "kernel-version", "network", "udev", "udev-init", "acpi", "logs", "clock", "daemon", "resource"} {
		args, e := normalSystemFixedRoleArgv(role)
		if e != nil || len(args) == 0 || !strings.HasPrefix(args[0], "/neonvm/bin/") {
			t.Fatal("fixed role absent", role)
		}
		original := args[0]
		args[0] = "/tmp/changed"
		again, e := normalSystemFixedRoleArgv(role)
		if e != nil || again[0] != original {
			t.Fatal("role table exposed mutable alias")
		}
	}
	for _, role := range []string{"", "/bin/sh", "root", "sshd", "compute"} {
		if _, e := normalSystemFixedRoleArgv(role); e == nil {
			t.Fatal("role bypassed manifest-bound selection")
		}
	}
}
