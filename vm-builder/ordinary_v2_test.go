package main

import (
	"bytes"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"os"
	"path/filepath"
	"testing"
)

func TestOrdinaryV2BuilderAccountPlanKeepsOriginalAndRejectsCollision(t *testing.T) {
	p := []byte("root:x:0:0:r:/:/bin/sh\npostgres:x:1000:1000:p:/:/bin/sh\n")
	g := []byte("root:x:0:\npostgres:x:1000:\n")
	before := bytes.Clone(p)
	plan, e := ordinaryV2AccountPlan(p, g, 1000, 1000)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(before, p) || len(plan.PasswdAdditions) == 0 {
		t.Fatal("original account source mutated")
	}
	if e := resourceguard.ValidateNormalSystemOrdinaryV2InstalledAccounts(append(bytes.Clone(p), plan.PasswdAdditions...), append(bytes.Clone(g), plan.GroupAdditions...), 1000, 1000); e != nil {
		t.Fatal(e)
	}
	if _, e := ordinaryV2AccountPlan(append(bytes.Clone(p), plan.PasswdAdditions...), g, 1000, 1000); e == nil {
		t.Fatal("adopted existing role")
	}
}
func TestOrdinaryV2BuilderRejectsAccountAlias(t *testing.T) {
	root := t.TempDir()
	if e := os.Mkdir(filepath.Join(root, "etc"), 0755); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(root, "etc/actual"), []byte("root:x:0:0:r:/:/bin/sh\n"), 0644); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink("actual", filepath.Join(root, "etc/passwd")); e != nil {
		t.Fatal(e)
	}
	if _, e := ordinaryV2AccountPlanForRoot(root, 1000, 1000); e == nil {
		t.Fatal("account alias accepted")
	}
}
