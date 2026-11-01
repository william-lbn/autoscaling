//go:build linux

package main

import (
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalStockFinalInventoryPinsActualPackageLock(t *testing.T) {
	root := normalStockBuilderFixture(t)
	raw, e := generateNormalSystemManifestForVersion(root, normalTestPlan(), strings.Repeat("ab", 32), 3)
	if e != nil {
		t.Fatal(e)
	}
	m, e := resourceguard.DecodeNormalSystemSourceManifest(raw)
	if e != nil {
		t.Fatal(e)
	}
	found := false
	name := resourceguard.NormalSystemOrdinaryV2PackageLockPath
	actual := normalV3MustRead(t, filepath.Join(root, name))
	for _, f := range m.Files {
		if f.Path == name {
			found = f.Kind == "config" && f.CanonicalPath == name && f.SHA256 == normalDigest(actual) && f.Bytes == uint64(len(actual))
		}
	}
	if !found {
		t.Fatal("package lock omitted or self reported")
	}
	if e = os.WriteFile(filepath.Join(root, name), []byte("foreign lock bytes"), 0444); e != nil {
		t.Fatal(e)
	}
	if _, e = generateNormalSystemManifestForVersion(root, normalTestPlan(), strings.Repeat("ab", 32), 3); e == nil {
		t.Fatal("changed actual package lock accepted")
	}
}
