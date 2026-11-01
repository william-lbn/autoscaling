//go:build linux

package resourceguard

import (
	"strings"
	"testing"
)

func TestNormalStockPackageLockRequiresOriginalSystemEntry(t *testing.T) {
	hash := strings.Repeat("ab", 32)
	entry := NormalSystemSourceFile{Path: NormalSystemOrdinaryV2PackageLockPath, CanonicalPath: NormalSystemOrdinaryV2PackageLockPath, Kind: "config", Bytes: 31, SHA256: hash}
	mf := normalOrdinaryStockUdevManifest{PackageLockSHA256: hash}
	system := NormalSystemSourceManifest{ProtocolVersion: 3, Files: []NormalSystemSourceFile{entry}}
	got, e := normalOrdinaryStockUdevPackageLockEntry(system, mf)
	if e != nil || *got != entry {
		t.Fatal(e)
	}
	for _, kind := range []string{"missing", "wrong-hash", "alias", "executable", "oversize", "old-system"} {
		t.Run(kind, func(t *testing.T) {
			s := system
			s.Files = append([]NormalSystemSourceFile(nil), system.Files...)
			switch kind {
			case "missing":
				s.Files = nil
			case "wrong-hash":
				s.Files[0].SHA256 = strings.Repeat("cd", 32)
			case "alias":
				s.Files[0].CanonicalPath = "/tmp/lock"
			case "executable":
				s.Files[0].Kind = "exec"
			case "oversize":
				s.Files[0].Bytes = 256<<10 + 1
			case "old-system":
				s.ProtocolVersion = 2
			}
			if _, e := normalOrdinaryStockUdevPackageLockEntry(s, mf); e == nil {
				t.Fatal("self reported digest accepted without original lock")
			}
		})
	}
}
