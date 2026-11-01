package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalOrdinaryBuildSelectionIsExplicit(t *testing.T) {
	for _, mode := range []string{"legacy", normalSplitBuildMode, normalSplitV3BuildMode} {
		if e := validateNormalOrdinaryV2BuildSelection(mode, 1, nil); e != nil {
			t.Fatal(e)
		}
		for _, n := range []uint{0, 2, 3, 1<<32 + 2} {
			if e := validateNormalOrdinaryV2BuildSelection(mode, n, nil); e == nil {
				t.Fatal("missing lock accepted")
			}
			e := validateNormalOrdinaryV2BuildSelection(mode, n, []byte("{}"))
			if (e == nil) != (mode == normalSplitV3BuildMode && n == 2) {
				t.Fatal("selection silently upgraded")
			}
		}
	}
	if validateNormalOrdinaryV2BuildSelection(normalSplitV3BuildMode, 2, make([]byte, 4<<20+1)) == nil {
		t.Fatal("unbounded lock")
	}
}
func TestNormalOrdinaryBuildLockRetainsOriginalBytes(t *testing.T) {
	p := filepath.Join(t.TempDir(), "input.json")
	raw := []byte("{\n  \"original\":true\n}\n")
	if e := os.WriteFile(p, raw, 0600); e != nil {
		t.Fatal(e)
	}
	got, e := readNormalOrdinaryV2BuildLock(normalSplitV3BuildMode, 2, p)
	if e != nil || string(got) != string(raw) {
		t.Fatal(e)
	}
	for _, mode := range []string{"legacy", normalSplitBuildMode} {
		if _, e = readNormalOrdinaryV2BuildLock(mode, 2, p); e == nil {
			t.Fatal("legacy read lock")
		}
	}
	if _, e = readNormalOrdinaryV2BuildLock(normalSplitV3BuildMode, 1, p); e == nil {
		t.Fatal("lock without explicit2")
	}
	if _, e = readNormalOrdinaryV2BuildLock(normalSplitV3BuildMode, 2, filepath.Dir(p)); e == nil {
		t.Fatal("directory accepted")
	}
}
