package resourceguard

import (
	"strings"
	"testing"
)

func TestNormalSystemV3ColdStartupOrdering(t *testing.T) {
	entries, e := NormalSystemSourceInitEntriesForVersion(3)
	if e != nil {
		t.Fatal(e)
	}
	initCount, udevCount := 0, 0
	for _, line := range entries {
		if strings.HasSuffix(line, "exec-system-role udev-init") {
			initCount++
			if !strings.HasPrefix(line, "::once:") {
				t.Fatalf("cold init must not block RESPWAN phase: %s", line)
			}
		}
		if strings.HasSuffix(line, "exec-system-role udev") {
			udevCount++
			if !strings.HasPrefix(line, "::respawn:") {
				t.Fatalf("udev daemon restart contract changed: %s", line)
			}
		}
	}
	if initCount != 1 || udevCount != 1 {
		t.Fatalf("fixed roles differ: init=%d udev=%d", initCount, udevCount)
	}
	for _, version := range []uint32{1, 2} {
		previous, e := NormalSystemSourceInitEntriesForVersion(version)
		if e != nil {
			t.Fatal(e)
		}
		found := false
		for _, line := range previous {
			if strings.Contains(line, "udev-init") {
				found = true
				if !strings.HasPrefix(line, "::wait:") {
					t.Fatalf("old version %d unexpectedly changed: %s", version, line)
				}
			}
		}
		if !found {
			t.Fatalf("missing old init v%d", version)
		}
	}
}
