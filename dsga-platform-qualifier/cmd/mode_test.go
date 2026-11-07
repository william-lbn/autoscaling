package main

import (
	"context"
	"path/filepath"
	"testing"
)

func TestQualificationSupplyRejectsAmbiguousOrMissingInputs(t *testing.T) {
	for mask := 0; mask < 8; mask++ {
		p := []string{"", "", ""}
		count := 0
		for n := 0; n < 3; n++ {
			if mask&(1<<n) != 0 {
				p[n] = "input"
				count++
			}
		}
		if oneMode(p...) != (count == 1) {
			t.Fatal("ambiguous mode")
		}
	}
	for _, publish := range []bool{false, true} {
		if err := runCollectMode(context.Background(), filepath.Join(t.TempDir(), "missing"), "", publish); err == nil {
			t.Fatal("missing original input reached publication")
		}
	}
}
