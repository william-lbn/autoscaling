package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestQualificationJobMakesOriginalEmptyDirPrivate(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("Kubernetes qualification Job runs as root")
	}
	parent := t.TempDir()
	volume := filepath.Join(parent, "empty-dir")
	if err := os.Mkdir(volume, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(volume, 0o777); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(volume, link); err != nil {
		t.Fatal(err)
	}
	if err := restrictJobOutputVolume(link); err == nil {
		t.Fatal("output volume symlink was accepted")
	}
	if before, err := os.Stat(volume); err != nil || before.Mode().Perm() != 0o777 {
		t.Fatal("symlink changed original target")
	}
	if err := restrictJobOutputVolume(volume); err != nil {
		t.Fatal(err)
	}
	if after, err := os.Stat(volume); err != nil || after.Mode().Perm() != 0o700 {
		t.Fatal("original emptyDir is not private")
	}
	evidence := filepath.Join(volume, "evidence")
	if err := prepareAutomaticOutput(evidence); err != nil {
		t.Fatal(err)
	}
	if err := prepareAutomaticOutput(evidence); err == nil {
		t.Fatal("existing evidence directory was accepted")
	}
}

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
