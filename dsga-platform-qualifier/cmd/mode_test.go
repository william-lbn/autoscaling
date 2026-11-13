package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/qualificationissuer"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func TestAutomaticQualificationRetriesOnlyReadOnlyCollection(t *testing.T) {
	transient := errors.New("original sources changed during collection")
	for _, test := range []struct {
		name      string
		automatic bool
		first     error
		calls     int
	}{
		{"automatic", true, transient, 2},
		{"manual", false, transient, 1},
		{"unauthorized", true, apierrors.NewUnauthorized("fixture"), 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			collect := func(ctx context.Context) (qualificationissuer.Collection, error) {
				calls++
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("read has no bound")
				}
				if calls == 1 {
					return qualificationissuer.Collection{}, test.first
				}
				return qualificationissuer.Collection{SnapshotSHA256: "fixture"}, nil
			}
			result, err := collectQualification(context.Background(), collect, test.automatic, 5*time.Second)
			if calls != test.calls || test.calls == 2 && (err != nil || result.SnapshotSHA256 != "fixture") ||
				test.calls == 1 && !errors.Is(err, test.first) {
				t.Fatalf("collection outcome differs: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestAutomaticQualificationStopsAtFrozenDeadline(t *testing.T) {
	reads := 0
	transient := errors.New("original source not settled")
	_, err := collectQualification(context.Background(), func(context.Context) (qualificationissuer.Collection, error) {
		reads++
		return qualificationissuer.Collection{}, transient
	}, true, 20*time.Millisecond)
	if !errors.Is(err, transient) || reads != 1 {
		t.Fatalf("collector retried past the frozen deadline: reads=%d err=%v", reads, err)
	}
}

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
