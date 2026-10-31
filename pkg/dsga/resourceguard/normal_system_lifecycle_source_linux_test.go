//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestNormalSystemLifecycleMissingOriginalInputNeverQualifies(t *testing.T) {
	if _, e := newNormalSystemOriginalTask(normalSystemOriginalTaskSource{}); e == nil {
		t.Fatal("numeric-only/missing original descriptors accepted")
	}
	if _, e := newNormalSystemOriginalTask(normalSystemOriginalTaskSource{PID: 123, StartTicks: 1, GuestBootID: "12345678-1234-1234-1234-123456789abc"}); e == nil {
		t.Fatal("original pidfd was reconstructed")
	}
	s := &normalSystemLifecycleSource{observed: normalSystemLifecycleObservation{Scope: normalSystemLifecycleScope, MissingSources: []string{"original-spawn"}}, files: map[string]*os.File{}}
	if e := s.requireContinuousQualification(); !errors.Is(e, ErrUnknown) {
		t.Fatal("static bytes became continuous authority")
	}
	out, e := s.snapshot()
	if e != nil {
		t.Fatal(e)
	}
	out.MissingSources[0] = "changed"
	again, e := s.snapshot()
	if e != nil || again.MissingSources[0] != "original-spawn" {
		t.Fatal("observation mutated opaque source")
	}
	if _, e = s.inspectOriginalTask(context.Background(), nil, "resource", []string{}, []string{}); !errors.Is(e, ErrUnknown) {
		t.Fatal("unbound process accepted")
	}
	s.close()
	if _, e = s.snapshot(); !errors.Is(e, ErrUnknown) {
		t.Fatal("closed source accepted")
	}
}
