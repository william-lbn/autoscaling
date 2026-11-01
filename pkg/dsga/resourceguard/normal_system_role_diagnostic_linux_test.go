//go:build linux

package resourceguard

import (
	"context"
	"sync"
	"testing"
)

func TestNormalRoleDiagnosticAbsentAndSnapshotIsolation(t *testing.T) {
	normalSystemRoleDiagnosticRecord(nil, normalRoleDiagKillClientPIDFD, "udev-init", 1, 0)
	normalSystemRoleDiagnosticRecord(context.Background(), normalRoleDiagFrameDecoded, "udev-init", 1, 0)
	d := &normalSystemRoleDiagnostic{}
	ctx := context.WithValue(context.Background(), normalSystemRoleDiagnosticKey{}, d)
	normalSystemRoleDiagnosticRecord(ctx, normalRoleDiagFrameDecoded, "udev-init", 123, 7)
	a := d.snapshot()
	if a.Count != 1 || a.Events[0].DecisionUnixNano != 123 || a.Events[0].RecordedUnixNano < 123 || a.Events[0].Value != 7 {
		t.Fatal(a)
	}
	a.Events[0].Value = 999
	if d.snapshot().Events[0].Value != 7 {
		t.Fatal("snapshot changed original fixed data")
	}
}
func TestNormalRoleDiagnosticConcurrentFixedCapacity(t *testing.T) {
	d := &normalSystemRoleDiagnostic{changed: make(chan struct{}, 1)}
	ctx := context.WithValue(context.Background(), normalSystemRoleDiagnosticKey{}, d)
	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			normalSystemRoleDiagnosticRecord(ctx, normalRoleDiagBeforeRecord, "udev-init", 0, 0)
		}()
	}
	wg.Wait()
	got := d.snapshot()
	if got.Count != 64 || len(got.Events) != 64 || got.Dropped != 64 {
		t.Fatal(got.Count, len(got.Events), got.Dropped)
	}
}
