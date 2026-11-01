//go:build linux && dsga_native_memory_origin_wire

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
)

// PG17-S-MEMORY-ORIGIN-WIRE-GUEST-001：真实固定ISO/virtio/KernelPrep+原8WAL/region/6–7。
// DEV-only停在memory来源；不调用/吞掉正式Provision或CheckGuest失败，不签normal。
func TestNativeNormalMemoryOriginGuestWireComponent(t *testing.T) {
	if os.Getenv("DSGA_MEMORY_ORIGIN_GUEST_NATIVE") != "isolated369-memory-origin-v1" {
		t.Fatal("explicit isolated Guest component required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	source, raw, assets, e := initialGuestChannelInputs()
	if e != nil {
		t.Fatal(e)
	}
	plan, e := DeriveRuntimeBootRequirements(raw, assets)
	if e != nil {
		t.Fatal(e)
	}
	if plan.MemoryOriginProtocolVersion() != 1 {
		t.Fatal("fixed ISO did not select new origin protocol")
	}
	var port *initialGuestStream
	for {
		port, e = openInitialGuestPort(ctx)
		if e == nil {
			break
		}
		if !errors.Is(e, errInitialGuestPortPending) {
			t.Fatal(e)
		}
		if e = initialGuestWait(ctx); e != nil {
			t.Fatal(e)
		}
	}
	defer port.close()
	challenge, e := bootprotocol.ReadInitialPrebootChallenge(port)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = initialPrebootBindInputs(challenge, raw, assets, source.Config); e != nil {
		t.Fatal(e)
	}
	g, e := initialGuestProvisionInputs(source, raw)
	if e != nil {
		t.Fatal(e)
	}
	if e = initialGuestExchangeWithRequirements(ctx, port, g, challenge, false, plan); e != nil {
		t.Fatal(e)
	}
	s, e := lockState(ctx, g.config.StateDir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	origin, e := g.recoverNormalMemoryOriginLocked(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	if e = origin.recheckLocked(ctx, s, g); e != nil {
		t.Fatal(e)
	}
	if origin.intent.MaximumBytes != uint64(challenge.Maximum) {
		t.Fatal("actual original Mmax differs")
	}
	if origin.intent.Region.RegionBytes+challenge.OriginalBase != challenge.Maximum {
		t.Fatal("region/base did not cross actual Host maximum")
	}
	b, e := os.ReadFile(filepath.Join(g.config.StateDir, normalMemoryGuestCompleteFile))
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(g.config.StateDir, "memory-origin-replacement.tmp")
	if e = os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(path, filepath.Join(g.config.StateDir, normalMemoryGuestCompleteFile)); e != nil {
		t.Fatal(e)
	}
	if e = origin.recheckLocked(ctx, s, g); e == nil {
		t.Fatal("same bytes new inode inherited durable source")
	}
	recovered, e := g.recoverNormalMemoryOriginLocked(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	if e = recovered.recheckLocked(ctx, s, g); e != nil {
		t.Fatal(e)
	}
	data := map[string]any{"stable_id": "PG17-S-MEMORY-ORIGIN-WIRE-GUEST-001", "Guest_boot": origin.intent.Physical.Identity.GuestBootID, "maximum_bytes": origin.intent.MaximumBytes, "region": origin.intent.Region, "host_reference_sha256": origin.complete.Ack.HostReferenceSHA256, "Guest_source_sha256": origin.intentSHA, "actual_new6_7": true, "same_bytes_inode_rejected": true, "explicit_recovery_passed": true, "normal_authorized": false, "E": 0}
	out, e := json.Marshal(data)
	if e != nil {
		t.Fatal(e)
	}
	fmt.Printf("NORMAL_MEMORY_GUEST_SOURCE=%s\n", out)
}
