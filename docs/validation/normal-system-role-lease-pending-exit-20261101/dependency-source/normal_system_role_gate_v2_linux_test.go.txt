//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"testing"
)

func TestNormalSystemRoleGateV2WireBindsExplicitCapability(t *testing.T) {
	c := normalSystemRoleGateCommand{ProtocolVersion: 2, Role: "allow-start", GateNonce: physicalDigest([]byte("gate")), OriginalNonce: physicalDigest([]byte("original")), PendingSHA256: physicalDigest([]byte("pending"))}
	v2, e := normalSystemRoleGateEncode(c)
	if e != nil {
		t.Fatal(e)
	}
	if string(v2[:8]) != "NSYSGO02" {
		t.Fatal("wrong version")
	}
	out, e := normalSystemRoleGateDecode(v2)
	if e != nil || out != c {
		t.Fatal("roundtrip failed", e)
	}
	c.ProtocolVersion = 1
	v1, e := normalSystemRoleGateEncode(c)
	if e != nil || string(v1[:8]) != "NSYSGO01" {
		t.Fatal("legacy changed", e)
	}
	for _, raw := range [][]byte{v1, v2} {
		mutated := bytes.Clone(raw)
		if string(raw[:8]) == "NSYSGO01" {
			copy(mutated, "NSYSGO02")
		} else {
			copy(mutated, "NSYSGO01")
		}
		if _, e := normalSystemRoleGateDecode(mutated); e == nil {
			t.Fatal("cross-version magic accepted")
		}
	}
	c.ProtocolVersion = 2
	c.Role = normalSystemRoleBrokerKind
	if _, e := normalSystemRoleGateEncode(c); e == nil {
		t.Fatal("broker cannot inherit ordinary2 gate")
	}
	c.Role = "allow-start"
	c.ProtocolVersion = 3
	if _, e := normalSystemRoleGateEncode(c); e == nil {
		t.Fatal("future protocol accepted")
	}
}
func TestNormalSystemRoleV2MissingRuntimeCannotSpawnOrPublish(t *testing.T) {
	if _, e := originalNormalSystemRoleV2Runtime(nil); e == nil {
		t.Fatal("no original broker runtime")
	}
	p := &normalSystemRoleV2Publication{}
	if _, e := p.spawnFiles(); e == nil {
		t.Fatal("no original OFDs")
	}
	if e := p.originalDetachSucceeded(context.Background()); e == nil {
		t.Fatal("no original stopped candidate")
	}
	i := &normalSystemRoleInstalledMain{}
	if e := i.recheckOriginalInstalledSourcesLocked(context.Background(), nil); e == nil {
		t.Fatal("missing controls/stdio/creator must not authorize")
	}
}
