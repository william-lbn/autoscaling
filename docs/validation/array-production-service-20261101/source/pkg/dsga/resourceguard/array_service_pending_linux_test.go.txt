//go:build linux

package resourceguard

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// PG17-U-ARRAY-SERVICE-001：真实O_EXCL/flock/WAL；内核写者明确model。
// 新动作与同原Replay都不能跨缺原pin的历史，不把空/可见JSON清成NoEffect。
func TestArrayServiceOrphanHistoryPreventsCapWrites(t *testing.T) {
	for _, name := range []string{"native-birth.orphan.pending.json", "array-registry.orphan.registered.json", "array-rpc.orphan.pending.json", "array-populate.orphan.pending.json"} {
		t.Run(name, func(t *testing.T) {
			g, k, c := fixtureGuard(t)
			path := filepath.Join(g.config.StateDir, name)
			raw := []byte(`{}`)
			if e := os.WriteFile(path, raw, 0600); e != nil {
				t.Fatal(e)
			}
			for _, call := range []func() (Evidence, error){func() (Evidence, error) { return g.Apply(context.Background(), c) }, func() (Evidence, error) { return g.ReplayOriginal(context.Background(), c) }} {
				if _, e := call(); !errors.Is(e, ErrUnknown) {
					t.Fatal("original unknown was bypassed", e)
				}
				k.mu.Lock()
				writes := k.writes
				k.mu.Unlock()
				if writes != 0 {
					t.Fatal("pending reached actual cap writer model", writes)
				}
				after, e := os.ReadFile(path)
				if e != nil || string(after) != string(raw) {
					t.Fatal("gate changed original pending", e)
				}
			}
		})
	}
}

// PG17-U-ARRAY-SERVICE-002：旧域无数组历史时零写通过；不是新profile资格。
func TestArrayServiceLegacyGateHasNoEffects(t *testing.T) {
	g, _, _ := fixtureGuard(t)
	e := g.withRecord(context.Background(), func(r *record, s *lockedState) error {
		before, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
		if e != nil {
			return e
		}
		if e = g.rejectArrayPendingLocked(s, r); e != nil {
			return e
		}
		after, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
		if e != nil {
			return e
		}
		if string(before) != string(after) {
			t.Fatal("read-only gate modified legacy source")
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
}

// PG17-U-ARRAY-SERVICE-003：RPC完整可见不等于本Root fsync；新Root、同字节
// 新inode、新nonce均不能借原见证。helper/PG来源是明确模型，磁盘IO是真实U。
func TestArrayRPCDurabilityAndOriginalFileIdentity(t *testing.T) {
	s, p, z := arrayPopulateDurabilityFixture(t)
	if _, e := arrayPopulateAppend(s, p.Nonce, "complete", z); e != nil {
		t.Fatal(e)
	}
	q := arrayRegistryWireFixture()
	q.Op = arrayRegistryPopulate
	q.Array = 1
	q.Bytes = q.StartupBuffers * q.Ranges[1].Element
	q.RegistrationNonce[0] = 9
	packet, e := encodeArrayRegistryWire(q)
	if e != nil {
		t.Fatal(e)
	}
	key := physicalDigest(packet)
	nonce, e := newNativeBirthNonce()
	if e != nil {
		t.Fatal(e)
	}
	intent := arrayRPCIntent{ProtocolVersion: 1, Scope: "original-four-array-rpc-v1", Nonce: nonce, Birth: NativeBirthRef{ProtocolVersion: 1, Nonce: p.Nonce, PendingSHA256: strings.Repeat("a", 64)}, RegistrationNonce: hex.EncodeToString(q.RegistrationNonce[:]), RequestHex: hex.EncodeToString(packet), RequestSHA256: key, ResourceFloor: 1}
	intentRaw, _ := json.Marshal(intent)
	if e = s.createPhysicalFile(arrayRPCName(key, "pending"), intentRaw); e != nil {
		t.Fatal(e)
	}
	doneRaw, _ := json.Marshal(arrayRPCComplete{ProtocolVersion: 1, IntentSHA256: physicalDigest(intentRaw), HelperNonces: []string{p.Nonce}})
	if e = s.createPhysicalFile(arrayRPCName(key, "complete"), doneRaw); e != nil {
		t.Fatal(e)
	}
	journal := new(arrayRPCJournal)
	before := map[string][]byte{}
	for _, phase := range []string{"pending", "complete"} {
		name := arrayRPCName(key, phase)
		before[name], e = os.ReadFile(filepath.Join(s.dir.Name(), name))
		if e != nil {
			t.Fatal(e)
		}
	}
	if e = journal.known(s, key, intent); !errors.Is(e, ErrUnknown) {
		t.Fatal("visibility became durability", e)
	}
	for name, raw := range before {
		after, e := os.ReadFile(filepath.Join(s.dir.Name(), name))
		if e != nil || !reflect.DeepEqual(raw, after) {
			t.Fatal("ordinary observation wrote original RPC", e)
		}
	}
	if e = journal.confirm(s, key); e != nil {
		t.Fatal(e)
	}
	if e = journal.known(s, key, intent); e != nil {
		t.Fatal(e)
	}
	if e = new(arrayRPCJournal).known(s, key, intent); !errors.Is(e, ErrUnknown) {
		t.Fatal("new Root inherited witness", e)
	}
	changed := intent
	changed.Nonce = strings.Repeat("b", 64)
	if e = journal.known(s, key, changed); !errors.Is(e, ErrUnknown) {
		t.Fatal("different nonce reused witness", e)
	}
	name := filepath.Join(s.dir.Name(), arrayRPCName(key, "complete"))
	replacement := name + ".new"
	if e = os.WriteFile(replacement, doneRaw, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(replacement, name); e != nil {
		t.Fatal(e)
	}
	if e = journal.known(s, key, intent); !errors.Is(e, ErrUnknown) {
		t.Fatal("same bytes in replacement inode reused durability", e)
	}
}
