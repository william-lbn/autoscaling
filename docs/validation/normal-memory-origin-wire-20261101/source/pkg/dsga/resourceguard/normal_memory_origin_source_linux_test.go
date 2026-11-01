//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
)

func TestNormalMemoryOriginGuestRejectsUnboundInputsBeforeEffects(t *testing.T) {
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	s, e := lockState(context.Background(), dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	g := &Guard{}
	if _, e = captureNormalMemoryOriginLocked(nil, s, g, bootprotocol.InitialPrebootReceipt{}, bootprotocol.InitialMovableReceipt{}, bootprotocol.InitialCompletion{}, bootprotocol.NormalMemoryOriginHostReference{}); !errors.Is(e, ErrUnknown) {
		t.Fatalf("unbound capture: %v", e)
	}
	if _, e = g.recoverNormalMemoryOriginLocked(context.Background(), nil); !errors.Is(e, ErrUnknown) {
		t.Fatalf("missing original lock: %v", e)
	}
	if _, e = g.normalMemoryOriginCurrentLocked(context.Background(), s, normalMemoryOriginIntent{}); !errors.Is(e, ErrUnknown) {
		t.Fatalf("self-asserted source: %v", e)
	}
	for _, n := range []string{normalMemoryGuestIntentFile, normalMemoryGuestCompleteFile} {
		if _, e = os.Stat(filepath.Join(dir, n)); !os.IsNotExist(e) {
			t.Fatalf("invalid inputs created source history %s: %v", n, e)
		}
	}
}
func TestNormalMemoryOriginGuestJournalDoesNotBorrowReplacedInode(t *testing.T) {
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	s, e := lockState(context.Background(), dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	raw := []byte("{}")
	for _, n := range []string{normalMemoryGuestIntentFile, normalMemoryGuestCompleteFile} {
		if e = os.WriteFile(filepath.Join(dir, n), raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
	first, e := normalMemoryGuestJournalIdentity(s)
	if e != nil {
		t.Fatal(e)
	}
	tmp := filepath.Join(dir, "replacement")
	if e = os.WriteFile(tmp, raw, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(tmp, filepath.Join(dir, normalMemoryGuestCompleteFile)); e != nil {
		t.Fatal(e)
	}
	changed, e := normalMemoryGuestJournalIdentity(s)
	if e != nil {
		t.Fatal(e)
	}
	if first == changed {
		t.Fatal("same bytes inherited original durable inode")
	}
	if e = os.Chmod(filepath.Join(dir, normalMemoryGuestIntentFile), 0644); e != nil {
		t.Fatal(e)
	}
	if _, e = normalMemoryGuestJournalIdentity(s); e == nil {
		t.Fatal("writable/shared source file accepted")
	}
}

// 生产者必须重核opaque原Runtime选择；不能仅靠normal369名字或外给wire版本。
func TestNormalMemoryOriginGuestRequiresExplicitImmutableSelector(t *testing.T) {
	c, raw, assets, config := initialPrebootFixture(t)
	old, e := initialPrebootBindInputs(c, raw, assets, config)
	if e != nil {
		t.Fatal(e)
	}
	if normalMemoryGuestSelection(old) == nil {
		t.Fatal("old normal369 v1 implicitly selected new origin protocol")
	}
	var d RuntimeDocument
	if e = json.Unmarshal(raw, &d); e != nil {
		t.Fatal(e)
	}
	templates, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	if e != nil {
		t.Fatal(e)
	}
	for i := range templates.Templates {
		if templates.Templates[i].Asset != "dsga-normal-split.json" {
			continue
		}
		var fields map[string]any
		if e = json.Unmarshal(templates.Templates[i].Policy, &fields); e != nil {
			t.Fatal(e)
		}
		fields["protocol_version"] = 2
		fields["memory_origin_protocol_version"] = 1
		templates.Templates[i].Policy, e = json.Marshal(fields)
		if e != nil {
			t.Fatal(e)
		}
	}
	d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(templates.Templates))
	current, e := json.Marshal(d)
	if e != nil {
		t.Fatal(e)
	}
	rendered, e := RenderRuntimePolicyAssets(current)
	if e != nil {
		t.Fatal(e)
	}
	rendered[RuntimeResourceAssetName] = current
	c.RuntimeSHA256 = physicalDigest(current)
	selected, e := initialPrebootBindInputs(c, current, rendered, config)
	if e != nil {
		t.Fatal(e)
	}
	if e = normalMemoryGuestSelection(selected); e != nil {
		t.Fatal(e)
	}
	selected.runtime = append(append([]byte(nil), selected.runtime...), byte(' '))
	if normalMemoryGuestSelection(selected) == nil {
		t.Fatal("same parsed policy accepted another original raw Runtime")
	}
}
