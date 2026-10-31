//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestNormalSystemSourceRuntimeOverrides(t *testing.T) {
	d := t.TempDir()
	if e := normalSystemCheckRuntimeOverrides(d); e != nil {
		t.Fatal(e)
	}
	for _, name := range normalSystemForbiddenOverrides() {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(d, name)
			if e := os.WriteFile(p, []byte("fixture only"), 0600); e != nil {
				t.Fatal(e)
			}
			if normalSystemCheckRuntimeOverrides(d) == nil {
				t.Fatal("legacy override accepted")
			}
			if e := os.Remove(p); e != nil {
				t.Fatal(e)
			}
		})
	}
	if e := os.Symlink("/missing", filepath.Join(d, "command.sh")); e != nil {
		t.Fatal(e)
	}
	if normalSystemCheckRuntimeOverrides(d) == nil {
		t.Fatal("broken executable override alias accepted")
	}
}
func TestNormalSystemSourceCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := CheckNormalSystemSource(ctx, normalSystemModelRaw(t)); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

func TestNormalSystemSourceConfigInventory(t *testing.T) {
	root := t.TempDir()
	for _, d := range NormalSystemSourceConfigDirectories() {
		if e := os.MkdirAll(filepath.Join(root, d), 0755); e != nil {
			t.Fatal(e)
		}
	}
	m := normalSystemModelManifest(t)
	p := filepath.Join(root, "/etc/vector/extra.yaml")
	if _, e := normalSystemConfigInventory(root, m); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(p, []byte("unlisted config"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := normalSystemConfigInventory(root, m); e == nil {
		t.Fatal("unknown glob config accepted")
	}
	m.Files = append(m.Files, NormalSystemSourceFile{Path: "/etc/vector/extra.yaml", Kind: "config"})
	if _, e := normalSystemConfigInventory(root, m); e != nil {
		t.Fatal(e)
	}
	if e := os.Remove(p); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink("/tmp", p); e != nil {
		t.Fatal(e)
	}
	if _, e := normalSystemConfigInventory(root, m); e == nil {
		t.Fatal("config directory alias accepted")
	}
}
