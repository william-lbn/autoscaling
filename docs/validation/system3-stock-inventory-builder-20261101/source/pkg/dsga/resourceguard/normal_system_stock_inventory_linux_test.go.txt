//go:build linux

package resourceguard

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalSystemV3StockActualHWDBInventory(t *testing.T) {
	m := normalSystemStockModel(t)
	root := t.TempDir()
	for _, dir := range NormalSystemSourceConfigDirectoriesForManifest(m) {
		if e := os.MkdirAll(filepath.Join(root, dir), 0755); e != nil {
			t.Fatal(e)
		}
	}
	listed := "/etc/udev/hwdb.d/20-OUI.hwdb"
	if e := os.WriteFile(filepath.Join(root, listed), []byte("fixture only"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := normalSystemConfigInventory(root, m); e != nil {
		t.Fatal(e)
	}
	extra := filepath.Join(root, "/etc/udev/hwdb.d/unlisted.hwdb")
	if e := os.WriteFile(extra, []byte("not in original inventory"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := normalSystemConfigInventory(root, m); e == nil {
		t.Fatal("unlisted hwdb accepted")
	}
	if e := os.Remove(extra); e != nil {
		t.Fatal(e)
	}
	if e := os.RemoveAll(filepath.Join(root, "/etc/udev/hwdb.d")); e != nil {
		t.Fatal(e)
	}
	if _, e := normalSystemConfigInventory(root, m); e == nil {
		t.Fatal("absent hwdb directory accepted")
	}
}
