package resourceguard

import (
	"sort"
	"strings"
	"testing"
)

func normalSystemStockModel(t *testing.T) NormalSystemSourceManifest {
	t.Helper()
	m := normalSystemV3ModelManifest(t)
	seen := map[string]bool{}
	for _, f := range m.Files {
		seen[f.Path] = true
	}
	for path, kind := range NormalSystemSourceStockUdevRequiredFiles() {
		if !seen[path] {
			m.Files = append(m.Files, NormalSystemSourceFile{Path: path, CanonicalPath: path, Kind: kind, Bytes: 4, SHA256: physicalDigest([]byte("STATIC_MODEL_NOT_SOURCE"))})
		}
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	return m
}
func TestNormalSystemV3StockRequiresWholeSelectedInventory(t *testing.T) {
	original := normalSystemV3ModelManifest(t)
	if e := original.Validate(); e != nil {
		t.Fatal(e)
	}
	m := normalSystemStockModel(t)
	if e := m.Validate(); e != nil {
		t.Fatal(e)
	}
	required := NormalSystemSourceStockUdevRequiredFiles()
	if len(required) != 71 || required["/etc/passwd"] != "config" || required["/lib/udev/rules.d/99-virtio-ports.rules"] != "config" || required["/neonvm/bin/udevadm"] != "exec" {
		t.Fatal("lost original fixed input")
	}
	for path := range required {
		t.Run(path, func(t *testing.T) {
			bad := m
			bad.Files = append([]NormalSystemSourceFile(nil), m.Files...)
			for i, f := range bad.Files {
				if f.Path == path {
					bad.Files = append(bad.Files[:i], bad.Files[i+1:]...)
					break
				}
			}
			if e := bad.Validate(); e == nil {
				t.Fatal("partial selected inventory accepted")
			}

		})
	}
}
func TestNormalSystemV3StockAliasAndClassBoundaries(t *testing.T) {
	for _, tc := range []struct {
		path, canonical, kind string
		ok                    bool
	}{
		{"/lib/udev/ata_id", "/usr/lib/udev/ata_id", "exec", true},
		{"/lib/udev/ata_id", "/usr/lib/udev/scsi_id", "exec", false},
		{"/lib/udev/ata_id", "/tmp/ata_id", "exec", false},
		{"/neonvm/bin/udevadm", "/usr/bin/udevadm", "exec", false},
		{NormalSystemStockUdevManifestPath, "/tmp/manifest", "config", false},
		{"/etc/passwd", "/etc/passwd", "exec", false},
	} {
		t.Run(tc.path+tc.canonical+tc.kind, func(t *testing.T) {
			m := normalSystemStockModel(t)
			for i := range m.Files {
				if m.Files[i].Path == tc.path {
					m.Files[i].CanonicalPath = tc.canonical
					m.Files[i].Kind = tc.kind
				}
			}
			if e := m.Validate(); (e == nil) != tc.ok {
				t.Fatalf("alias/class acceptance=%v want%v: %v", e == nil, tc.ok, e)
			}
		})
	}
	for _, version := range []uint32{1, 2} {
		for _, path := range []string{"/neonvm/bin/udevadm", "/lib/udev/ata_id", "/etc/udev/hwdb.d/20-OUI.hwdb", "/etc/group"} {
			if normalSystemAllowedVersionFile(version, path, "exec") || normalSystemAllowedVersionFile(version, path, "config") {
				t.Fatalf("old%d expanded for %s", version, path)
			}
		}
	}
	for _, version := range []uint32{1, 2} {
		// /neonvm/config/*原来即可列为config，保原行为；不会选中新stock。
		m := NormalSystemSourceManifest{ProtocolVersion: version, Files: []NormalSystemSourceFile{{Path: NormalSystemStockUdevManifestPath, Kind: "config"}}}
		if !normalSystemAllowedVersionFile(version, NormalSystemStockUdevManifestPath, "config") || normalSystemStockUdevSelected(m) {
			t.Fatal("old config allowance/stock selector changed")
		}
	}
	for _, path := range []string{"/lib/udev/evil", "/etc/udev/hwdb.d/extra.hwdb", "/etc/neon-dsga/another.json"} {
		if normalSystemAllowedVersionFile(3, path, "config") || normalSystemAllowedVersionFile(3, path, "exec") {
			t.Fatal("arbitrary stock path accepted", path)
		}
	}
}
func TestNormalSystemV3StockConfigurationSelection(t *testing.T) {
	old := NormalSystemSourceConfigDirectories()
	m := normalSystemV3ModelManifest(t)
	if strings.Join(NormalSystemSourceConfigDirectoriesForManifest(m), "\n") != strings.Join(old, "\n") {
		t.Fatal("old metadata-only v3 changed")
	}
	got := NormalSystemSourceConfigDirectoriesForManifest(normalSystemStockModel(t))
	if len(got) != 5 || got[4] != "/etc/udev/hwdb.d" {
		t.Fatal("hwdb not covered")
	}
	got[0] = "/caller/changed"
	if NormalSystemSourceConfigDirectories()[0] == got[0] {
		t.Fatal("mutable shared directory table")
	}
}

func TestNormalSystemV3StockRequiresSelectorForNewPaths(t *testing.T) {
	for _, row := range []struct{ path, kind string }{
		{NormalSystemStockUdevManifestPath, "config"},
		{"/lib/udev/ata_id", "exec"}, {"/neonvm/bin/udevadm", "exec"},
		{"/etc/group", "config"}, {"/etc/passwd", "config"}, {"/etc/udev/udev.conf", "config"},
		{"/etc/udev/hwdb.d/20-OUI.hwdb", "config"}, {"/lib/udev/hwdb.bin", "config"},
	} {
		t.Run(row.path, func(t *testing.T) {
			m := normalSystemV3ModelManifest(t)
			m.Files = append(m.Files, NormalSystemSourceFile{Path: row.path, CanonicalPath: row.path, Kind: row.kind, Bytes: 4, SHA256: physicalDigest([]byte("model only"))})
			sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
			if e := m.Validate(); e == nil {
				t.Fatal("new partial input accepted without full selected inventory")
			}
		})
	}
	for _, row := range []struct{ path, kind string }{{"/lib/udev/rules.d/50-original.rules", "config"}, {"/lib/original-loader.so", "loader"}} {
		m := normalSystemV3ModelManifest(t)
		m.Files = append(m.Files, NormalSystemSourceFile{Path: row.path, CanonicalPath: row.path, Kind: row.kind, Bytes: 4, SHA256: physicalDigest([]byte("old permitted path"))})
		sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
		if e := m.Validate(); e != nil {
			t.Fatal("old allowed inventory changed", e)
		}
	}
}
