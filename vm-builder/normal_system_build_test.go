//go:build linux && amd64

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 保留原真实GNU Compute/PG测试文件，其他角色最小静态模型不执行、不签来源。
func normalSystemCompleteFixture(t *testing.T) (string, []byte) {
	t.Helper()
	root, _ := normalNativeFixture(t)
	paths, e := resourceguard.NormalSystemSourceRequiredFilesForVersion(3)
	if e != nil {
		t.Fatal(e)
	}
	static := make([]byte, 64)
	copy(static, []byte{0x7f, 'E', 'L', 'F', 2, 1, 1})
	binary.LittleEndian.PutUint16(static[16:18], 2)
	binary.LittleEndian.PutUint16(static[18:20], 62)
	binary.LittleEndian.PutUint32(static[20:24], 1)
	binary.LittleEndian.PutUint16(static[52:54], 64)
	for path, kind := range paths {
		canonical, e := normalImageCanonical(root, path)
		if e != nil {
			t.Fatal(e)
		}
		if kind == "exec" {
			raw := static
			if path == "/usr/local/bin/compute_ctl" {
				raw, e = os.ReadFile("/bin/true")
				if e != nil {
					t.Fatal(e)
				}
			}
			if e = os.WriteFile(filepath.Join(root, canonical), raw, 0555); e != nil {
				t.Fatal(e)
			}
		}
		if kind == "script" {
			if e = os.WriteFile(filepath.Join(root, canonical), []byte("#!/neonvm/bin/sh\nexit 0\n"), 0555); e != nil {
				t.Fatal(e)
			}
		}
	}
	raw, e := generateNormalSystemManifestForVersion(root, normalTestPlan(), strings.Repeat("ab", 32), 3)
	if e != nil {
		t.Fatal(e)
	}
	return root, raw
}
func TestNormalSystemBuildCompletePublicationUsesFull18cAndNativeBinding(t *testing.T) {
	root, old := normalSystemCompleteFixture(t)
	var before resourceguard.NormalSystemSourceManifest
	if e := json.Unmarshal(old, &before); e != nil {
		t.Fatal(e)
	}
	if before.Kernel.ProfileID != resourceguard.NormalSplitV1PhysicalKernelProfileID {
		t.Fatal("default0 old kernel selection changed")
	}
	if e := publishCompleteNormalNativeBirthImage(root, old); e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(filepath.Join(root, resourceguard.NormalSystemSourceManifestPath))
	if e != nil {
		t.Fatal(e)
	}
	m, e := resourceguard.DecodeNormalSystemSourceManifest(raw)
	if e != nil {
		t.Fatal(e)
	}
	ref, e := resourceguard.LookupPhysicalKernelBuild(resourceguard.NormalSplitMetadataV1PhysicalKernelProfileID)
	if e != nil || m.Kernel != ref {
		t.Fatal("not exact registry18c tuple", e)
	}
	if len(m.Files) <= len(before.Files) {
		t.Fatal("actual GNU dependencies were not collected")
	}
	if !bytes.HasSuffix(raw, []byte("\n")) || bytes.HasSuffix(raw, []byte("\n\n")) {
		t.Fatal("final System3 newline differs")
	}
	var native struct {
		Closure struct {
			SHA string `json:"system_manifest_sha256"`
		} `json:"runtime_closure"`
	}
	b, e := os.ReadFile(filepath.Join(root, normalNativeBuildPath))
	if e != nil || json.Unmarshal(b, &native) != nil || native.Closure.SHA != normalDigest(raw) {
		t.Fatal("NativeBirth did not bind same final System bytes", e)
	}
}
func TestNormalSystemBuildCompletePublicationRejectsBadFirstHitAndUnused(t *testing.T) {
	for _, name := range []string{"badFirstWithGoodLater", "unusedDeclaredLoader", "badScript"} {
		t.Run(name, func(t *testing.T) {
			root, raw := normalSystemCompleteFixture(t)
			seed, e := os.ReadFile(filepath.Join(root, normalNativeBuildPath))
			if e != nil {
				t.Fatal(e)
			}
			switch name {
			case "badFirstWithGoodLater":
				p := filepath.Join(root, "/lib/x86_64-linux-gnu/libc.so.6")
				good, e := os.ReadFile(p)
				if e != nil {
					t.Fatal(e)
				}
				later := filepath.Join(root, "/usr/lib/x86_64-linux-gnu/libc.so.6")
				if e = os.MkdirAll(filepath.Dir(later), 0755); e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(later, good, 0555); e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(p, []byte("bad first object"), 0555); e != nil {
					t.Fatal(e)
				}
			case "unusedDeclaredLoader":
				b, e := os.ReadFile(filepath.Join(root, "/neonvm/bin/busybox"))
				if e != nil {
					t.Fatal(e)
				}
				p := "/lib/unrelated.so"
				if e = os.MkdirAll(filepath.Join(root, "/lib"), 0755); e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(filepath.Join(root, p), b, 0555); e != nil {
					t.Fatal(e)
				}
				plan := normalTestPlan()
				plan.LoaderPaths = []string{p}
				raw, e = generateNormalSystemManifestForVersion(root, plan, strings.Repeat("ab", 32), 3)
				if e != nil {
					t.Fatal(e)
				}
			case "badScript":
				if e = os.WriteFile(filepath.Join(root, "/neonvm/bin/vmstart"), []byte("#!/bin/sh\n"), 0555); e != nil {
					t.Fatal(e)
				}
				raw, e = generateNormalSystemManifestForVersion(root, normalTestPlan(), strings.Repeat("ab", 32), 3)
				if e != nil {
					t.Fatal(e)
				}
			}
			if e = publishCompleteNormalNativeBirthImage(root, raw); e == nil {
				t.Fatal("invalid full role closure published")
			}
			if _, e = os.Lstat(filepath.Join(root, resourceguard.NormalSystemSourceManifestPath)); !os.IsNotExist(e) {
				t.Fatal("failure published System file", e)
			}
			after, e := os.ReadFile(filepath.Join(root, normalNativeBuildPath))
			if e != nil || !bytes.Equal(seed, after) {
				t.Fatal("failure changed seed", e)
			}
		})
	}
}

func TestNormalSystemBuildOriginal105To119And40ELFs(t *testing.T) {
	root := os.Getenv("NEON_SYSTEM_ORIGINAL_ROOTFS")
	oracleRoot := os.Getenv("NEON_SYSTEM_COMPLETE_ORACLE_ROOTFS")
	if root == "" || oracleRoot == "" {
		t.Skip("requires original immutable105 rootfs and independently recorded119 oracle")
	}
	input, e := os.ReadFile(filepath.Join(root, resourceguard.NormalSystemSourceManifestPath))
	if e != nil {
		t.Fatal(e)
	}
	before, e := resourceguard.DecodeNormalSystemSourceManifest(input)
	if e != nil || len(before.Files) != 105 {
		t.Fatal("wrong original105 provider", e)
	}
	raw, e := completeNormalSystemBuildManifest(root, input)
	if e != nil {
		t.Fatal(e)
	}
	got, e := resourceguard.DecodeNormalSystemSourceManifest(raw)
	if e != nil || len(got.Files) != 119 {
		t.Fatal("did not derive119", e)
	}
	oracleRaw, e := os.ReadFile(filepath.Join(oracleRoot, resourceguard.NormalSystemSourceManifestPath))
	if e != nil {
		t.Fatal(e)
	}
	oracle, e := resourceguard.DecodeNormalSystemSourceManifest(oracleRaw)
	if e != nil || len(oracle.Files) != 119 {
		t.Fatal("wrong independent119 oracle", e)
	}
	old := map[string]resourceguard.NormalSystemSourceFile{}
	expected := map[string]resourceguard.NormalSystemSourceFile{}
	for _, entry := range before.Files {
		old[entry.Path] = entry
	}
	for _, entry := range oracle.Files {
		expected[entry.Path] = entry
	}
	canonicalELFs := map[string]string{}
	oracleELFs := map[string]string{}
	expectedELFs := map[string]string{}
	for _, entry := range before.Files {
		if entry.Kind == "exec" || entry.Kind == "loader" {
			expectedELFs[entry.CanonicalPath] = entry.SHA256
		}
	}
	for _, entry := range oracle.Files {
		if _, existed := old[entry.Path]; !existed && (entry.Kind == "exec" || entry.Kind == "loader") {
			expectedELFs[entry.CanonicalPath] = entry.SHA256
		}
	}
	added := 0
	execs := 0
	for _, entry := range oracle.Files {
		if entry.Kind == "exec" || entry.Kind == "loader" {
			oracleELFs[entry.CanonicalPath] = entry.SHA256
		}
	}
	for _, entry := range got.Files {
		if original, ok := old[entry.Path]; ok {
			if entry != original {
				t.Fatal("constructor changed original plan/config/source bytes", entry.Path)
			}
		} else {
			added++
			if entry != expected[entry.Path] {
				t.Fatal("new canonical loader differs from independent oracle", entry.Path)
			}
		}
		if entry.Kind == "exec" {
			execs++
		}
		if entry.Kind == "exec" || entry.Kind == "loader" {
			canonicalELFs[entry.CanonicalPath] = entry.SHA256
		}
	}
	sameCanonicalPaths := len(canonicalELFs) == len(oracleELFs)
	for path := range canonicalELFs {
		if _, exists := oracleELFs[path]; !exists {
			sameCanonicalPaths = false
		}
	}
	if added != 14 || execs != 22 || len(canonicalELFs) != 40 || !sameCanonicalPaths || !reflect.DeepEqual(canonicalELFs, expectedELFs) || got.Kernel != oracle.Kernel {
		t.Fatal("independent role/loader/registry graph differs")
	}
	im, e := newNormalNativeImage(root)
	if e != nil {
		t.Fatal(e)
	}
	seed, _, e := im.seed()
	if e != nil {
		t.Fatal(e)
	}
	finalSystem := append(raw, '\n')
	native, e := resourceguard.BuildNativeBirthManifestJSON(seed, finalSystem, im.canonical, im.open, im.lstat)
	if e != nil {
		t.Fatal(e)
	}
	var n, oldN struct {
		Files []struct {
			Path string `json:"path"`
			SHA  string `json:"sha256"`
		}
		Closure struct {
			SHA string `json:"system_manifest_sha256"`
		} `json:"runtime_closure"`
	}
	if json.Unmarshal(native, &n) != nil || json.Unmarshal(seed, &oldN) != nil || len(n.Files) != 16 || !reflect.DeepEqual(n.Files, oldN.Files) || n.Closure.SHA != normalDigest(finalSystem) {
		t.Fatal("same original NativeBirth16 did not bind derived complete System119")
	}
	t.Logf("different DEV Guest inputs preserve original guardSHA=%s; oracle guardSHA=%s; expected ELF SHA from original26 plus independent added14", expectedELFs["/neonvm/bin/dsga-resource-guard"], oracleELFs["/neonvm/bin/dsga-resource-guard"])
	t.Logf("actual original105 -> complete119,14 added,22 exec logical,40 canonical ELF; GNU/musl/static original source; PG16 same; SystemSHA=%s nativeSHA=%s Source=false normal=false", normalDigest(finalSystem), normalDigest(native))
}
