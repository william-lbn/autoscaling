package resourceguard

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func normalSystemV3ModelManifest(t *testing.T) NormalSystemSourceManifest {
	t.Helper()
	m := normalSystemModelManifest(t)
	m.ProtocolVersion = 3
	m.ProfileID = NormalSystemSourceV3ProfileID
	m.Files = nil
	paths, err := NormalSystemSourceRequiredFilesForVersion(3)
	require.NoError(t, err)
	for path, kind := range paths {
		m.Files = append(m.Files, NormalSystemSourceFile{Path: path, CanonicalPath: path, Kind: kind, Bytes: 4, SHA256: physicalDigest([]byte("TEST_ONLY model inventory"))})
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	require.NoError(t, m.Validate())
	return m
}
func TestNormalSystemV3RequiresOriginalMetadataAssets(t *testing.T) {
	m := normalSystemV3ModelManifest(t)
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	_, err = DecodeNormalSystemSourceManifest(raw)
	require.NoError(t, err)
	for _, path := range normalSystemV3MetadataPaths() {
		t.Run(path, func(t *testing.T) {
			for _, mode := range []string{"missing", "alias", "kind"} {
				t.Run(mode, func(t *testing.T) {
					bad := m
					bad.Files = append([]NormalSystemSourceFile(nil), m.Files...)
					for i := range bad.Files {
						if bad.Files[i].Path == path {
							switch mode {
							case "missing":
								bad.Files = append(bad.Files[:i], bad.Files[i+1:]...)
							case "alias":
								bad.Files[i].CanonicalPath = "/tmp/other-object"
							case "kind":
								bad.Files[i].Kind = "script"
							}
							break
						}
					}
					require.Error(t, bad.Validate())
				})
			}
		})
	}
}
func TestNormalSystemV3JSONRemainsExact(t *testing.T) {
	raw, err := json.Marshal(normalSystemV3ModelManifest(t))
	require.NoError(t, err)
	cases := map[string][]byte{
		"top-case":    bytes.Replace(raw, []byte("protocol_version"), []byte("Protocol_Version"), 1),
		"kernel-case": bytes.Replace(raw, []byte("kernel_artifact_sha256"), []byte("Kernel_Artifact_SHA256"), 1),
		"file-case":   bytes.Replace(raw, []byte("canonical_path"), []byte("Canonical_Path"), 1),
		"duplicate":   bytes.Replace(raw, []byte(`"protocol_version":3`), []byte(`"protocol_version":3,"protocol_version":3`), 1),
		"alias-pair":  bytes.Replace(raw, []byte(`"protocol_version":3`), []byte(`"protocol_version":3,"Protocol_Version":3`), 1),
		"unknown":     bytes.Replace(raw, []byte(`"protocol_version":3`), []byte(`"protocol_version":3,"qualified":true`), 1),
		"trailing":    append(append([]byte(nil), raw...), []byte("{}")...),
		"nil-files":   []byte(`{"protocol_version":3,"files":null}`),
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			require.NotEqual(t, string(raw), string(bad))
			_, err := DecodeNormalSystemSourceManifest(bad)
			require.Error(t, err)
		})
	}
}
func TestNormalSystemV3FixedInitPreservesComputeWrapper(t *testing.T) {
	entries, err := NormalSystemSourceInitEntriesForVersion(3)
	require.NoError(t, err)
	require.Len(t, entries, 13)
	require.Equal(t, "::sysinit:/neonvm/bin/vminit", entries[0])
	require.Equal(t, "::respawn:/neonvm/bin/vmstart", entries[11])
	require.Equal(t, "::shutdown:/neonvm/bin/vmshutdown", entries[12])
	roles := []string{"allow-start", "kernel-version", "network", "udev", "udev-init", "acpi", "logs", "clock", "daemon", "resource"}
	for i, role := range roles {
		require.True(t, strings.HasSuffix(entries[i+1], "/neonvm/bin/busybox env -i PATH=/neonvm/bin /neonvm/bin/dsga-resource-guard exec-system-role "+role))
		require.NotContains(t, entries[i+1], "dsga-resource-service")
	}
	good := []byte(strings.Join(entries, "\n") + "\n")
	require.NoError(t, normalSystemCheckVersionInittab(3, good))
	for name, replace := range map[string]string{"old-wrapper": "/neonvm/bin/sh /neonvm/bin/dsga-resource-service network", "caller-env": "/neonvm/bin/busybox env PATH=/tmp /neonvm/bin/dsga-resource-guard exec-system-role network", "arbitrary-payload": "/neonvm/bin/busybox env -i PATH=/neonvm/bin /neonvm/bin/dsga-resource-guard exec-system-role /bin/sh"} {
		t.Run(name, func(t *testing.T) {
			bad := append([]string(nil), entries...)
			bad[3] = "::respawn:" + replace
			require.Error(t, normalSystemCheckVersionInittab(3, []byte(strings.Join(bad, "\n"))))
		})
	}
	mutated := append([]string(nil), entries...)
	mutated[11] = "::respawn:/neonvm/bin/dsga-resource-guard exec-system-role compute"
	require.Error(t, normalSystemCheckVersionInittab(3, []byte(strings.Join(mutated, "\n"))))
	entries[0] = "changed"
	again, err := NormalSystemSourceInitEntriesForVersion(3)
	require.NoError(t, err)
	require.Equal(t, "::sysinit:/neonvm/bin/vminit", again[0])
}
func TestNormalSystemV3RolePayloadsKeepExactV2Vectors(t *testing.T) {
	for _, role := range []string{"allow-start", "kernel-version", "network", "udev", "udev-init", "acpi", "logs", "clock", "daemon", "resource"} {
		t.Run(role, func(t *testing.T) {
			old, err := normalSystemFixedRoleArgv(role)
			require.NoError(t, err)
			v2, err := normalSystemFixedRoleArgvForVersion(2, role)
			require.NoError(t, err)
			v3, err := normalSystemFixedRoleArgvForVersion(3, role)
			require.NoError(t, err)
			require.Equal(t, old, v2)
			require.Equal(t, v2, v3)
			v3[0] = "/tmp/changed"
			again, err := normalSystemFixedRoleArgvForVersion(3, role)
			require.NoError(t, err)
			require.Equal(t, v2, again)
		})
	}
	udev, err := normalSystemFixedRoleArgvForVersion(3, "udev-init")
	require.NoError(t, err)
	require.Equal(t, []string{"/neonvm/bin/sh", "/neonvm/bin/udev-init.sh"}, udev)
	for _, version := range []uint32{0, 1, 4, ^uint32(0)} {
		_, err := normalSystemFixedRoleArgvForVersion(version, "network")
		require.Error(t, err)
	}
	for _, role := range []string{"compute", "/bin/sh", "root", "network --override"} {
		_, err := normalSystemFixedRoleArgvForVersion(3, role)
		require.Error(t, err)
	}
}
func TestNormalSystemV3KeepsVersionAndKernelBoundaries(t *testing.T) {
	good := normalSystemV3ModelManifest(t)
	v2 := good
	v2.ProtocolVersion = 2
	v2.ProfileID = NormalSystemSourceV2ProfileID
	require.True(t, normalSystemLifecycleManifestVersion(good))
	require.True(t, normalSystemLifecycleManifestVersion(v2))
	require.False(t, normalSystemLifecycleManifestVersion(normalSystemModelManifest(t)))
	for name, mutate := range map[string]func(*NormalSystemSourceManifest){"profile": func(m *NormalSystemSourceManifest) { m.ProfileID = NormalSystemSourceV2ProfileID }, "version": func(m *NormalSystemSourceManifest) { m.ProtocolVersion = 4 }, "kernel": func(m *NormalSystemSourceManifest) {
		m.Kernel, _ = LookupPhysicalKernelBuild(NormalV1PIDCappedPhysicalKernelProfileID)
	}, "compute-path": func(m *NormalSystemSourceManifest) { m.ComputePath = "/bin/sh" }, "argv-source": func(m *NormalSystemSourceManifest) { m.ComputeArgvSHA256 = "" }} {
		t.Run(name, func(t *testing.T) { m := good; mutate(&m); require.Error(t, m.Validate()) })
	}
	paths, err := NormalSystemSourceRequiredFilesForVersion(3)
	require.NoError(t, err)
	delete(paths, normalSystemV3BrokerDeclarationPath)
	again, err := NormalSystemSourceRequiredFilesForVersion(3)
	require.NoError(t, err)
	require.Equal(t, "config", again[normalSystemV3BrokerDeclarationPath])
	legacy, err := NormalSystemSourceRequiredFilesForVersion(2)
	require.NoError(t, err)
	for _, path := range normalSystemV3MetadataPaths() {
		require.NotContains(t, legacy, path)
	}
	for _, version := range []uint32{0, 4} {
		_, err = NormalSystemSourceInitEntriesForVersion(version)
		require.Error(t, err)
		_, err = NormalSystemSourceRequiredFilesForVersion(version)
		require.Error(t, err)
	}
}
