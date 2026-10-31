package resourceguard

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

func normalSystemModelManifest(t *testing.T) NormalSystemSourceManifest {
	t.Helper()
	ref, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if e != nil {
		t.Fatal(e)
	}
	m := NormalSystemSourceManifest{ProtocolVersion: 1, ProfileID: NormalSystemSourceProfileID, Kernel: ref, DeclaredImageSHA256: physicalDigest([]byte("model image not actual source")), ComputePath: "/usr/local/bin/compute_ctl", ComputeArgvSHA256: physicalDigest([]byte("model argv")), ComputeEnvironmentSHA256: physicalDigest([]byte("model env"))}
	for p, k := range normalSystemRequiredPaths() {
		m.Files = append(m.Files, NormalSystemSourceFile{Path: p, CanonicalPath: p, Kind: k, Bytes: 4, SHA256: physicalDigest([]byte("test"))})
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	return m
}
func normalSystemModelRaw(t *testing.T) []byte {
	t.Helper()
	raw, e := json.Marshal(normalSystemModelManifest(t))
	if e != nil {
		t.Fatal(e)
	}
	return raw
}
func TestNormalSystemSourceManifestStrict(t *testing.T) {
	raw := normalSystemModelRaw(t)
	if _, e := DecodeNormalSystemSourceManifest(raw); e != nil {
		t.Fatal(e)
	}
	bad := map[string][]byte{
		"empty": nil, "unknown": append(append([]byte(nil), raw[:len(raw)-1]...), []byte(",\"trusted\":true}")...),
		"duplicate": bytes.Replace(raw, []byte("\"protocol_version\":1"), []byte("\"protocol_version\":1,\"protocol_version\":1"), 1),
		"trailing":  append(append([]byte(nil), raw...), []byte("{}")...), "oversize": bytes.Repeat([]byte(" "), (1<<20)+1),
		"depth": []byte(strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34)),
	}
	for n, b := range bad {
		t.Run(n, func(t *testing.T) {
			if _, e := DecodeNormalSystemSourceManifest(b); e == nil {
				t.Fatal("invalid raw policy accepted")
			}
		})
	}
}
func TestNormalSystemSourceManifestRejectDrift(t *testing.T) {
	cases := map[string]func(*NormalSystemSourceManifest){
		"version": func(m *NormalSystemSourceManifest) { m.ProtocolVersion = 2 }, "profile": func(m *NormalSystemSourceManifest) { m.ProfileID = "legacy" },
		"other-kernel": func(m *NormalSystemSourceManifest) {
			m.Kernel, _ = LookupPhysicalKernelBuild(NormalV1PIDCappedPhysicalKernelProfileID)
		},
		"image": func(m *NormalSystemSourceManifest) { m.DeclaredImageSHA256 = "" }, "compute-shell": func(m *NormalSystemSourceManifest) { m.ComputePath = "/neonvm/bin/sh" },
		"argv-source": func(m *NormalSystemSourceManifest) { m.ComputeArgvSHA256 = "" }, "env-source": func(m *NormalSystemSourceManifest) { m.ComputeEnvironmentSHA256 = "" },
		"missing-file": func(m *NormalSystemSourceManifest) { m.Files = m.Files[1:] }, "duplicate": func(m *NormalSystemSourceManifest) { m.Files = append(m.Files, m.Files[0]) },
		"unordered":    func(m *NormalSystemSourceManifest) { m.Files[0], m.Files[1] = m.Files[1], m.Files[0] },
		"unknown-exec": func(m *NormalSystemSourceManifest) { m.Files[0].Path = "/usr/local/bin/evil"; m.Files[0].Kind = "exec" },
		"exec-alias": func(m *NormalSystemSourceManifest) {
			for i := range m.Files {
				if m.Files[i].Kind == "exec" {
					m.Files[i].CanonicalPath = "/bin/sh"
					return
				}
			}
		},
		"guard-as-busybox": func(m *NormalSystemSourceManifest) {
			for i := range m.Files {
				if m.Files[i].Path == "/neonvm/bin/dsga-resource-guard" {
					m.Files[i].CanonicalPath = "/neonvm/bin/busybox"
				}
			}
		},
		"hash": func(m *NormalSystemSourceManifest) { m.Files[0].SHA256 = "" }, "size": func(m *NormalSystemSourceManifest) { m.Files[0].Bytes = 257 << 20 },
	}
	for n, f := range cases {
		t.Run(n, func(t *testing.T) {
			m := normalSystemModelManifest(t)
			f(&m)
			if m.Validate() == nil {
				t.Fatal("manifest drift accepted")
			}
		})
	}
}
func TestNormalSystemSourceFixedInittab(t *testing.T) {
	entries := NormalSystemSourceInitEntries()
	good := strings.Join(entries, "\n") + "\n"
	if e := normalSystemCheckInittab([]byte("# fixed managed profile\n" + good)); e != nil {
		t.Fatal(e)
	}
	bad := map[string]string{
		"root-login": good + "ttyS0::respawn:/neonvm/bin/agetty --autologin root 115200 ttyS0\n",
		"sshd":       good + "::respawn:/neonvm/bin/sshd\n", "custom": good + "::once:/neonvm/bin/sh -c evil\n",
		"missing": strings.Join(entries[1:], "\n"), "duplicate": good + entries[0] + "\n", "whitespace": " " + good,
		"bare-path": strings.Replace(good, "/neonvm/bin/cat /proc/version", "cat /proc/version", 1),
		"CRLF":      strings.ReplaceAll(good, "\n", "\r\n"), "wrong-args": strings.Replace(good, "--addr=0.0.0.0:25183", "--addr=127.0.0.1:1", 1),
	}
	for n, s := range bad {
		t.Run(n, func(t *testing.T) {
			if normalSystemCheckInittab([]byte(s)) == nil {
				t.Fatal("unsafe inittab accepted")
			}
		})
	}
}
func TestNormalSystemSourceCatalogCopy(t *testing.T) {
	a := NormalSystemSourceInitEntries()
	a[0] = "evil"
	if NormalSystemSourceInitEntries()[0] == "evil" {
		t.Fatal("caller mutated fixed source catalog")
	}
}
