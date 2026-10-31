package resourceguard

import (
	"encoding/hex"
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// 仅明确模型原ISO/构建摘要，不把这份数字hash声明为发行构建或闭包S。
func nativeBirthSourceFixture(t *testing.T) (nativeBirthSource, []byte, []byte, []byte) {
	t.Helper()
	_, _, c := fixtureGuard(t)
	c.AddressSpaceMaxBytes = 2 << 30
	c.PidsMaxTasks = 64
	source := sourceFixture(c)
	raw, err := json.Marshal(source)
	require.NoError(t, err)
	doc := RuntimeDocument{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ConfigSecretUID: "native-model-config-uid", SourceJSON: raw, SourceSHA256: physicalDigest(raw)}
	runtimeRaw, err := json.Marshal(doc)
	require.NoError(t, err)
	m := nativeBirthBuildManifest{ProtocolVersion: 1, Scope: nativeBirthBuildScope, PGmajor: 17, LaunchPath: FixedPostgresPath, PostgresPath: FixedPostgresPath, Files: []nativeBirthBuildFile{{FixedPostgresPath, strings.Repeat("12", 32)}}}
	manifestRaw, err := json.Marshal(m)
	require.NoError(t, err)
	policy := nativeBirthPolicy{ProtocolVersion: 1, Scope: nativeBirthPolicyScope, RuntimeDocumentSHA256: physicalDigest(runtimeRaw), ResourceSourceSHA256: doc.SourceSHA256, BuildManifestSHA256: physicalDigest(manifestRaw), PGmajor: 17, MaxBuffers: 8192, StartupBuffers: 256, PageBytes: 4096, ElementBytes: [4]uint64{64, 8192, 16, 20}}
	policyRaw, err := json.Marshal(policy)
	require.NoError(t, err)
	out, err := decodeNativeBirthSource(runtimeRaw, policyRaw, manifestRaw)
	require.NoError(t, err)
	return out, runtimeRaw, policyRaw, manifestRaw
}
func TestNativeBirthSourceExactBytesAndUnsupportedABI(t *testing.T) {
	source, runtimeRaw, policyRaw, manifestRaw := nativeBirthSourceFixture(t)
	nonce, err := newNativeBirthNonce()
	require.NoError(t, err)
	require.Len(t, nonce, 64)
	expected, err := source.binding.expected(nonce, source.resource.Limits.AddressSpaceMaxBytes)
	require.NoError(t, err)
	require.Equal(t, source.binding.Policy.ResourceSourceSHA256, nativeBirthDigestText(expected.SourceSHA))
	require.Equal(t, physicalDigest(policyRaw), nativeBirthDigestText(expected.ProfileSHA))
	mutations := []struct {
		name  string
		alter func(*nativeBirthPolicy)
	}{
		{"PG18-unimplemented", func(p *nativeBirthPolicy) { p.PGmajor = 18 }},
		{"startup-over-max", func(p *nativeBirthPolicy) { p.StartupBuffers = p.MaxBuffers + 1 }},
		{"wrong-element", func(p *nativeBirthPolicy) { p.ElementBytes[3] = 24 }},
		{"wrong-page", func(p *nativeBirthPolicy) { p.PageBytes = 65536 }},
		{"wrong-runtime", func(p *nativeBirthPolicy) { p.RuntimeDocumentSHA256 = strings.Repeat("ab", 32) }},
		{"wrong-resource", func(p *nativeBirthPolicy) { p.ResourceSourceSHA256 = strings.Repeat("ab", 32) }},
		{"wrong-build", func(p *nativeBirthPolicy) { p.BuildManifestSHA256 = strings.Repeat("ab", 32) }},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			p := source.binding.Policy
			tc.alter(&p)
			raw, err := json.Marshal(p)
			require.NoError(t, err)
			_, err = decodeNativeBirthSource(runtimeRaw, raw, manifestRaw)
			require.Error(t, err)
		})
	}
	for _, raw := range [][]byte{append([]byte(" "), runtimeRaw...), append(runtimeRaw, byte(' '))} {
		_, err := decodeNativeBirthSource(raw, policyRaw, manifestRaw)
		require.Error(t, err, "exact runtime bytes must not normalize")
	}
	for _, raw := range [][]byte{
		[]byte(strings.Replace(string(policyRaw), "{", "{\"unexpected\":true,", 1)),
		[]byte(strings.Replace(string(policyRaw), "{", "{\"pg_major\":17,", 1)),
	} {
		_, err := decodeNativeBirthSource(runtimeRaw, raw, manifestRaw)
		require.Error(t, err)
	}
}
func nativeBirthDigestText(b [32]byte) string { return hex.EncodeToString(b[:]) }
func TestNativeBirthSourceNonceAndGeometryStrict(t *testing.T) {
	source, _, _, _ := nativeBirthSourceFixture(t)
	for _, nonce := range []string{"", strings.Repeat("1", 32), strings.Repeat("0", 64), strings.Repeat("AB", 32), strings.Repeat("01", 33), strings.Repeat("gg", 32)} {
		_, err := source.binding.expected(nonce, source.resource.Limits.AddressSpaceMaxBytes)
		require.Error(t, err)
	}
	a, err := newNativeBirthNonce()
	require.NoError(t, err)
	b, err := newNativeBirthNonce()
	require.NoError(t, err)
	require.NotEqual(t, a, b)
	_, err = source.binding.expected(a, control.Uint64(64<<10))
	require.Error(t, err)
}
func TestNativeBirthSourceManifestIsBoundedAndStrict(t *testing.T) {
	source, _, _, _ := nativeBirthSourceFixture(t)
	m := source.manifest
	for _, alter := range []func(*nativeBirthBuildManifest){
		func(m *nativeBirthBuildManifest) { m.LaunchPath = "/tmp/postgres" },
		func(m *nativeBirthBuildManifest) { m.PostgresPath = "/a/../b" },
		func(m *nativeBirthBuildManifest) { m.PGmajor = 18 },
		func(m *nativeBirthBuildManifest) { m.Files = append(m.Files, m.Files[0]) },
		func(m *nativeBirthBuildManifest) { m.InterpreterPath = "/missing" },
		func(m *nativeBirthBuildManifest) { m.Files = nil },
	} {
		next := m
		next.Files = append([]nativeBirthBuildFile(nil), m.Files...)
		alter(&next)
		require.Error(t, next.validate())
	}
	for _, path := range []string{"/", "relative", "/x/../y", "/x\x00y", "/x\ny"} {
		require.False(t, nativeBirthCleanPath(path))
	}
}
