package resourceguard

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func nativeBirthVersionSourceFixture(t *testing.T, major uint32, binary string) (nativeBirthSource, []byte, []byte, []byte) {
	t.Helper()
	original, runtimeRaw, _, _ := nativeBirthSourceFixture(t)
	manifest := original.manifest
	manifest.PGmajor = major
	if binary != "" {
		manifest.Files[0].SHA256 = binary
	}
	manifestRaw, err := json.Marshal(manifest)
	require.NoError(t, err)
	policy := original.binding.Policy
	policy.PGmajor = major
	policy.BuildManifestSHA256 = physicalDigest(manifestRaw)
	policyRaw, err := json.Marshal(policy)
	require.NoError(t, err)
	source, err := decodeNativeBirthSource(runtimeRaw, policyRaw, manifestRaw)
	require.NoError(t, err)
	return source, runtimeRaw, policyRaw, manifestRaw
}

func TestNativeBirthImplementedPGVersions(t *testing.T) {
	for _, major := range []uint32{17, 18} {
		abi, err := nativeBirthABIForPG(major)
		require.NoError(t, err)
		require.Equal(t, uint32(4096), abi.pageBytes)
		require.Equal(t, [4]uint64{64, 8192, 16, 20}, abi.elementBytes)
		if major == 17 {
			require.Equal(t, normalPGMemoryControlScope, abi.memoryControlScope, "old17 reference bytes must remain")
		} else {
			require.Equal(t, "original-pg18-memory-syscall-controls-v1-non-authorizing", abi.memoryControlScope)
		}
		abi.elementBytes[0] = 1
		again, err := nativeBirthABIForPG(major)
		require.NoError(t, err)
		require.Equal(t, uint64(64), again.elementBytes[0], "caller cannot mutate later ABI selection")
	}
	for _, major := range []uint32{0, 14, 16, 19, ^uint32(0)} {
		_, err := nativeBirthABIForPG(major)
		require.Error(t, err)
	}
}

func TestNativeBirthPG18BindsExactOriginalSource(t *testing.T) {
	old, oldRuntime, oldPolicy, oldManifest := nativeBirthSourceFixture(t)
	unchanged, err := decodeNativeBirthSource(oldRuntime, oldPolicy, oldManifest)
	require.NoError(t, err)
	require.Equal(t, old, unchanged)
	source, runtimeRaw, policyRaw, manifestRaw := nativeBirthVersionSourceFixture(t, 18, "")
	require.Equal(t, uint32(18), source.manifest.PGmajor)
	require.Equal(t, source.manifest.PGmajor, source.binding.Policy.PGmajor)
	expected, err := source.binding.expected(strings.Repeat("21", 32), source.resource.Limits.AddressSpaceMaxBytes)
	require.NoError(t, err)
	require.Equal(t, source.binding.PolicySHA256, nativeBirthDigestText(expected.ProfileSHA))
	require.Equal(t, source.binding.Policy.ResourceSourceSHA256, nativeBirthDigestText(expected.SourceSHA))
	require.NotEqual(t, old.binding.PolicySHA256, source.binding.PolicySHA256, "18 cannot borrow17 policy receipt")
	ack, err := encodeNativeBirthACK(expected)
	require.NoError(t, err)
	require.Len(t, ack, nativeBirthHeaderBytes, "no public/wire version extension")
	for _, name := range []string{"policy17-build18", "policy18-build17", "unknown19", "wrong-layout", "wrong-page", "changed-build-bytes", "changed-runtime-bytes"} {
		t.Run(name, func(t *testing.T) {
			p, m := source.binding.Policy, source.manifest
			rawRuntime, rawManifest := runtimeRaw, manifestRaw
			switch name {
			case "policy17-build18":
				p.PGmajor = 17
			case "policy18-build17":
				m.PGmajor = 17
				rawManifest, err = json.Marshal(m)
				require.NoError(t, err)
				p.BuildManifestSHA256 = physicalDigest(rawManifest)
			case "unknown19":
				p.PGmajor, m.PGmajor = 19, 19
				rawManifest, err = json.Marshal(m)
				require.NoError(t, err)
				p.BuildManifestSHA256 = physicalDigest(rawManifest)
			case "wrong-layout":
				p.ElementBytes[3] = 24
			case "wrong-page":
				p.PageBytes = 65536
			case "changed-build-bytes":
				rawManifest = append([]byte(" "), rawManifest...)
			case "changed-runtime-bytes":
				rawRuntime = append([]byte(" "), rawRuntime...)
			}
			rawPolicy, err := json.Marshal(p)
			require.NoError(t, err)
			_, err = decodeNativeBirthSource(rawRuntime, rawPolicy, rawManifest)
			require.Error(t, err)
		})
	}
	_, err = decodeNativeBirthSource(runtimeRaw, policyRaw, manifestRaw)
	require.NoError(t, err)
}
