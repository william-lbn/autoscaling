package resourceguard

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalSystemMetadataExactTupleOnlyInInventoryThree(t *testing.T) {
	base := normalSystemV3ModelManifest(t)
	legacyBytes, err := json.Marshal(base)
	require.NoError(t, err)
	metadata, err := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	require.NoError(t, err)
	next := base
	next.Kernel = metadata
	require.NoError(t, next.Validate())
	raw, err := json.Marshal(next)
	require.NoError(t, err)
	decoded, err := DecodeNormalSystemSourceManifest(raw)
	require.NoError(t, err)
	require.Equal(t, next, decoded)
	for _, version := range []uint32{1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			old := normalSystemModelManifest(t)
			old.ProtocolVersion = version
			if version == 2 {
				old.ProfileID = NormalSystemSourceV2ProfileID
			}
			old.Kernel = metadata
			require.Error(t, old.Validate(), "legacy inventory cannot adopt new kernel")
		})
	}
	for _, name := range []string{"profile", "release", "artifact", "config", "notes"} {
		t.Run(name, func(t *testing.T) {
			bad := next
			switch name {
			case "profile":
				bad.Kernel.ProfileID = base.Kernel.ProfileID
			case "release":
				bad.Kernel.KernelRelease = base.Kernel.KernelRelease
			case "artifact":
				bad.Kernel.KernelArtifactSHA256 = base.Kernel.KernelArtifactSHA256
			case "config":
				bad.Kernel.KernelConfigSHA256 = base.Kernel.KernelConfigSHA256
			case "notes":
				bad.Kernel.KernelNotesSHA256 = base.Kernel.KernelNotesSHA256
			}
			require.Error(t, bad.Validate(), "cross-paired known fields are not a registered tuple")
		})
	}
	after, err := json.Marshal(base)
	require.NoError(t, err)
	require.Equal(t, legacyBytes, after)
	require.NoError(t, base.Validate())
}
