//go:build linux

package resourceguard

import (
	"context"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMetadataWiringInitialEightFixedFamilyIgnoresLaterWAL(t *testing.T) {
	for _, family := range []string{"", normalInitialWALFamilyKernelPrepV2} {
		t.Run(family, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			state, e := lockState(context.Background(), dir)
			require.NoError(t, e)
			defer state.close()
			names, e := normalInitialWALFiles(family)
			require.NoError(t, e)
			for _, name := range names {
				require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(`{"original":1}`), 0600))
			}
			c, _, _, _ := initialPrebootFixture(t)
			before, e := initialPrebootGuestWitnessFamilyLocked(state, c, family)
			require.NoError(t, e)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "state.json"), []byte(`{"later_floor":99}`), 0600))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "normal-role-later.json"), []byte(`{"later":true}`), 0600))
			after, e := initialPrebootGuestWitnessFamilyLocked(state, c, family)
			require.NoError(t, e)
			require.Equal(t, before, after)
			for i := 2; i < 8; i++ {
				require.Equal(t, initialPrebootGuestFiles[i], names[i])
			}
			if family != "" {
				require.Equal(t, normalKernelPrepV2IntentName, names[0])
				require.Equal(t, normalKernelPrepV2CompleteName, names[1])
			}
			require.NoError(t, os.WriteFile(filepath.Join(dir, names[0]), []byte(`{"original":2}`), 0600))
			changed, e := initialPrebootGuestWitnessFamilyLocked(state, c, family)
			require.NoError(t, e)
			require.NotEqual(t, before, changed)
		})
	}
	_, e := normalInitialWALFiles("auto-detect")
	require.Error(t, e)
}
func metadataWiringConfirmedReference() normalMemoryOriginConfirmedReference {
	h := strings.Repeat("ab", 32)
	return normalMemoryOriginConfirmedReference{ProtocolVersion: 2, OriginalRoot: nativeBirthConsumer{PID: 101, StartTicks: 1, InstanceNonce: h}, GuestBootID: "11111111-1111-1111-1111-111111111111", RuntimeDocumentSHA256: h, ResourceSourceSHA256: h, SHA256: [3]string{h, h, h}}
}
func TestMetadataWiringFilesystemSyncListIsExactFamily(t *testing.T) {
	ref := metadataWiringConfirmedReference()
	ref.InitialWALFamily = normalInitialWALFamilyKernelPrepV2
	names, e := normalFilesystemSyncFilesForWitness(normalFilesystemOriginWitnessKey{Memory: ref})
	require.NoError(t, e)
	require.Len(t, names, 13)
	require.Equal(t, normalKernelPrepV2IntentName, names[0])
	require.Equal(t, normalKernelPrepV2CompleteName, names[1])
	require.Equal(t, normalMemoryGuestCommitFile, names[12])
	ref.InitialWALFamily = ""
	legacy, e := normalFilesystemSyncFilesForWitness(normalFilesystemOriginWitnessKey{Memory: ref})
	require.NoError(t, e)
	require.Equal(t, initialPrebootGuestFiles[0], legacy[0])
	require.Equal(t, names[2:], legacy[2:])
	ref.InitialWALFamily = "unknown"
	_, e = normalFilesystemSyncFilesForWitness(normalFilesystemOriginWitnessKey{Memory: ref})
	require.Error(t, e)
}
