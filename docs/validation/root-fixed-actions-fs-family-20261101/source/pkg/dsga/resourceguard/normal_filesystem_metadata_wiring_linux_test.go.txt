//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TEST_ONLY纯投影与实际同步回调，不据此登记任何FS/内核/normal权限。
func TestNormalFilesystemMetadataFamilyAllThirteenDurableFiles(t *testing.T) {
	in := normalFilesystemMemory2TestProjection()
	in.Memory.InitialWALFamily = normalInitialWALFamilyKernelPrepV2
	key := normalFilesystemOriginWitnessKey{Guard: &Guard{}, Memory: *in.Memory}
	expected := append([]string(nil), initialPrebootGuestFiles[:]...)
	expected[0], expected[1] = normalKernelPrepV2IntentName, normalKernelPrepV2CompleteName
	expected = append(expected, normalFilesystemOriginIntentName, normalFilesystemOriginCompleteName)
	memoryFiles := normalMemoryConfirmedFiles()
	expected = append(expected, memoryFiles[:]...)
	require.Len(t, expected, 13)
	var seen []string
	_, err := normalFilesystemConfirmPrefix(context.Background(), func(name string) error {
		seen = append(seen, name)
		return nil
	}, func() (normalFilesystemOriginWitnessKey, error) { return key, nil })
	require.NoError(t, err)
	require.Equal(t, expected, seen)
	for failed := range expected {
		t.Run(fmt.Sprintf("sync-%d", failed), func(t *testing.T) {
			calls := 0
			_, err := normalFilesystemConfirmPrefix(context.Background(), func(string) error {
				calls++
				if calls == failed+1 {
					return errors.New("TEST_ONLY durable failure")
				}
				return nil
			}, func() (normalFilesystemOriginWitnessKey, error) { return key, nil })
			require.Error(t, err)
			require.Equal(t, failed+1, calls)
			_, present := normalFilesystemOriginWitnesses.Load(key)
			require.False(t, present)
		})
	}
}

func TestNormalFilesystemMetadataFamilyDriftAndUnknownDoNotSync(t *testing.T) {
	key := normalFilesystemOriginWitnessKey{Guard: &Guard{}, Memory: *normalFilesystemMemory2TestProjection().Memory}
	key.Memory.InitialWALFamily = "caller-unknown-family"
	syncs := 0
	_, err := normalFilesystemConfirmPrefix(context.Background(), func(string) error { syncs++; return nil },
		func() (normalFilesystemOriginWitnessKey, error) { return key, nil })
	require.Error(t, err)
	require.Zero(t, syncs)
	key.Memory.InitialWALFamily = normalInitialWALFamilyKernelPrepV2
	fingerprints := 0
	_, err = normalFilesystemConfirmPrefix(context.Background(), func(string) error { return nil },
		func() (normalFilesystemOriginWitnessKey, error) {
			fingerprints++
			next := key
			if fingerprints > 1 {
				next.Memory.InitialWALFamily = ""
			}
			return next, nil
		})
	require.Error(t, err)
	_, present := normalFilesystemOriginWitnesses.Load(key)
	require.False(t, present)
}
