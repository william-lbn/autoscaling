//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMetadataWiringConfirmedReferenceLegacyBytesAndFamilyComparison(t *testing.T) {
	m := metadataWiringConfirmedReference()
	type legacy struct {
		ProtocolVersion       uint32                          `json:"protocol_version"`
		OriginalRoot          nativeBirthConsumer             `json:"original_root"`
		GuestBootID           string                          `json:"guest_boot_id"`
		RuntimeDocumentSHA256 string                          `json:"runtime_document_sha256"`
		ResourceSourceSHA256  string                          `json:"resource_source_sha256"`
		InitialWAL            initialPrebootGuestWitnessKey   `json:"original_initial_wal"`
		Files                 [3]arrayPopulateHistoryIdentity `json:"files"`
		SHA256                [3]string                       `json:"sha256"`
	}
	old := legacy{m.ProtocolVersion, m.OriginalRoot, m.GuestBootID, m.RuntimeDocumentSHA256, m.ResourceSourceSHA256, m.InitialWAL, m.Files, m.SHA256}
	before, e := json.Marshal(old)
	require.NoError(t, e)
	after, e := json.Marshal(m)
	require.NoError(t, e)
	require.Equal(t, before, after)
	h, e := m.referenceSHA256()
	require.NoError(t, e)
	require.Equal(t, physicalDigest(before), h)
	next := m
	next.InitialWALFamily = normalInitialWALFamilyKernelPrepV2
	require.False(t, next == m)
	raw, e := json.Marshal(next)
	require.NoError(t, e)
	var decoded normalMemoryOriginConfirmedReference
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.True(t, next == decoded)
	next.InitialWALFamily = "unknown"
	_, e = next.referenceSHA256()
	require.Error(t, e)
}
func TestMetadataWiringRegionRequiresNewExactVersionAndNoRawR2Source(t *testing.T) {
	old := guestMaximumFixture(t)
	raw, e := json.Marshal(old)
	require.NoError(t, e)
	metadata := old
	metadata.Build, e = LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	require.NoError(t, e)
	require.Error(t, metadata.validate())
	metadata.ProtocolVersion = 2
	metadata.Scope = guestMemoryMaximumMetadataScope
	require.NoError(t, metadata.validate())
	bad := metadata
	bad.Build = old.Build
	require.Error(t, bad.validate())
	after, e := json.Marshal(old)
	require.NoError(t, e)
	require.Equal(t, raw, after)
	g := &Guard{}
	in := normalMemoryOriginIntent{}
	in.Host.ProfileID = NormalSplitMetadataV1PhysicalKernelProfileID
	_, e = normalMemoryBrokerCurrentLocked(context.Background(), g, nil, in, nil)
	require.ErrorIs(t, e, ErrUnknown)
}
