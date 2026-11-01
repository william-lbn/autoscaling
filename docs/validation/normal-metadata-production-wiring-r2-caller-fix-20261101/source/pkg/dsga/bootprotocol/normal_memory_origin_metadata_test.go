package bootprotocol

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func metadataWireReference(t *testing.T) NormalMemoryOriginHostReference {
	r := memoryOriginFixture(t)
	r.ProfileID = NormalMemoryOriginMetadataProfileID
	r.Prepared.Challenge.KernelBuildSHA256 = NormalMemoryOriginMetadataKernelSHA256
	r.Movable.Challenge.Preboot = r.Prepared
	var e error
	r.Movable.PreparedReceiptSHA256, e = r.Prepared.SHA256()
	require.NoError(t, e)
	r.Completion.Challenge = r.Movable.Challenge
	r.Completion.MovableReceiptSHA256, e = r.Movable.SHA256()
	require.NoError(t, e)
	return r
}
func TestMetadataWiringWireKeepsSixSevenEightShapeAndExactNewPair(t *testing.T) {
	old := memoryOriginFixture(t)
	before, e := json.Marshal(old)
	require.NoError(t, e)
	r := metadataWireReference(t)
	require.NoError(t, r.Validate())
	var wire bytes.Buffer
	require.NoError(t, WriteNormalMemoryOriginHostReference(&wire, r))
	back, e := ReadNormalMemoryOriginHostReference(&wire)
	require.NoError(t, e)
	h, e := r.SHA256()
	require.NoError(t, e)
	h2, e := back.SHA256()
	require.NoError(t, e)
	require.Equal(t, h, h2)
	require.EqualValues(t, 1, back.ProtocolVersion)
	ack := NormalMemoryOriginGuestAck{ProtocolVersion: 1, Scope: NormalMemoryOriginAckScope, HostReferenceSHA256: h, GuestSourceSHA256: h, GuestBootID: r.Prepared.GuestBootID, RootInstanceNonce: r.Movable.RootInstanceNonce}
	require.NoError(t, ack.MatchReference(r))
	commit, e := NewNormalMemoryOriginHostCommit(r, ack)
	require.NoError(t, e)
	require.NoError(t, commit.MatchReferenceAndAck(r, ack))
	after, e := json.Marshal(old)
	require.NoError(t, e)
	require.Equal(t, before, after)
	for _, name := range []string{"known-cross-pair", "unknown-profile", "new-profile-old-build"} {
		t.Run(name, func(t *testing.T) {
			bad := r
			if name == "unknown-profile" {
				bad.ProfileID = "recognized-somewhere-else"
			} else {
				bad.Prepared.Challenge.KernelBuildSHA256 = old.Prepared.Challenge.KernelBuildSHA256
				bad.Movable.Challenge.Preboot = bad.Prepared
				bad.Movable.PreparedReceiptSHA256, _ = bad.Prepared.SHA256()
				bad.Completion.Challenge = bad.Movable.Challenge
				bad.Completion.MovableReceiptSHA256, _ = bad.Movable.SHA256()
			}
			require.Error(t, bad.Validate())
		})
	}
}
