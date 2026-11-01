//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func metadataWiringHandoffFixture(t *testing.T) normalKernelPrepV2HandoffReference {
	t.Helper()
	f := newKernelPrepV2Fixture(t)
	e, x := f.run(t, kernelPrepPrepare)
	require.NoError(t, x)
	source, x := e.originalUserNamespaceBarrierSource()
	require.NoError(t, x)
	sha, x := e.ReferenceSHA256()
	require.NoError(t, x)
	build, x := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	require.NoError(t, x)
	a := e.After
	r := normalKernelPrepV2HandoffReference{ProtocolVersion: 1, Family: normalInitialWALFamilyKernelPrepV2, PreparationSHA256: sha, RuntimeDocumentSHA256: e.RuntimeDocumentSHA256, PhysicalPolicySHA256: e.PhysicalOriginPolicySHA256, KernelPolicySHA256: e.PolicySHA256, ProducerELFSHA256: a.ProducerELFSHA256, OriginalRoot: a.OriginalRoot, Identity: a.Kernel.Identity, Build: build, StateDevice: uint64(a.Kernel.StateDevice), StateInode: uint64(a.Kernel.StateInode), OriginalLock: a.Lock, OriginalControl: a.Control, OriginalWAL: [2]normalSystemRoleFileWitness{source.witnesses[normalKernelPrepV2IntentName], source.witnesses[normalKernelPrepV2CompleteName]}}
	copy(r.OriginalNamespaces[:], a.Namespaces)
	require.NoError(t, r.validate())
	return r
}
func TestKernelPrepV2HandoffComparableReferenceAndCrossTuple(t *testing.T) {
	r := metadataWiringHandoffFixture(t)
	h, e := r.referenceSHA256()
	require.NoError(t, e)
	raw, e := json.Marshal(r)
	require.NoError(t, e)
	var decoded normalKernelPrepV2HandoffReference
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.True(t, r == decoded)
	h2, e := decoded.referenceSHA256()
	require.NoError(t, e)
	require.Equal(t, h, h2)
	for _, name := range []string{"family", "version", "profile", "artifact", "config", "notes", "release", "Root", "state", "writable-control", "no-userns", "duplicate-ns", "WAL"} {
		t.Run(name, func(t *testing.T) {
			bad := r
			switch name {
			case "family":
				bad.Family = ""
			case "version":
				bad.ProtocolVersion = 2
			case "profile":
				bad.Build.ProfileID = NormalSplitV1PhysicalKernelProfileID
			case "artifact":
				bad.Build.KernelArtifactSHA256 = h
			case "config":
				bad.Build.KernelConfigSHA256 = h
			case "notes":
				bad.Build.KernelNotesSHA256 = h
			case "release":
				bad.Build.KernelRelease = "other"
			case "Root":
				bad.OriginalRoot.PID = 0
			case "state":
				bad.StateInode = 0
			case "writable-control":
				bad.OriginalControl.Mode |= 0022
			case "no-userns":
				bad.OriginalNamespaces[2].Inode = 123
			case "duplicate-ns":
				bad.OriginalNamespaces[1] = bad.OriginalNamespaces[0]
			case "WAL":
				bad.OriginalWAL[0].SHA256 = ""
			}
			require.Error(t, bad.validate())
		})
	}
}
func TestKernelPrepV2HandoffJSONAndNewGuardCannotProducePin(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	state, e := lockState(ctx, dir)
	require.NoError(t, e)
	defer state.close()
	g := &Guard{config: Config{StateDir: dir}}
	other := &Guard{config: g.config}
	for _, guard := range []*Guard{g, other} {
		o, e := guard.normalKernelPrepV2OriginalLocked(ctx, state)
		require.Nil(t, o)
		require.ErrorIs(t, e, ErrUnknown)
	}
	var raw NormalV2KernelBootstrapEvidence
	require.NoError(t, json.Unmarshal([]byte(`{}`), &raw))
	require.ErrorIs(t, g.installNormalKernelPrepV2OriginalLocked(ctx, state, raw), ErrUnknown)
	var pin normalKernelPrepV2HandoffPin
	require.NoError(t, json.Unmarshal([]byte(`{}`), &pin))
	_, fd, e := pin.originalTransferDescriptorLocked(ctx, state)
	require.Nil(t, fd)
	require.ErrorIs(t, e, ErrUnknown)
	f := newKernelPrepV2Fixture(t)
	evidence, e := f.run(t, kernelPrepPrepare)
	require.NoError(t, e)
	s, e := lockState(ctx, f.dir)
	require.NoError(t, e)
	defer s.close()
	require.ErrorIs(t, f.g.installNormalKernelPrepV2OriginalLocked(ctx, s, evidence), ErrUnknown, "model backend is not a production same-Guard Linux source")
}
