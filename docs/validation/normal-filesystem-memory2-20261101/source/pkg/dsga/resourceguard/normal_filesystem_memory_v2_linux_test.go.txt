//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// 仅验证纯合同投影/确定性同步故障，不创建实际Memory opaque或normal许可。
func normalFilesystemMemory2TestProjection() normalFilesystemOriginIntent {
	h := strings.Repeat("a", 64)
	root := nativeBirthConsumer{PID: 123, StartTicks: 456, InstanceNonce: h}
	prefix := initialPrebootGuestWitnessKey{Device: 11, Inode: 22, LockDevice: 11, LockInode: 33}
	for i := range prefix.SHA256 {
		prefix.SHA256[i] = h
	}
	m := normalMemoryOriginConfirmedReference{ProtocolVersion: 2, OriginalRoot: root, GuestBootID: "11111111-1111-1111-1111-111111111111", RuntimeDocumentSHA256: h, ResourceSourceSHA256: h, InitialWAL: prefix}
	for i := range m.SHA256 {
		m.SHA256[i] = h
	}
	return normalFilesystemOriginIntent{ProtocolVersion: 2, Scope: normalFilesystemOriginScopeV2, LayoutVersion: normalFilesystemLayoutVersion, StateDevice: 11, StateInode: 22, LockDevice: 11, LockInode: 33, System: normalSystemLifecycleBinding{RootConsumer: root, GuestBootID: m.GuestBootID, RuntimeDocumentSHA256: h}, InitialPrefixSHA256: prefix.SHA256, Memory: &m}
}

func TestNormalFilesystemMemory2VersionedProjection(t *testing.T) {
	in := normalFilesystemMemory2TestProjection()
	require.NoError(t, normalFilesystemValidateMemoryReference(in))
	digest, e := normalFilesystemMemoryReferenceSHA(in)
	require.NoError(t, e)
	require.Len(t, digest, 64)
	ref := normalFilesystemReferenceFromIntent(in, "intent", "complete")
	require.Equal(t, uint32(2), ref.ProtocolVersion)
	require.Equal(t, digest, ref.MemorySourceSHA256)
	require.NoError(t, normalFilesystemValidateReferenceVersion(ref))
	raw, e := json.Marshal(in)
	require.NoError(t, e)
	require.Contains(t, string(raw), `"original_confirmed_memory":`)
	legacy := in
	legacy.ProtocolVersion = 1
	legacy.Scope = normalFilesystemOriginScope
	legacy.Memory = nil
	require.NoError(t, normalFilesystemValidateMemoryReference(legacy))
	raw, e = json.Marshal(legacy)
	require.NoError(t, e)
	require.NotContains(t, string(raw), "original_confirmed_memory")
	old := normalFilesystemReferenceFromIntent(legacy, "intent", "complete")
	require.Equal(t, uint32(1), old.ProtocolVersion)
	require.Empty(t, old.MemorySourceSHA256)
	require.NoError(t, normalFilesystemValidateReferenceVersion(old))
	raw, e = json.Marshal(old)
	require.NoError(t, e)
	require.NotContains(t, string(raw), "memory_source_sha256")
}

func TestNormalFilesystemMemory2RejectsCrossSource(t *testing.T) {
	cases := map[string]func(*normalFilesystemOriginIntent){
		"no-memory":          func(in *normalFilesystemOriginIntent) { in.Memory = nil },
		"legacy-with-memory": func(in *normalFilesystemOriginIntent) { in.ProtocolVersion = 1; in.Scope = normalFilesystemOriginScope },
		"future":             func(in *normalFilesystemOriginIntent) { in.ProtocolVersion = 3 },
		"old-scope":          func(in *normalFilesystemOriginIntent) { in.Scope = normalFilesystemOriginScope },
		"new-root":           func(in *normalFilesystemOriginIntent) { in.Memory.OriginalRoot.InstanceNonce = strings.Repeat("b", 64) },
		"boot":               func(in *normalFilesystemOriginIntent) { in.Memory.GuestBootID = "22222222-2222-2222-2222-222222222222" },
		"runtime":            func(in *normalFilesystemOriginIntent) { in.Memory.RuntimeDocumentSHA256 = strings.Repeat("b", 64) },
		"state":              func(in *normalFilesystemOriginIntent) { in.Memory.InitialWAL.Inode++ },
		"lock":               func(in *normalFilesystemOriginIntent) { in.Memory.InitialWAL.LockInode++ },
		"initial-wal":        func(in *normalFilesystemOriginIntent) { in.Memory.InitialWAL.SHA256[7] = strings.Repeat("b", 64) },
		"memory-wal":         func(in *normalFilesystemOriginIntent) { in.Memory.SHA256[2] = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := normalFilesystemMemory2TestProjection()
			mutate(&in)
			require.Error(t, normalFilesystemValidateMemoryReference(in))
		})
	}
	for _, version := range []uint32{0, 3} {
		require.Error(t, normalFilesystemValidateReferenceVersion(normalFilesystemOriginReference{LayoutVersion: 2, ProtocolVersion: version}))
	}
	require.Error(t, normalFilesystemValidateReferenceVersion(normalFilesystemOriginReference{LayoutVersion: 2, ProtocolVersion: 1, MemorySourceSHA256: strings.Repeat("a", 64)}))
	require.Error(t, normalFilesystemValidateReferenceVersion(normalFilesystemOriginReference{LayoutVersion: 2, ProtocolVersion: 2}))
}

func TestNormalFilesystemMemory2AllThirteenSyncsRequired(t *testing.T) {
	in := normalFilesystemMemory2TestProjection()
	key := normalFilesystemOriginWitnessKey{Guard: &Guard{}, Memory: *in.Memory}
	names := append(append([]string(nil), initialPrebootGuestFiles[:]...), normalFilesystemOriginIntentName, normalFilesystemOriginCompleteName)
	for _, name := range normalMemoryConfirmedFiles() {
		names = append(names, name)
	}
	require.Len(t, names, 13)
	var seen []string
	got, e := normalFilesystemConfirmPrefix(context.Background(), func(name string) error { seen = append(seen, name); return nil }, func() (normalFilesystemOriginWitnessKey, error) { return key, nil })
	require.NoError(t, e)
	require.Equal(t, key, got)
	require.Equal(t, names, seen)
	for failed := range names {
		t.Run(fmt.Sprintf("sync-%d", failed), func(t *testing.T) {
			n := 0
			_, e := normalFilesystemConfirmPrefix(context.Background(), func(string) error {
				at := n
				n++
				if at == failed {
					return errors.New("TEST_ONLY sync failure")
				}
				return nil
			}, func() (normalFilesystemOriginWitnessKey, error) { return key, nil })
			require.Error(t, e)
			require.Equal(t, failed+1, n)
			_, ok := normalFilesystemOriginWitnesses.Load(key)
			require.False(t, ok)
		})
	}
	calls := 0
	_, e = normalFilesystemConfirmPrefix(context.Background(), func(string) error { return nil }, func() (normalFilesystemOriginWitnessKey, error) {
		calls++
		changed := key
		if calls > 1 {
			changed.Memory.Files[0].Inode++
		}
		return changed, nil
	})
	require.Error(t, e, "same bytes under a new memory inode cannot be confirmed")
}

func TestNormalFilesystemMemory2CannotCreateBrokerOpaqueFromJSON(t *testing.T) {
	g := &Guard{}
	_, e := g.bindNormalMemoryBrokerConsumerLocked(context.Background(), nil, nil, nil)
	require.ErrorIs(t, e, ErrUnknown)
	raw, e := json.Marshal(normalFilesystemMemory2TestProjection().Memory)
	require.NoError(t, e)
	var ref normalMemoryOriginConfirmedReference
	require.NoError(t, json.Unmarshal(raw, &ref))
	source := &normalMemoryBrokerConsumerSource{g: g, reference: ref}
	require.ErrorIs(t, source.recheckLocked(context.Background(), nil, g), ErrUnknown)
	require.ErrorIs(t, source.requireContinuousQualification(), ErrUnknown)
}
