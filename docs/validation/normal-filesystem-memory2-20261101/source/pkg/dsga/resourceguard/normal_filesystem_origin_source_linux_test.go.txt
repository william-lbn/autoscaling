//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalFilesystemVisibleCompleteCannotCreateWitness(t *testing.T) {
	// 确定性sync故障模型，不冒称Guest真实磁盘故障。
	for failed := 0; failed < 10; failed++ {
		t.Run(fmt.Sprintf("sync-%d", failed), func(t *testing.T) {
			key := normalFilesystemOriginWitnessKey{Guard: &Guard{}}
			key.SHA256[1] = "complete-visible"
			n := 0
			_, e := normalFilesystemConfirmPrefix(context.Background(), func(string) error {
				at := n
				n++
				if at == failed {
					return errors.New("TEST_ONLY sync failed after complete visible")
				}
				return nil
			}, func() (normalFilesystemOriginWitnessKey, error) { return key, nil })
			require.Error(t, e)
			_, visible := normalFilesystemOriginWitnesses.Load(key)
			require.False(t, visible)
			require.Equal(t, failed+1, n)
		})
	}
}
func TestNormalFilesystemExplicitSyncIdentityAndCurrentGuard(t *testing.T) {
	key := normalFilesystemOriginWitnessKey{Guard: &Guard{}}
	key.SHA256[0], key.SHA256[1] = "original-intent", "original-complete"
	count := 0
	got, e := normalFilesystemConfirmPrefix(context.Background(), func(string) error { count++; return nil }, func() (normalFilesystemOriginWitnessKey, error) { return key, nil })
	require.NoError(t, e)
	require.Equal(t, 10, count)
	normalFilesystemOriginWitnesses.Store(got, struct{}{})
	defer normalFilesystemOriginWitnesses.Delete(got)
	_, ok := normalFilesystemOriginWitnesses.Load(key)
	require.True(t, ok)
	other := key
	other.Guard = &Guard{}
	_, ok = normalFilesystemOriginWitnesses.Load(other)
	require.False(t, ok, "new Guard cannot borrow durability")
	other = key
	other.Consumer.InstanceNonce = "new-root"
	_, ok = normalFilesystemOriginWitnesses.Load(other)
	require.False(t, ok)
	other = key
	other.SHA256[0] = "another-original"
	_, ok = normalFilesystemOriginWitnesses.Load(other)
	require.False(t, ok)
	call := 0
	_, e = normalFilesystemConfirmPrefix(context.Background(), func(string) error { return nil }, func() (normalFilesystemOriginWitnessKey, error) {
		call++
		if call > 1 {
			return other, nil
		}
		return key, nil
	})
	require.Error(t, e, "changed original must not be enrolled after sync")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e = normalFilesystemConfirmPrefix(ctx, func(string) error { t.Fatal("canceled prefix must not sync"); return nil }, func() (normalFilesystemOriginWitnessKey, error) { return key, nil })
	require.ErrorIs(t, e, context.Canceled)
}
func TestNormalFilesystemRealStateRejectsSameBytesNewInode(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "original-state")
	require.NoError(t, os.MkdirAll(dir, 0700))
	s, e := lockState(context.Background(), dir)
	require.NoError(t, e)
	defer s.close()
	for _, name := range []string{normalFilesystemOriginIntentName, normalFilesystemOriginCompleteName} {
		raw := []byte("{}")
		if name == normalFilesystemOriginIntentName {
			// 仍只测试inode替换；新witness须解析显式旧协议，不能用空对象冒充intent。
			raw, e = json.Marshal(normalFilesystemOriginIntent{ProtocolVersion: 1, Scope: normalFilesystemOriginScope})
			require.NoError(t, e)
		}
		require.NoError(t, s.createPhysicalFile(name, raw))
	}
	g := &Guard{}
	key, e := normalFilesystemWitnessLocked(g, s, nativeBirthConsumer{}, initialPrebootGuestWitnessKey{})
	require.NoError(t, e)
	normalFilesystemOriginWitnesses.Store(key, struct{}{})
	defer normalFilesystemOriginWitnesses.Delete(key)
	old := filepath.Join(dir, normalFilesystemOriginCompleteName)
	require.NoError(t, os.Rename(old, old+".original"))
	require.NoError(t, os.WriteFile(old, []byte("{}"), 0600))
	newKey, e := normalFilesystemWitnessLocked(g, s, nativeBirthConsumer{}, initialPrebootGuestWitnessKey{})
	require.NoError(t, e)
	require.Equal(t, key.SHA256, newKey.SHA256)
	require.NotEqual(t, key.Files, newKey.Files)
	_, ok := normalFilesystemOriginWitnesses.Load(newKey)
	require.False(t, ok)
}

func TestNormalFilesystemSameSuperblockPreflightRejectsNonFixedState(t *testing.T) {
	for _, cfg := range []Config{{}, {StateDir: "/tmp/caller-state", SystemCgroup: true}, {StateDir: FixedStateDir, SystemCgroup: false}} {
		require.Error(t, normalFilesystemPreflightSameSuperblockState(cfg))
	}
	var g *Guard
	_, e := g.captureNormalFilesystemOrigin(nil, nil)
	require.ErrorIs(t, e, ErrUnknown)
	_, e = g.recoverNormalFilesystemOrigin(nil, nil, nil)
	require.ErrorIs(t, e, ErrUnknown)
}

func TestNormalFilesystemTransferredFDRemainsOriginalAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "original")
	require.NoError(t, os.WriteFile(path, []byte("original"), 0600))
	f, e := os.Open(path)
	require.NoError(t, e)
	duplicate, e := normalFilesystemDuplicateFile(f)
	require.NoError(t, e)
	defer duplicate.Close()
	require.NoError(t, f.Close())
	_, e = normalFilesystemDuplicateFile(f)
	require.Error(t, e)
	replacement, e := os.Open(os.DevNull)
	require.NoError(t, e)
	defer replacement.Close()
	data := make([]byte, 8)
	n, e := duplicate.ReadAt(data, 0)
	require.NoError(t, e)
	require.Equal(t, "original", string(data[:n]))
}

// PG17-U-FS-010：固定17mount原始来源的三条引用路径不得丢失布局版本。
// 这里只检验纯投影；实际namespace/root FD及拒绝发生在任何Recover之前由Guest用例验证。
func TestNormalFilesystemReferenceCarriesOriginalLayout(t *testing.T) {
	in := normalFilesystemOriginIntent{ProtocolVersion: 1, Scope: normalFilesystemOriginScope, LayoutVersion: normalFilesystemLayoutVersion, VminitSHA256: "vminit", Namespace: normalFilesystemObject{Device: 1, Inode: 2}, Root: normalFilesystemObject{Device: 3, Inode: 4, MountID: 5}}
	in.System.RootConsumer = nativeBirthConsumer{PID: 123, StartTicks: 456, InstanceNonce: "original-root"}
	in.System.GuestBootID = "original-boot"
	in.System.RuntimeDocumentSHA256 = "runtime"
	in.System.NativeBuildSHA256 = "build"
	in.System.SystemManifestSHA256 = "system"
	want := normalFilesystemOriginReference{LayoutVersion: normalFilesystemLayoutVersion, ProtocolVersion: 1, IntentSHA256: "intent", CompletionSHA256: "complete", OriginalRoot: in.System.RootConsumer, GuestBootID: in.System.GuestBootID, RuntimeDocumentSHA256: in.System.RuntimeDocumentSHA256, NativeBuildSHA256: in.System.NativeBuildSHA256, SystemManifestSHA256: in.System.SystemManifestSHA256, VminitSHA256: in.VminitSHA256, Namespace: in.Namespace, Root: in.Root}
	got := normalFilesystemReferenceFromIntent(in, "intent", "complete")
	require.Equal(t, uint32(2), got.LayoutVersion)
	require.Equal(t, want, got, "every original binding survives namespace/root FD handoff")
	legacy := got
	legacy.LayoutVersion = 0
	require.NotEqual(t, legacy, got, "an omitted layout cannot identify the current original source")
}
