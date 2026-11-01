//go:build linux

package vmguard

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/stretchr/testify/require"
)

func TestNormalMemoryCommit2HostSyncNeverRepairsMissingOrChangedHistory(t *testing.T) {
	s, dir := initialBootTestState(t)
	defer s.close()
	name := normalMemoryHostReferenceFile
	raw := []byte(`{"original":1}`)
	require.NoError(t, normalMemoryHostWriteOnce(s, name, raw))
	id, e := normalMemoryHostFileIdentity(s, name, raw)
	require.NoError(t, e)
	require.NoError(t, normalMemoryHostSyncExisting(s, name, raw, id))
	require.Error(t, normalMemoryHostSyncExisting(s, "state.json", raw, id), "fixed name only")
	require.Error(t, normalMemoryHostSyncExisting(s, name, []byte(`{"original":2}`), id))
	replacement := filepath.Join(dir, "replacement")
	require.NoError(t, os.WriteFile(replacement, raw, 0600))
	require.NoError(t, os.Rename(replacement, filepath.Join(dir, name)))
	require.Error(t, normalMemoryHostSyncExisting(s, name, raw, id), "same bytes do not inherit old inode")
	require.NoError(t, os.Remove(filepath.Join(dir, name)))
	require.Error(t, normalMemoryHostSyncExisting(s, name, raw, id))
	_, e = os.Stat(filepath.Join(dir, name))
	require.True(t, os.IsNotExist(e), "sync must not reconstruct old history")
}
func TestNormalMemoryCommit2HostRequiresRealOriginalSourceBeforeWAL(t *testing.T) {
	s, dir := initialBootTestState(t)
	defer s.close()
	for _, o := range []*normalMemoryHostOriginSource{nil, {}, {selection: 2, ack: &bootprotocol.NormalMemoryOriginGuestAck{}}} {
		_, e := normalMemoryPersistHostCommit2Locked(context.Background(), s, o)
		require.Error(t, e)
		require.Error(t, o.recheckCommit2Locked(s), "JSON or callback existence is not durable source")
	}
	for _, name := range []string{normalMemoryHostReferenceFile, normalMemoryHostAckFile, normalMemoryHostCommitFile} {
		_, e := os.Stat(filepath.Join(dir, name))
		require.True(t, os.IsNotExist(e), name)
	}
}
func TestNormalMemoryCommit2HostCommitInodeCannotBeBorrowed(t *testing.T) {
	s, dir := initialBootTestState(t)
	defer s.close()
	raw := []byte(`{"test_only":"inode mechanism, not Source"}`)
	require.NoError(t, normalMemoryHostWriteOnce(s, normalMemoryHostCommitFile, raw))
	id, e := normalMemoryHostFileIdentity(s, normalMemoryHostCommitFile, raw)
	require.NoError(t, e)
	require.NoError(t, normalMemoryHostSyncExisting(s, normalMemoryHostCommitFile, raw, id))
	replacement := filepath.Join(dir, "replacement")
	require.NoError(t, os.WriteFile(replacement, raw, 0600))
	require.NoError(t, os.Rename(replacement, filepath.Join(dir, normalMemoryHostCommitFile)))
	require.Error(t, normalMemoryHostSyncExisting(s, normalMemoryHostCommitFile, raw, id))
}
