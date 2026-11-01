//go:build linux

package vmguard

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNormalMemoryOriginHostCrossDecoderAndOriginalGeometry(t *testing.T) {
	in := geometryInputFixture()
	id := geometryIdentityFixture()
	source := initialBootSource{Identity: id, OriginalBase: 256 << 20}
	n, e := normalMemoryCrossGeometry(source, in)
	require.NoError(t, e)
	require.Equal(t, id.MaximumBytes, n.MaximumBytes)
	require.Equal(t, n.BaseBytes+n.BackendBytes, n.MaximumBytes)
	require.NotEqual(t, n.ActualBytes, n.MaximumBytes, "current plugged pages cannot replace maximum")
	in.BackendSize = []byte("1")
	_, e = normalMemoryCrossGeometry(source, in)
	require.Error(t, e)
}
func TestNormalMemoryOriginHostWriteOnceAndIdentity(t *testing.T) {
	s, dir := initialBootTestState(t)
	defer s.close()
	raw := []byte("{\"original\":1}")
	require.NoError(t, normalMemoryHostWriteOnce(s, normalMemoryHostReferenceFile, raw))
	one, e := normalMemoryHostFileIdentity(s, normalMemoryHostReferenceFile, raw)
	require.NoError(t, e)
	require.NoError(t, normalMemoryHostWriteOnce(s, normalMemoryHostReferenceFile, raw))
	again, e := normalMemoryHostFileIdentity(s, normalMemoryHostReferenceFile, raw)
	require.NoError(t, e)
	require.Equal(t, one, again)
	require.Error(t, normalMemoryHostWriteOnce(s, normalMemoryHostReferenceFile, []byte("{}")))
	require.Error(t, normalMemoryHostWriteOnce(s, "state.json", raw))
	tmp := filepath.Join(dir, "replacement")
	require.NoError(t, os.WriteFile(tmp, raw, 0600))
	require.NoError(t, os.Rename(tmp, filepath.Join(dir, normalMemoryHostReferenceFile)))
	changed, e := normalMemoryHostFileIdentity(s, normalMemoryHostReferenceFile, raw)
	require.NoError(t, e)
	require.NotEqual(t, one, changed, "same bytes cannot inherit original inode proof")
}
func TestNormalMemoryOriginHostOpaqueUsesRealStartedPeerAndWAL(t *testing.T) {
	// QEMU/API/Guest是原明确模型；真实Start/唯一Wait/pidfd/peer/exe/flock/WAL。
	f, original, trace := initialLifecycleFixture(t, "success")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, original.Path, original.Args[1:]...)
	cmd.Env = original.Env
	result := make(chan error, 1)
	go func() { result <- f.runner.RunQEMU(ctx, cmd) }()
	defer func() {
		cancel()
		select {
		case <-result:
		case <-time.After(10 * time.Second):
			t.Error("original Wait did not terminate")
		}
	}()
	require.NoError(t, waitInitialModelTrace(ctx, trace, "completion_received"))
	l, ok := f.runner.initial.(*linuxInitialLifecycle)
	require.True(t, ok)
	for {
		l.mu.Lock()
		ready := l.ready != nil
		l.mu.Unlock()
		if ready {
			break
		}
		require.NoError(t, initialBootPause(ctx))
	}
	l.processGuard.RLock()
	defer l.processGuard.RUnlock()
	e := f.runner.engine.withRecord(ctx, func(_ *record, s *lockedState) error {
		proof, e := l.publication(s)
		if e != nil {
			return e
		}
		source, e := captureNormalMemoryHostOriginLocked(ctx, s, l, proof)
		if e != nil {
			return e
		}
		require.NoError(t, source.reference.Validate())
		require.Zero(t, source.selection, "old immutable v1 only produces component facts")
		_, err := (&initialBootChannel{}).completeWithNormalMemoryOrigin(ctx, s, source)
		require.Error(t, err, "old profile cannot implicitly send new5/6/7")
		if _, err = os.Stat(filepath.Join(f.config.StateDir, normalMemoryHostAckFile)); !os.IsNotExist(err) {
			t.Fatalf("old protocol wrote ACK: %v", err)
		}

		require.Equal(t, f.runner.birth.Identity.MaximumBytes, source.reference.Prepared.Challenge.Maximum)
		require.Error(t, source.recheckLocked(ctx, s, l, proof), "source cannot become released before actual Guest ACK")
		require.True(t, source.process == l.process)
		raw, e := initialBootReadFile(s, normalMemoryHostReferenceFile)
		if e != nil {
			return e
		}
		require.Equal(t, initialBootCanonical(source.reference), raw)
		return nil
	})
	require.NoError(t, e)
	b, e := os.ReadFile(trace)
	require.NoError(t, e)
	require.Equal(t, 1, bytes.Count(b, []byte("qom_set:")), "source capture only reads QMP")
	require.NotContains(t, string(b), "BAD_")
}
