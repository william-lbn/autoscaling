package resourceguard

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type normalImmutableRejectKernel struct {
	kernel
	calls int
}

var normalImmutableFixtureRefusal = errors.New("TEST_ONLY original normal immutable target rejected")

func (k *normalImmutableRejectKernel) normalImmutableBounds(Limits) error {
	k.calls++
	return normalImmutableFixtureRefusal
}

// 确定性拒绝模型只验证真正Guard锁/WAL边界，不冒称已实读ISO或实际cgroup拒绝。
func TestNormalImmutableBoundsRejectsBeforeNewIntent(t *testing.T) {
	ctx := context.Background()
	unchanged := func(t *testing.T, path string, before []byte, st os.FileInfo) {
		t.Helper()
		after, e := os.ReadFile(path)
		require.NoError(t, e)
		require.Equal(t, before, after)
		now, e := os.Stat(path)
		require.NoError(t, e)
		require.True(t, os.SameFile(st, now), "rejection must not rewrite state inode")
	}
	t.Run("bootstrap", func(t *testing.T) {
		g, k, b, _ := freshBootstrapFixture(t)
		reject := &normalImmutableRejectKernel{kernel: k}
		g.kernel = reject
		_, e := g.Bootstrap(ctx, b)
		require.ErrorIs(t, e, normalImmutableFixtureRefusal)
		require.Equal(t, 1, reject.calls)
		require.Equal(t, 0, k.writes)
		_, e = os.Stat(filepath.Join(g.config.StateDir, "state.json"))
		require.True(t, os.IsNotExist(e), "rejected target wrote new Bootstrap intent")
	})
	t.Run("apply", func(t *testing.T) {
		g, k, c := fixtureGuard(t)
		path := filepath.Join(g.config.StateDir, "state.json")
		before, e := os.ReadFile(path)
		require.NoError(t, e)
		st, e := os.Stat(path)
		require.NoError(t, e)
		reject := &normalImmutableRejectKernel{kernel: k}
		g.kernel = reject
		_, e = g.Apply(ctx, c)
		require.ErrorIs(t, e, normalImmutableFixtureRefusal)
		require.Equal(t, 1, reject.calls)
		require.Equal(t, 0, k.writes)
		unchanged(t, path, before, st)
	})
	t.Run("replay-pending", func(t *testing.T) {
		g, k, c := fixtureGuard(t)
		k.fail = true
		_, e := g.Apply(ctx, c)
		require.ErrorIs(t, e, ErrUnknown)
		path := filepath.Join(g.config.StateDir, "state.json")
		before, e := os.ReadFile(path)
		require.NoError(t, e)
		st, e := os.Stat(path)
		require.NoError(t, e)
		writes := k.writes
		reject := &normalImmutableRejectKernel{kernel: k}
		g.kernel = reject
		k.fail = false
		_, e = g.ReplayOriginal(ctx, c)
		require.ErrorIs(t, e, normalImmutableFixtureRefusal)
		require.Equal(t, 1, reject.calls)
		require.Equal(t, writes, k.writes)
		unchanged(t, path, before, st)
	})
	t.Run("repair-held", func(t *testing.T) {
		g, k, c, request := heldRepairFixture(t, true)
		k.e.MemoryMaxBytes = c.MemoryMaxBytes / 2
		path := filepath.Join(g.config.StateDir, "state.json")
		before, e := os.ReadFile(path)
		require.NoError(t, e)
		st, e := os.Stat(path)
		require.NoError(t, e)
		writes := k.writes
		reject := &normalImmutableRejectKernel{kernel: g.kernel}
		g.kernel = reject
		_, e = g.RepairHeld(ctx, request)
		require.ErrorIs(t, e, normalImmutableFixtureRefusal)
		require.Equal(t, 1, reject.calls)
		require.Equal(t, writes, k.writes)
		unchanged(t, path, before, st)
	})
}

func TestNormalImmutableBoundsKeepsCompletedReadbackReadOnly(t *testing.T) {
	g, k, c := fixtureGuard(t)
	ctx := context.Background()
	_, e := g.Apply(ctx, c)
	require.NoError(t, e)
	writes := k.writes
	reject := &normalImmutableRejectKernel{kernel: k}
	g.kernel = reject
	_, e = g.Apply(ctx, c)
	require.NoError(t, e)
	_, e = g.Observe(ctx, c)
	require.NoError(t, e)
	require.Equal(t, 0, reject.calls)
	require.Equal(t, writes, k.writes)
}
