package hostguard

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 真实Node UID/RV账本；VM实际预算作为显式领域输入，生产Publisher的来源
// 读取由组合另测。原Shrink目标1GiB，取消后VM仍3GiB，不得提前退2GiB。
func TestHostActualCapacitySettlementRetainsCancelledShrinkBefore(t *testing.T) {
	f, store, snapshot, old, current, closed := settlementFixture(t)
	ctx := context.Background()
	closed.ActualMemoryBytes = current.BeforeBytes
	before, err := snapshot.State().UsedBytes()
	require.NoError(t, err)
	snapshot, retired, err := store.Settle(ctx, snapshot, current.RunnerPodUID, closed)
	require.NoError(t, err)
	require.Len(t, retired, 1)
	require.Equal(t, old, retired[0].Plan)
	m := snapshot.State()
	require.EqualValues(t, 7, m.ProtocolVersion)
	used, err := m.UsedBytes()
	require.NoError(t, err)
	require.Equal(t, before, used, "删除旧操作hold不等于可以少记仍大的实际VM")
	p, err := m.Placement(current.RunnerPodUID)
	require.NoError(t, err)
	require.Equal(t, current.BeforeBytes, p.MemoryBytes)
	require.Equal(t, current.Request.TargetVMMemoryBytes, p.Settled.Request.TargetVMMemoryBytes)
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	var restored Model
	require.NoError(t, json.Unmarshal(raw, &restored))
	require.Equal(t, m, restored)
	require.Contains(t, string(raw), `"actual_memory_bytes":"3221225472"`)
	require.NotContains(t, string(raw), `"through_key"`, "无Through时不得写空Key污染新结清")
	bad := restored.Clone()
	bad.ProtocolVersion = 6
	require.Error(t, bad.Validate(), "旧v6不能承载实际容量语义")
	bad = restored.Clone()
	(*bad.Placements)[0].Settled.ActualMemoryBytes = 2 << 30
	require.Error(t, bad.Validate(), "实际容量必须是该原计划的before或target")
	f.mu.Lock()
	node := f.node.DeepCopy()
	f.mu.Unlock()
	require.ErrorIs(t, InspectClaim(node, current, SourceID(m.Origin, current), closed.HostClaim), ErrSettled)
	require.NoError(t, InspectRetainedClaim(node, current, SourceID(m.Origin, current), closed.HostClaim, closed))
	_, _, err = store.Reserve(ctx, snapshot, current)
	require.Error(t, err)
	version := snapshot.ResourceVersion()
	snapshot, _, err = store.Settle(ctx, snapshot, current.RunnerPodUID, closed)
	require.NoError(t, err)
	require.Equal(t, version, snapshot.ResourceVersion())
	if dir := os.Getenv("DSGA_HOST_ACTUAL_WIRE_DIR"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "node-v7.json"), raw, 0o600))
	}
}

func TestHostActualCapacityCannotRegressOrLoseUnknownSettlement(t *testing.T) {
	f, store, snapshot, _, current, closed := settlementFixture(t)
	ctx := context.Background()
	closed.ActualMemoryBytes = current.BeforeBytes
	f.mu.Lock()
	f.loseReply = true
	f.mu.Unlock()
	_, _, err := store.Settle(ctx, snapshot, current.RunnerPodUID, closed)
	var unknown *UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	snapshot, err = store.Load(ctx)
	require.NoError(t, err)
	for _, actual := range []control.Uint64{0, 1 << 30, 2 << 30, 4 << 30} {
		changed := closed
		changed.ActualMemoryBytes = actual
		_, _, err = store.Settle(ctx, snapshot, current.RunnerPodUID, changed)
		require.Error(t, err, "同预算不可改实际容量或投影为原目标")
	}
	future := current
	future.Request.Key.Revision++
	future.Request.Key.OperationID.Low++
	future.Request.Kind, future.Request.TargetVMMemoryBytes, future.BeforeBytes = control.Grow, 4<<30, closed.ActualMemoryBytes
	snapshot, _, err = store.Reserve(ctx, snapshot, future)
	require.NoError(t, err)
	used, err := snapshot.State().UsedBytes()
	require.NoError(t, err)
	require.EqualValues(t, (5<<30)+(64<<20), used)
	_, _, err = store.Settle(ctx, snapshot, current.RunnerPodUID, closed)
	require.NoError(t, err, "同原结清恢复不能清掉未来真实峰值")
}
