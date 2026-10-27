package hostguard

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 这里只验证真实Node模型/UID-RV CAS；VM/PG关闭证明作为显式领域夹具输入。
// 真实生产Publisher从VM永久边界派生Through，组合用例另覆盖该读取链。
func TestHostClosedThroughSettlesCancelledPeakAndRejectsDelayedReserve(t *testing.T) {
	api, store, snapshot, _, current, closed := settlementFixture(t)
	ctx := context.Background()
	snapshot, _, err := store.Settle(ctx, snapshot, current.RunnerPodUID, closed)
	require.NoError(t, err)
	if dir := os.Getenv("DSGA_HOST_WIRE_DIR"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		legacy := snapshot.State()
		legacy.ProtocolVersion = 5
		raw, err := json.Marshal(legacy)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "node-v5.json"), raw, 0o600))
	}
	cancelled := current
	cancelled.Request.Key.Revision++
	cancelled.Request.Key.OperationID.Low++
	cancelled.Request.Kind = control.Grow
	cancelled.BeforeBytes, cancelled.Request.TargetVMMemoryBytes = 1<<30, 3<<30
	snapshot, _, err = store.Reserve(ctx, snapshot, cancelled)
	require.NoError(t, err)
	snapshot, err = store.BindClaim(ctx, snapshot, cancelled, budget.ObjectRef{Name: "cancelled-host", UID: "cancelled-host-uid"})
	require.NoError(t, err)
	future := cancelled
	future.Request.Key.Revision++
	future.Request.Key.OperationID.Low++
	future.Request.TargetVMMemoryBytes = 4 << 30
	snapshot, _, err = store.Reserve(ctx, snapshot, future)
	require.NoError(t, err)
	extended := closed
	extended.ThroughKey, extended.ThroughSequence = cancelled.Request.Key, closed.BudgetSequence+10
	api.mu.Lock()
	node := api.node.DeepCopy()
	api.mu.Unlock()
	require.NoError(t, InspectRetainedClaim(node, current, SourceID(snapshot.State().Origin, current), closed.HostClaim, extended), "只读预算可核尚未推进水位的同原Node来源")
	snapshot, retired, err := store.Settle(ctx, snapshot, current.RunnerPodUID, extended)
	require.NoError(t, err)
	require.Len(t, retired, 1)
	require.Equal(t, cancelled, retired[0].Plan)
	require.EqualValues(t, 6, snapshot.State().ProtocolVersion)
	_, err = snapshot.Hold(current)
	require.NoError(t, err, "当前预算来源永久保留")
	_, err = snapshot.Hold(future)
	require.NoError(t, err, "关闭水位之后的真实在途承诺保留")
	used, err := snapshot.State().UsedBytes()
	require.NoError(t, err)
	require.EqualValues(t, (5<<30)+(64<<20), used)
	for _, p := range []Plan{current, cancelled} {
		_, _, err = store.Reserve(ctx, snapshot, p)
		require.Error(t, err, "较新取消不能在峰值结清后被迟到重放")
	}
	alias := cancelled
	alias.Request.Key.OperationID.Low++
	_, _, err = store.Reserve(ctx, snapshot, alias)
	require.Error(t, err, "同代次/Revision换operation ID也不能绕过永久水位")
	_, _, err = store.Settle(ctx, snapshot, current.RunnerPodUID, closed)
	require.Error(t, err, "Node不能回退到不含取消证明的旧结清")
	api.mu.Lock()
	node = api.node.DeepCopy()
	api.mu.Unlock()
	require.Error(t, InspectRetainedClaim(node, current, SourceID(snapshot.State().Origin, current), closed.HostClaim, closed))
	require.NoError(t, InspectRetainedClaim(node, current, SourceID(snapshot.State().Origin, current), closed.HostClaim, extended))
	version := snapshot.ResourceVersion()
	snapshot, retired, err = store.Settle(ctx, snapshot, current.RunnerPodUID, extended)
	require.NoError(t, err)
	require.Empty(t, retired)
	require.Equal(t, version, snapshot.ResourceVersion())
	raw, err := json.Marshal(snapshot.State())
	require.NoError(t, err)
	var restored Model
	require.NoError(t, json.Unmarshal(raw, &restored))
	require.Equal(t, snapshot.State(), restored)
	legacy := restored.Clone()
	legacy.ProtocolVersion = 5
	require.Error(t, legacy.Validate(), "新水位语义不能投影为旧v5")
	if dir := os.Getenv("DSGA_HOST_WIRE_DIR"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "node-v6.json"), raw, 0o600))
	}
}

func TestHostClosedThroughRejectsForgedOrNonMonotonicWitness(t *testing.T) {
	_, store, snapshot, _, current, closed := settlementFixture(t)
	ctx := context.Background()
	snapshot, _, err := store.Settle(ctx, snapshot, current.RunnerPodUID, closed)
	require.NoError(t, err)
	for _, mode := range []string{"missing-key", "same-key", "other-boot", "old-sequence", "changed-budget", "alias-key"} {
		t.Run(mode, func(t *testing.T) {
			next := closed
			next.ThroughKey, next.ThroughSequence = closed.Request.Key, closed.BudgetSequence+10
			next.ThroughKey.Revision++
			next.ThroughKey.OperationID.Low++
			switch mode {
			case "missing-key":
				var empty control.Key
				next.ThroughKey = empty
			case "same-key":
				next.ThroughKey = closed.Request.Key
			case "other-boot":
				next.ThroughKey.Instance.PGBootID++
			case "old-sequence":
				next.ThroughSequence = closed.BudgetSequence
			case "changed-budget":
				next.FinalizationSHA = "different-current-budget"
			case "alias-key":
				next.ThroughKey = closed.Request.Key
				next.ThroughKey.OperationID.Low++
			}
			_, _, err := store.Settle(ctx, snapshot, current.RunnerPodUID, next)
			require.Error(t, err)
		})
	}
	// 原形状不输出空结构字段，防止普通旧账本无故被新字段污染。
	raw, err := json.Marshal(closed)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "through_")
}

func TestHostClosedThroughUnknownCASPreservesFenceAndFutureBudget(t *testing.T) {
	api, store, snapshot, _, current, closed := settlementFixture(t)
	ctx := context.Background()
	extended := closed
	extended.ThroughKey, extended.ThroughSequence = closed.Request.Key, closed.BudgetSequence+10
	extended.ThroughKey.Revision++
	extended.ThroughKey.OperationID.Low++
	api.mu.Lock()
	api.loseReply = true
	api.mu.Unlock()
	_, _, err := store.Settle(ctx, snapshot, current.RunnerPodUID, extended)
	var unknown *UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	snapshot, err = store.Load(ctx)
	require.NoError(t, err)
	next := current
	next.Request.Key = extended.ThroughKey
	_, _, err = store.Reserve(ctx, snapshot, next)
	require.Error(t, err, "结清CAS丢回复后原水位已持久，不能补发旧授予")
	_, _, err = store.Settle(ctx, snapshot, current.RunnerPodUID, extended)
	require.NoError(t, err)
	// 较新的完整成功预算允许替换旧水位，但序列须严格晚于整条关闭链。
	next.Request.Key.Revision++
	next.Request.Key.OperationID.Low++
	snapshot, _, err = store.Reserve(ctx, snapshot, next)
	require.NoError(t, err)
	ref := budget.ObjectRef{Name: "future-host", UID: "future-host-uid"}
	snapshot, err = store.BindClaim(ctx, snapshot, next, ref)
	require.NoError(t, err)
	newBudget := closed
	newBudget.Request, newBudget.HostClaim = next.Request, ref
	newBudget.BudgetSequence = extended.ThroughSequence
	_, _, err = store.Settle(ctx, snapshot, next.RunnerPodUID, newBudget)
	require.Error(t, err)
	newBudget.BudgetSequence++
	snapshot, _, err = store.Settle(ctx, snapshot, next.RunnerPodUID, newBudget)
	require.NoError(t, err)
	require.EqualValues(t, 6, snapshot.State().ProtocolVersion, "后续成功和退休均不能降级新持久语义")
}
