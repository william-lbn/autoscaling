package hostguard

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

func settlementFixture(t *testing.T) (*nodeAPI, *Store, *Snapshot, Plan, Plan, Settlement) {
	t.Helper()
	f, store := newNodeAPI(t)
	ctx := context.Background()
	old := plan("vm-one")
	old.Request.TargetVMMemoryBytes = 3 << 30
	p := Placement{Namespace: old.Namespace, PodName: "runner", PodUID: old.RunnerPodUID, VMName: old.VMName, VMUID: old.Request.Key.Instance.VMUID, Incarnation: old.Incarnation, Managed: true, MemoryBytes: 3 << 30, HostOverheadBytes: old.HostOverheadBytes, Settled: nil}
	snapshot, err := store.InitializeAllocations(ctx, "node-rv-1", 1<<30, allocationProfile(), []Placement{p})
	require.NoError(t, err)
	snapshot, _, err = store.Reserve(ctx, snapshot, old)
	require.NoError(t, err)
	snapshot, err = store.BindClaim(ctx, snapshot, old, budget.ObjectRef{Name: "old-host", UID: "old-host-uid"})
	require.NoError(t, err)
	current := old
	current.Request.Key.Revision++
	current.Request.Key.OperationID.Low++
	current.Request.Kind, current.Request.TargetVMMemoryBytes, current.BeforeBytes = control.Shrink, 1<<30, 3<<30
	ref := budget.ObjectRef{Name: "current-host", UID: "current-host-uid"}
	snapshot, _, err = store.Reserve(ctx, snapshot, current)
	require.NoError(t, err)
	snapshot, err = store.BindClaim(ctx, snapshot, current, ref)
	require.NoError(t, err)
	closed := Settlement{Request: current.Request, HostClaim: ref, Runner: budget.RunnerSources{BirthClaim: budget.ObjectRef{Name: "birth", UID: "birth-uid"}, StateClaim: budget.ObjectRef{Name: "state", UID: "state-uid"}, ReceiptClaim: budget.ObjectRef{Name: "receipt", UID: "receipt-uid"}, CommandSHA256: strings.Repeat("a", 64), JournalSHA256: strings.Repeat("b", 64)}, BudgetSequence: 12, FinalizationSHA: strings.Repeat("c", 64)}
	return f, store, snapshot, old, current, closed
}

func TestHostSettlementDropsOnlyClosedPeakAndCannotReopenExecution(t *testing.T) {
	f, store, snapshot, old, current, closed := settlementFixture(t)
	ctx := context.Background()
	before, err := snapshot.State().UsedBytes()
	require.NoError(t, err)
	require.EqualValues(t, (4<<30)+(64<<20), before)
	snapshot, retired, err := store.Settle(ctx, snapshot, current.RunnerPodUID, closed)
	require.NoError(t, err)
	require.Len(t, retired, 1)
	require.Equal(t, old, retired[0].Plan)
	m := snapshot.State()
	require.EqualValues(t, 4, m.ProtocolVersion)
	used, err := m.UsedBytes()
	require.NoError(t, err)
	require.EqualValues(t, (2<<30)+(64<<20), used, "仅归还旧峰值，目标/Host开销/系统域继续持有")
	require.Len(t, m.Holds, 1)
	f.mu.Lock()
	node := f.node.DeepCopy()
	f.mu.Unlock()
	require.ErrorIs(t, InspectClaim(node, current, SourceID(m.Origin, current), closed.HostClaim), ErrSettled)
	require.NoError(t, InspectRetainedClaim(node, current, SourceID(m.Origin, current), closed.HostClaim, closed))
	changed := closed
	changed.FinalizationSHA = strings.Repeat("d", 64)
	require.Error(t, InspectRetainedClaim(node, current, SourceID(m.Origin, current), closed.HostClaim, changed))
	for _, p := range []Plan{old, current} {
		_, _, err := store.Reserve(ctx, snapshot, p)
		require.Error(t, err, "历史淘汰后也不能重建旧Host执行授权")
	}
	version := snapshot.ResourceVersion()
	snapshot, retired, err = store.Settle(ctx, snapshot, current.RunnerPodUID, closed)
	require.NoError(t, err)
	require.Empty(t, retired)
	require.Equal(t, version, snapshot.ResourceVersion())
	raw, err := json.Marshal(snapshot.State())
	require.NoError(t, err)
	var restored Model
	require.NoError(t, json.Unmarshal(raw, &restored))
	require.Equal(t, snapshot.State(), restored)
	copyModel := snapshot.State()
	(*copyModel.Placements)[0].Settled.BudgetSequence++
	require.NotEqual(t, snapshot.State(), copyModel, "结清水位必须深拷贝")
}

func TestHostSettlementUnknownCASAndNewGrantRemainConservative(t *testing.T) {
	f, store, snapshot, _, current, closed := settlementFixture(t)
	ctx := context.Background()
	future := current
	future.Request.Key.Revision++
	future.Request.Key.OperationID.Low++
	future.Request.Kind, future.Request.TargetVMMemoryBytes, future.BeforeBytes = control.Grow, 4<<30, 1<<30
	latest, _, err := store.Reserve(ctx, snapshot, future)
	require.NoError(t, err)
	_, _, err = store.Settle(ctx, snapshot, current.RunnerPodUID, closed)
	require.Error(t, err, "并发新授予改变RV，不能在旧快照释放")
	f.mu.Lock()
	f.loseReply = true
	f.mu.Unlock()
	_, _, err = store.Settle(ctx, latest, current.RunnerPodUID, closed)
	var unknown *UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	latest, err = store.Load(ctx)
	require.NoError(t, err)
	used, err := latest.State().UsedBytes()
	require.NoError(t, err)
	require.EqualValues(t, (5<<30)+(64<<20), used, "更晚在途承诺完整保留")
	_, err = latest.Hold(future)
	require.NoError(t, err)
	version := latest.ResourceVersion()
	latest, _, err = store.Settle(ctx, latest, current.RunnerPodUID, closed)
	require.NoError(t, err)
	require.Equal(t, version, latest.ResourceVersion(), "结清写丢回复后只读恢复")
	for _, what := range []string{"budget", "claim", "target", "owner"} {
		t.Run(what, func(t *testing.T) {
			changed, owner := closed, latest.State().LeaseFloor
			switch what {
			case "budget":
				changed.BudgetSequence++
			case "claim":
				changed.HostClaim.UID = "replacement-uid"
			case "target":
				changed.Request.TargetVMMemoryBytes += 8 << 20
			case "owner":
				owner.Epoch++
			}
			_, _, err := latest.State().settle(owner, current.RunnerPodUID, changed)
			require.Error(t, err)
		})
	}
}

func TestHostSettlementCompactsBeyondOperationBoundButKeepsWatermark(t *testing.T) {
	_, _, snapshot, old, current, closed := settlementFixture(t)
	m, _, err := snapshot.State().settle(snapshot.State().LeaseFloor, current.RunnerPodUID, closed)
	require.NoError(t, err)
	for i := range MaxHolds + 5 {
		p := current
		p.Request.Key.Revision += control.Uint64(i + 1)
		p.Request.Key.OperationID.Low += control.Uint64(i + 1)
		p.Request.Kind, p.BeforeBytes = control.Grow, 1<<30
		m, _, err = m.reserve(m.LeaseFloor, p, 8<<30)
		require.NoError(t, err)
		ref := budget.ObjectRef{Name: fmt.Sprintf("host-%d", i), UID: fmt.Sprintf("host-uid-%d", i)}
		for j := range m.Holds {
			if m.Holds[j].Plan == p {
				m.Holds[j].Claim = &ref
			}
		}
		next := closed
		next.Request, next.HostClaim, next.BudgetSequence = p.Request, ref, closed.BudgetSequence+control.Uint64(i+1)
		m, _, err = m.settle(m.LeaseFloor, p.RunnerPodUID, next)
		require.NoError(t, err)
		require.Len(t, m.Holds, 1)
	}
	_, _, err = m.reserve(m.LeaseFloor, old, 8<<30)
	require.Error(t, err, "压缩不能抹掉最早原Key已失执行资格的事实")
}
