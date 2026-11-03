package hostguard

import (
	"context"
	"encoding/json"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
)

// 既有明确Node服务夹具：只核真实Store的11Gi计划/屏障/退休合同，不冒跨域生产资格。
func TestHostPodFloorBarrierKeepsOriginalFloorAfterRetirement(t *testing.T) {
	f, store, _, p := preparedBarrierFixture(t)
	ctx := context.Background()
	f.mu.Lock()
	var m Model
	require.NoError(t, json.Unmarshal([]byte(f.node.Annotations[LedgerAnnotation]), &m))
	m.ProtocolVersion = 10
	for i := range *m.Placements {
		(*m.Placements)[i].PodMemoryLimitBytes = 11 << 30
	}
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	f.node.Annotations[LedgerAnnotation] = string(raw)
	f.node.Status.Allocatable[core.ResourceMemory] = resource.MustParse("24Gi")
	f.node.ResourceVersion = "v10-barrier-fixture-rv"
	f.mu.Unlock()
	n, err := store.Load(ctx)
	require.NoError(t, err)
	p.PodMemoryLimitBytes = 11 << 30
	n, _, err = store.Reserve(ctx, n, p)
	require.NoError(t, err)
	n, err = store.BindClaim(ctx, n, p, budget.ObjectRef{Name: "host-floor-claim", UID: "host-floor-claim-uid"})
	require.NoError(t, err)
	n, b, err := store.SealPrepared(ctx, n, preparedClosure(p, 10))
	require.NoError(t, err)
	require.NotNil(t, b.Held)
	require.Equal(t, SourceID(n.State().Origin, p), b.Held.SourceID)
	_, err = n.PreparedBarrier(p)
	require.NoError(t, err)
	missing := p
	missing.PodMemoryLimitBytes = 0
	_, err = n.PreparedBarrier(missing)
	require.Error(t, err)
	n, removed, err := store.RetireSealedHolds(ctx, n, b)
	require.NoError(t, err)
	require.Len(t, removed, 1)
	row, err := n.State().Placement(p.RunnerPodUID)
	require.NoError(t, err)
	require.Equal(t, p.BeforeBytes, row.MemoryBytes)
	require.EqualValues(t, 11<<30, row.PodMemoryLimitBytes)
	used, err := n.State().UsedBytes()
	require.NoError(t, err)
	require.EqualValues(t, 12<<30, used)
	_, _, err = store.Reserve(ctx, n, p)
	require.ErrorIs(t, err, ErrSealed, "退休旧Hold不能抹掉Key封存或保守floor")
}
