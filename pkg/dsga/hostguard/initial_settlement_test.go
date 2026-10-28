package hostguard

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// Node纯模型不证明真实Runner。这里单独保护v8/来源互斥/持久水位，真实
// 生产读取、丢回复及后继Grow由owner组合覆盖，不能当Linux回收证据。
func TestInitialHostSettlementRequiresV8AndPreservesComparableSource(t *testing.T) {
	_, _, snapshot, _, current, closed := settlementFixture(t)
	var noRunner budget.RunnerSources
	closed.Runner = noRunner
	closed.ActualMemoryBytes = current.BeforeBytes
	closed.InitialVM = budget.InitialVMSources{Instance: current.Request.Key.Instance, BirthClaim: budget.ObjectRef{Name: "birth", UID: "birth-uid"}, StateClaim: budget.ObjectRef{Name: "state", UID: "state-uid"}, IdentitySHA256: strings.Repeat("a", 64), MemoryBytes: current.BeforeBytes}
	m, _, err := snapshot.State().settle(snapshot.State().LeaseFloor, current.RunnerPodUID, closed)
	require.NoError(t, err)
	require.EqualValues(t, 8, m.ProtocolVersion)
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	require.NotContains(t, string(raw), `"runner":`)
	var restored Model
	require.NoError(t, json.Unmarshal(raw, &restored))
	require.Equal(t, m, restored)
	for _, version := range []uint32{4, 6, 7} {
		old := m.Clone()
		old.ProtocolVersion = version
		require.Error(t, old.Validate(), "旧Node语义不能接启动来源")
	}
	same, retired, err := restored.settle(restored.LeaseFloor, current.RunnerPodUID, closed)
	require.NoError(t, err)
	require.Empty(t, retired)
	require.Equal(t, m, same)
	_, _, _, _, _, ordinary := settlementFixture(t)
	raw, err = json.Marshal(ordinary)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "initial_vm", "旧合同不输出零值新字段")
	for _, what := range []string{"mixed-receipt", "wrong-VM", "wrong-memory", "missing-actual", "missing-State"} {
		bad := closed
		switch what {
		case "mixed-receipt":
			bad.Runner = ordinary.Runner
		case "wrong-VM":
			bad.InitialVM.Instance.PGBootID++
		case "wrong-memory":
			bad.InitialVM.MemoryBytes++
		case "missing-actual":
			bad.ActualMemoryBytes = 0
		case "missing-State":
			bad.InitialVM.StateClaim.UID = ""
		}
		require.Error(t, bad.Validate(), what)
	}
}
