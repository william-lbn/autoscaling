package hostexec

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/stretchr/testify/require"
)

// 夹具来自 PG17 三节点同一次真实请求。仅含原动作、物理回执和单条 Host 计费行。
func TestCancelledShrinkAccounting(t *testing.T) {
	f, err := os.Open("testdata/online-shrink-cancelled-accounting.json.gz")
	require.NoError(t, err)
	defer f.Close()
	r, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer r.Close()
	var fixture struct {
		Journal   journal.Model       `json:"journal"`
		Placement hostguard.Placement `json:"placement"`
	}
	require.NoError(t, json.NewDecoder(r).Decode(&fixture))
	m, row := fixture.Journal, fixture.Placement
	require.NoError(t, m.Validate())
	require.True(t, CancelledShrinkNeedsAccounting(m))

	var prepared budget.Prepared
	require.NoError(t, json.Unmarshal(m.Active.Steps[0].Observation.Data, &prepared))
	plan := hostguard.Plan{Namespace: row.Namespace, VMName: row.VMName, Incarnation: row.Incarnation,
		RunnerPodUID: row.PodUID, Request: m.Active.Request, BeforeBytes: prepared.VMBeforeMemoryBytes,
		HostOverheadBytes: row.HostOverheadBytes, PodMemoryLimitBytes: row.PodMemoryLimitBytes}
	nodeWith := func(p hostguard.Placement) hostguard.Model {
		rows := []hostguard.Placement{p}
		return hostguard.Model{ProtocolVersion: 10, Placements: &rows}
	}
	require.NoError(t, checkPaidShrink(m, nodeWith(row), plan))
	paid, err := row.Bytes()
	require.NoError(t, err)
	peak, err := plan.Bytes()
	require.NoError(t, err)
	require.Equal(t, paid, peak)
	for name, change := range map[string]func(*journal.Model){
		"仅超时未取消":   func(m *journal.Model) { m.Active.AbortRequested = false },
		"没有原结算水位":  func(m *journal.Model) { m.Active.PriorBudgetBoundary = nil },
		"实际仍是扩容":   func(m *journal.Model) { m.Active.Request.Kind = control.Grow },
		"不同原操作":    func(m *journal.Model) { m.HighWater = nil },
		"原PG释放未知":  func(m *journal.Model) { m.Active.Steps[3].Phase = journal.Unconfirmed },
		"已派发Guest": func(m *journal.Model) { m.Active.Steps[4].Phase = journal.Unconfirmed },
		"已派发VM":    func(m *journal.Model) { m.Active.Steps[5].Phase = journal.Unconfirmed },
	} {
		t.Run(name, func(t *testing.T) {
			copy := m.Clone()
			change(&copy)
			require.False(t, CancelledShrinkNeedsAccounting(copy))
		})
	}
	for name, change := range map[string]func(*hostguard.Placement){
		"旧额度不足":   func(p *hostguard.Placement) { p.MemoryBytes-- },
		"原Pod已替换": func(p *hostguard.Placement) { p.PodUID = "replacement" },
		"前一预算未知":  func(p *hostguard.Placement) { p.Settled = nil },
		"原代次已变化":  func(p *hostguard.Placement) { p.Incarnation = "another" },
		"Pod硬限变化": func(p *hostguard.Placement) { p.PodMemoryLimitBytes++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := row
			change(&changed)
			require.Error(t, checkPaidShrink(m, nodeWith(changed), plan))
		})
	}
}
