package hostguard

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

func ledgerModel() Model {
	return Model{ProtocolVersion: 1, Binding: nodeBinding(), Origin: strings.Repeat("a", 32), LeaseFloor: journal.Owner{HolderID: "allocator-original", Epoch: 1}, Sequence: 1, OutsideBytes: 1 << 30, Holds: []Hold{}}
}

func TestNodeLedgerRejectsChangedAliasesBootsAndOverflow(t *testing.T) {
	p := plan("vm-one")
	p.Request.Key.Revision = 2
	m, _, err := ledgerModel().reserve(ledgerModel().LeaseFloor, p, 8<<30)
	require.NoError(t, err)
	for _, mode := range []string{"same-key-target", "same-key-deadline", "older-key", "same-slot-new-operation", "same-pod-new-pg", "vm-name-alias", "incarnation-alias", "zero-overhead", "overflow", "bad-direction"} {
		t.Run(mode, func(t *testing.T) {
			changed := p
			switch mode {
			case "same-key-target":
				changed.Request.TargetVMMemoryBytes += 8 << 20
			case "same-key-deadline":
				changed.Request.DeadlineUnixMillis++
			case "older-key":
				changed.Request.Key.Revision--
			case "same-slot-new-operation":
				changed.Request.Key.OperationID.Low++
			case "same-pod-new-pg":
				changed.Request.Key.Instance.PGBootID++
			case "vm-name-alias":
				changed.Request.Key.Revision++
				changed.VMName = "same-uid-other-name"
			case "incarnation-alias":
				changed.Request.Key.Revision++
				changed.Incarnation = strings.Repeat("b", 32)
			case "zero-overhead":
				changed.HostOverheadBytes = 0
			case "overflow":
				changed.Request.Kind = control.Shrink
				changed.BeforeBytes = control.Uint64(math.MaxUint64)
			case "bad-direction":
				changed.Request.Kind = control.Shrink
			}
			got, _, err := m.reserve(m.LeaseFloor, changed, 8<<30)
			require.Error(t, err)
			require.Equal(t, m, got, "拒绝不能清旧持有或改变原高水位")
		})
	}
}

func TestNodeLedgerStrictSourcesAndBoundNeverEvictHeldMemory(t *testing.T) {
	m := ledgerModel()
	p := plan("vm-one")
	for i := range MaxHolds {
		p.Request.Key.Revision = control.Uint64(i + 1)
		p.Request.Key.OperationID.Low = control.Uint64(i + 1)
		var err error
		m, _, err = m.reserve(m.LeaseFloor, p, 8<<30)
		require.NoError(t, err)
	}
	used, err := m.UsedBytes()
	require.NoError(t, err)
	require.EqualValues(t, (3<<30)+(64<<20), used)
	p.Request.Key.Revision++
	p.Request.Key.OperationID.Low++
	unchanged, _, err := m.reserve(m.LeaseFloor, p, 8<<30)
	require.ErrorContains(t, err, "bound exhausted")
	require.Equal(t, m, unchanged, "容量条目上限不能淘汰仍在持有的旧承诺")
	wrong := m.Clone()
	wrong.Holds[0].SourceID = strings.Repeat("c", 64)
	require.Error(t, wrong.Validate())
	wrong = m.Clone()
	wrong.Sequence = control.Uint64(math.MaxUint64)
	_, _, err = wrong.reserve(wrong.LeaseFloor, p, 8<<30)
	require.Error(t, err)
	fixture := ledgerModel()
	raw, err := json.Marshal(fixture)
	require.NoError(t, err)
	for _, bad := range []string{string(raw[:len(raw)-1]) + `,"sequence":"2"}`, string(raw[:len(raw)-1]) + `,"ttl_release":true}`, strings.Replace(string(raw), `"holds":[]`, `"holds":null`, 1)} {
		var got Model
		require.Error(t, json.Unmarshal([]byte(bad), &got))
	}
}
