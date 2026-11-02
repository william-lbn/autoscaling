package prepareexec

import (
	"bytes"
	"encoding/json"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/stretchr/testify/require"
	"testing"
)

// 读取执行器真正写入的原命令，验证普通及 Unknown 重读走同一严格 DTO。
func TestOriginalPrepareCommandReadUsesCanonicalDTO(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		name := "planned_wire"
		if unknown {
			name = "stored_unknown_wire"
		}
		t.Run(name, func(t *testing.T) {
			f := newClosureFixture(t, unknown)
			raw, err := json.Marshal(f.command)
			require.NoError(t, err)
			if unknown {
				step := f.snapshot.State().Active.Steps[0]
				require.Equal(t, journal.Unconfirmed, step.Phase)
				raw = append([]byte(nil), step.Command...)
			}
			decoded, err := decodeOriginalPrepareCommand(raw)
			require.NoError(t, err)
			reencoded, err := json.Marshal(decoded)
			require.NoError(t, err)
			require.True(t, bytes.Equal(raw, reencoded))
			name, err := outsideName(decoded)
			require.NoError(t, err)
			require.Equal(t, decoded.OutsideName, name)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &fields))
			delete(fields, "root_sha256")
			missing, err := json.Marshal(fields)
			require.NoError(t, err)
			_, err = decodeOriginalPrepareCommand(missing)
			require.Error(t, err)
		})
	}
}
