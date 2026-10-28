package budget

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// 只验证严格合同与算术；原Runner/资格/Host的真实性由生产组合另验。
func TestReconciledInitialVMUsesExclusiveBootSourceAndStrictWire(t *testing.T) {
	inputs, initial := reconciledInitialInputs(t)
	native := inputs.VM
	var noNative NativeCapacity
	inputs.VM = noNative
	inputs.InitialVM = &InitialVMSources{Instance: initial.Instance, BirthClaim: initial.RunnerBirth, StateClaim: native.Sources.StateClaim, IdentitySHA256: initial.RunnerIdentitySHA256, MemoryBytes: initial.VMMemoryBytes}
	c, err := ReconcileInitial(inputs, initial)
	require.NoError(t, err)
	require.EqualValues(t, 3, c.ProtocolVersion)
	require.Empty(t, c.VM)
	require.Equal(t, inputs.Prepared.Request, c.Request)
	raw, err := json.Marshal(c)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "native_vm")
	require.NotContains(t, string(raw), "outside_request")
	var restored ReconciledCapacity
	require.NoError(t, json.Unmarshal(raw, &restored))
	require.Equal(t, c, restored)
	inputs.InitialVM.MemoryBytes++
	require.NotEqual(t, inputs.InitialVM, c.InitialVM, "纯函数不能保留调用方可变来源指针")
	for _, what := range []string{"legacy-protocol", "missing-boot", "missing-initial", "mixed-native", "wrong-instance", "wrong-birth", "wrong-identity", "wrong-memory", "missing-state"} {
		t.Run(what, func(t *testing.T) {
			var bad ReconciledCapacity
			require.NoError(t, json.Unmarshal(raw, &bad))
			switch what {
			case "legacy-protocol":
				bad.ProtocolVersion = 2
			case "missing-boot":
				bad.InitialVM = nil
			case "missing-initial":
				bad.InitialOutside = nil
			case "mixed-native":
				bad.VM = native
			case "wrong-instance":
				bad.InitialVM.Instance.PGBootID++
			case "wrong-birth":
				bad.InitialVM.BirthClaim.UID += "-replacement"
			case "wrong-identity":
				bad.InitialVM.IdentitySHA256 = native.PreparedSHA256
			case "wrong-memory":
				bad.InitialVM.MemoryBytes++
			case "missing-state":
				bad.InitialVM.StateClaim.UID = ""
			}
			require.Error(t, bad.Validate())
		})
	}
	inputs.InitialVM = c.InitialVM
	inputs.VM = native
	_, err = ReconcileInitial(inputs, initial)
	require.Error(t, err, "生产入口也拒绝混用来源")
	inputs.VM = noNative
	_, err = Reconcile(inputs)
	require.Error(t, err, "无完整原Initial不能只凭数值使用启动容量")
	for _, value := range []any{c, *c.InitialVM} {
		body, err := json.Marshal(value)
		require.NoError(t, err)
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(body, &fields))
		decode := func(b []byte) error {
			if _, ok := value.(ReconciledCapacity); ok {
				var v ReconciledCapacity
				return json.Unmarshal(b, &v)
			}
			var v InitialVMSources
			return json.Unmarshal(b, &v)
		}
		for name := range fields {
			var missing map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(body, &missing))
			delete(missing, name)
			b, err := json.Marshal(missing)
			require.NoError(t, err)
			require.Error(t, decode(b), name)
		}
		require.Error(t, decode(append([]byte(`{"unexpected":true,`), body[1:]...)))
		require.Error(t, decode(append([]byte(`{"protocol_version":3,"protocol_version":3,`), body[1:]...)))
	}
}
