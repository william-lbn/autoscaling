//go:build linux

package resourceguard

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestNormalSystemRoleBrokerDeclarationV3RequiresKernelPrep2(t *testing.T) {
	digest := strings.Repeat("ab", 32)
	raw, e := NormalSystemRoleBrokerDeclarationV3JSON(digest, digest)
	require.NoError(t, e)
	d, e := decodeNormalSystemRoleBrokerDeclaration(raw)
	require.NoError(t, e)
	require.True(t, d.kernelPreparationV2())
	require.True(t, d.brokerOwnsAuthority())
	for _, tc := range []string{"missing", "null", "empty", "wrong", "wrong-case", "alias", "duplicate", "authority-missing", "authority-null", "version4", "scope2", "unknown", "old1-null", "old2-null", "old2-empty"} {
		t.Run(tc, func(t *testing.T) {
			var f map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &f))
			switch tc {
			case "missing":
				delete(f, "kernel_preparation_mode")
			case "null":
				f["kernel_preparation_mode"] = json.RawMessage("null")
			case "empty":
				f["kernel_preparation_mode"] = json.RawMessage(`""`)
			case "wrong":
				f["kernel_preparation_mode"] = json.RawMessage(`"current-sysctl-fd"`)
			case "wrong-case":
				f["Kernel_Preparation_Mode"] = f["kernel_preparation_mode"]
				delete(f, "kernel_preparation_mode")
			case "alias":
				f["Kernel_Preparation_Mode"] = f["kernel_preparation_mode"]
			case "authority-missing":
				delete(f, "resource_authority_mode")
			case "authority-null":
				f["resource_authority_mode"] = json.RawMessage("null")
			case "version4":
				f["protocol_version"] = json.RawMessage("4")
			case "scope2":
				f["scope"] = json.RawMessage(`"fixed-system-role-broker-declaration-v2"`)
			case "unknown":
				f["verified"] = json.RawMessage("true")
			case "old1-null":
				f["protocol_version"] = json.RawMessage("1")
				f["scope"] = json.RawMessage(`"fixed-system-role-broker-declaration-v1"`)
				delete(f, "resource_authority_mode")
				f["kernel_preparation_mode"] = json.RawMessage("null")
			case "old2-null", "old2-empty":
				f["protocol_version"] = json.RawMessage("2")
				f["scope"] = json.RawMessage(`"fixed-system-role-broker-declaration-v2"`)
				f["kernel_preparation_mode"] = json.RawMessage("null")
				if tc == "old2-empty" {
					f["kernel_preparation_mode"] = json.RawMessage(`""`)
				}
			}
			b, e := json.Marshal(f)
			require.NoError(t, e)
			if tc == "duplicate" {
				b = append(b[:len(b)-1], []byte(`,"kernel_preparation_mode":"initial8-kernel-prep-userns-v2"}`)...)
			}
			_, e = decodeNormalSystemRoleBrokerDeclaration(b)
			require.Error(t, e)
		})
	}
	for _, version := range []uint32{1, 2} {
		var b []byte
		if version == 1 {
			b, e = NormalSystemRoleBrokerDeclarationJSON(digest, digest)
		} else {
			b, e = NormalSystemRoleBrokerDeclarationV2JSON(digest, digest)
		}
		require.NoError(t, e)
		require.NotContains(t, string(b), "kernel_preparation_mode")
		d, e = decodeNormalSystemRoleBrokerDeclaration(b)
		require.NoError(t, e)
		require.Equal(t, version, d.ProtocolVersion)
		require.False(t, d.kernelPreparationV2())
	}
}
