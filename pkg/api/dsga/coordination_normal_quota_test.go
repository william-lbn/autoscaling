package dsga

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// 只选择已持久原Key；该schema不接受新目标，不授过期或闭门恢复许可。
func TestCoordinationRecoverNormalQuotaOriginalKeyOnly(t *testing.T) {
	original := coordinationFixture(t)
	key := original.Request.Key
	command := original.Clone()
	command.Action, command.Key, command.Request = CoordinationRecoverNormalQuota, &key, nil
	require.NoError(t, command.Validate())
	raw, err := json.Marshal(command)
	require.NoError(t, err)
	var decoded CoordinationCommand
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, command, decoded)
	mutations := map[string]func(*CoordinationCommand){
		"missing-key":           func(c *CoordinationCommand) { c.Key = nil },
		"mixed-request":         func(c *CoordinationCommand) { c.Request = original.Request },
		"mixed-selector":        func(c *CoordinationCommand) { c.Selector = strings.Repeat("ab", 32) },
		"other-vm":              func(c *CoordinationCommand) { c.Key.Instance.VMUID = "other-vm" },
		"other-compute":         func(c *CoordinationCommand) { c.Key.Instance.ComputeInstanceID = "other-compute" },
		"other-boot":            func(c *CoordinationCommand) { c.Key.Instance.PGBootID++ },
		"future-original-epoch": func(c *CoordinationCommand) { c.Key.IssuerEpoch = c.ExpectedOwnerEpoch + 1 },
		"zero-owner":            func(c *CoordinationCommand) { c.ExpectedOwnerEpoch = 0 },
		"zero-pg-boot":          func(c *CoordinationCommand) { c.Instance.PGBootID = 0; c.Key.Instance.PGBootID = 0 },
		"unknown-action":        func(c *CoordinationCommand) { c.Action = "recover_normal_quota_future" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) { bad := command.Clone(); mutate(&bad); require.Error(t, bad.Validate()) })
	}
	older := command.Clone()
	older.ExpectedOwnerEpoch++
	require.NoError(t, older.Validate(), "新owner可选择原epoch，不能重写原Key")
	require.False(t, command.Action.ResourceOnly(), "零PG boot资源动作不能冒该原PG操作")
}
