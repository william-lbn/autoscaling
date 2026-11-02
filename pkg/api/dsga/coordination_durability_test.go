package dsga

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestCoordinationConfirmNormalQuotaDurabilityUsesOnlyOriginalKey(t *testing.T) {
	old := coordinationFixture(t)
	key := old.Request.Key
	command := old.Clone()
	command.Action = CoordinationConfirmNormalQuotaDurability
	command.Request = nil
	command.Key = &key
	require.NoError(t, command.Validate())
	raw, err := json.Marshal(command)
	require.NoError(t, err)
	var decoded CoordinationCommand
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, command, decoded)
	require.False(t, command.Action.ResourceOnly())
	for name, change := range map[string]func(*CoordinationCommand){
		"missing-key": func(c *CoordinationCommand) { c.Key = nil }, "mixed-request": func(c *CoordinationCommand) { c.Request = old.Request }, "mixed-selector": func(c *CoordinationCommand) { c.Selector = strings.Repeat("ab", 32) }, "other-boot": func(c *CoordinationCommand) { c.Key.Instance.PGBootID-- }, "future-epoch": func(c *CoordinationCommand) { c.Key.IssuerEpoch = c.ExpectedOwnerEpoch + 1 }, "zero-boot": func(c *CoordinationCommand) { c.Instance.PGBootID = 0; c.Key.Instance.PGBootID = 0 }, "unknown-action": func(c *CoordinationCommand) { c.Action = "confirm_normal_quota_durability_future" },
	} {
		t.Run(name, func(t *testing.T) { bad := command.Clone(); change(&bad); require.Error(t, bad.Validate()) })
	}
}
