package dsga

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func coordinationFixture(t *testing.T) CoordinationCommand {
	t.Helper()
	_, request := goldenRequest(t)
	return CoordinationCommand{ProtocolVersion: 1, Instance: request.Key.Instance, ExpectedOwnerEpoch: request.Key.IssuerEpoch, Action: CoordinationBegin, Request: &request, Key: nil, Selector: ""}
}

func TestCoordinationSelectorsCannotReplaceOriginalCommands(t *testing.T) {
	original := coordinationFixture(t)
	for _, action := range []CoordinationAction{CoordinationState, CoordinationResourceState, CoordinationBegin, CoordinationCancel, CoordinationReplayPG, CoordinationReplayQuiesce, CoordinationReplayAdmission, CoordinationReplayResource, CoordinationReplayPrepare, CoordinationReplayInitial, CoordinationReplayVM, CoordinationReplayAbort, CoordinationRecoverUnchanged, CoordinationRecoverCapacity, CoordinationRepairHeld, CoordinationObserveHeld} {
		t.Run(string(action), func(t *testing.T) {
			command := original.Clone()
			command.Action = action
			if action != CoordinationBegin {
				command.Request = nil
			}
			switch action {
			case CoordinationBegin, CoordinationState, CoordinationResourceState:
			case CoordinationReplayInitial, CoordinationRepairHeld, CoordinationObserveHeld:
				command.Selector = strings.Repeat("ab", 32)
			default:
				key := original.Request.Key
				command.Key = &key
			}
			require.NoError(t, command.Validate())
			body, err := json.Marshal(command)
			require.NoError(t, err)
			var decoded CoordinationCommand
			require.NoError(t, json.Unmarshal(body, &decoded))
			require.Equal(t, command, decoded)
			bad := command.Clone()
			bad.ExpectedOwnerEpoch = 0
			require.Error(t, bad.Validate())
			bad = command.Clone()
			bad.Instance.PGBootID--
			if command.Key != nil || command.Request != nil {
				require.Error(t, bad.Validate(), "Key/Request cannot be rebound to a new boot")
			}
			if command.Request == nil {
				bad = command.Clone()
				bad.Request = original.Request
				require.Error(t, bad.Validate(), "recovery cannot smuggle new target parameters")
			}
		})
	}
	old := original.Clone()
	old.ExpectedOwnerEpoch++
	require.NoError(t, old.Validate(), "new owner may look up unchanged old Begin, not redispatch it")
	old = original.Clone()
	old.ExpectedOwnerEpoch--
	require.Error(t, old.Validate(), "future original epoch is not selectable")
}

func TestCoordinationStrictDecodeAndIndependentCopies(t *testing.T) {
	original := coordinationFixture(t)
	data, err := json.Marshal(original)
	require.NoError(t, err)
	for _, body := range []string{
		`null`, `[]`, string(data) + `{}`,
		strings.Replace(string(data), "{", `{"action":"state",`, 1),
		strings.Replace(string(data), "{", `{"url":"https://attacker.invalid",`, 1),
		strings.Replace(string(data), `"action":"begin"`, `"action":"unknown"`, 1),
		strings.Replace(string(data), `"request":{`, `"request":null,"ignored":{`, 1),
		strings.Replace(string(data), `"protocol_version":1`, `"Protocol_Version":1`, 1),
		strings.Replace(string(data), `"protocol_version":1,`, "", 1),
	} {
		next := original.Clone()
		require.Error(t, json.Unmarshal([]byte(body), &next), body)
		require.Equal(t, original, next, "bad decode must preserve original selection")
	}
	cloned := original.Clone()
	cloned.Request.TargetBuffers++
	require.NotEqual(t, original.Request.TargetBuffers, cloned.Request.TargetBuffers)
	cloned = original.Clone()
	key := original.Request.Key
	cloned.Request, cloned.Key, cloned.Action = nil, &key, CoordinationReplayPG
	next := cloned.Clone()
	next.Key.Revision++
	require.NotEqual(t, cloned.Key.Revision, next.Key.Revision)
	for _, selector := range []string{"", strings.Repeat("A", 64), strings.Repeat("00", 31), strings.Repeat("zz", 32)} {
		cloned.Action, cloned.Key, cloned.Selector = CoordinationReplayInitial, nil, selector
		require.Error(t, cloned.Validate())
	}
}
