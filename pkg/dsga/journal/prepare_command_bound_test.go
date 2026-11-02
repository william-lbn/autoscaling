package journal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/stretchr/testify/require"
)

// 使用既有完整初开夹具验证持久化边界；不认证实际来源或执行资源写入。
func completePrepareFixture(t *testing.T) (Model, budget.PrepareCommand) {
	t.Helper()
	i, first, opened := initialDomain(t)
	m := initialBoundModel(t)
	var err error
	m, err = m.StartInitialAdmission(m.Owner, first)
	require.NoError(t, err)
	m, err = m.RecordInitialAdmission(m.Owner, opened)
	require.NoError(t, err)
	request := testRequest(m, 1, control.Grow)
	request.TargetBuffers, request.TargetVMMemoryBytes = 1024, 2<<30
	m = beginModel(t, m, request)
	shared := opened.Shared
	upper, err := budget.GrowSharedUpper(shared, request.TargetBuffers)
	require.NoError(t, err)
	prepared := budget.Prepared{ProtocolVersion: 1, Request: request, Identity: i.PGAggregate.Identity, RunnerPodName: i.RunnerPodName, RunnerRestartCount: i.RunnerRestartCount, VMBeforeMemoryBytes: i.VMMemoryBytes, GuestUsableMemoryBytes: (2 << 30) - (256 << 20), Shared: shared, TargetSharedUpperBytes: upper, Limits: i.PGAggregate.Bootstrap.Limits, SystemOutsidePG: i.SystemOutsidePG, Transition: i.Transition, Safety: i.Safety}
	prepared.NormalQuotaProtocolVersion = 1
	prepared.NormalQuotaFence = &control.FenceResponse{ProtocolVersion: 1, RequestedInstance: request.Key.Instance, RequestedEpoch: request.Key.IssuerEpoch, Disposition: control.Fenced, Observed: shared.Observed}
	source := budget.QualificationSource{Claim: budget.ObjectRef{Name: "qualification", UID: "qualification-uid"}, SHA256: strings.Repeat("ab", 32)}
	prefix := fmt.Sprintf("qualified-%s-%d-", source.SHA256, request.TargetVMMemoryBytes)
	prepared.SystemOutsidePG.SourceID, prepared.Transition.SourceID, prepared.Safety.SourceID = prefix+"system", prefix+"transition", prefix+"safety"
	c := budget.PrepareCommand{ProtocolVersion: 1, Qualification: source, Prepared: prepared, RootSHA256: strings.Repeat("cd", 32)}
	c.OutsideName, err = c.CanonicalOutsideName()
	require.NoError(t, err)
	require.NoError(t, c.Validate())
	return m, c
}

func prepareBytesAt(t *testing.T, c budget.PrepareCommand, size int) []byte {
	t.Helper()
	raw, err := json.Marshal(c)
	require.NoError(t, err)
	require.LessOrEqual(t, len(raw), size)
	return append(raw, bytes.Repeat([]byte(" "), size-len(raw))...)
}

func TestCompleteNormalPrepareBoundPreservesStartAndCodec(t *testing.T) {
	for _, size := range []int{MaxCommandBytes + 1, MaxPrepareBudgetCommandBytes} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			m, c := completePrepareFixture(t)
			raw := prepareBytesAt(t, c, size)
			next, err := m.Start(m.Owner, c.Prepared.Request.Key, PrepareBudget, c.Prepared.Request.Key.Instance.ComputeInstanceID, raw)
			require.NoError(t, err)
			require.Equal(t, Unconfirmed, next.Active.Steps[0].Phase)
			require.Equal(t, raw, []byte(next.Active.Steps[0].Command))
			encodedModel, encoded, err := encode(next, 192*1024)
			require.NoError(t, err)
			decoded, err := decode(encoded, encodedModel.VMUID, encodedModel.Incarnation)
			require.NoError(t, err)
			require.Equal(t, next.Active.Steps[0].CommandSHA256, decoded.Active.Steps[0].CommandSHA256)
			require.Equal(t, raw, []byte(decoded.Active.Steps[0].Command))
		})
	}
}

func TestLargePrepareRequiresCompleteOriginalContract(t *testing.T) {
	cases := []string{"legacy", "missing_fence", "unknown_normal_version", "wrong_fence", "outside_claim", "wrong_root_sha", "wrong_qualification", "wrong_bound_source", "wrong_outside_name", "wrong_request_hash", "wrong_owner_epoch", "wrong_target", "missing_dispatcher", "wrong_action", "too_large"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			m, c := completePrepareFixture(t)
			switch name {
			case "legacy":
				c.Prepared.NormalQuotaProtocolVersion, c.Prepared.NormalQuotaFence = 0, nil
			case "missing_fence":
				c.Prepared.NormalQuotaFence = nil
			case "unknown_normal_version":
				c.Prepared.NormalQuotaProtocolVersion = 2
			case "wrong_fence":
				c.Prepared.NormalQuotaFence.Observed.CapacityGeneration++
			case "outside_claim":
				c.Prepared.OutsideClaim = &budget.ObjectRef{Name: "already-held", UID: "already-held-uid"}
			case "wrong_root_sha":
				c.RootSHA256 = "invalid"
			case "wrong_qualification":
				c.Qualification.SHA256 = "invalid"
			case "wrong_bound_source":
				c.Prepared.Transition.SourceID = "unrelated"
			}
			var err error
			c.OutsideName, err = c.CanonicalOutsideName()
			require.NoError(t, err)
			if name == "wrong_outside_name" {
				c.OutsideName = "dsga-o-" + strings.Repeat("0", 56)
			}
			size := MaxCommandBytes + 1
			if name == "too_large" {
				size = MaxPrepareBudgetCommandBytes + 1
			}
			raw := prepareBytesAt(t, c, size)
			step := m.Active.Steps[0]
			owner := m.Owner
			step.Phase, step.TargetID, step.DispatchedBy, step.Command = Unconfirmed, c.Prepared.Request.Key.Instance.ComputeInstanceID, &owner, raw
			hash, err := RequestDigest(c.Prepared.Request)
			require.NoError(t, err)
			switch name {
			case "wrong_request_hash":
				hash = strings.Repeat("ef", 32)
			case "wrong_owner_epoch":
				owner.Epoch++
			case "wrong_target":
				step.TargetID = "other-compute"
			case "missing_dispatcher":
				step.DispatchedBy = nil
			case "wrong_action":
				step.Action = GuestReady
			}
			_, err = commandDigest(hash, step)
			require.Error(t, err)
		})
	}
}

func TestLargePrepareRetainsStrictJSONBounds(t *testing.T) {
	for _, name := range []string{"unknown_field", "missing_field", "duplicate_key", "deep", "too_many_values", "ordinary_body"} {
		t.Run(name, func(t *testing.T) {
			m, c := completePrepareFixture(t)
			raw, err := json.Marshal(c)
			require.NoError(t, err)
			var object map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &object))
			switch name {
			case "unknown_field":
				object["unknown"] = json.RawMessage(`true`)
			case "missing_field":
				delete(object, "root_sha256")
			case "duplicate_key":
				raw = append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"protocol_version":1}`)...)
			case "deep":
				object["unknown"] = json.RawMessage(strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34))
			case "too_many_values":
				object["unknown"] = json.RawMessage("[" + strings.Repeat("0,", 2050) + "0]")
			case "ordinary_body":
				object = map[string]json.RawMessage{"protocol_version": json.RawMessage(`1`)}
			}
			if name != "duplicate_key" {
				raw, err = json.Marshal(object)
				require.NoError(t, err)
			}
			if len(raw) <= MaxCommandBytes {
				raw = append(raw, bytes.Repeat([]byte(" "), MaxCommandBytes+1-len(raw))...)
			}
			step := m.Active.Steps[0]
			owner := m.Owner
			step.Phase, step.TargetID, step.DispatchedBy, step.Command = Unconfirmed, c.Prepared.Request.Key.Instance.ComputeInstanceID, &owner, raw
			hash, err := RequestDigest(c.Prepared.Request)
			require.NoError(t, err)
			_, err = commandDigest(hash, step)
			require.Error(t, err)
		})
	}
}
