package journal

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const testIncarnation = "1234567890abcdef1234567890abcdef"

var testCommand = []byte(`{"model_test":true}`)

func testModel(t testing.TB) Model {
	t.Helper()
	model, err := New("vm-uid", testIncarnation, "process-a")
	require.NoError(t, err)
	return model
}

func testRequest(model Model, revision uint64, kind control.Kind) control.Request {
	return control.Request{
		ProtocolVersion: 1, Key: control.Key{Instance: control.Instance{VMUID: model.VMUID, ComputeInstanceID: "compute-1", PGBootID: 3}, IssuerEpoch: model.Owner.Epoch, Revision: control.Uint64(revision), OperationID: control.OperationID{High: 123, Low: control.Uint64(revision)}},
		Kind: kind, ExpectedCapacityGeneration: 1, TargetBuffers: 512, TargetVMMemoryBytes: 1 << 30, BudgetCommitmentID: "commitment-1", DeadlineUnixMillis: 1000,
	}
}

func beginModel(t testing.TB, model Model, request control.Request) Model {
	t.Helper()
	next, disposition, err := model.Begin(model.Owner, request, 0)
	require.NoError(t, err)
	require.Equal(t, Accepted, disposition)
	return next
}

func observation(model Model, step Step, result Result) Observation {
	return Observation{Key: model.Active.Request.Key, RequestSHA256: model.Active.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: step.Action, Result: result, SourceID: step.TargetID, Data: []byte(`{"state_machine_test_only":true}`)}
}

func completeModel(t testing.TB, model Model) Model {
	t.Helper()
	key := model.Active.Request.Key
	for _, step := range model.Active.Steps {
		var err error
		model, err = model.Start(model.Owner, key, step.Action, "component-instance", testCommand)
		require.NoError(t, err)
		model, err = model.Record(model.Owner, key, observation(model, model.Active.Steps[int(step.Ordinal)-1], Completed))
		require.NoError(t, err)
	}
	model, err := model.Finish(model.Owner, key)
	require.NoError(t, err)
	return model
}

func TestJournalRequiresOrderedWriteAheadAndAllReceipts(t *testing.T) {
	for _, kind := range []control.Kind{control.Grow, control.Shrink} {
		t.Run(string(kind), func(t *testing.T) {
			model := testModel(t)
			request := testRequest(model, 1, kind)
			model = beginModel(t, model, request)
			_, err := model.Start(model.Owner, request.Key, ResizeVM, "runner", testCommand)
			require.Error(t, err)
			_, err = model.Finish(model.Owner, request.Key)
			require.Error(t, err)
			started, err := model.Start(model.Owner, request.Key, PrepareBudget, "budget", testCommand)
			require.NoError(t, err)
			require.Equal(t, Planned, model.Active.Steps[0].Phase, "value receiver must not mutate source snapshot")
			require.Equal(t, Unconfirmed, started.Active.Steps[0].Phase)
			_, err = started.Start(started.Owner, request.Key, started.Active.Steps[1].Action, "next", testCommand)
			require.Error(t, err)
			model = completeModel(t, model)
			require.Nil(t, model.Active)
			require.Equal(t, Succeeded, model.History[0].Outcome)
			require.Equal(t, Replayed, model.Lookup(request))
			require.NoError(t, model.Validate())
		})
	}
}

func TestJournalTakeoverPreservesUnknownAndOldIntentAcrossBoot(t *testing.T) {
	model := testModel(t)
	request := testRequest(model, 1, control.Shrink)
	model = beginModel(t, model, request)
	oldOwner := model.Owner
	model, err := model.Start(oldOwner, request.Key, PrepareBudget, "budget-instance", testCommand)
	require.NoError(t, err)
	model, err = model.Takeover(oldOwner, "process-b")
	require.NoError(t, err)
	require.EqualValues(t, 2, model.Owner.Epoch)
	require.Equal(t, oldOwner, model.Active.Origin)
	require.Equal(t, request, model.Active.Request)
	require.Equal(t, Unconfirmed, model.Active.Steps[0].Phase)
	_, err = model.Start(oldOwner, request.Key, PrepareBudget, "budget-instance", testCommand)
	require.Error(t, err)
	_, err = model.Start(model.Owner, request.Key, PrepareBudget, "budget-instance", testCommand)
	require.Error(t, err)
	next := testRequest(model, 1, control.Grow)
	next.Key.Instance.PGBootID = 4
	_, disposition, err := model.Begin(model.Owner, next, 0)
	require.NoError(t, err)
	require.Equal(t, Busy, disposition)
	// 期限到达/boot变化都不能擦掉旧effects；新owner按旧完整身份对账，并先恢复预算。
	model, err = model.RequestAbort(model.Owner, request.Key)
	require.NoError(t, err)
	_, err = model.StartCleanup(model.Owner, request.Key, "cleanup", testCommand)
	require.Error(t, err)
	_, err = model.Finish(model.Owner, request.Key)
	require.Error(t, err)
	model, err = model.Record(model.Owner, request.Key, observation(model, model.Active.Steps[0], NoEffect))
	require.NoError(t, err)
	_, err = model.Finish(model.Owner, request.Key)
	require.Error(t, err)
	model, err = model.StartCleanup(model.Owner, request.Key, "new-owner-cleanup", testCommand)
	require.NoError(t, err)
	model, err = model.Record(model.Owner, request.Key, observation(model, *model.Active.Cleanup, Completed))
	require.NoError(t, err)
	model, err = model.Finish(model.Owner, request.Key)
	require.NoError(t, err)
	require.Equal(t, AbortedAfterReconcile, model.History[0].Outcome)
	model = beginModel(t, model, next)
	require.Equal(t, next, model.Active.Request)
}

func TestJournalReplayBeforeDeadlineAndImmutableFullContext(t *testing.T) {
	model := testModel(t)
	request := testRequest(model, 1, control.Grow)
	model = beginModel(t, model, request)
	unchanged, disposition, err := model.Begin(model.Owner, request, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, Replayed, disposition)
	require.Equal(t, model, unchanged)
	for _, change := range []func(*control.Request){func(r *control.Request) { r.BudgetCommitmentID = "different" }, func(r *control.Request) { r.DeadlineUnixMillis++ }, func(r *control.Request) { r.TargetBuffers++ }, func(r *control.Request) { r.TargetVMMemoryBytes++ }} {
		changed := request
		change(&changed)
		_, disposition, err = model.Begin(model.Owner, changed, 0)
		require.NoError(t, err)
		require.Equal(t, Conflict, disposition)
	}
	model = completeModel(t, model)
	old := request
	old.Key.OperationID.Low++
	_, disposition, err = model.Begin(model.Owner, old, 0)
	require.NoError(t, err)
	require.Equal(t, Conflict, disposition)
	newRequest := testRequest(model, 2, control.Grow)
	_, disposition, err = model.Begin(model.Owner, newRequest, 1000)
	require.NoError(t, err)
	require.Equal(t, DeadlineExpired, disposition)
}

func TestJournalObservationCannotBorrowOtherCommandsOrChangeTerminalFacts(t *testing.T) {
	model := testModel(t)
	request := testRequest(model, 1, control.Grow)
	model = beginModel(t, model, request)
	model, err := model.Start(model.Owner, request.Key, PrepareBudget, "target-1", testCommand)
	require.NoError(t, err)
	fact := observation(model, model.Active.Steps[0], Completed)
	for _, change := range []func(*Observation){func(o *Observation) { o.Key.Revision++ }, func(o *Observation) {
		o.RequestSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	}, func(o *Observation) {
		o.CommandSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	}, func(o *Observation) { o.Ordinal = 2 }, func(o *Observation) { o.Action = ResizePG }, func(o *Observation) { o.SourceID = "other" }, func(o *Observation) { o.Data = []byte(`{"x":1,"x":2}`) }} {
		wrong := fact
		change(&wrong)
		_, err = model.Record(model.Owner, request.Key, wrong)
		require.Error(t, err)
	}
	model, err = model.Record(model.Owner, request.Key, fact)
	require.NoError(t, err)
	replay, err := model.Record(model.Owner, request.Key, fact)
	require.NoError(t, err)
	require.Equal(t, model, replay)
	fact.Data = []byte(`{"changed":true}`)
	_, err = model.Record(model.Owner, request.Key, fact)
	require.Error(t, err)
	cloned := model.Clone()
	cloned.Active.Steps[0].Command[0] = 'x'
	cloned.Active.Steps[0].Observation.Data[0] = 'x'
	require.NoError(t, model.Validate())
	require.Error(t, cloned.Validate())
}

func TestJournalRejectedEffectNeedsAbortAndCleanup(t *testing.T) {
	model := testModel(t)
	request := testRequest(model, 1, control.Shrink)
	model = beginModel(t, model, request)
	model, err := model.Start(model.Owner, request.Key, PrepareBudget, "budget", testCommand)
	require.NoError(t, err)
	model, err = model.Record(model.Owner, request.Key, observation(model, model.Active.Steps[0], Rejected))
	require.NoError(t, err)
	_, err = model.Start(model.Owner, request.Key, Quiesce, "pg", testCommand)
	require.Error(t, err)
	_, err = model.Finish(model.Owner, request.Key)
	require.Error(t, err)
	model, err = model.RequestAbort(model.Owner, request.Key)
	require.NoError(t, err)
	model, err = model.StartCleanup(model.Owner, request.Key, "cleanup", testCommand)
	require.NoError(t, err)
	model, err = model.Record(model.Owner, request.Key, observation(model, *model.Active.Cleanup, Rejected))
	require.NoError(t, err)
	_, err = model.Finish(model.Owner, request.Key)
	require.Error(t, err)
}

func TestJournalHistoryRetiresKeysAndNeverReexecutesExpiredResults(t *testing.T) {
	model := testModel(t)
	first := testRequest(model, 1, control.Grow)
	for i := uint64(1); i <= HistoryLimit+4; i++ {
		request := testRequest(model, i, control.Grow)
		model = beginModel(t, model, request)
		var err error
		model, err = model.RequestAbort(model.Owner, request.Key)
		require.NoError(t, err)
		model, err = model.Finish(model.Owner, request.Key)
		require.NoError(t, err)
	}
	require.Len(t, model.History, HistoryLimit)
	require.EqualValues(t, 4, model.ExpiredResults)
	_, disposition, err := model.Begin(model.Owner, first, 0)
	require.NoError(t, err)
	require.Equal(t, ResultExpired, disposition)
	require.NoError(t, model.Validate())
}

func TestJournalCountersAndPersistedSchemaFailClosed(t *testing.T) {
	model := testModel(t)
	model.Sequence = control.Uint64(math.MaxUint64)
	_, _, err := model.Begin(model.Owner, testRequest(model, 1, control.Grow), 0)
	require.Error(t, err)
	model = testModel(t)
	model.Owner.Epoch = control.Uint64(math.MaxUint64)
	_, err = model.Takeover(model.Owner, "other")
	require.Error(t, err)
	model = testModel(t)
	data, err := json.Marshal(model)
	require.NoError(t, err)
	var object map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &object))
	for _, field := range []string{"active", "high_water", "history", "expired_results"} {
		missing := map[string]json.RawMessage{}
		for k, v := range object {
			if k != field {
				missing[k] = v
			}
		}
		bad, err := json.Marshal(missing)
		require.NoError(t, err)
		original := model.Clone()
		require.Error(t, json.Unmarshal(bad, &original))
		require.Equal(t, model, original)
	}
	bad := model.Clone()
	bad.Incarnation = "bad"
	encoded, err := json.Marshal(bad)
	require.NoError(t, err)
	original := model.Clone()
	require.Error(t, json.Unmarshal(encoded, &original))
	require.Equal(t, model, original)
}
