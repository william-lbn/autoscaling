package journal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 真实Model/codec不变量；各领域来源继承既有显式协议夹具，不冒充Linux事实。
func closedRunnerBudget(t *testing.T) (Model, control.Request) {
	t.Helper()
	m, obs := heldStart(t, heldBoundModel(t, testModel(t)), 1, runnerSources())
	request := m.Active.Request
	var err error
	m, err = m.Record(m.Owner, request.Key, obs)
	require.NoError(t, err)
	m = heldFinish(t, m)
	require.EqualValues(t, 5, m.ProtocolVersion)
	require.NoError(t, m.CheckClosedBudget(request))
	return m, request
}

func unstartedAbort(t *testing.T, m Model, revision uint64) Model {
	t.Helper()
	request := testRequest(m, revision, control.Grow)
	m = beginModel(t, m, request)
	var err error
	m, err = m.RequestAbort(m.Owner, request.Key)
	require.NoError(t, err)
	m, err = m.Finish(m.Owner, request.Key)
	require.NoError(t, err)
	return m
}

func TestClosedBudgetBoundarySurvivesTakeoverAndExpiredHistory(t *testing.T) {
	m, request := closedRunnerBudget(t)
	held := m.Clone().BudgetHeld
	boundary := *m.BudgetBoundary
	cloned := m.Clone()
	cloned.BudgetBoundary.ThroughKey.OperationID.Low++
	require.Equal(t, boundary, *m.BudgetBoundary, "Clone must not share the permanent boundary pointer")
	var err error
	m, err = m.Takeover(m.Owner, "new-owner")
	require.NoError(t, err)
	require.Equal(t, boundary, *m.BudgetBoundary)
	for i := 1; i <= HistoryLimit+2; i++ {
		m = unstartedAbort(t, m, uint64(i))
		require.NoError(t, m.CheckClosedBudget(request))
	}
	require.NotZero(t, m.ExpiredResults)
	for _, record := range m.History {
		require.NotEqual(t, request.Key, record.Operation.Request.Key, "original success really left the bounded history")
	}
	require.Equal(t, held, m.BudgetHeld)
	require.Equal(t, boundary.SucceededSequence, m.BudgetBoundary.SucceededSequence)
	require.Equal(t, *m.HighWater, m.BudgetBoundary.ThroughKey)
	require.Equal(t, m.Sequence, m.BudgetBoundary.ThroughSequence)
	_, raw, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	restored, err := decode(raw, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m, restored)
	require.NoError(t, restored.CheckClosedBudget(request))
	if dir := os.Getenv("DSGA_VM_WIRE_DIR"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		body, err := json.Marshal(restored)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "model-v5.json"), body, 0o600))
	}
}

func TestClosedBudgetBoundaryInvalidatesBeforeDispatchAndCannotRevive(t *testing.T) {
	for _, mode := range []string{"unknown", "rejected-cleanup", "new-success"} {
		t.Run(mode, func(t *testing.T) {
			m, request := closedRunnerBudget(t)
			held := m.Clone().BudgetHeld
			if mode == "new-success" {
				next, obs := heldStart(t, m, 2, runnerSources())
				require.Nil(t, next.BudgetBoundary, "first Start already removed the old closure proof")
				var err error
				next, err = next.Record(next.Owner, obs.Key, obs)
				require.NoError(t, err)
				require.EqualValues(t, 5, next.ProtocolVersion, "recording a new complete budget cannot downgrade v5")
				require.Nil(t, next.BudgetBoundary)
				nextRequest := next.Active.Request
				next = heldFinish(t, next)
				require.NoError(t, next.CheckClosedBudget(nextRequest))
				require.Error(t, next.CheckClosedBudget(request))
				require.Equal(t, nextRequest.Key, next.BudgetBoundary.BudgetKey)
				return
			}
			nextRequest := testRequest(m, 2, control.Grow)
			next := beginModel(t, m, nextRequest)
			require.NotNil(t, next.BudgetBoundary, "Begin alone grants no dispatch ticket")
			require.Error(t, next.CheckClosedBudget(request), "Active still blocks Host settlement")
			var err error
			next, err = next.Start(next.Owner, nextRequest.Key, PrepareBudget, "test-only-source", testCommand)
			require.NoError(t, err)
			require.Nil(t, next.BudgetBoundary, "write-ahead Start must invalidate before any native effect")
			require.Equal(t, held, next.BudgetHeld)
			require.NotNil(t, m.BudgetBoundary, "input model remains immutable")
			if mode == "unknown" {
				next, err = next.RequestAbort(next.Owner, nextRequest.Key)
				require.NoError(t, err)
				_, err = next.Finish(next.Owner, nextRequest.Key)
				require.Error(t, err)
				require.Equal(t, Unconfirmed, next.Active.Steps[0].Phase)
				return
			}
			next, err = next.Record(next.Owner, nextRequest.Key, observation(next, next.Active.Steps[0], Rejected))
			require.NoError(t, err)
			next, err = next.RequestAbort(next.Owner, nextRequest.Key)
			require.NoError(t, err)
			next, err = next.StartCleanup(next.Owner, nextRequest.Key, "test-only-cleanup", testCommand)
			require.NoError(t, err)
			next, err = next.Record(next.Owner, nextRequest.Key, observation(next, *next.Active.Cleanup, Completed))
			require.NoError(t, err)
			next, err = next.Finish(next.Owner, nextRequest.Key)
			require.NoError(t, err)
			require.Nil(t, next.BudgetBoundary)
			next = unstartedAbort(t, next, 3)
			require.Nil(t, next.BudgetBoundary, "later zero-dispatch cancel cannot resurrect proof across an earlier dispatched abort")
			require.Equal(t, held, next.BudgetHeld)
			require.Error(t, next.CheckClosedBudget(request))
		})
	}
}

func TestClosedBudgetBoundaryRejectsCorruptWitnessAndOldProjection(t *testing.T) {
	m, request := closedRunnerBudget(t)
	for _, mode := range []string{"old-version", "wrong-command", "wrong-recorded", "success-before-record", "future-sequence", "future-key", "same-revision-alias"} {
		t.Run(mode, func(t *testing.T) {
			bad := m.Clone()
			switch mode {
			case "old-version":
				bad.ProtocolVersion = 4
			case "wrong-command":
				bad.BudgetBoundary.FinalizationSHA256 = "wrong-original-command"
			case "wrong-recorded":
				bad.BudgetBoundary.RecordedSequence++
			case "success-before-record":
				bad.BudgetBoundary.SucceededSequence = bad.BudgetHeld.RecordedSequence
			case "future-sequence":
				bad.BudgetBoundary.ThroughSequence++
			case "future-key":
				bad.BudgetBoundary.ThroughKey.Revision++
			case "same-revision-alias":
				bad.BudgetBoundary.ThroughKey.OperationID.Low++
			}
			require.Error(t, bad.Validate())
			require.Error(t, bad.CheckClosedBudget(request))
		})
	}
	missing := m.Clone()
	missing.BudgetBoundary = nil
	require.NoError(t, missing.Validate(), "v5 may legitimately hold a new budget that has not closed")
	require.Error(t, missing.CheckClosedBudget(request), "absence of proof cannot fall back to old success history")
	var boundary ClosedBudgetBoundary
	require.Error(t, json.Unmarshal([]byte(`{"protocol_version":1,"unrecognized":true}`), &boundary))
}

func TestLegacyClosedBudgetBoundaryUpgradeRequiresOriginalSuccessChain(t *testing.T) {
	for _, mode := range []string{"contiguous", "history-expired", "dispatched-abort"} {
		t.Run(mode, func(t *testing.T) {
			m, request := closedRunnerBudget(t)
			// 显式旧格式存储夹具；生产没有降级入口。模拟升级前已结束的v4。
			m.ProtocolVersion, m.BudgetBoundary = 4, nil
			if mode == "history-expired" {
				m.ExpiredResults += control.Uint64(len(m.History))
				m.History = []Record{}
			}
			require.NoError(t, m.Validate())
			if mode == "dispatched-abort" {
				next := testRequest(m, 2, control.Grow)
				m = beginModel(t, m, next)
				var err error
				m, err = m.Start(m.Owner, next.Key, PrepareBudget, "test-only-source", testCommand)
				require.NoError(t, err)
				m, err = m.Record(m.Owner, next.Key, observation(m, m.Active.Steps[0], Rejected))
				require.NoError(t, err)
				m, err = m.RequestAbort(m.Owner, next.Key)
				require.NoError(t, err)
				m, err = m.StartCleanup(m.Owner, next.Key, "test-only-cleanup", testCommand)
				require.NoError(t, err)
				m, err = m.Record(m.Owner, next.Key, observation(m, *m.Active.Cleanup, Completed))
				require.NoError(t, err)
				m, err = m.Finish(m.Owner, next.Key)
				require.NoError(t, err)
			}
			m = unstartedAbort(t, m, 3)
			if mode == "contiguous" {
				require.EqualValues(t, 5, m.ProtocolVersion)
				require.NoError(t, m.CheckClosedBudget(request))
			} else {
				require.EqualValues(t, 4, m.ProtocolVersion)
				require.Nil(t, m.BudgetBoundary)
				require.Error(t, m.CheckClosedBudget(request), "no reconstruction from expired success or mixed dispatch history")
			}
		})
	}
}
