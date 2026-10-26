package journal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

func runnerSources() budget.RunnerSources {
	return budget.RunnerSources{BirthClaim: budget.ObjectRef{Name: "original-birth", UID: "birth-uid"}, StateClaim: budget.ObjectRef{Name: "original-state", UID: "state-uid"}, ReceiptClaim: budget.ObjectRef{Name: "original-receipt", UID: "receipt-uid"}, CommandSHA256: strings.Repeat("d", 64), JournalSHA256: strings.Repeat("e", 64)}
}

// 验证永久来源不是历史缓存：真Model/codec推进、接管、历史淘汰后仍保原UID，
// 值相同的新引用或降回旧预算均不能被当作原长期结算。
func TestRunnerBudgetV4RetainsOriginalSourcesBeyondHistory(t *testing.T) {
	sources := runnerSources()
	m, obs := heldStart(t, heldBoundModel(t, testModel(t)), 1, sources)
	request := m.Active.Request
	var err error
	m, err = m.Record(m.Owner, request.Key, obs)
	require.NoError(t, err)
	require.EqualValues(t, 4, m.ProtocolVersion)
	f, err := m.BudgetFor(request)
	require.NoError(t, err)
	require.Equal(t, sources, *f.Sources.Runner)
	f.Sources.Runner.ReceiptClaim.UID = "caller-mutated"
	again, err := m.BudgetFor(request)
	require.NoError(t, err)
	require.Equal(t, sources, *again.Sources.Runner)
	held := m.Clone().BudgetHeld
	m = heldFinish(t, m)
	m, err = m.Takeover(m.Owner, "next-process")
	require.NoError(t, err)
	for i := 1; i <= HistoryLimit+1; i++ {
		next := testRequest(m, uint64(i), control.Grow)
		m = beginModel(t, m, next)
		m, err = m.RequestAbort(m.Owner, next.Key)
		require.NoError(t, err)
		m, err = m.Finish(m.Owner, next.Key)
		require.NoError(t, err)
	}
	require.NotZero(t, m.ExpiredResults)
	require.Equal(t, held, m.BudgetHeld)
	_, raw, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	restored, err := decode(raw, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	again, err = restored.BudgetFor(request)
	require.NoError(t, err)
	require.Equal(t, sources, *again.Sources.Runner)
	if dir := os.Getenv("DSGA_VM_WIRE_DIR"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		body, err := json.Marshal(restored)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "model-v4.json"), body, 0o600))
	}
}

func TestRunnerBudgetV4RejectsProjectionAndLegacyReplacement(t *testing.T) {
	m, obs := heldStart(t, heldBoundModel(t, testModel(t)), 1, runnerSources())
	var err error
	m, err = m.Record(m.Owner, obs.Key, obs)
	require.NoError(t, err)
	wrong := m.Clone()
	wrong.ProtocolVersion = 3
	require.Error(t, wrong.Validate(), "新来源不能伪装为旧版本")
	// 构造去掉新来源、同时重算原命令和回执摘要的字段投影，保持其他不变量。
	// v4仍必须拒绝，不能只依靠未知字段解码来阻止旧格式放行。
	projected := m.Clone()
	var command budget.FinalizationCommand
	require.NoError(t, json.Unmarshal(projected.BudgetHeld.Finalization.Command, &command))
	command.Finalized.Sources.Runner = nil
	step := projected.BudgetHeld.Finalization
	step.Command, err = json.Marshal(command)
	require.NoError(t, err)
	step.Observation.Data, err = json.Marshal(command.Finalized)
	require.NoError(t, err)
	op := projected.Active
	step.CommandSHA256, err = commandDigest(op.RequestSHA256, step)
	require.NoError(t, err)
	step.Observation.CommandSHA256 = step.CommandSHA256
	projected.BudgetHeld.Finalization = step
	projected.Active.Steps[int(step.Ordinal)-1] = step
	require.ErrorContains(t, projected.Validate(), "Runner budget sources")
	if dir := os.Getenv("DSGA_VM_WIRE_DIR"); dir != "" {
		body, err := json.Marshal(projected)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "model-v4-projection.json"), body, 0o600))
	}
	m = heldFinish(t, m)
	next, pending := heldStart(t, m, 2)
	_, err = next.Record(next.Owner, pending.Key, pending)
	require.ErrorContains(t, err, "cannot downgrade")
	require.EqualValues(t, 4, next.ProtocolVersion)
	require.Equal(t, m.BudgetHeld, next.BudgetHeld)
}
