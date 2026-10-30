package budgetclaims

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 仅核来源读者选择原永久预算anchor的纯状态机合同；所有领域事实明确
// 复用完整fixture，经公开Model方法安装，不将本测试当真实Host来源认证。
func activeCurrentModel(t *testing.T) journal.Model {
	t.Helper()
	raw, err := os.ReadFile("../admissionexec/testdata/reconciled-domain.json")
	require.NoError(t, err)
	var domain struct {
		Prepared budget.Prepared           `json:"prepared"`
		Capacity budget.ReconciledCapacity `json:"capacity"`
	}
	require.NoError(t, json.Unmarshal(raw, &domain))
	p, capacity := domain.Prepared, domain.Capacity
	m, err := journal.New(p.Identity.VMUID, "1234567890abcdef1234567890abcdef", "source-owner")
	require.NoError(t, err)
	m, err = m.BindResource(m.Owner, p.Identity)
	require.NoError(t, err)
	m, err = m.BindPG(m.Owner, p.Request.Key.Instance)
	require.NoError(t, err)
	request := p.Request
	request.Key.IssuerEpoch = m.Owner.Epoch
	m, disposition, err := m.Begin(m.Owner, request, 0)
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	shared := p.Shared
	shared.Observed.CapacityGeneration = request.ExpectedCapacityGeneration + 1
	shared.Observed.LowBuffers, shared.Observed.HighBuffers = request.TargetBuffers, request.TargetBuffers
	digest, err := request.ContextSHA256()
	require.NoError(t, err)
	shared.Observed.Operation = &control.OperationFact{Key: request.Key, Kind: request.Kind, Phase: control.Committed, Reason: control.ReasonNone, TargetBuffers: request.TargetBuffers, ExpectedCapacityGeneration: request.ExpectedCapacityGeneration, ContextSHA256: digest, CancelClosed: true, CancelRequested: false, Replayed: false}
	final := budget.Finalized{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, IssuerEpoch: m.Owner.Epoch, SettlementEpoch: 0, RunnerPodName: p.RunnerPodName, RunnerRestartCount: p.RunnerRestartCount, HostReservationID: capacity.HostReservationID, GuestUsableMemoryBytes: p.GuestUsableMemoryBytes, Shared: shared, SystemOutsidePG: p.SystemOutsidePG, Aggregate: &capacity.PGAggregate, PrivateAndDSM: nil, LFCOutsideShared: nil, HugePoolOutsideShared: nil, Transition: p.Transition, Safety: p.Safety, Sources: &budget.SettlementSources{OutsideClaim: capacity.OutsideClaim, HostClaim: capacity.HostClaim, VMReceiptSHA256: strings.Repeat("c", 64), Runner: &capacity.VM.Sources}}
	command := budget.FinalizationCommand{ProtocolVersion: 1, Finalized: final, PreparedSHA256: capacity.VM.PreparedSHA256, GuestSHA256: strings.Repeat("d", 64), VMSHA256: final.Sources.VMReceiptSHA256}
	require.NoError(t, command.Validate())
	for _, original := range m.Active.Steps {
		body, target, audit := []byte(`{"explicit_domain_fixture":true}`), request.Key.Instance.ComputeInstanceID, []byte(`{"explicit_domain_fixture":true}`)
		if original.Action == journal.FinalizeBudget {
			body, err = json.Marshal(command)
			require.NoError(t, err)
			audit, err = json.Marshal(final)
			require.NoError(t, err)
		}
		m, err = m.Start(m.Owner, request.Key, original.Action, target, body)
		require.NoError(t, err)
		step := m.Active.Steps[int(original.Ordinal)-1]
		m, err = m.Record(m.Owner, request.Key, journal.Observation{Key: request.Key, RequestSHA256: m.Active.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: target, Data: audit})
		require.NoError(t, err)
	}
	m, err = m.Finish(m.Owner, request.Key)
	require.NoError(t, err)
	request.Key.Revision++
	request.Key.OperationID.Low++
	request.BudgetCommitmentID += "-new"
	m, disposition, err = m.Begin(m.Owner, request, 0)
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	return m
}

func TestActiveHeldSettlementUsesOnlyOneOriginalCapturedAnchor(t *testing.T) {
	m := activeCurrentModel(t)
	closed, err := activeHeldSettlement(m)
	require.NoError(t, err)
	require.Equal(t, m.BudgetBoundary.BudgetKey, closed.Request.Key)
	before := m.Clone()
	m, err = m.Start(m.Owner, m.Active.Request.Key, journal.PrepareBudget, m.Active.Request.Key.Instance.ComputeInstanceID, []byte(`{"explicit_prepare_intent":true}`))
	require.NoError(t, err)
	require.Nil(t, m.BudgetBoundary)
	captured, err := activeHeldSettlement(m)
	require.NoError(t, err)
	require.Equal(t, closed, captured)
	require.NotNil(t, before.BudgetBoundary)
	for _, mode := range []string{"two-anchors", "dispatched-global", "missing-anchor", "wrong-sha", "wrong-recorded-sequence"} {
		t.Run(mode, func(t *testing.T) {
			bad := m.Clone()
			switch mode {
			case "two-anchors":
				boundary := *bad.Active.PriorBudgetBoundary
				bad.BudgetBoundary = &boundary
			case "dispatched-global":
				bad.BudgetBoundary, bad.Active.PriorBudgetBoundary = bad.Active.PriorBudgetBoundary, nil
			case "missing-anchor":
				bad.Active.PriorBudgetBoundary = nil
			case "wrong-sha":
				bad.Active.PriorBudgetBoundary.FinalizationSHA256 = strings.Repeat("e", 64)
			case "wrong-recorded-sequence":
				bad.Active.PriorBudgetBoundary.RecordedSequence++
			}
			require.NoError(t, bad.Validate(), "结构有效仍需本只读消费者精确核原anchor")
			_, err := activeHeldSettlement(bad)
			require.Error(t, err)
		})
	}
}
