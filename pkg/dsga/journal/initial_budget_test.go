package journal

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// Fixture只验证领域合同/完整WAL；PG/root/平台/Runner/Host均为明确模拟事实，
// 不能将本文件结果写为首次服务生产者或Linux认证。Store用真实typed HTTP CAS。
func initialDomain(t *testing.T) (budget.Initial, budget.InitialAdmissionCommand, budget.InitialAdmissionReceipt) {
	t.Helper()
	raw, err := os.ReadFile("testdata/initial-domain.json")
	require.NoError(t, err)
	var fixture struct {
		Initial budget.Initial                 `json:"initial"`
		Command budget.InitialAdmissionCommand `json:"command"`
		Receipt budget.InitialAdmissionReceipt `json:"receipt"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.NoError(t, fixture.Receipt.ValidateFor(fixture.Initial, fixture.Command, 1))
	return fixture.Initial, fixture.Command, fixture.Receipt
}

func initialBoundModel(t *testing.T) Model {
	t.Helper()
	i, _, _ := initialDomain(t)
	m := testModel(t)
	var err error
	m, err = m.BindResource(m.Owner, i.PGAggregate.Identity)
	require.NoError(t, err)
	m, err = m.BindPG(m.Owner, i.Instance)
	require.NoError(t, err)
	m, err = m.BindInitialBudget(m.Owner, i)
	require.NoError(t, err)
	return m
}

func TestInitialWALSurvivesCodecTakeoverWithoutResizeAndRetainsOriginalCommand(t *testing.T) {
	i, c, r := initialDomain(t)
	m := initialBoundModel(t)
	require.EqualValues(t, 9, m.ProtocolVersion)
	require.Nil(t, m.HighWater)
	require.Nil(t, m.Active)
	_, disposition, err := m.Begin(m.Owner, testRequest(m, 1, control.Grow), 0)
	require.NoError(t, err)
	require.Equal(t, Busy, disposition)
	wrong := i
	wrong.HostMemoryBytes++
	_, err = m.BindInitialBudget(m.Owner, wrong)
	require.Error(t, err, "完整原预算不能同数值重新命名或替换")
	m, err = m.StartInitialAdmission(m.Owner, c)
	require.NoError(t, err)
	_, disposition, err = m.Begin(m.Owner, testRequest(m, 1, control.Grow), 0)
	require.NoError(t, err)
	require.Equal(t, Busy, disposition)
	_, encoded, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(encoded, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m.InitialHeld, loaded.InitialHeld)
	cloned := loaded.Clone()
	cloned.InitialHeld.Admission.Command[0] = 'x'
	require.NotEqual(t, cloned.InitialHeld.Admission.Command, loaded.InitialHeld.Admission.Command)
	loaded, err = loaded.Takeover(loaded.Owner, "new-owner")
	require.NoError(t, err)
	require.Equal(t, m.InitialHeld, loaded.InitialHeld)
	_, err = loaded.StartInitialAdmission(loaded.Owner, c)
	require.Error(t, err)
	r.Query, r.Response.Disposition = true, control.AdmissionReplayed
	r.ResourceEpoch, r.Fence.RequestedEpoch = loaded.Owner.Epoch, loaded.Owner.Epoch
	r.Shared.Observed.IssuerFloorEpoch = loaded.Owner.Epoch
	r.Fence.Observed, r.Response.Observed = r.Shared.Observed, r.Shared.Observed
	loaded, err = loaded.RecordInitialAdmission(loaded.Owner, r)
	require.NoError(t, err)
	require.Equal(t, Settled, loaded.InitialHeld.Admission.Phase)
	request := testRequest(loaded, 1, control.Grow)
	request.TargetBuffers = 1024
	loaded, disposition, err = loaded.Begin(loaded.Owner, request, 0)
	require.NoError(t, err)
	require.Equal(t, Accepted, disposition)
	retained, err := loaded.InitialBudgetFor()
	require.NoError(t, err)
	require.Equal(t, i.Qualification, retained.Qualification)
}

func TestInitialWALRejectsProjectionTamperingAndIncompleteOpening(t *testing.T) {
	_, c, receipt := initialDomain(t)
	m := initialBoundModel(t)
	var err error
	m, err = m.StartInitialAdmission(m.Owner, c)
	require.NoError(t, err)
	m, err = m.RecordInitialAdmission(m.Owner, receipt)
	require.NoError(t, err)
	for _, mode := range []string{"old-version", "no-budget", "raw-budget", "command-sha", "no-receipt", "bad-sequence", "wrong-owner"} {
		t.Run(mode, func(t *testing.T) {
			next := m.Clone()
			switch mode {
			case "old-version":
				next.ProtocolVersion = 8
			case "no-budget":
				next.InitialHeld = nil
			case "raw-budget":
				next.InitialHeld.Budget[0] = 'x'
			case "command-sha":
				next.InitialHeld.Admission.CommandSHA256 = "wrong"
			case "no-receipt":
				next.InitialHeld.Admission.Receipt = nil
			case "bad-sequence":
				next.InitialHeld.Admission.RecordedSequence = next.InitialHeld.Admission.StartedSequence
			case "wrong-owner":
				next.InitialHeld.Origin.HolderID = "same-epoch-wrong-holder"
			}
			require.Error(t, next.Validate())
		})
	}
}

func TestInitialBudgetReplacedOnlyByAtomicCompleteNormalFinalization(t *testing.T) {
	_, c, receipt := initialDomain(t)
	m := initialBoundModel(t)
	var err error
	m, err = m.StartInitialAdmission(m.Owner, c)
	require.NoError(t, err)
	m, err = m.RecordInitialAdmission(m.Owner, receipt)
	require.NoError(t, err)
	original := cloneInitial(m.InitialHeld)
	m, obs := heldStart(t, m, 1, runnerSources())
	require.Equal(t, original, m.InitialHeld, "普通Finalize未知仍须保留原启动完整预算")
	m, err = m.Record(m.Owner, obs.Key, obs)
	require.NoError(t, err)
	require.Nil(t, m.InitialHeld)
	require.NotNil(t, m.BudgetHeld)
	require.EqualValues(t, 9, m.ProtocolVersion)
	require.NoError(t, m.Validate())
}

func TestStoreInitialUnknownWritesNeverReturnDispatchOrLosePermanentState(t *testing.T) {
	i, c, receipt := initialDomain(t)
	for _, mode := range []string{"lost-budget", "lost-start", "lost-record"} {
		t.Run(mode, func(t *testing.T) {
			api, store := newStoreAPI(t)
			s := initializeStore(t, api, store)
			var err error
			s, err = store.BindResource(context.Background(), s, s.State().Owner, i.PGAggregate.Identity)
			require.NoError(t, err)
			s, err = store.BindPG(context.Background(), s, s.State().Owner, i.Instance)
			require.NoError(t, err)
			if mode == "lost-budget" {
				api.mu.Lock()
				api.failAfterCommit = true
				api.mu.Unlock()
			}
			bound, err := store.BindInitialBudget(context.Background(), s, s.State().Owner, i)
			if mode == "lost-budget" {
				require.Error(t, err)
				require.Nil(t, bound)
			} else {
				require.NoError(t, err)
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-start"
			api.mu.Unlock()
			s, err = store.Load(context.Background())
			require.NoError(t, err)
			_, ticket, err := store.StartInitialAdmission(context.Background(), s, s.State().Owner, c)
			if mode == "lost-start" {
				require.Error(t, err)
				require.Nil(t, ticket)
			} else {
				require.NoError(t, err)
				require.NotNil(t, ticket)
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-record"
			api.mu.Unlock()
			s, err = store.Load(context.Background())
			require.NoError(t, err)
			require.Equal(t, Unconfirmed, s.State().InitialHeld.Admission.Phase)
			_, err = store.RecordInitialAdmission(context.Background(), s, s.State().Owner, receipt)
			if mode == "lost-record" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			rebuilt := api.client(t)
			s, err = rebuilt.Load(context.Background())
			require.NoError(t, err)
			require.Equal(t, Settled, s.State().InitialHeld.Admission.Phase)
			require.Nil(t, s.State().Active)
			require.Nil(t, s.State().HighWater)
		})
	}
}
