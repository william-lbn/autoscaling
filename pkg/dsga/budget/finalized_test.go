package budget

import (
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestFinalizedBudgetRequiresFullBindingsAndDistinctEnforcedBounds(t *testing.T) {
	raw, err := os.ReadFile("../../api/dsga/testdata/request-v1.json")
	require.NoError(t, err)
	var request control.Request
	require.NoError(t, json.Unmarshal(raw, &request))
	raw, err = os.ReadFile("../../api/dsga/testdata/response-v1.json")
	require.NoError(t, err)
	var response control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &response))
	startup := response.Observed.StartupSharedUpperBytes
	shared := control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: startup / 2, StartupArraysUpperBytes: startup - startup/2, Observed: response.Observed}
	shared.CurrentMainAndArraysUpperBytes = shared.StartupMainUpperBytes
	for _, a := range shared.Observed.Arrays {
		shared.CurrentMainAndArraysUpperBytes += a.CommittedUpperBytes
	}
	bound := func(id string) *Bound { return &Bound{Kind: CommittedUpper, UpperBytes: 1 << 20, SourceID: id} }
	fact := Finalized{ProtocolVersion: 1, Request: request, IssuerEpoch: request.Key.IssuerEpoch, RunnerPodName: "runner-1", RunnerRestartCount: 0, HostReservationID: "host-1", GuestUsableMemoryBytes: request.TargetVMMemoryBytes, Shared: shared,
		SystemOutsidePG: *bound("system"), PrivateAndDSM: bound("private-dsm"), LFCOutsideShared: bound("lfc"), HugePoolOutsideShared: bound("huge-pool"), Transition: *bound("transition"), Safety: *bound("safety")}
	validate := func(f Finalized) error {
		return f.ValidateFor(request, shared, request.Key.IssuerEpoch, "runner-1", 0, uint64(request.TargetVMMemoryBytes))
	}
	require.NoError(t, validate(fact))
	encoded, err := json.Marshal(fact)
	require.NoError(t, err)
	for _, mutate := range []func(*Finalized){
		func(f *Finalized) { f.PrivateAndDSM.Kind = "estimate" },
		func(f *Finalized) { f.Safety.UpperBytes = 0 },
		func(f *Finalized) { f.LFCOutsideShared.SourceID = f.PrivateAndDSM.SourceID },
		func(f *Finalized) { f.GuestUsableMemoryBytes = 1 },
		func(f *Finalized) { f.RunnerRestartCount++ },
		func(f *Finalized) { f.Request.BudgetCommitmentID = "changed" },
		func(f *Finalized) { f.VMShrinkPermit = true },
	} {
		var copy Finalized
		require.NoError(t, json.Unmarshal(encoded, &copy))
		mutate(&copy)
		require.Error(t, validate(copy))
	}
	var restored Finalized
	require.NoError(t, json.Unmarshal(encoded, &restored))
	require.NoError(t, validate(restored))
	// 聚合额度含PG全部成本和整个Guest巨页池；不再叠加Shared或私有/LFC/池。
	aggregate := restored
	aggregate.PrivateAndDSM = nil
	aggregate.LFCOutsideShared = nil
	aggregate.HugePoolOutsideShared = nil
	aggregate.Aggregate = &Bound{Kind: HardLimit, UpperBytes: shared.CurrentMainAndArraysUpperBytes + 3*(1<<20), SourceID: "whole-workload-and-huge-pool"}
	require.NoError(t, validate(aggregate))
	aggregate.PrivateAndDSM = bound("must-not-recount")
	require.Error(t, validate(aggregate))
}
