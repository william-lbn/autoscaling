package budget

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func initialAdmissionFixture(t *testing.T) (Initial, InitialAdmissionCommand, InitialAdmissionReceipt) {
	t.Helper()
	i, _ := initialFixture(t)
	digest, err := i.Digest()
	require.NoError(t, err)
	c := InitialAdmissionCommand{ProtocolVersion: 1, InitialSHA256: digest, Request: control.AdmissionRequest{ProtocolVersion: 1, Instance: i.Instance, IssuerEpoch: i.IssuerEpoch, Revision: 1, Open: true, ExpectedCapacityGeneration: 1, BudgetCommitmentID: i.PGAggregate.Bootstrap.BudgetCommitmentID}}
	shared := i.Shared
	shared.Observed.AdmissionOpen, shared.Observed.AdmissionRevision = true, 1
	hash, err := c.Request.ContextSHA256()
	require.NoError(t, err)
	r := InitialAdmissionReceipt{ProtocolVersion: 1, Command: c, Query: false, Fence: control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: i.Instance, RequestedEpoch: i.IssuerEpoch, Disposition: control.Fenced, Observed: shared.Observed}, Shared: shared, Response: control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: c.Request, Disposition: control.AdmissionApplied, ContextSHA256: hash, Observed: shared.Observed}, PGAggregate: i.PGAggregate, ResourceEpoch: i.IssuerEpoch, VMMemoryBytes: i.VMMemoryBytes, GuestUsableMemoryBytes: i.GuestUsableMemoryBytes}
	require.NoError(t, c.ValidateFor(i))
	require.NoError(t, r.ValidateFor(i, c, i.IssuerEpoch))
	return i, c, r
}

func TestInitialAdmissionRequiresCompleteOriginalNativeReceipt(t *testing.T) {
	i, c, receipt := initialAdmissionFixture(t)
	raw, err := json.Marshal(receipt)
	require.NoError(t, err)
	for name, change := range map[string]func(*InitialAdmissionReceipt){
		"not-found": func(r *InitialAdmissionReceipt) {
			r.Query = true
			r.Response.Disposition = control.AdmissionNotFound
			r.Response.Observed = i.Shared.Observed
		},
		"expired": func(r *InitialAdmissionReceipt) {
			r.Query = true
			r.Response.Disposition = control.AdmissionResultExpired
		},
		"different-observation": func(r *InitialAdmissionReceipt) { r.Shared.Observed.AdmissionRevision++ },
		"different-budget":      func(r *InitialAdmissionReceipt) { r.Command.InitialSHA256 = "wrong" },
		"different-root-floor":  func(r *InitialAdmissionReceipt) { r.ResourceEpoch++ },
		"different-vm":          func(r *InitialAdmissionReceipt) { r.VMMemoryBytes-- },
		"insufficient-guest":    func(r *InitialAdmissionReceipt) { r.GuestUsableMemoryBytes-- },
		"new-pg-generation":     func(r *InitialAdmissionReceipt) { r.Shared.Observed.CapacityGeneration++ },
		"not-a-query":           func(r *InitialAdmissionReceipt) { r.Query = true },
	} {
		t.Run(name, func(t *testing.T) {
			var r InitialAdmissionReceipt
			require.NoError(t, json.Unmarshal(raw, &r))
			change(&r)
			require.Error(t, r.ValidateFor(i, c, i.IssuerEpoch))
		})
	}
	bad := c
	bad.Request.Revision++
	require.Error(t, bad.ValidateFor(i), "首次生产者不能覆盖已前进的维护门禁")
	bad = c
	bad.Request.ExpectedCapacityGeneration++
	require.Error(t, bad.ValidateFor(i))
	// 新floor可只读确认旧命令，但不能重写原命令的issuer或内容摘要。
	receipt.Query, receipt.Response.Disposition = true, control.AdmissionReplayed
	receipt.ResourceEpoch++
	receipt.Shared.Observed.IssuerFloorEpoch++
	receipt.Fence.RequestedEpoch++
	receipt.Fence.Observed, receipt.Response.Observed = receipt.Shared.Observed, receipt.Shared.Observed
	require.NoError(t, receipt.ValidateFor(i, c, i.IssuerEpoch+1))
}
