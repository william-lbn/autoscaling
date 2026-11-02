package resourceexec

import (
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

// 只验证生产执行器确实消费原持久ReleasePG，而非当前采样/旧接口；不签真实PG来源。
func TestNormalQuotaExecutorConsumesOnlyRetainedCompletedRelease(t *testing.T) {
	raw, e := os.ReadFile("../../api/dsga/testdata/request-v1.json")
	require.NoError(t, e)
	var q control.Request
	require.NoError(t, json.Unmarshal(raw, &q))
	q.Kind = control.Shrink
	q.TargetBuffers = 256
	raw, e = os.ReadFile("../../api/dsga/testdata/response-v1.json")
	require.NoError(t, e)
	var response control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &response))
	var before control.Snapshot
	beforeRaw, e := json.Marshal(response.Observed)
	require.NoError(t, e)
	require.NoError(t, json.Unmarshal(beforeRaw, &before))
	before.Instance = q.Key.Instance
	before.IssuerFloorEpoch = q.Key.IssuerEpoch
	before.CapacityGeneration = q.ExpectedCapacityGeneration
	before.HighBuffers = 512
	before.LowBuffers = 512
	before.Operation = nil
	before.StartupSharedUpperBytes = 32 << 20
	for i := range before.Arrays {
		before.Arrays[i].CommittedUpperBytes = 20 << 20
		before.Arrays[i].AllocationUnknown = false
		before.Arrays[i].ReleaseKey = nil
		before.Arrays[i].ReleaseState = control.ReleaseNone
		before.Arrays[i].ReleaseCapacityGeneration = 0
		before.Arrays[i].Provider = control.ProviderFact{Status: control.ProviderUnknown}
	}
	shared := control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: 16 << 20, StartupArraysUpperBytes: 16 << 20, CurrentMainAndArraysUpperBytes: 96 << 20, Observed: before}
	id := resourceguard.Identity{VMUID: q.Key.Instance.VMUID, ComputeInstanceID: q.Key.Instance.ComputeInstanceID, GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
	limits := resourceguard.Limits{MemoryMaxBytes: 128 << 20, OutsideUpperBytes: 32 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 8}}}
	prepFence := control.FenceResponse{ProtocolVersion: 1, RequestedInstance: q.Key.Instance, RequestedEpoch: q.Key.IssuerEpoch, Disposition: control.Fenced, Observed: before}
	p := budget.Prepared{ProtocolVersion: 1, NormalQuotaProtocolVersion: 1, NormalQuotaFence: &prepFence, Request: q, Identity: id, RunnerPodName: "runner", VMBeforeMemoryBytes: q.TargetVMMemoryBytes, GuestUsableMemoryBytes: q.TargetVMMemoryBytes, Shared: shared, TargetSharedUpperBytes: 96 << 20, Limits: limits, SystemOutsidePG: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 24 << 20, SourceID: "system"}, Transition: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "transition"}, Safety: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "safety"}}
	require.NoError(t, p.ValidateFor(q, id))
	hash, e := q.ContextSHA256()
	require.NoError(t, e)
	response.Receipt.Key = q.Key
	for _, s := range []*control.Snapshot{response.Receipt.Result, &response.Observed} {
		s.Instance = q.Key.Instance
		s.IssuerFloorEpoch = q.Key.IssuerEpoch
		s.CapacityGeneration = q.ExpectedCapacityGeneration + 1
		s.HighBuffers = q.TargetBuffers
		s.LowBuffers = q.TargetBuffers
		s.StartupSharedUpperBytes = 32 << 20
		s.AdmissionOpen = false
		s.Operation = &control.OperationFact{Key: q.Key, Kind: q.Kind, Phase: control.Committed, Reason: control.ReasonNone, CancelClosed: true, TargetBuffers: q.TargetBuffers, ExpectedCapacityGeneration: q.ExpectedCapacityGeneration, ContextSHA256: hash}
		for i := range s.Arrays {
			a := &s.Arrays[i]
			key := q.Key
			a.CommittedUpperBytes = 4 << 20
			a.AllocationUnknown = false
			a.ReleaseKey = &key
			a.ReleaseCapacityGeneration = s.CapacityGeneration
			a.ReleaseState = control.ReleaseVerified
			a.Provider = control.ProviderFact{HasResult: true, Status: control.ProviderOK, RequestedOffset: 4096, RequestedBytes: 4096, AffectedOffset: 4096, AffectedBytes: 4096, PhysicalReleaseVerified: true}
		}
	}
	releaseFence := control.FenceResponse{ProtocolVersion: 1, RequestedInstance: q.Key.Instance, RequestedEpoch: q.Key.IssuerEpoch, Disposition: control.Fenced, Observed: response.Observed}
	data, e := json.Marshal(struct {
		ProtocolVersion uint32                  `json:"protocol_version"`
		Fence           control.FenceResponse   `json:"issuer_fence"`
		Response        control.ControlResponse `json:"response"`
	}{1, releaseFence, response})
	require.NoError(t, e)
	command := resourceguard.Command{ProtocolVersion: 1, Identity: id, IssuerEpoch: q.Key.IssuerEpoch, Revision: 1, BudgetCommitmentID: q.BudgetCommitmentID, PostmasterPID: 123, PostmasterStartTicks: 10, MemoryMaxBytes: limits.MemoryMaxBytes, OutsideUpperBytes: limits.OutsideUpperBytes, HugePools: limits.HugePools}
	body, e := json.Marshal(q)
	require.NoError(t, e)
	op := &journal.Operation{Request: q, Steps: []journal.Step{{Action: journal.ReleasePG, Phase: journal.Settled, TargetID: id.ComputeInstanceID, Command: body, Observation: &journal.Observation{Result: journal.Completed, Data: data}}}}
	envelope, e := normalQuotaEnvelope(op, command, p)
	require.NoError(t, e)
	require.Equal(t, "released-shrink", envelope.Phase)
	t.Run("recovery-continuation-exact-only", func(t *testing.T) {
		guest := journal.Step{Action: journal.GuestReady, Phase: journal.Unconfirmed}
		require.NoError(t, normalQuotaContinuationMatches(&envelope, op, &guest, command))
		changed := *op
		changed.Request.Key.Revision++
		require.ErrorIs(t, normalQuotaContinuationMatches(&envelope, &changed, &guest, command), resourceguard.ErrUnknown, "不跟随新active")
		changed = *op
		changed.Request.TargetBuffers++
		require.ErrorIs(t, normalQuotaContinuationMatches(&envelope, &changed, &guest, command), resourceguard.ErrUnknown, "同Key也不能换目标")
		planned := guest
		planned.Phase = journal.Planned
		require.ErrorIs(t, normalQuotaContinuationMatches(&envelope, op, &planned, command), resourceguard.ErrUnknown, "不得开始新的Apply")
		another := command
		another.BudgetCommitmentID = "changed-budget"
		require.ErrorIs(t, normalQuotaContinuationMatches(&envelope, op, &guest, another), resourceguard.ErrUnknown, "不替换原Command预算")
	})
	t.Run("pre-grow-is-plan-without-false-release", func(t *testing.T) {
		grow := q
		grow.Kind = control.Grow
		grow.TargetBuffers = 1024
		grow.Key.Revision++
		grow.Key.OperationID.Low++
		prepared := p
		prepared.Request = grow
		prepared.Limits.MemoryMaxBytes = 256 << 20
		upper, err := budget.GrowSharedUpper(prepared.Shared, grow.TargetBuffers)
		require.NoError(t, err)
		prepared.TargetSharedUpperBytes = upper
		c := command
		c.BudgetCommitmentID = grow.BudgetCommitmentID
		c.MemoryMaxBytes = prepared.Limits.MemoryMaxBytes
		v, err := normalQuotaEnvelope(&journal.Operation{Request: grow}, c, prepared)
		require.NoError(t, err)
		require.Equal(t, "pre-grow", v.Phase)
		require.Nil(t, v.PGResponse)
		require.NotNil(t, v.PGShared)
	})
	for name, change := range map[string]func(*journal.Operation){
		"no-release":      func(o *journal.Operation) { o.Steps = nil },
		"unconfirmed":     func(o *journal.Operation) { o.Steps[0].Phase = journal.Unconfirmed },
		"rejected":        func(o *journal.Operation) { o.Steps[0].Observation.Result = journal.Rejected },
		"changed-command": func(o *journal.Operation) { o.Steps[0].Command = []byte("{}") },
	} {
		t.Run(name, func(t *testing.T) {
			// 仅复制此helper实际消费的原步骤；Operation的完整JSON解码另会先校验
			// owner，此处不能让无关的空owner把目标负例提前挡住。
			x := *op
			x.Steps = append([]journal.Step(nil), op.Steps...)
			for i := range x.Steps {
				x.Steps[i].Command = append([]byte(nil), op.Steps[i].Command...)
				if op.Steps[i].Observation != nil {
					observation := *op.Steps[i].Observation
					observation.Data = append([]byte(nil), observation.Data...)
					x.Steps[i].Observation = &observation
				}
			}
			change(&x)
			_, e := normalQuotaEnvelope(&x, command, p)
			require.Error(t, e)
		})
	}
}
