package budget

import (
	"encoding/json"
	"math"
	"os"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
)

func TestPreparedGrowBudgetUsesRetainedPrefixesAndCompleteOutside(t *testing.T) {
	raw, err := os.ReadFile("../../api/dsga/testdata/request-v1.json")
	require.NoError(t, err)
	var request control.Request
	require.NoError(t, json.Unmarshal(raw, &request))
	raw, err = os.ReadFile("../../api/dsga/testdata/response-v1.json")
	require.NoError(t, err)
	var fact control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &fact))
	s := fact.Observed
	s.Operation = nil
	s.CapacityGeneration = request.ExpectedCapacityGeneration
	s.LowBuffers, s.HighBuffers = 512, 512
	shared := control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: 16 << 20, StartupArraysUpperBytes: 16 << 20, CurrentMainAndArraysUpperBytes: 80 << 20, Observed: s}
	u, err := GrowSharedUpper(shared, 1024)
	require.NoError(t, err)
	require.Equal(t, control.Uint64(144<<20), u, "主段不重复，保留尾部随数组前缀保守计入")
	request.TargetBuffers = 1025
	u, err = GrowSharedUpper(shared, request.TargetBuffers)
	require.NoError(t, err)
	require.Equal(t, control.Uint64(208<<20), u, "不足整倍数也必须向上取整")
	id := resourceguard.Identity{VMUID: request.Key.Instance.VMUID, ComputeInstanceID: request.Key.Instance.ComputeInstanceID, GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
	p := Prepared{ProtocolVersion: 1, Request: request, Identity: id, RunnerPodName: "runner", VMBeforeMemoryBytes: 512 << 20, GuestUsableMemoryBytes: 768 << 20, Shared: shared, TargetSharedUpperBytes: u, Limits: resourceguard.Limits{MemoryMaxBytes: 512 << 20, OutsideUpperBytes: 32 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 8}}}, SystemOutsidePG: Bound{Kind: CommittedUpper, UpperBytes: 24 << 20, SourceID: "system"}, Transition: Bound{Kind: CommittedUpper, UpperBytes: 4 << 20, SourceID: "transition"}, Safety: Bound{Kind: CommittedUpper, UpperBytes: 4 << 20, SourceID: "safety"}}
	require.NoError(t, p.ValidateFor(request, id))
	require.NoError(t, p.ValidateCurrent(shared, request.Key.IssuerEpoch))
	drift := shared
	drift.Observed.MaximumBuffers++
	require.Error(t, p.ValidateCurrent(drift, request.Key.IssuerEpoch), "同boot最大映射不得悄悄变化")
	encoded, err := json.Marshal(p)
	require.NoError(t, err)
	for name, change := range map[string]func(*Prepared){
		"underquoted":         func(p *Prepared) { p.TargetSharedUpperBytes-- },
		"overlapping":         func(p *Prepared) { p.Safety.SourceID = p.SystemOutsidePG.SourceID },
		"observed_outside":    func(p *Prepared) { p.SystemOutsidePG.Kind = "rss" },
		"outside_mismatch":    func(p *Prepared) { p.Limits.OutsideUpperBytes++ },
		"guest_overcommitted": func(p *Prepared) { p.GuestUsableMemoryBytes = 64 << 20 },
		"unknown_tail":        func(p *Prepared) { p.Shared.Observed.Arrays[0].AllocationUnknown = true },
		"old_generation":      func(p *Prepared) { p.Shared.Observed.CapacityGeneration++ },
		"permit":              func(p *Prepared) { p.VMShrinkPermit = true },
	} {
		t.Run(name, func(t *testing.T) {
			var next Prepared
			require.NoError(t, json.Unmarshal(encoded, &next))
			change(&next)
			require.Error(t, next.ValidateFor(request, id))
		})
	}
	// 即使输入DTO内部总和仍合法，未来乘积也必须独立检查溢出。
	shared.Observed.Arrays[0].CommittedUpperBytes = control.Uint64(math.MaxUint64 / 2)
	shared.CurrentMainAndArraysUpperBytes = shared.StartupMainUpperBytes
	for _, a := range shared.Observed.Arrays {
		shared.CurrentMainAndArraysUpperBytes += a.CommittedUpperBytes
	}
	_, err = GrowSharedUpper(shared, 1536)
	require.Error(t, err)
}
