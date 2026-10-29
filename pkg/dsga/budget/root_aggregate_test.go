package budget

import (
	"encoding/json"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
)

func aggregateView() resourceguard.View {
	id := resourceguard.Identity{VMUID: "vm-1", ComputeInstanceID: "compute-1", GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
	pools := []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 8}, {PageSizeKB: 1048576, Pages: 0}}
	return resourceguard.View{ProtocolVersion: 1, Identity: id, IssuerFloorEpoch: 1,
		Bootstrap: resourceguard.BootstrapCommand{ProtocolVersion: 1, Identity: id, IssuerEpoch: 1, BudgetCommitmentID: "bootstrap-original", Limits: resourceguard.Limits{MemoryMaxBytes: 64 << 20, OutsideUpperBytes: 32 << 20, HugePools: pools}}, BootstrapApplied: true,
		Birth:  &resourceguard.ProcessIdentity{ProtocolVersion: 1, Identity: id, PostmasterPID: 123, PostmasterStartTicks: 456},
		Actual: resourceguard.Evidence{ProtocolVersion: 1, Identity: id, MemoryMaxBytes: 64 << 20, MemoryCurrentBytes: 4 << 20, GuestMemTotalBytes: 256 << 20, HugePools: append([]resourceguard.HugePool(nil), pools...), WholePoolUpperBytes: 16 << 20, HugeOvercommitDisabled: true, CgroupPopulated: true}}
}

func TestRootAggregateUsesConfirmedDomainOnceAndNeverPromotesOutside(t *testing.T) {
	view := aggregateView()
	source, err := RootAggregateFromView(view)
	require.NoError(t, err)
	require.Equal(t, control.Uint64(80<<20), source.Bound.UpperBytes, "normal64+wholepool16, not outside32, not RSS or Shared again")
	require.Equal(t, HardLimit, source.Bound.Kind)
	require.False(t, source.VMShrinkPermit)
	require.NoError(t, source.Validate())
	require.NoError(t, source.Match(view))
	before := source.Bound
	view.Actual.MemoryCurrentBytes = 12 << 20
	view.IssuerFloorEpoch = 2
	require.NoError(t, source.Match(view), "owner接管和实际占用变化不能重造仍保留的原资源承诺")
	require.Equal(t, before, source.Bound)
	// caller随后复用切片不能改变已经生成的原source。
	view.Bootstrap.Limits.HugePools[0].Pages++
	require.NoError(t, source.Validate())
	require.Equal(t, control.Uint64(8), source.Bootstrap.Limits.HugePools[0].Pages)
	require.Error(t, source.Match(view))
}

func TestRootAggregateKeepsOriginalHeldReferenceAcrossUnknownAndReplacement(t *testing.T) {
	view := aggregateView()
	held := resourceguard.Command{ProtocolVersion: 1, Identity: view.Identity, IssuerEpoch: 1, Revision: 7, BudgetCommitmentID: "original-runtime", PostmasterPID: 123, PostmasterStartTicks: 456, MemoryMaxBytes: 128 << 20, OutsideUpperBytes: 32 << 20, HugePools: append([]resourceguard.HugePool(nil), view.Bootstrap.Limits.HugePools...)}
	view.Original = &held
	view.Held = &held
	view.Applied = true
	view.Actual.MemoryMaxBytes = held.MemoryMaxBytes
	source, err := RootAggregateFromView(view)
	require.NoError(t, err)
	require.Equal(t, control.Uint64(144<<20), source.Bound.UpperBytes)
	require.Equal(t, held, *source.Held)
	later := held
	later.Revision++
	later.BudgetCommitmentID = "new-unknown"
	view.Original = &later
	view.Applied = false
	require.Error(t, source.Match(view), "不能借旧Held把新Unknown变成完成")
	require.Equal(t, control.Uint64(144<<20), source.Bound.UpperBytes, "错误读回不释放原承诺")
	view.Held = &later
	view.Applied = true
	replacement, err := RootAggregateFromView(view)
	require.NoError(t, err)
	require.NotEqual(t, source.Bound.SourceID, replacement.Bound.SourceID)
	require.Error(t, source.Match(view), "同数字的新资源命令仍是另一来源")
}

func TestRootAggregateRejectsPartialKernelOrUnboundSourceAndStrictTampering(t *testing.T) {
	original := aggregateView()
	raw, err := json.Marshal(original)
	require.NoError(t, err)
	for _, change := range []func(*resourceguard.View){
		func(v *resourceguard.View) { v.Actual.MemoryCurrentBytes = v.Actual.MemoryMaxBytes + 1 },
		func(v *resourceguard.View) { v.Actual.SwapCurrentBytes = 1 }, func(v *resourceguard.View) { v.Actual.SwapMaxBytes = 1 },
		func(v *resourceguard.View) { v.Actual.HugeSurplusPages = 1 }, func(v *resourceguard.View) { v.Actual.HugeOvercommitDisabled = false },
		func(v *resourceguard.View) { v.Actual.WholePoolUpperBytes++ }, func(v *resourceguard.View) { v.BootstrapApplied = false },
		func(v *resourceguard.View) { v.Birth = nil }, func(v *resourceguard.View) { v.Actual.CgroupPopulated = false },
	} {
		var view resourceguard.View
		require.NoError(t, json.Unmarshal(raw, &view))
		change(&view)
		_, err := RootAggregateFromView(view)
		require.Error(t, err)
	}
	source, err := RootAggregateFromView(original)
	require.NoError(t, err)
	encoded, err := json.Marshal(source)
	require.NoError(t, err)
	var decoded RootAggregate
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.NoError(t, decoded.Match(original))
	bad := source
	bad.Bound.UpperBytes += 32 << 20
	require.Error(t, bad.Validate(), "outside不能加到PG聚合源")
	bad = source
	bad.VMShrinkPermit = true
	require.Error(t, bad.Validate())
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	for key := range fields {
		var copy map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(encoded, &copy))
		delete(copy, key)
		missing, _ := json.Marshal(copy)
		require.Error(t, json.Unmarshal(missing, &decoded), key)
	}
	// 完全自造但格式正确的SourceID仍非认证证据，必须和真正root View重新核对。
	forgedView := aggregateView()
	forgedView.Bootstrap.Limits.MemoryMaxBytes += 1 << 20
	forgedView.Actual.MemoryMaxBytes += 1 << 20
	forged, err := RootAggregateFromView(forgedView)
	require.NoError(t, err)
	require.NoError(t, forged.Validate())
	require.Error(t, forged.Match(original))
}

// 真实root的读回由resourceguard负责；这里只核原来源摘要和字节去重，不模拟物理内核。
func TestRootAggregateFiniteTasksBindSourceWithoutAddingBytes(t *testing.T) {
	view := aggregateView()
	legacy, err := RootAggregateFromView(view)
	require.NoError(t, err)
	view.Bootstrap.Limits.PidsMaxTasks = 64
	view.Actual.TaskLimit = &resourceguard.TaskLimitEvidence{MaxTasks: 64, CurrentTasks: 4}
	finite, err := RootAggregateFromView(view)
	require.NoError(t, err)
	require.Equal(t, legacy.Bound.UpperBytes, finite.Bound.UpperBytes)
	require.NotEqual(t, legacy.Bound.SourceID, finite.Bound.SourceID)
	view.Actual.TaskLimit.CurrentTasks = 5
	require.NoError(t, finite.Match(view), "瞬时task占用变化不改原来源")
	view.Bootstrap.Limits.PidsMaxTasks = 65
	view.Actual.TaskLimit.MaxTasks = 65
	require.Error(t, finite.Match(view), "相同字节的不同task限额仍是不同来源")
	view.Bootstrap.Limits.PidsMaxTasks = 64
	view.Actual.TaskLimit.MaxTasks = 64
	held := resourceguard.Command{ProtocolVersion: 1, Identity: view.Identity, IssuerEpoch: 1, Revision: 1, BudgetCommitmentID: "held-tasks", PostmasterPID: 123, PostmasterStartTicks: 456, MemoryMaxBytes: view.Bootstrap.Limits.MemoryMaxBytes, PidsMaxTasks: 64, OutsideUpperBytes: view.Bootstrap.Limits.OutsideUpperBytes, HugePools: view.Bootstrap.Limits.HugePools}
	view.Original, view.Held, view.Applied = &held, &held, true
	source, err := RootAggregateFromView(view)
	require.NoError(t, err)
	require.Equal(t, legacy.Bound.UpperBytes, source.Bound.UpperBytes)
	source.Held.PidsMaxTasks = 0
	require.Error(t, source.Validate(), "不能删除Held字段伪造旧未约束来源")
	view.Actual.TaskLimit.CurrentTasks = 65
	_, err = RootAggregateFromView(view)
	require.Error(t, err, "迁入允许超额，不可发证")
}

func TestRootAggregateAddressSpaceBindsBirthWithoutAddingOrSubtractingBytes(t *testing.T) {
	view := aggregateView()
	legacy, err := RootAggregateFromView(view)
	require.NoError(t, err)
	view.Bootstrap.Limits.PidsMaxTasks = 64
	view.Bootstrap.Limits.AddressSpaceMaxBytes = 128 << 20
	view.Birth.AddressSpaceMaxBytes = 128 << 20
	view.Actual.TaskLimit = &resourceguard.TaskLimitEvidence{MaxTasks: 64, CurrentTasks: 4}
	view.Actual.AddressSpace = &resourceguard.AddressSpaceEvidence{Processes: 4, MaxHardBytes: 128 << 20, MaxVirtualBytes: 64 << 20, Irreversible: true}
	finite, err := RootAggregateFromView(view)
	require.NoError(t, err)
	require.Equal(t, legacy.Bound.UpperBytes, finite.Bound.UpperBytes, "AS包含共享VA和占位，不能加成私有RSS或扣减原normal预算")
	require.NotEqual(t, legacy.Bound.SourceID, finite.Bound.SourceID)
	view.Actual.AddressSpace.MaxVirtualBytes++
	require.NoError(t, finite.Match(view), "瞬时VA不改变原硬约束摘要")
	finite.Birth.AddressSpaceMaxBytes = 0
	require.Error(t, finite.Validate(), "独立来源校验也必须拒绝出生漏限额")
	view.Birth.AddressSpaceMaxBytes = 0
	_, err = RootAggregateFromView(view)
	require.Error(t, err)
	view.Birth.AddressSpaceMaxBytes = 128 << 20
	view.Actual.AddressSpace.Irreversible = false
	_, err = RootAggregateFromView(view)
	require.Error(t, err)
}
