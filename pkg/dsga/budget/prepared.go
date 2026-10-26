package budget

import (
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// Prepared是受信PrepareBudget生产者写入同一VM账本的目标承诺，尚不是
// Finalized、Host释放或VM许可。生产者必须真实保留Outside三类上界并维持其
// 寿命；本结构只核验完整绑定、去重和算术，不能认证自报的kind/SourceID。
// GuestReady只消费原Completed步骤，不接受HTTP调用者临时传入另一份预算。
type Prepared struct {
	ProtocolVersion        uint32                     `json:"protocol_version"`
	VMShrinkPermit         bool                       `json:"vm_shrink_permit"`
	Request                control.Request            `json:"request"`
	Identity               resourceguard.Identity     `json:"identity"`
	RunnerPodName          string                     `json:"runner_pod_name"`
	RunnerRestartCount     int32                      `json:"runner_restart_count"`
	VMBeforeMemoryBytes    control.Uint64             `json:"vm_before_memory_bytes"`
	GuestUsableMemoryBytes control.Uint64             `json:"guest_usable_memory_bytes"`
	Shared                 control.SharedCostResponse `json:"shared"`
	TargetSharedUpperBytes control.Uint64             `json:"target_shared_upper_bytes"`
	Limits                 resourceguard.Limits       `json:"limits"`
	SystemOutsidePG        Bound                      `json:"system_outside_pg"`
	Transition             Bound                      `json:"transition"`
	Safety                 Bound                      `json:"safety"`
	// 新生产结算必须读取原可信持久对象；旧无引用格式仅保留历史读取，不能补造。
	OutsideClaim *ObjectRef `json:"outside_claim,omitempty"`
}

// GrowSharedUpper使用ABI6四数组固定元素/前缀分配的不变量给出保守上界。
// 每数组U>=ceil(H*element/page)*page，故ceil(T/H)*U覆盖目标T的页对齐分配；
// 固定主段只加一次。无需猜测descriptor尺寸/页大小或把RSS乘比例。保留尾部
// 也计入U，结果可能偏大；要放宽该限制须增加原生精确quote，不能削减安全项。
// 只接收稳定、分配已知的当前成本；PREPARING/未知尾部不能成为新计划基线。
func GrowSharedUpper(shared control.SharedCostResponse, target uint32) (control.Uint64, error) {
	if err := shared.ValidateFor(shared.Observed.Instance); err != nil {
		return 0, err
	}
	s := shared.Observed
	if s.LowBuffers != s.HighBuffers || target <= s.HighBuffers || target > s.MaximumBuffers || (s.Operation != nil && s.Operation.Phase == control.Preparing) {
		return 0, fmt.Errorf("grow quote requires a stable native prefix")
	}
	factor := (uint64(target) + uint64(s.HighBuffers) - 1) / uint64(s.HighBuffers)
	total := uint64(shared.StartupMainUpperBytes)
	for _, array := range s.Arrays {
		u := uint64(array.CommittedUpperBytes)
		if array.AllocationUnknown || u > math.MaxUint64/factor || u*factor > math.MaxUint64-total {
			return 0, fmt.Errorf("grow quote is unknown or overflows")
		}
		total += u * factor
	}
	return control.Uint64(total), nil
}

func (p Prepared) ValidateFor(request control.Request, id resourceguard.Identity) error {
	if request.Validate() != nil || id.Validate() != nil || p.Shared.ValidateFor(request.Key.Instance) != nil || p.Limits.Validate() != nil {
		return fmt.Errorf("prepared budget contains invalid native/resource context")
	}
	if p.OutsideClaim != nil && p.OutsideClaim.Validate() != nil {
		return fmt.Errorf("invalid prepared outside reservation reference")
	}
	s := p.Shared.Observed
	if p.ProtocolVersion != 1 || p.VMShrinkPermit || p.Request != request || p.Identity != id || id.VMUID != request.Key.Instance.VMUID || id.ComputeInstanceID != request.Key.Instance.ComputeInstanceID || p.RunnerRestartCount < 0 || control.ValidateID(p.RunnerPodName, "Runner") != nil || p.VMBeforeMemoryBytes == 0 || p.GuestUsableMemoryBytes == 0 || p.GuestUsableMemoryBytes > request.TargetVMMemoryBytes || s.IssuerFloorEpoch != request.Key.IssuerEpoch || s.CapacityGeneration != request.ExpectedCapacityGeneration || s.LowBuffers != s.HighBuffers || (s.Operation != nil && s.Operation.Phase == control.Preparing) {
		return fmt.Errorf("prepared budget differs from original operation/Guest/Runner")
	}
	minimum := p.Shared.StartupMainUpperBytes
	if request.Kind == control.Grow {
		var err error
		minimum, err = GrowSharedUpper(p.Shared, request.TargetBuffers)
		if err != nil || request.TargetVMMemoryBytes < p.VMBeforeMemoryBytes {
			return fmt.Errorf("prepared grow quote or VM direction differs")
		}
	} else if request.TargetBuffers >= s.LowBuffers || request.TargetVMMemoryBytes > p.VMBeforeMemoryBytes {
		return fmt.Errorf("prepared shrink direction differs")
	}
	for _, a := range s.Arrays {
		if a.AllocationUnknown {
			return fmt.Errorf("prepared native allocation is unknown")
		}
	}
	if p.TargetSharedUpperBytes < minimum || p.TargetSharedUpperBytes > p.Limits.MemoryMaxBytes || p.SystemOutsidePG.UpperBytes == 0 || p.Safety.UpperBytes == 0 {
		return fmt.Errorf("prepared resource cap cannot contain shared/system/safety commitment")
	}
	seen, outside := map[string]bool{}, uint64(0)
	for _, b := range []Bound{p.SystemOutsidePG, p.Transition, p.Safety} {
		if (b.Kind != HardLimit && b.Kind != CommittedUpper) || control.ValidateID(b.SourceID, "outside commitment") != nil || seen[b.SourceID] || uint64(b.UpperBytes) > math.MaxUint64-outside {
			return fmt.Errorf("prepared outside commitments overlap or overflow")
		}
		seen[b.SourceID] = true
		outside += uint64(b.UpperBytes)
	}
	total, err := p.Limits.TotalUpperBytes()
	if err != nil || outside != uint64(p.Limits.OutsideUpperBytes) || total > uint64(p.GuestUsableMemoryBytes) {
		return fmt.Errorf("prepared complete aggregate exceeds target usable Guest budget")
	}
	return nil
}

// ValidateCurrent只核验本领域推进时的当前PG成本；Shrink还须原Resize/Release
// Completed前驱和当前四数组物理回执，Grow尚未执行resize，不能冒认最终PG提交。
func (p Prepared) ValidateCurrent(latest control.SharedCostResponse, epoch control.Uint64) error {
	if p.ValidateFor(p.Request, p.Identity) != nil || latest.ValidateFor(p.Request.Key.Instance) != nil {
		return fmt.Errorf("invalid prepared/current PG cost")
	}
	s := latest.Observed
	if s.IssuerFloorEpoch != epoch || s.MaximumBuffers != p.Shared.Observed.MaximumBuffers || latest.StartupMainUpperBytes != p.Shared.StartupMainUpperBytes || latest.StartupArraysUpperBytes != p.Shared.StartupArraysUpperBytes || latest.CurrentMainAndArraysUpperBytes > p.TargetSharedUpperBytes {
		return fmt.Errorf("current PG cost/floor exceeds original commitment")
	}
	for _, a := range s.Arrays {
		if a.AllocationUnknown {
			return fmt.Errorf("current PG allocation remains unknown")
		}
	}
	if p.Request.Kind == control.Grow {
		u, err := GrowSharedUpper(latest, p.Request.TargetBuffers)
		if err != nil || u > p.TargetSharedUpperBytes || s.CapacityGeneration != p.Request.ExpectedCapacityGeneration || s.HighBuffers != p.Shared.Observed.HighBuffers {
			return fmt.Errorf("current grow baseline or future allocation exceeds commitment")
		}
		return nil
	}
	r, op := p.Request, s.Operation
	hash, err := r.ContextSHA256()
	if err != nil || op == nil || op.Key != r.Key || op.Kind != r.Kind || op.Phase != control.Committed || op.ContextSHA256 != hash || op.TargetBuffers != r.TargetBuffers || op.ExpectedCapacityGeneration != r.ExpectedCapacityGeneration || r.ExpectedCapacityGeneration == control.Uint64(math.MaxUint64) || s.CapacityGeneration != r.ExpectedCapacityGeneration+1 || s.HighBuffers != r.TargetBuffers || s.LowBuffers != r.TargetBuffers {
		return fmt.Errorf("current shrink lacks complete original native commit")
	}
	for _, a := range s.Arrays {
		v := a.Provider
		if a.ReleaseState != control.ReleaseVerified || a.ReleaseKey == nil || *a.ReleaseKey != r.Key || a.ReleaseCapacityGeneration != s.CapacityGeneration || !v.HasResult || v.AllocationMayBePartial || v.ReleaseMayBePartial || v.SystemErrno != 0 {
			return fmt.Errorf("current shrink provider is not verified")
		}
		if v.Status == control.ProviderNoop && v.RequestedBytes == 0 && v.AffectedBytes == 0 {
			continue
		}
		if v.Status != control.ProviderOK || !v.PhysicalReleaseVerified || v.RequestedOffset != v.AffectedOffset || v.RequestedBytes == 0 || v.RequestedBytes != v.AffectedBytes {
			return fmt.Errorf("current shrink physical release is not verified")
		}
	}
	return nil
}

func (v *Prepared) UnmarshalJSON(data []byte) error {
	type plain Prepared
	var p plain
	if err := control.UnmarshalObject(data, &p); err != nil {
		return err
	}
	*v = Prepared(p)
	return nil
}
