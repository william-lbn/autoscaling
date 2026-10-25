// Package budget核验持久预算域的完整结算事实，不把PG成本、RSS采样或HTTP成功
// 当成私有/DSM/Guest/Host约束已经生效。实际约束/承诺由各生产适配器建立并认证。
package budget

import (
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

type BoundKind string

const (
	HardLimit      BoundKind = "hard_limit"
	CommittedUpper BoundKind = "committed_upper"
)

// Bound是互不重叠成本分类的硬额度或持久承诺上界。SourceID是已认证生产者的
// 原约束引用，不是自报身份；约束须持续到显式对账/替换，不能随Agent租约到期撤销。
// 观测、估计、memory.high、单次RSS均不能使用这两个kind冒充完成证据。
type Bound struct {
	Kind       BoundKind      `json:"kind"`
	UpperBytes control.Uint64 `json:"upper_bytes"`
	SourceID   string         `json:"source_id"`
}

// Finalized由已认证预算域在实际约束/Host承诺/Guest/VM完成后写入FinalizeBudget
// observation。此数据本身不认证生产者，不发VM许可；消费者必须来自同一受信journal。
// PG主段/四数组只计一次；其余六类明确不含该范围，不能将包含全部PG的cgroup总量
// 再当private叠加。聚合硬上限需要由生产者先拆解/替换成不重叠的保守成本类别。
// 也支持Aggregate整体覆盖：SystemOutsidePG此时须排除整个Guest巨页池和PG计费域，
// 不要求对无法独立硬限的私有/DSM强行拆账；聚合约束真实性仍由生产者独立核验。
// 进程迁入不迁移既有共享/file页收费；其他计费域必须由外部来源持续覆盖，不能
// 从PG进程当前归组推断所有相关内存都在Aggregate内。硬限额也不证明免OOM。
type Finalized struct {
	ProtocolVersion        uint32                     `json:"protocol_version"`
	VMShrinkPermit         bool                       `json:"vm_shrink_permit"`
	Request                control.Request            `json:"request"`
	IssuerEpoch            control.Uint64             `json:"issuer_epoch"`
	RunnerPodName          string                     `json:"runner_pod_name"`
	RunnerRestartCount     int32                      `json:"runner_restart_count"`
	HostReservationID      string                     `json:"host_reservation_id"`
	GuestUsableMemoryBytes control.Uint64             `json:"guest_usable_memory_bytes"`
	Shared                 control.SharedCostResponse `json:"shared"`
	SystemOutsidePG        Bound                      `json:"system_outside_pg"`
	// Aggregate覆盖PG计费域中主段/DSG/私有/DSM/LFC及整个Guest HugeTLB池（含空闲池）。
	// 可由经核验的聚合硬约束组成，不要求把memory.max错误拆成私有硬限额。
	// 有此项时以下三类必须省略，原生Shared仅核对容量，不再额外相加。
	Aggregate             *Bound `json:"aggregate,omitempty"`
	PrivateAndDSM         *Bound `json:"private_and_dsm,omitempty"`
	LFCOutsideShared      *Bound `json:"lfc_outside_shared,omitempty"`
	HugePoolOutsideShared *Bound `json:"huge_pool_outside_shared,omitempty"`
	Transition            Bound  `json:"transition"`
	Safety                Bound  `json:"safety"`
}

// ValidateFor在恢复准入之前复核原完整请求、同boot/容量代次、原Runner与当前VM
// 实际内存读回。较新floor只允许只读对账旧动作；生产约束不能因PG换boot而遗忘。
// 当前PG上界可因实际释放下降，不可超过预算原保留值；未知尾部仍由PG原生上界计入。
func (f Finalized) ValidateFor(request control.Request, latest control.SharedCostResponse, epoch control.Uint64, runner string, restarts int32, vmMemoryBytes uint64) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := f.Shared.ValidateFor(request.Key.Instance); err != nil {
		return err
	}
	if err := latest.ValidateFor(request.Key.Instance); err != nil {
		return err
	}
	if f.ProtocolVersion != 1 || f.VMShrinkPermit || f.Request != request || f.IssuerEpoch != request.Key.IssuerEpoch || f.IssuerEpoch > epoch || f.RunnerPodName != runner || f.RunnerRestartCount != restarts || restarts < 0 || vmMemoryBytes == 0 || vmMemoryBytes != uint64(request.TargetVMMemoryBytes) || uint64(f.GuestUsableMemoryBytes) > vmMemoryBytes {
		return fmt.Errorf("finalized budget differs from original operation or Runner/VM")
	}
	if control.ValidateID(f.RunnerPodName, "Runner") != nil || control.ValidateID(f.HostReservationID, "host reservation") != nil {
		return fmt.Errorf("invalid finalized budget source binding")
	}
	if latest.Observed.IssuerFloorEpoch != epoch || f.Shared.Observed.IssuerFloorEpoch != f.IssuerEpoch || latest.Observed.CapacityGeneration != f.Shared.Observed.CapacityGeneration || latest.StartupMainUpperBytes != f.Shared.StartupMainUpperBytes || latest.StartupArraysUpperBytes != f.Shared.StartupArraysUpperBytes || latest.CurrentMainAndArraysUpperBytes > f.Shared.CurrentMainAndArraysUpperBytes {
		return fmt.Errorf("PG budget generation/floor or retained allocation differs")
	}
	digest, err := request.ContextSHA256()
	op := latest.Observed.Operation
	if err != nil || op == nil || op.Key != request.Key || op.Kind != request.Kind || op.Phase != control.Committed || op.ContextSHA256 != digest || op.ExpectedCapacityGeneration != request.ExpectedCapacityGeneration || op.TargetBuffers != request.TargetBuffers || latest.Observed.HighBuffers != request.TargetBuffers || latest.Observed.LowBuffers != request.TargetBuffers || request.ExpectedCapacityGeneration == control.Uint64(math.MaxUint64) || latest.Observed.CapacityGeneration != request.ExpectedCapacityGeneration+1 {
		return fmt.Errorf("finalized budget lacks complete original PG commit")
	}
	if f.SystemOutsidePG.UpperBytes == 0 || f.Safety.UpperBytes == 0 {
		return fmt.Errorf("required system/safety budget is absent")
	}
	bounds := []Bound{f.SystemOutsidePG, f.Transition, f.Safety}
	total := uint64(f.Shared.CurrentMainAndArraysUpperBytes)
	if f.Aggregate != nil {
		if f.PrivateAndDSM != nil || f.LFCOutsideShared != nil || f.HugePoolOutsideShared != nil || f.Aggregate.UpperBytes < f.Shared.CurrentMainAndArraysUpperBytes {
			return fmt.Errorf("aggregate budget duplicates categories or cannot contain PG")
		}
		total = 0
		bounds = append(bounds, *f.Aggregate)
	} else {
		if f.PrivateAndDSM == nil || f.LFCOutsideShared == nil || f.HugePoolOutsideShared == nil || f.PrivateAndDSM.UpperBytes == 0 {
			return fmt.Errorf("categorized private/DSM/LFC/HugeTLB budget is incomplete")
		}
		bounds = append(bounds, *f.PrivateAndDSM, *f.LFCOutsideShared, *f.HugePoolOutsideShared)
	}
	seen := map[string]bool{}
	for _, bound := range bounds {
		if (bound.Kind != HardLimit && bound.Kind != CommittedUpper) || control.ValidateID(bound.SourceID, "budget bound") != nil || seen[bound.SourceID] {
			return fmt.Errorf("budget bound is not distinct or authoritative")
		}
		seen[bound.SourceID] = true
		if uint64(bound.UpperBytes) > math.MaxUint64-total {
			return fmt.Errorf("complete budget overflows")
		}
		total += uint64(bound.UpperBytes)
	}
	if total > uint64(f.GuestUsableMemoryBytes) {
		return fmt.Errorf("complete retained budget exceeds usable Guest memory")
	}
	return nil
}

func (v *Bound) UnmarshalJSON(data []byte) error {
	type plain Bound
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	*v = Bound(next)
	return nil
}

func (v *Finalized) UnmarshalJSON(data []byte) error {
	type plain Finalized
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	*v = Finalized(next)
	return nil
}
