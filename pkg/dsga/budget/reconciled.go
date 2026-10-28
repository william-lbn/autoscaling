package budget

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// NativeCapacity保留最终Runner域的准确原意图。完成表示该命令的target；未发送
// 取消表示该命令的before。它们都不是被取消业务请求的成功回执。较早原命令可
// 为本次未触及VM的取消提供来源，但必须同PG/Guest并由实际读者核当前原State。
type NativeCapacity struct {
	Sources        RunnerSources   `json:"sources"`
	Request        control.Request `json:"request"`
	BeforeBytes    control.Uint64  `json:"before_bytes"`
	Cancelled      bool            `json:"cancelled_before_send"`
	PreparedSHA256 string          `json:"prepared_sha256"`
	GuestSHA256    string          `json:"guest_sha256"`
	HostClaim      ObjectRef       `json:"host_claim"`
}

func (n NativeCapacity) MemoryBytes() control.Uint64 {
	if n.Cancelled {
		return n.BeforeBytes
	}
	return n.Request.TargetVMMemoryBytes
}

func (n NativeCapacity) Validate() error {
	if n.Request.Validate() != nil || n.Sources.Validate() != nil || n.HostClaim.Validate() != nil || n.BeforeBytes == 0 || !unchangedDigest(n.PreparedSHA256) ||
		(n.Request.Kind == control.Shrink && !unchangedDigest(n.GuestSHA256)) || (n.Request.Kind == control.Grow && n.GuestSHA256 != "") ||
		(n.Request.Kind == control.Grow && n.Request.TargetVMMemoryBytes < n.BeforeBytes) || (n.Request.Kind == control.Shrink && n.Request.TargetVMMemoryBytes > n.BeforeBytes) {
		return fmt.Errorf("invalid original native capacity source")
	}
	return nil
}

// ReconciledCapacity是取消后实际容量的完整预算，刻意独立于Finalized：原请求
// target仍原样保留，实际Buffer/VM及PG操作另列。普通成功消费者不能把本结构
// 解成Finalized，从而把VM已大/PG未长或PG已小/VM未缩写成完整目标成功。
//
// 这里保存能力相关的PG事实，不复制并伪造一份带准入状态的原生回复。准入代次
// 和当前floor在生产执行器每次认证读取；PG成本可以减少，但不能超过保留上界。
// Root/Outside/Host/Runner都是原持久来源；本合同只核结构和算术，不认证来源。
type ReconciledCapacity struct {
	ProtocolVersion          uint32                 `json:"protocol_version"`
	VMShrinkPermit           bool                   `json:"vm_shrink_permit"`
	Request                  control.Request        `json:"request"`
	IssuerEpoch              control.Uint64         `json:"issuer_epoch"`
	RunnerPodName            string                 `json:"runner_pod_name"`
	RunnerRestartCount       int32                  `json:"runner_restart_count"`
	VMBeforeMemoryBytes      control.Uint64         `json:"vm_before_memory_bytes"`
	VMMemoryBytes            control.Uint64         `json:"vm_memory_bytes"`
	GuestUsableMemoryBytes   control.Uint64         `json:"guest_usable_memory_bytes"`
	CapacityGeneration       control.Uint64         `json:"capacity_generation"`
	Buffers                  uint32                 `json:"buffers"`
	MaximumBuffers           uint32                 `json:"maximum_buffers"`
	StartupMainUpperBytes    control.Uint64         `json:"startup_main_upper_bytes"`
	StartupArraysUpperBytes  control.Uint64         `json:"startup_arrays_upper_bytes"`
	RetainedSharedUpperBytes control.Uint64         `json:"retained_shared_upper_bytes"`
	PGOperation              *control.OperationFact `json:"pg_operation"`
	PGAggregate              Bound                  `json:"pg_aggregate"`
	RootSHA256               string                 `json:"root_sha256"`
	OutsideClaim             ObjectRef              `json:"outside_claim,omitempty"`
	OutsideRequest           control.Request        `json:"outside_request,omitempty"`
	ResidualProfileID        string                 `json:"residual_profile_id"`
	SystemOutsidePG          Bound                  `json:"system_outside_pg"`
	Transition               Bound                  `json:"transition"`
	Safety                   Bound                  `json:"safety"`
	HostClaim                ObjectRef              `json:"host_claim"`
	HostReservationID        string                 `json:"host_reservation_id"`
	VM                       NativeCapacity         `json:"native_vm,omitempty"`
	// v2只供准确仍处原启动容量的取消：Outside来自原Initial中的
	// 完整资格，而非伪造一条目标等于before的Resize/OutsideRequest。
	// 与OutsideClaim/OutsideRequest互斥；原Initial随实际预算永久保留。
	// v1省略此字段，旧读者须因capacity协议v2拒绝而不能按空来源放行。
	InitialOutside *Initial `json:"initial_outside,omitempty"`
	// v3在VM尚未派发且原Runner从未有内存命令时使用真实启动来源。
	// 与VM互斥，不给原启动状态伪造Resize/NoEffect或普通完成回执。
	InitialVM *InitialVMSources `json:"initial_vm,omitempty"`
}

// ValidateCurrent不要求原目标已全部完成；要求当前PG是准确同代次、稳定前缀和
// 已终止的原操作。只有expected或expected+1两种代次可由本次取消覆盖；不同
// 操作、PREPARING、未知分配、成本增长及另一个boot均不得恢复普通服务。
func (c ReconciledCapacity) ValidateCurrent(latest control.SharedCostResponse, epoch control.Uint64, memory control.Uint64) error {
	if c.Validate() != nil || latest.ValidateFor(c.Request.Key.Instance) != nil || latest.Observed.IssuerFloorEpoch != epoch || epoch < c.IssuerEpoch || memory != c.VMMemoryBytes ||
		latest.Observed.CapacityGeneration != c.CapacityGeneration || latest.Observed.HighBuffers != c.Buffers || latest.Observed.LowBuffers != c.Buffers || latest.Observed.MaximumBuffers != c.MaximumBuffers ||
		latest.StartupMainUpperBytes != c.StartupMainUpperBytes || latest.StartupArraysUpperBytes != c.StartupArraysUpperBytes || latest.CurrentMainAndArraysUpperBytes > c.RetainedSharedUpperBytes || !reflect.DeepEqual(latest.Observed.Operation, c.PGOperation) {
		return fmt.Errorf("reconciled PG or VM capacity changed from original retained facts")
	}
	for _, a := range latest.Observed.Arrays {
		if a.AllocationUnknown {
			return fmt.Errorf("reconciled PG allocation remains unknown")
		}
	}
	return nil
}

func (c ReconciledCapacity) Validate() error {
	r := c.Request
	if c.VMShrinkPermit || r.Validate() != nil || c.IssuerEpoch < r.Key.IssuerEpoch || control.ValidateID(c.RunnerPodName, "Runner") != nil || c.RunnerRestartCount < 0 ||
		c.VMBeforeMemoryBytes == 0 || (c.VMMemoryBytes != c.VMBeforeMemoryBytes && c.VMMemoryBytes != r.TargetVMMemoryBytes) || c.GuestUsableMemoryBytes == 0 || c.GuestUsableMemoryBytes > c.VMMemoryBytes || c.CapacityGeneration == 0 || c.Buffers == 0 || c.Buffers > c.MaximumBuffers ||
		c.StartupMainUpperBytes == 0 || c.StartupArraysUpperBytes == 0 || c.RetainedSharedUpperBytes < c.StartupMainUpperBytes || c.PGAggregate.Kind != HardLimit || c.PGAggregate.UpperBytes < c.RetainedSharedUpperBytes || !unchangedDigest(c.RootSHA256) ||
		control.ValidateID(c.ResidualProfileID, "qualified residual profile") != nil ||
		c.HostClaim.Validate() != nil || control.ValidateID(c.HostReservationID, "host reservation") != nil ||
		c.SystemOutsidePG.UpperBytes == 0 || c.Safety.UpperBytes == 0 {
		return fmt.Errorf("incomplete original reconciled capacity budget")
	}
	if err := c.validateVMSource(); err != nil {
		return err
	}
	if c.InitialOutside == nil {
		if c.ProtocolVersion != 1 || c.OutsideClaim.Validate() != nil || c.OutsideRequest.Validate() != nil || c.OutsideRequest.Key.Instance != r.Key.Instance || c.OutsideRequest.TargetVMMemoryBytes != c.VMMemoryBytes {
			return fmt.Errorf("reconciled outside source lost exact original operation")
		}
	} else if err := c.validateInitialOutside(); err != nil {
		return err
	}
	if r.Kind == control.Grow && r.TargetVMMemoryBytes < c.VMBeforeMemoryBytes || r.Kind == control.Shrink && r.TargetVMMemoryBytes > c.VMBeforeMemoryBytes {
		return fmt.Errorf("reconciled original VM direction changed")
	}
	if c.InitialVM == nil && compareCapacityKey(c.VM.Request.Key, r.Key) > 0 || c.InitialOutside == nil && compareCapacityKey(c.OutsideRequest.Key, r.Key) > 0 {
		return fmt.Errorf("reconciled budget cannot borrow a future or aliased source")
	}
	if c.InitialVM == nil && c.VM.Request.Key == r.Key && (c.VM.Request != r || c.VM.HostClaim != c.HostClaim) || c.InitialOutside == nil && c.OutsideRequest.Key == r.Key && c.OutsideRequest != r {
		return fmt.Errorf("reconciled budget changed same-key original request or Host source")
	}
	op := c.PGOperation
	if op != nil && (op.Key.Validate() != nil || op.Key.Instance != r.Key.Instance || (op.Kind != control.Grow && op.Kind != control.Shrink) || (op.Phase != control.Committed && op.Phase != control.Denied) || !unchangedDigest(op.ContextSHA256) || compareCapacityKey(op.Key, r.Key) > 0) {
		return fmt.Errorf("reconciled PG operation is not terminal in the original domain")
	}
	if c.CapacityGeneration == r.ExpectedCapacityGeneration {
		if op != nil && op.Key == r.Key && (op.Phase == control.Committed || !sameCapacityOperation(*op, r)) {
			return fmt.Errorf("reconciled unchanged PG generation contradicts original operation")
		}
	} else if r.ExpectedCapacityGeneration == control.Uint64(math.MaxUint64) || c.CapacityGeneration != r.ExpectedCapacityGeneration+1 || op == nil || op.Phase != control.Committed || !sameCapacityOperation(*op, r) || c.Buffers != r.TargetBuffers {
		return fmt.Errorf("reconciled PG generation is outside original operation")
	}
	seen, total := map[string]bool{}, uint64(0)
	for _, b := range []Bound{c.PGAggregate, c.SystemOutsidePG, c.Transition, c.Safety} {
		if (b.Kind != HardLimit && b.Kind != CommittedUpper) || control.ValidateID(b.SourceID, "capacity bound") != nil || seen[b.SourceID] || uint64(b.UpperBytes) > math.MaxUint64-total {
			return fmt.Errorf("reconciled capacity bounds overlap or overflow")
		}
		seen[b.SourceID] = true
		total += uint64(b.UpperBytes)
	}
	if total > uint64(c.GuestUsableMemoryBytes) {
		return fmt.Errorf("reconciled full budget exceeds actual usable Guest memory")
	}
	return nil
}

func sameCapacityOperation(op control.OperationFact, r control.Request) bool {
	hash, err := r.ContextSHA256()
	return err == nil && op.Key == r.Key && op.Kind == r.Kind && op.TargetBuffers == r.TargetBuffers && op.ExpectedCapacityGeneration == r.ExpectedCapacityGeneration && op.ContextSHA256 == hash
}

// 原Key的operation ID不可作为排序补丁：相同epoch/revision但换ID仍是冲突。
func compareCapacityKey(a, b control.Key) int {
	if a.IssuerEpoch < b.IssuerEpoch || a.IssuerEpoch == b.IssuerEpoch && a.Revision < b.Revision {
		return -1
	}
	if a == b {
		return 0
	}
	return 1
}

func (v *NativeCapacity) UnmarshalJSON(raw []byte) error {
	type plain NativeCapacity
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	*v = NativeCapacity(p)
	return v.Validate()
}

func (v *ReconciledCapacity) UnmarshalJSON(raw []byte) error {
	type plain ReconciledCapacity
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	*v = ReconciledCapacity(p)
	return v.Validate()
}

// RootDigest绑定完整原Root来源而非只比较同数值限额。写前后重读仍由生产者
// 执行；摘要不赋root权限，也不把一次内核观测变成永久物理释放证明。
func RootDigest(a RootAggregate) (string, error) {
	if err := a.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return "", err
	}
	return capacityDigest(raw), nil
}
