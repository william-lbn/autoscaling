package budget

import (
	"encoding/hex"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// UnchangedAbortCommand固定“新Grow的VM未发送取消、旧完整预算仍原样有效”的
// 清理。新请求目标不是当前容量，不能替换旧Finalized或冒充新目标已成功。
// Admission为nil只确认当前已开放；非nil是旧预算下独立写前持久的开放命令。
type UnchangedAbortCommand struct {
	ProtocolVersion     uint32                    `json:"protocol_version"`
	Request             control.Request           `json:"request"`
	FinalizationSHA256  string                    `json:"finalization_sha256"`
	CancelledVMSHA256   string                    `json:"cancelled_vm_sha256"`
	CancellationSources RunnerSources             `json:"cancellation_sources"`
	Admission           *control.AdmissionRequest `json:"admission"`
}

func unchangedDigest(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == 32 && hex.EncodeToString(raw) == value
}

func (c UnchangedAbortCommand) ValidateFor(f Finalized, dispatchEpoch control.Uint64) error {
	old, next := f.Request.Key, c.Request.Key
	if c.ProtocolVersion != 1 || c.Request.Validate() != nil || c.Request.Kind != control.Grow || next.Instance != old.Instance || next.IssuerEpoch < old.IssuerEpoch || next.IssuerEpoch == old.IssuerEpoch && next.Revision <= old.Revision || dispatchEpoch < next.IssuerEpoch ||
		!unchangedDigest(c.FinalizationSHA256) || !unchangedDigest(c.CancelledVMSHA256) || c.CancellationSources.Validate() != nil || f.Sources == nil || f.Sources.Runner == nil || c.CancellationSources.BirthClaim != f.Sources.Runner.BirthClaim || c.CancellationSources.StateClaim != f.Sources.Runner.StateClaim ||
		f.ValidateFor(f.Request, f.Shared, f.IssuerEpoch, f.RunnerPodName, f.RunnerRestartCount, uint64(f.Request.TargetVMMemoryBytes)) != nil {
		return fmt.Errorf("unchanged abort lost original budget or later cancellation context")
	}
	if a := c.Admission; a != nil && (a.Validate() != nil || !a.Open || a.Instance != old.Instance || a.IssuerEpoch != dispatchEpoch || a.BudgetCommitmentID != f.Request.BudgetCommitmentID || a.ExpectedCapacityGeneration != f.Shared.Observed.CapacityGeneration) {
		return fmt.Errorf("unchanged abort reopening changed original retained budget")
	}
	return nil
}

// Receipt保留旧完整预算与新VM取消来源的独立审计。这里只核结构和成本；实际
// Reader必须同时认证原不可变完成回执和新取消终态，且当前无pending/实际值吻合。
// Query不等于没有执行过；SDK未知和跨owner只可按同一原命令查询，不修改原文。
type UnchangedAbortReceipt struct {
	ProtocolVersion uint32                     `json:"protocol_version"`
	Command         UnchangedAbortCommand      `json:"command"`
	Query           bool                       `json:"query"`
	Fence           control.FenceResponse      `json:"issuer_fence"`
	Shared          control.SharedCostResponse `json:"shared"`
	Response        *control.AdmissionResponse `json:"response"`
	PGAggregate     RootAggregate              `json:"pg_aggregate"`
	ResourceEpoch   control.Uint64             `json:"resource_issuer_epoch"`
}

func (r UnchangedAbortReceipt) ValidateFor(c UnchangedAbortCommand, f Finalized, dispatchEpoch, maximumEpoch control.Uint64) error {
	epoch := r.ResourceEpoch
	if r.ProtocolVersion != 1 || !reflect.DeepEqual(r.Command, c) || c.ValidateFor(f, dispatchEpoch) != nil || epoch < dispatchEpoch || epoch > maximumEpoch || !r.Shared.Observed.AdmissionOpen || r.PGAggregate.Validate() != nil || f.Aggregate == nil || r.PGAggregate.Bound != *f.Aggregate || r.PGAggregate.Identity.VMUID != f.Request.Key.Instance.VMUID || r.PGAggregate.Identity.ComputeInstanceID != f.Request.Key.Instance.ComputeInstanceID ||
		r.Fence.ValidateFor(control.FenceRequest{ProtocolVersion: 1, Instance: f.Request.Key.Instance, IssuerEpoch: epoch}, f.Request.Key.Instance) != nil || r.Fence.Disposition != control.Fenced || r.Shared.Observed.CapacityGeneration < r.Fence.Observed.CapacityGeneration || f.ValidateFor(f.Request, r.Shared, epoch, f.RunnerPodName, f.RunnerRestartCount, uint64(f.Request.TargetVMMemoryBytes)) != nil {
		return fmt.Errorf("unchanged abort lacks current complete original budget/open admission")
	}
	if c.Admission == nil {
		if !r.Query || r.Response != nil {
			return fmt.Errorf("read-only unchanged abort cannot invent admission mutation")
		}
		return nil
	}
	if r.Response == nil || !r.Query && epoch != dispatchEpoch || r.Response.ValidateFor(*c.Admission, f.Request.Key.Instance, r.Query) != nil || !r.Response.Confirms() || r.Response.Observed.IssuerFloorEpoch != epoch || !r.Response.Observed.AdmissionOpen || r.Response.Observed.AdmissionRevision != r.Shared.Observed.AdmissionRevision {
		return fmt.Errorf("unchanged abort original reopening remains unconfirmed or superseded")
	}
	returned := r.Shared
	returned.Observed = r.Response.Observed
	total := uint64(returned.StartupMainUpperBytes)
	for _, a := range returned.Observed.Arrays {
		if uint64(a.CommittedUpperBytes) > math.MaxUint64-total {
			return fmt.Errorf("unchanged abort returned PG cost overflows")
		}
		total += uint64(a.CommittedUpperBytes)
	}
	returned.CurrentMainAndArraysUpperBytes = control.Uint64(total)
	return f.ValidateFor(f.Request, returned, epoch, f.RunnerPodName, f.RunnerRestartCount, uint64(f.Request.TargetVMMemoryBytes))
}

func (c *UnchangedAbortCommand) UnmarshalJSON(raw []byte) error {
	type plain UnchangedAbortCommand
	var decoded plain
	if err := control.UnmarshalObject(raw, &decoded); err != nil {
		return err
	}
	*c = UnchangedAbortCommand(decoded)
	return nil
}

func (r *UnchangedAbortReceipt) UnmarshalJSON(raw []byte) error {
	type plain UnchangedAbortReceipt
	var decoded plain
	if err := control.UnmarshalObject(raw, &decoded); err != nil {
		return err
	}
	*r = UnchangedAbortReceipt(decoded)
	return nil
}
