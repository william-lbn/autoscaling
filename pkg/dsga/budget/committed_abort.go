package budget

import (
	"encoding/hex"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// CommittedAbortCommand只用于所有容量域已完成、完整预算已永久落账后的取消。
// 它不回滚PG/VM、不降低Root、不改变原目标；只恢复被确认预算覆盖的工作准入。
// MaySend=false时只能查询原ResumeAdmission，不能把已开放事实改造成新setter。
// 原命令必须先写入同一VM Cleanup WAL；重复轮询不能重发或更换revision/epoch。
type CommittedAbortCommand struct {
	ProtocolVersion    uint32                   `json:"protocol_version"`
	Request            control.Request          `json:"request"`
	FinalizationSHA256 string                   `json:"finalization_sha256"`
	Admission          control.AdmissionRequest `json:"admission"`
	MaySend            bool                     `json:"may_send"`
}

func (c CommittedAbortCommand) ValidateFor(f Finalized, dispatchEpoch control.Uint64) error {
	digest, err := hex.DecodeString(c.FinalizationSHA256)
	a := c.Admission
	if c.ProtocolVersion != 1 || c.Request != f.Request || f.ValidateFor(f.Request, f.Shared, f.IssuerEpoch, f.RunnerPodName, f.RunnerRestartCount, uint64(f.Request.TargetVMMemoryBytes)) != nil ||
		err != nil || len(digest) != 32 || hex.EncodeToString(digest) != c.FinalizationSHA256 || a.Validate() != nil || !a.Open || a.Instance != c.Request.Key.Instance || a.BudgetCommitmentID != c.Request.BudgetCommitmentID || a.ExpectedCapacityGeneration != f.Shared.Observed.CapacityGeneration || dispatchEpoch < c.Request.Key.IssuerEpoch || a.IssuerEpoch < c.Request.Key.IssuerEpoch || a.IssuerEpoch > dispatchEpoch || (c.MaySend && a.IssuerEpoch != dispatchEpoch) {
		return fmt.Errorf("committed abort changed original complete budget or admission binding")
	}
	return nil
}

// CommittedAbortReceipt保存恢复前后认证来源的原审计。它不替代API认证；生产
// 执行器须重读原Root/Host/Outside/Runner，并以VM UID/RV CAS记录。Query明确
// 区分原setter回复与只读恢复；接管只能查询，不能按新owner重发旧Cleanup。
type CommittedAbortReceipt struct {
	ProtocolVersion uint32                     `json:"protocol_version"`
	Command         CommittedAbortCommand      `json:"command"`
	Query           bool                       `json:"query"`
	Fence           control.FenceResponse      `json:"issuer_fence"`
	Shared          control.SharedCostResponse `json:"shared"`
	Response        control.AdmissionResponse  `json:"response"`
	PGAggregate     RootAggregate              `json:"pg_aggregate"`
	ResourceEpoch   control.Uint64             `json:"resource_issuer_epoch"`
}

func (r CommittedAbortReceipt) ValidateFor(c CommittedAbortCommand, f Finalized, dispatchEpoch, maximumEpoch control.Uint64) error {
	epoch := r.ResourceEpoch
	if r.ProtocolVersion != 1 || r.Command != c || c.ValidateFor(f, dispatchEpoch) != nil || epoch < dispatchEpoch || epoch > maximumEpoch ||
		(!r.Query && (!c.MaySend || epoch != dispatchEpoch)) || r.Fence.ValidateFor(control.FenceRequest{ProtocolVersion: 1, Instance: c.Request.Key.Instance, IssuerEpoch: epoch}, c.Request.Key.Instance) != nil || r.Fence.Disposition != control.Fenced ||
		r.Shared.Observed.CapacityGeneration < r.Fence.Observed.CapacityGeneration || r.Response.ValidateFor(c.Admission, c.Request.Key.Instance, r.Query) != nil || !r.Response.Confirms() || r.Response.Observed.IssuerFloorEpoch != epoch || !r.Response.Observed.AdmissionOpen || !r.Shared.Observed.AdmissionOpen || r.Shared.Observed.AdmissionRevision != r.Response.Observed.AdmissionRevision || r.PGAggregate.Validate() != nil || f.Aggregate == nil || r.PGAggregate.Bound != *f.Aggregate || r.PGAggregate.Identity.VMUID != c.Request.Key.Instance.VMUID || r.PGAggregate.Identity.ComputeInstanceID != c.Request.Key.Instance.ComputeInstanceID {
		return fmt.Errorf("committed abort lacks original authenticated final budget/admission outcome")
	}
	if f.ValidateFor(c.Request, r.Shared, epoch, f.RunnerPodName, f.RunnerRestartCount, uint64(c.Request.TargetVMMemoryBytes)) != nil {
		return fmt.Errorf("committed abort current PG cost exceeds original complete budget")
	}
	returned := r.Shared
	returned.Observed = r.Response.Observed
	total := uint64(returned.StartupMainUpperBytes)
	for _, array := range returned.Observed.Arrays {
		if uint64(array.CommittedUpperBytes) > math.MaxUint64-total {
			return fmt.Errorf("committed abort returned native allocation overflows")
		}
		total += uint64(array.CommittedUpperBytes)
	}
	returned.CurrentMainAndArraysUpperBytes = control.Uint64(total)
	if f.ValidateFor(c.Request, returned, epoch, f.RunnerPodName, f.RunnerRestartCount, uint64(c.Request.TargetVMMemoryBytes)) != nil {
		return fmt.Errorf("committed abort returned cost differs from original full budget")
	}
	return nil
}

func (c *CommittedAbortCommand) UnmarshalJSON(data []byte) error {
	type plain CommittedAbortCommand
	var decoded plain
	if err := control.UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*c = CommittedAbortCommand(decoded)
	return nil
}

func (r *CommittedAbortReceipt) UnmarshalJSON(data []byte) error {
	type plain CommittedAbortReceipt
	var decoded plain
	if err := control.UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*r = CommittedAbortReceipt(decoded)
	return nil
}
