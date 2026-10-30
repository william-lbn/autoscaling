package budget

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ReconciledAbortCommand在原Cleanup写前固定实际预算；Admission为nil只确认
// 当前服务已开放。非nil开放仍需独立可信服务选择，不能按取消或超时推测授权。
// 写前后复核同一预算，不能为使命令成功而重算限额、换代次或修改原请求目标。
type ReconciledAbortCommand struct {
	ProtocolVersion uint32                    `json:"protocol_version"`
	Capacity        ReconciledCapacity        `json:"capacity"`
	PreparedSHA256  string                    `json:"prepared_sha256"`
	Admission       *control.AdmissionRequest `json:"admission"`
}

func (c ReconciledAbortCommand) ValidateFor(dispatchEpoch control.Uint64) error {
	if c.ProtocolVersion != 1 || c.Capacity.ProtocolVersion == 4 || c.Capacity.Validate() != nil || c.Capacity.IssuerEpoch != dispatchEpoch || !unchangedDigest(c.PreparedSHA256) {
		return fmt.Errorf("reconciled abort lacks complete original capacity budget")
	}
	if a := c.Admission; a != nil && (a.Validate() != nil || !a.Open || a.Instance != c.Capacity.Request.Key.Instance || a.IssuerEpoch != dispatchEpoch || a.ExpectedCapacityGeneration != c.Capacity.CapacityGeneration || a.BudgetCommitmentID != c.Capacity.Request.BudgetCommitmentID) {
		return fmt.Errorf("reconciled abort opening differs from actual capacity budget")
	}
	return nil
}

// Receipt逐字绑定原写前命令，PG/Root只记录真实认证回复。VM/Host/Outside来源
// 的网络真实性由对应Reader核验；此结构不能由业务调用者自报后当作执行许可。
// Query表示本次读回，不表示原命令从未执行。原unknown只能查询或显式原重放。
type ReconciledAbortReceipt struct {
	ProtocolVersion uint32                     `json:"protocol_version"`
	Command         ReconciledAbortCommand     `json:"command"`
	Query           bool                       `json:"query"`
	Fence           control.FenceResponse      `json:"issuer_fence"`
	Shared          control.SharedCostResponse `json:"shared"`
	Response        *control.AdmissionResponse `json:"response"`
	PGAggregate     RootAggregate              `json:"pg_aggregate"`
	ResourceEpoch   control.Uint64             `json:"resource_issuer_epoch"`
}

func (r ReconciledAbortReceipt) ValidateFor(c ReconciledAbortCommand, dispatchEpoch, maximumEpoch control.Uint64) error {
	a, epoch := c.Capacity, r.ResourceEpoch
	digest, err := RootDigest(r.PGAggregate)
	if r.ProtocolVersion != 1 || !reflect.DeepEqual(r.Command, c) || c.ValidateFor(dispatchEpoch) != nil || epoch < dispatchEpoch || epoch > maximumEpoch ||
		err != nil || digest != a.RootSHA256 || r.PGAggregate.Bound != a.PGAggregate || r.PGAggregate.Identity.VMUID != a.Request.Key.Instance.VMUID || r.PGAggregate.Identity.ComputeInstanceID != a.Request.Key.Instance.ComputeInstanceID ||
		r.Fence.ValidateFor(control.FenceRequest{ProtocolVersion: 1, Instance: a.Request.Key.Instance, IssuerEpoch: epoch}, a.Request.Key.Instance) != nil || r.Fence.Disposition != control.Fenced || r.Shared.Observed.CapacityGeneration < r.Fence.Observed.CapacityGeneration ||
		a.ValidateCurrent(r.Shared, epoch, a.VMMemoryBytes) != nil || !r.Shared.Observed.AdmissionOpen {
		return fmt.Errorf("reconciled abort has no current complete actual budget and open admission")
	}
	if c.Admission == nil {
		if !r.Query || r.Response != nil {
			return fmt.Errorf("read-only reconciled abort cannot invent an admission mutation")
		}
		return nil
	}
	if r.Response == nil || !r.Query && epoch != dispatchEpoch || r.Response.ValidateFor(*c.Admission, a.Request.Key.Instance, r.Query) != nil || !r.Response.Confirms() || r.Response.Observed.IssuerFloorEpoch != epoch || !r.Response.Observed.AdmissionOpen || r.Response.Observed.AdmissionRevision != r.Shared.Observed.AdmissionRevision {
		return fmt.Errorf("reconciled original opening remains unknown or superseded")
	}
	returned := r.Shared
	returned.Observed = r.Response.Observed
	total := uint64(returned.StartupMainUpperBytes)
	for _, array := range returned.Observed.Arrays {
		if uint64(array.CommittedUpperBytes) > math.MaxUint64-total {
			return fmt.Errorf("reconciled returned PG cost overflows")
		}
		total += uint64(array.CommittedUpperBytes)
	}
	returned.CurrentMainAndArraysUpperBytes = control.Uint64(total)
	return a.ValidateCurrent(returned, epoch, a.VMMemoryBytes)
}

func capacityDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (c *ReconciledAbortCommand) UnmarshalJSON(raw []byte) error {
	type plain ReconciledAbortCommand
	var decoded plain
	if err := control.UnmarshalObject(raw, &decoded); err != nil {
		return err
	}
	*c = ReconciledAbortCommand(decoded)
	return nil
}

func (r *ReconciledAbortReceipt) UnmarshalJSON(raw []byte) error {
	type plain ReconciledAbortReceipt
	var decoded plain
	if err := control.UnmarshalObject(raw, &decoded); err != nil {
		return err
	}
	*r = ReconciledAbortReceipt(decoded)
	return nil
}
