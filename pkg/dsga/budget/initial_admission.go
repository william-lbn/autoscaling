package budget

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// Digest固定完整启动预算，不能借另一实例/来源的相同额度开放准入。使用
// 本合同的规范JSON，不与PG自己的jsonb context摘要混用；摘要本身不是认证。
func (i Initial) Digest() (string, error) {
	if err := i.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(i)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

// InitialAdmissionCommand只开放原启动容量，没有Resize Key/目标或Host操作。
// 可在预算持久后由当前owner首次发出；原完整命令必须先经VM UID/RV WAL确认。
// 维护已推进门禁revision时，执行器不能生成一个更高revision自动覆盖它。
type InitialAdmissionCommand struct {
	ProtocolVersion uint32                   `json:"protocol_version"`
	InitialSHA256   string                   `json:"initial_sha256"`
	Request         control.AdmissionRequest `json:"request"`
}

func (c InitialAdmissionCommand) ValidateFor(i Initial) error {
	digest, err := i.Digest()
	r := c.Request
	if err != nil || c.ProtocolVersion != 1 || c.InitialSHA256 != digest || r.Validate() != nil || !r.Open || r.Instance != i.Instance || r.IssuerEpoch < i.IssuerEpoch || r.Revision != 1 || r.ExpectedCapacityGeneration != i.Shared.Observed.CapacityGeneration || r.BudgetCommitmentID != i.PGAggregate.Bootstrap.BudgetCommitmentID {
		return fmt.Errorf("first admission command differs from complete original initial budget")
	}
	return nil
}

// InitialAdmissionReceipt保存真正原生准入结论与同点PG/root预算审计。只允许
// Applied/Replayed；NotFound、Expired、布尔Open或新floor都不能单独完成原WAL。
// 外部资格/Host/Runner来源仍须认证读者在落账前后复核，不由本结构自行签发。
type InitialAdmissionReceipt struct {
	ProtocolVersion        uint32                     `json:"protocol_version"`
	Command                InitialAdmissionCommand    `json:"command"`
	Query                  bool                       `json:"query"`
	Fence                  control.FenceResponse      `json:"issuer_fence"`
	Shared                 control.SharedCostResponse `json:"shared"`
	Response               control.AdmissionResponse  `json:"response"`
	PGAggregate            RootAggregate              `json:"pg_aggregate"`
	ResourceEpoch          control.Uint64             `json:"resource_issuer_epoch"`
	VMMemoryBytes          control.Uint64             `json:"vm_memory_bytes"`
	GuestUsableMemoryBytes control.Uint64             `json:"guest_usable_memory_bytes"`
}

func (r InitialAdmissionReceipt) ValidateFor(i Initial, c InitialAdmissionCommand, epoch control.Uint64) error {
	fence := control.FenceRequest{ProtocolVersion: 1, Instance: i.Instance, IssuerEpoch: epoch}
	if r.ProtocolVersion != 1 || r.Command != c || c.ValidateFor(i) != nil || epoch < c.Request.IssuerEpoch || r.ResourceEpoch != epoch || r.Fence.ValidateFor(fence, i.Instance) != nil || r.Fence.Disposition != control.Fenced || r.Response.ValidateFor(c.Request, i.Instance, r.Query) != nil || !r.Response.Confirms() || !reflect.DeepEqual(r.Fence.Observed, r.Shared.Observed) || !reflect.DeepEqual(r.Response.Observed, r.Shared.Observed) {
		return fmt.Errorf("first admission receipt lacks exact original native completion and current fence")
	}
	return i.MatchCurrent(r.Shared, r.PGAggregate, epoch, r.VMMemoryBytes, r.GuestUsableMemoryBytes)
}

func (c *InitialAdmissionCommand) UnmarshalJSON(raw []byte) error {
	type plain InitialAdmissionCommand
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	*c = InitialAdmissionCommand(p)
	return nil
}

func (r *InitialAdmissionReceipt) UnmarshalJSON(raw []byte) error {
	type plain InitialAdmissionReceipt
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	*r = InitialAdmissionReceipt(p)
	return nil
}
