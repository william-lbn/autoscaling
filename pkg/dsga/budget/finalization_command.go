package budget

import (
	"encoding/hex"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// FinalizationCommand是FinalizeBudget写前日志的完整原命令。与执行器共享这个
// 类型，使账本能够验证原命令与结算回执完全相同，不能将任意JSON标为Completed
// 后永久保存为当前预算。字段和旧命令wire完全相同；它不认证外部持有来源。
// 三个摘要仅绑定已认证前驱的原body，不代替Host/Outside/Root的活性与权限检查。
type FinalizationCommand struct {
	ProtocolVersion uint32    `json:"protocol_version"`
	Finalized       Finalized `json:"finalized"`
	PreparedSHA256  string    `json:"prepared_sha256"`
	GuestSHA256     string    `json:"guest_sha256"`
	VMSHA256        string    `json:"vm_sha256"`
}

func (c FinalizationCommand) Validate() error {
	f := c.Finalized
	if c.ProtocolVersion != 1 || f.Sources == nil || f.Sources.Validate() != nil || f.Aggregate == nil || f.Aggregate.Kind != HardLimit {
		return fmt.Errorf("finalization requires original aggregate and persistent sources")
	}
	for _, value := range []string{c.PreparedSHA256, c.GuestSHA256, c.VMSHA256} {
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != value {
			return fmt.Errorf("invalid finalization predecessor digest")
		}
	}
	if c.VMSHA256 != f.Sources.VMReceiptSHA256 {
		return fmt.Errorf("finalization VM source differs from original predecessor")
	}
	// 此处是持久结构自洽性检查：当前PG/VM/资源仍须由执行器独立只读核验。
	// 原issuer及原Shared保持不变，接管不能把旧事实改写成新的owner代次。
	return f.ValidateFor(f.Request, f.Shared, f.IssuerEpoch, f.RunnerPodName, f.RunnerRestartCount, uint64(f.Request.TargetVMMemoryBytes))
}

func (c *FinalizationCommand) UnmarshalJSON(b []byte) error {
	type plain FinalizationCommand
	var p plain
	if err := control.UnmarshalObject(b, &p); err != nil {
		return err
	}
	*c = FinalizationCommand(p)
	return nil
}
