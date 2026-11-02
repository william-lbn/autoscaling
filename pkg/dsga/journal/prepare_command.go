package journal

import (
	"encoding/json"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 只给完整 normal Prepare 合同留出双 PG 快照及物理释放事实的空间。
// 默认命令、JSON 节点/深度和整个账本边界均不扩展。
const MaxPrepareBudgetCommandBytes = 16 * 1024

func completeNormalPrepareCommand(requestHash string, step Step) bool {
	if step.Action != PrepareBudget || step.DispatchedBy == nil || step.DispatchedBy.Validate() != nil || len(step.Command) <= MaxCommandBytes || len(step.Command) > MaxPrepareBudgetCommandBytes {
		return false
	}
	if control.ValidateJSON(step.Command, MaxPrepareBudgetCommandBytes, 2048) != nil {
		return false
	}
	var c budget.PrepareCommand
	if json.Unmarshal(step.Command, &c) != nil || c.Prepared.NormalQuotaProtocolVersion != 1 || c.Prepared.NormalQuotaFence == nil || c.Validate() != nil {
		return false
	}
	hash, err := RequestDigest(c.Prepared.Request)
	return err == nil && hash == requestHash && c.Prepared.Request.Key.IssuerEpoch == step.DispatchedBy.Epoch && step.TargetID == c.Prepared.Request.Key.Instance.ComputeInstanceID
}
