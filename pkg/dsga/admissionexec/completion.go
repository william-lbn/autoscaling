package admissionexec

import (
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// validateCompletionAudit核对已落账开放的完整原审计，不能拿通用Completed
// 或当前另一条Open替换。它只验证历史自洽；调用方随后仍须查询原准入，并重新
// 认证当前PG/Root/Outside/Host/Runner。接管允许当前floor更高，原命令和审计
// 代次保持不变；历史格式缺完整来源时拒绝自动结案，不在Load时补造事实。
func validateCompletionAudit(step journal.Step, request control.AdmissionRequest, settled budget.Finalized) error {
	var saved audit
	if step.Observation == nil || control.UnmarshalObject(step.Observation.Data, &saved) != nil || saved.ProtocolVersion != 2 || saved.ResourceEpoch == 0 || saved.ResourceEpoch < request.IssuerEpoch || saved.PGAggregate.Validate() != nil || settled.Aggregate == nil || saved.PGAggregate.Bound != *settled.Aggregate || saved.Shared.ValidateFor(request.Instance) != nil {
		return fmt.Errorf("normal completion requires complete original admission audit")
	}
	fence := control.FenceRequest{ProtocolVersion: 1, Instance: request.Instance, IssuerEpoch: saved.ResourceEpoch}
	if saved.Fence.ValidateFor(fence, request.Instance) != nil || saved.Fence.Disposition != control.Fenced || !saved.Response.Confirms() || saved.Response.ValidateFor(request, request.Instance, saved.Query) != nil || saved.Response.Observed.IssuerFloorEpoch != saved.ResourceEpoch || saved.Response.Observed.CapacityGeneration != request.ExpectedCapacityGeneration || saved.Shared.Observed.IssuerFloorEpoch != saved.ResourceEpoch || saved.Shared.Observed.CapacityGeneration != request.ExpectedCapacityGeneration || settled.ValidateFor(settled.Request, saved.Shared, saved.ResourceEpoch, settled.RunnerPodName, settled.RunnerRestartCount, uint64(settled.Request.TargetVMMemoryBytes)) != nil {
		return fmt.Errorf("normal completion original admission audit contradicts retained budget or issuer")
	}
	return nil
}
