package budgetclaims

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 认证来源而不选择/执行目标；最终计划仍须按原from/to重新认证完整向量。
func (r *Reader) AuthenticateAllocationSource(ctx context.Context, source, expectedBase budget.QualificationSource) error {
	if source.Validate() != nil || expectedBase.Validate() != nil {
		return fmt.Errorf("invalid cache source reference")
	}
	cm, err := r.claim(ctx, source.Claim, AllocationQualificationKind)
	if err != nil {
		return err
	}
	var q budget.AllocationQualification
	if json.Unmarshal([]byte(cm.Data[ClaimKey]), &q) != nil || q.Base != expectedBase || len(q.Transitions) == 0 {
		return fmt.Errorf("cache source does not retain original base")
	}
	edge := q.Transitions[0]
	actual, err := r.AllocationIDs(ctx, source, edge.From, edge.To)
	if err != nil {
		return err
	}
	if actual.Base != expectedBase || actual.Source != source {
		return fmt.Errorf("cache source changed original base")
	}
	return nil
}
