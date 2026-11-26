package budgetclaims

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// AllocationTransition 新鲜认证完整原文及 Base，并要求有方向的原资格边。
// 返回值不跨调用授权；每个实际动作及最终结算仍重新认证来源和领域事实。
func (r *Reader) AllocationTransition(ctx context.Context, source budget.QualificationSource,
	from, to budget.AllocationSelector) (budget.ExactAllocationTransition, error) {
	if source.Validate() != nil || from.Validate() != nil || to.Validate() != nil {
		return budget.ExactAllocationTransition{}, fmt.Errorf("invalid allocation transition selection")
	}
	cm, err := r.claim(ctx, source.Claim, AllocationQualificationKind)
	if err != nil {
		return budget.ExactAllocationTransition{}, err
	}
	raw := []byte(cm.Data[ClaimKey])
	digest := sha256.Sum256(raw)
	var q budget.AllocationQualification
	if hex.EncodeToString(digest[:]) != source.SHA256 || json.Unmarshal(raw, &q) != nil ||
		q.VMUID != string(r.binding.VMUID) || q.Incarnation != r.binding.Incarnation {
		return budget.ExactAllocationTransition{}, fmt.Errorf("allocation transition source changed")
	}
	base, err := r.readLegacyQualification(ctx, q.Base)
	if err != nil {
		return budget.ExactAllocationTransition{}, err
	}
	transition, err := q.Transition(source, from, to, base)
	if err != nil {
		return budget.ExactAllocationTransition{}, err
	}
	// 沿正式只读链再次核两端 UID/SHA/RV/父来源，不缓存原文认证为动作许可。
	for _, selector := range []budget.AllocationSelector{from, to} {
		_, err := r.ReadQualification(ctx, budget.QualificationSelection{ProtocolVersion: 2, Source: source, Allocation: &selector})
		if err != nil {
			return budget.ExactAllocationTransition{}, err
		}
	}
	return transition, nil
}

// 从已固定资格原文计算完整selector，不允许管理请求自行提供向量数字。
func (r *Reader) AllocationIDs(ctx context.Context, source budget.QualificationSource, fromID, toID string) (budget.ExactAllocationTransition, error) {
	if source.Validate() != nil {
		return budget.ExactAllocationTransition{}, fmt.Errorf("invalid original allocation source")
	}
	cm, err := r.claim(ctx, source.Claim, AllocationQualificationKind)
	if err != nil {
		return budget.ExactAllocationTransition{}, err
	}
	raw := []byte(cm.Data[ClaimKey])
	digest := sha256.Sum256(raw)
	var q budget.AllocationQualification
	if hex.EncodeToString(digest[:]) != source.SHA256 || json.Unmarshal(raw, &q) != nil || q.VMUID != string(r.binding.VMUID) || q.Incarnation != r.binding.Incarnation {
		return budget.ExactAllocationTransition{}, fmt.Errorf("allocation ID source changed")
	}
	var from, to budget.AllocationSelector
	for _, value := range q.Targets {
		if value.ID == fromID {
			from, err = value.Selector()
			if err != nil {
				return budget.ExactAllocationTransition{}, err
			}
		}
		if value.ID == toID {
			to, err = value.Selector()
			if err != nil {
				return budget.ExactAllocationTransition{}, err
			}
		}
	}
	return r.AllocationTransition(ctx, source, from, to)
}
