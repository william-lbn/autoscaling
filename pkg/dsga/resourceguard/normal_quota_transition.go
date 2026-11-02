package resourceguard

import (
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"reflect"
)

// 只由同resource flock的生产工厂从真实旧Held及完整认证原Request创建；不在JSON
// 或Command中暴露。nil保持旧私有调用者的固定预算ID语义。
type normalQuotaBudgetHandoff struct {
	fromBudgetCommitmentID string
	toBudgetCommitmentID   string
	requestSHA256          string
}

func normalQuotaBudgetHandoffMatches(h *normalQuotaBudgetHandoff, from string, c Command, requestSHA string) bool {
	return h != nil && control.ValidateID(from, "old budget") == nil && h.fromBudgetCommitmentID == from && h.toBudgetCommitmentID == c.BudgetCommitmentID && physicalDigestValid(requestSHA) && h.requestSHA256 == requestSHA
}
func normalQuotaBudgetHeldMatches(r *record, c Command, h *normalQuotaBudgetHandoff) bool {
	if r == nil || r.Bootstrap == nil || h == nil {
		return false
	}
	id := r.Bootstrap.Original.BudgetCommitmentID
	if r.Held != nil {
		id = r.Held.BudgetCommitmentID
	}
	if normalQuotaBudgetHandoffMatches(h, id, c, h.requestSHA256) {
		return true
	}
	// 只允许本完整原命令在最后持久完成后持有to；未知阶段不能提前换旧Held。
	return r.Applied && r.Original != nil && r.Held != nil && reflect.DeepEqual(*r.Original, c) && reflect.DeepEqual(*r.Held, c) && id == h.toBudgetCommitmentID
}
