package vmguard

import "fmt"

// OriginalIdentity 只返回本 Runner 已冻结 Birth 的值副本。
// 它不是预算/执行许可，不能用于重新出生、重绑定或跳过原状态/来源核验。
func (r *Runner) OriginalIdentity() (Identity, error) {
	if r == nil || r.birth.Identity.Validate() != nil {
		return Identity{}, fmt.Errorf("original Runner birth identity unavailable")
	}
	return r.birth.Identity, nil
}
