//go:build linux

package resourceguard

import (
	"reflect"
)

// normalQuotaReplayModeLocked只选择显式恢复的内部动作，不构成cap授权。
// 调用者持原resource锁，exists/count必须来自本次同一锁内原信封/四阶段WAL读取；
// 后继仍须完整PG/数组/预算工厂与handoff原资格，且耐久信封先于任何cap效应。
func normalQuotaReplayModeLocked(r *record, c Command, exists bool, count int) (string, error) {
	if r == nil || count < 0 || count > 4 {
		return "", ErrUnknown
	}
	if exists {
		// 同一信封已逐字匹配；保留既有entry-only/partial/complete路径。
		if count == 0 && (r.Original == nil || !reflect.DeepEqual(*r.Original, c)) {
			return "apply", nil
		}
		return "replay", nil
	}
	// 仅Root亲自证实尚无本次持久记录，才能交付原控制器已落账的同epoch命令。
	// 缺信封但已有quota/Original不算未开始，不能通过补元数据重新签发。
	if count != 0 || c.Validate() != nil || r.validate(c.Identity) != nil ||
		r.Floor != c.IssuerEpoch || r.Identity != c.Identity ||
		r.Bootstrap == nil || !r.Bootstrap.Applied || r.Birth == nil || !r.Birth.matches(c) ||
		r.Original != nil && (!r.Applied || r.Held == nil || !reflect.DeepEqual(*r.Held, *r.Original)) {
		return "", ErrUnknown
	}
	next, err := r.nextRevision()
	if err != nil || c.Revision != next {
		return "", ErrUnknown
	}
	return "apply", nil
}
