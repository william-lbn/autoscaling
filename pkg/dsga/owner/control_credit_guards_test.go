package owner

import "testing"

// 单次有界credit仍须执行原调用取消、Owner租约和Native过期门；
// 原生产写队列保持无缓冲，纯读路由另由现有真实Submit组合验证。
func TestPGControlCreditKeepsCancelLeaseAndExpiredChecks(t *testing.T) {
	for _, mode := range []string{"cancelled", "lease-changed", "expired"} {
		t.Run(mode, func(t *testing.T) {
			runControlCreditSchedule(t, mode, 1)
		})
	}
}
