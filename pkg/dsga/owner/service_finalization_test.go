package owner

import "testing"

// 正式首次预算、完整Grow资源前驱、原Finalize未派发或写前未知、来源异常关闭，
// 再经Run管理恢复独立结算和准入。协议peer边界与既有生产组合一致。
func TestProductionServiceFinalizationRecovery(t *testing.T) {
	for _, mode := range []string{"planned", "unknown", "planned-takeover", "unknown-takeover", "lost-complete", "complete-takeover"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "finalize-safety-recover-"+mode) })
	}
}

func TestProductionServiceFinalizationRecoveryThenAutomaticShrink(t *testing.T) {
	productionInitialService(t, "finalize-safety-recover-automatic")
}

func TestProductionServiceFinalizationRecoveryCrashWindows(t *testing.T) {
	for _, mode := range []string{"lost-proof", "setter-before", "setter-after", "lost-record", "source-after-selection", "source-after-unknown-setter", "terminal-unavailable", "terminal-conflict"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "finalize-safety-recover-"+mode) })
	}
}

func TestProductionServiceFinalizationRecoveryExpiredLeaseAfterElectorTakeover(t *testing.T) {
	productionInitialService(t, "finalize-safety-recover-expired")
}
