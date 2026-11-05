package owner

import (
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"k8s.io/klog/v2"
	"time"
)

// 固定叶只比较可信标准错误的静态literal；不输出Error、URL、token或请求正文。
// 原error不包装、不覆盖、不清Unknown；复用既有有界类型分类。
func serviceReopenErrorLeaf(err error) string {
	if err == nil {
		return "none"
	}
	if kind := idleServiceFailureType(err); kind != "validation" {
		return kind
	}
	queue := []error{err}
	for remaining := 16; remaining > 0 && len(queue) > 0; remaining-- {
		e := queue[0]
		queue = queue[1:]
		if e == nil {
			continue
		}
		if e == ErrLeadershipLost {
			return "owner_lost"
		}
		if e == errNativeAdmissionExpired {
			return "native_lease_expired"
		}
		if call, ok := e.(*compute.CallError); ok && call != nil {
			switch call.Code {
			case "invalid_admission_lease_fact":
				return "native_lease_fact_invalid"
			case "invalid_admission_lease_binding":
				return "native_lease_binding_invalid"
			default:
				return "native_call"
			}
		}
		switch fmt.Sprintf("%T", e) {
		case "*errors.errorString", "*fmt.wrapError", "*fmt.wrapErrors":
			switch e.Error() {
			case "journal snapshot is stale":
				return "journal_snapshot_stale"
			case "invalid journal snapshot":
				return "journal_snapshot_invalid"
			case "service reopen cannot retain complete bounded lifecycle evidence":
				return "lifecycle_capacity_bound"
			case "reopen requires original confirmed idle closure without another selection":
				return "reopen_selection_not_ready"
			case "reopen selector differs from latest closed command":
				return "reopen_selector_mismatch"
			case "service reopen changed original stable closure, budget or owner":
				return "reopen_model_binding"
			case "service reopen lacks exact live native lease and next admission context":
				return "native_reopen_context"
			case "service reopen predates confirmed original rearm":
				return "native_rearm_predecessor"
			case "service recovery requires original stable finalized closure":
				return "stable_service_model"
			}
		}
		switch wrapped := e.(type) {
		case interface{ Unwrap() []error }:
			for _, inner := range wrapped.Unwrap() {
				if len(queue) == 16 {
					break
				}
				queue = append(queue, inner)
			}
		case interface{ Unwrap() error }:
			queue = append(queue, wrapped.Unwrap())
		}
	}
	return "validation_unclassified"
}
func serviceReopenDiagnosticStage(stage string) string {
	switch stage {
	case "load", "selection", "finalization", "stable_model", "source_audit", "native_observe", "snapshot_check", "store_start", "send", "audit_source", "audit_renew":
		return stage
	default:
		return "unclassified"
	}
}
func recordServiceReopenCheck(stage string, started time.Time, err error) error {
	result := "confirmed"
	if err != nil {
		result = "stopped"
	}
	klog.InfoS("DSGA service reopen check", "stage", serviceReopenDiagnosticStage(stage), "elapsed_ms", time.Since(started).Milliseconds(), "result", result, "error_type", fmt.Sprintf("%T", err), "error_leaf", serviceReopenErrorLeaf(err))
	return err
}
