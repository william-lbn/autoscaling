package owner

import (
	"context"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// IdleServiceDiagnostic仅为当前串行Tick的进程内诊断，不进入WAL或恢复选择。
// 字段私有且访问器固定白名单，禁止把错误正文、URL、凭据或外部类型名写进日志。
type IdleServiceDiagnostic struct{ stage, kind string }

func (d IdleServiceDiagnostic) Stage() string {
	switch d.stage {
	case "resource_binding", "root_state_read", "root_limits", "root_process_read", "root_identity_floor", "root_aggregate", "retained_root_bound", "initial_budget_model", "initial_budget_read", "current_budget_read", "qualification_read", "bootstrap_encode", "qualified_root_runner", "runner_publication_read", "runner_identity_capacity", "vm_capacity", "runner_original_receipt", "runner_original_terminal", "runner_current_budget", "idle_pg_budget":
		return d.stage
	default:
		return "unclassified"
	}
}
func (d IdleServiceDiagnostic) Type() string {
	switch d.kind {
	case "validation", "context_canceled", "context_deadline", "k8s_forbidden", "k8s_not_found", "k8s_conflict", "k8s_unavailable", "resource_unknown", "runner_unknown", "resource_client", "resource_credential_unavailable", "resource_owner_unavailable", "resource_context_cancelled", "resource_transport_unconfirmed", "resource_response_unconfirmed", "resource_postmaster_birth_unconfirmed", "resource_guest_identity_changed", "unclassified":
		return d.kind
	default:
		return "unclassified"
	}
}

// 有界遍历原错误链且不调用Error；原错误按原样返回，诊断不包装或清除Unknown。
func idleServiceFailureType(err error) string {
	queue := []error{err}
	for remaining := 16; remaining > 0 && len(queue) > 0; remaining-- {
		e := queue[0]
		queue = queue[1:]
		switch e {
		case nil:
			continue
		case context.Canceled:
			return "context_canceled"
		case context.DeadlineExceeded:
			return "context_deadline"
		case resourceguard.ErrUnknown:
			return "resource_unknown"
		case vmguard.ErrUnknown, vmguard.ErrNotSent:
			return "runner_unknown"
		}
		switch typed := e.(type) {
		case *apierrors.StatusError:
			if typed == nil {
				return "unclassified"
			}
			switch typed.ErrStatus.Reason {
			case metav1.StatusReasonForbidden:
				return "k8s_forbidden"
			case metav1.StatusReasonNotFound:
				return "k8s_not_found"
			case metav1.StatusReasonConflict:
				return "k8s_conflict"
			case metav1.StatusReasonTimeout, metav1.StatusReasonServerTimeout, metav1.StatusReasonServiceUnavailable, metav1.StatusReasonTooManyRequests:
				return "k8s_unavailable"
			}
		case *resourceguard.ClientError:
			// 生产首因必须区分管理读取、凭据与原进程身份。仅允许SDK的
			// 固定码，保留原错误与Unknown，外部正文和凭据绝不进入日志。
			if typed != nil {
				switch typed.Code {
				case "credential_unavailable", "owner_unavailable", "context_cancelled", "transport_unconfirmed", "response_unconfirmed", "postmaster_birth_unconfirmed", "guest_identity_changed":
					return "resource_" + typed.Code
				}
			}
			return "resource_client"
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
	return "validation"
}
func (w *PGWorker) recordIdleServiceDiagnostic(stage string, err error) {
	if err == nil || w.idleServiceDiagnostic != nil {
		return
	}
	d := IdleServiceDiagnostic{stage: stage, kind: idleServiceFailureType(err)}
	w.idleServiceDiagnostic = &d
}

// 原关闭成功也可能返回nil；独立携带首次检查失败，不能因后继读成功抹掉同Tick原因。
// 管理分支未观察时保留到下个原周期；只在交付Tick后清除，不跨Run收养旧诊断。
func (w *PGWorker) diagnosticTick(progress pgexec.Progress, err error) Tick {
	t := Tick{Progress: progress, Err: err, IdleService: w.idleServiceDiagnostic}
	w.idleServiceDiagnostic = nil
	return t
}
