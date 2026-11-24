package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/qualificationissuer"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
)

func configureQualifierReads(config *rest.Config) {
	// 完整平台快照含多次API读取，默认5QPS会扩大并发漂移窗口。
	// 仅设置有限的缺省读取配额，尊重调用者已有配置。
	if config.QPS == 0 {
		config.QPS = 50
	}
	if config.Burst == 0 {
		config.Burst = 100
	}
}

type qualificationPublish func(context.Context, qualificationissuer.Config) (qualificationissuer.Result, error)

// 自动发布最多CREATE一次。只有该次响应已返回原UID时，才以同一输入/SHA
// 和ResumeClaimUID执行SDK完整的只读确认；不按名称收养对象，不重采集/换报告。
func publishAutomatic(ctx context.Context, config qualificationissuer.Config, publish qualificationPublish) (qualificationissuer.Result, error) {
	return publishAutomaticWithInterval(ctx, config, publish, time.Second)
}

func publishAutomaticWithInterval(ctx context.Context, config qualificationissuer.Config, publish qualificationPublish, interval time.Duration) (qualificationissuer.Result, error) {
	var zero qualificationissuer.Result
	if err := config.Validate(); err != nil {
		return zero, err
	}
	wait, cancel := context.WithTimeout(ctx, time.Duration(config.TimeoutSeconds)*time.Second)
	defer cancel()
	var original budget.ObjectRef
	for {
		result, err := publish(wait, config)
		if err == nil {
			if original.UID != "" && (result.Source.Claim != original || result.ReportSHA256 != config.ReportSHA256) {
				return zero, &qualificationissuer.Unconfirmed{Claim: original, Err: fmt.Errorf("qualification recovery identity changed")}
			}
			return result, nil
		}
		var unknown *qualificationissuer.Unconfirmed
		// SDK明确证明尚未CREATE的来源漂移，只重读同一报告；不换报告、不改变资格条件。
		if original.UID == "" && !errors.As(err, &unknown) && errors.Is(err, qualificationissuer.ErrSourcesChangedBeforePublication) {
			select {
			case <-wait.Done():
				return zero, errors.Join(err, wait.Err())
			case <-time.After(interval):
				continue
			}
		}
		if original.UID == "" {
			if !errors.As(err, &unknown) || unknown.Claim.UID == "" || unknown.Claim.Name != "dsga-q-"+config.ReportSHA256[:56] {
				return zero, err
			}
			original = unknown.Claim
			config.ResumeClaimUID = original.UID
		} else if errors.As(err, &unknown) && unknown.Claim != original {
			return zero, &qualificationissuer.Unconfirmed{Claim: original, Err: fmt.Errorf("qualification recovery identity changed")}
		}
		select {
		case <-wait.Done():
			return zero, &qualificationissuer.Unconfirmed{Claim: original, Err: errors.Join(err, wait.Err())}
		case <-time.After(interval):
		}
	}
}

// 只输出固定诊断码；不把Kube/Root错误正文、URL、报告或令牌拼入日志。
func publicationDiagnostic(err error) string {
	if apierrors.IsForbidden(err) {
		return "api_forbidden"
	}
	if apierrors.IsUnauthorized(err) {
		return "api_unauthorized"
	}
	for message, code := range map[string]string{
		"qualification requires stable original idle resource binding":              "idle_binding_unconfirmed",
		"original VM owner Lease unconfirmed":                                       "vm_lease_unconfirmed",
		"original Host Lease unconfirmed":                                           "host_lease_unconfirmed",
		"original Host placement or profile unconfirmed":                            "host_placement_unconfirmed",
		"original complete Guest artifact unavailable during qualification capture": "guest_artifact_unavailable",
		"original complete Guest artifact changed during qualification capture":     "guest_artifact_changed",
		"Root identity, original birth or current owner changed":                    "root_owner_changed",
		"live Host Pod lacks original full placement":                               "host_pod_placement_unconfirmed",
		"ordinary Host Pod requires actual positive hard memory limits":             "host_pod_limit_unconfirmed",
		"original platform sources changed after publication":                       "source_changed_after",
		"original platform sources changed before publication":                      "source_changed_before",
		"original qualification consumer readback unconfirmed":                      "consumer_readback",
		"original protected qualification object changed":                           "protected_object_changed",
		"qualification VM controller changed":                                       "vm_owner_changed",
		"qualification provenance changed":                                          "provenance_changed",
		"qualification recovery identity changed":                                   "recovery_identity_changed",
	} {
		if strings.Contains(err.Error(), message) {
			return code
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	return "unconfirmed"
}
