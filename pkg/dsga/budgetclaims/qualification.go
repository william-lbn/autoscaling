package budgetclaims

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

const QualificationKind = "platform-qualification-v1"

// Qualification只从构造器固定的集群/namespace读取可信配置选定的原UID。
// 资格对象与Outside一样须immutable、同VM且有保护finalizer；签发者必须另有
// 受限权限。本读者不生成认证、不从Node标签或当前RSS猜测平台余量。
func (r *Reader) Qualification(ctx context.Context, source budget.QualificationSource) (budget.PlatformQualification, error) {
	var q budget.PlatformQualification
	if err := source.Validate(); err != nil {
		return q, err
	}
	cm, err := r.claim(ctx, source.Claim, QualificationKind)
	if err != nil {
		return q, err
	}
	digest := sha256.Sum256([]byte(cm.Data[ClaimKey]))
	if hex.EncodeToString(digest[:]) != source.SHA256 || json.Unmarshal([]byte(cm.Data[ClaimKey]), &q) != nil || q.Identity.VMUID != string(r.binding.VMUID) || q.Incarnation != r.binding.Incarnation {
		return q, fmt.Errorf("original platform qualification content or VM identity changed")
	}
	return q, nil
}

// 每次消费v2 Outside都传递复核其原资格，而非仅在Prepare时认证一次。这样
// Finalize、长期预算、取消重整和Host分配均不能绕过原资格的寿命。历史v1保持
// 原严格行为；它不能被新生产者生成，也不能加字段伪装升级。
func (r *Reader) checkOutsideQualification(ctx context.Context, out budget.OutsideReservation) error {
	if err := out.ValidateSource(); err != nil {
		return err
	}
	if out.Qualification == nil {
		return nil
	}
	q, err := r.Qualification(ctx, *out.Qualification)
	if err != nil {
		return err
	}
	if err := q.MatchOutside(out); err != nil {
		return err
	}
	pod, err := r.core.Pods(r.binding.Namespace).Get(ctx, q.RunnerPodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if pod == nil {
		return fmt.Errorf("qualified original Runner Pod unavailable")
	}
	owner := metav1.GetControllerOf(pod)
	if pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || string(pod.UID) != q.RunnerPodUID || pod.Spec.NodeName != q.NodeName || pod.Status.Phase != core.PodRunning || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != r.binding.Name || owner.UID != r.binding.VMUID {
		return fmt.Errorf("qualified original live Runner Pod changed")
	}
	node, err := r.core.Nodes().Get(ctx, q.NodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if node == nil || node.ResourceVersion == "" || node.DeletionTimestamp != nil || string(node.UID) != q.NodeUID {
		return fmt.Errorf("qualified original Host Node changed")
	}
	return nil
}

// ReadOutside给Prepare生产者查询完整原对象；缺失返回API原错误，不将404改成
// NoEffect。初次UID须由成功Create/原受保护对象读回获得，落账后不能按同名收养。
func (r *Reader) ReadOutside(ctx context.Context, ref budget.ObjectRef) (budget.OutsideReservation, error) {
	var out budget.OutsideReservation
	cm, err := r.claim(ctx, ref, OutsideKind)
	if err != nil {
		return out, err
	}
	if json.Unmarshal([]byte(cm.Data[ClaimKey]), &out) != nil {
		return out, fmt.Errorf("original outside reservation is malformed")
	}
	return out, r.checkOutsideQualification(ctx, out)
}
