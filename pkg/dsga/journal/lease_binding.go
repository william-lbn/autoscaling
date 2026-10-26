package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	coordination "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/types"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 此组函数从owner原实现原样集中到journal层，供Agent和最终Runner共用同一
// Lease绑定规则；不提供选主、凭据或资源许可，不以Lease expiry清除任何WAL。
const LeaseBindingAnnotation = "internal.autoscaling.neon.tech/dsga-lease-binding-v1"

// LeaseName包含完整VM UID/incarnation，不使用可复用名称作唯一身份。保留原
// 224位域分离摘要；原文marker和ownerRef仍强校验，不依赖名称散列证明归属。
func LeaseName(binding Binding) string {
	value, _ := json.Marshal([]string{binding.Namespace, binding.Name, string(binding.VMUID), binding.Incarnation})
	hash := sha256.Sum256(append([]byte("neon-dsga-lease-binding-v1\x00"), value...))
	return "dsga-" + hex.EncodeToString(hash[:28])
}

func LeaseMarker(binding Binding) string {
	return "1:" + string(binding.VMUID) + ":" + binding.Incarnation
}

func ValidateLease(lease *coordination.Lease, binding Binding, name string, expectedUID types.UID) error {
	if lease == nil || lease.Name != name || lease.Namespace != binding.Namespace || lease.UID == "" || lease.ResourceVersion == "" ||
		lease.DeletionTimestamp != nil || (expectedUID != "" && lease.UID != expectedUID) ||
		lease.Annotations[LeaseBindingAnnotation] != LeaseMarker(binding) || len(lease.OwnerReferences) != 1 {
		return fmt.Errorf("DSGA lease VM binding or object identity differs")
	}
	ref := lease.OwnerReferences[0]
	if ref.UID != binding.VMUID || ref.Name != binding.Name || ref.Kind != "VirtualMachine" ||
		ref.APIVersion != vmv1.SchemeGroupVersion.String() || ref.Controller == nil || !*ref.Controller {
		return fmt.Errorf("DSGA lease belongs to another VM")
	}
	spec := lease.Spec
	if spec.HolderIdentity == nil || control.ValidateID(*spec.HolderIdentity, "lease holder") != nil ||
		spec.LeaseDurationSeconds == nil || *spec.LeaseDurationSeconds < 1 || *spec.LeaseDurationSeconds > 86400 ||
		spec.LeaseTransitions == nil || *spec.LeaseTransitions < 0 || spec.AcquireTime == nil || spec.RenewTime == nil ||
		spec.PreferredHolder != nil || spec.Strategy != nil {
		return fmt.Errorf("DSGA lease record is malformed or uses an unsupported election mode")
	}
	return nil
}
