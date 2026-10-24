// Package bootstrap核验持久创建屏障的输入，不签发预算/VM许可，也不读取私钥。
package bootstrap

import (
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// CheckReady复用唯一完整journal解码，调用方必须使用自己的可信、有限期API
// reader取得同namespace Secret。Secret UID/VM ownerref不能以同名/普通label
// 替代；配置只检查与屏障相同的原生v2身份。全部安全启动预算由生产Compute
// 原生规范校验，不在这里假造R02已完成或把身份ready当成允许物理缩容。
func CheckReady(vm *vmv1.VirtualMachine, secret *corev1.Secret, initial bool) error {
	held, err := vm.DSGABootstrapHeld()
	if err != nil || held || vm.Spec.DSGABootstrap == nil || vm.DeletionTimestamp != nil {
		return fmt.Errorf("DSGA bootstrap is not ready")
	}
	b := vm.Spec.DSGABootstrap
	snapshot, err := journal.Inspect(vm, journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: b.Incarnation})
	if err != nil || (initial && !journal.PristineBootstrap(snapshot.State())) {
		return fmt.Errorf("DSGA bootstrap journal is not the original initial state")
	}
	if secret == nil || secret.Namespace != vm.Namespace || secret.Name != vm.Name+"-config" || string(secret.UID) != b.ConfigSecretUID || secret.ResourceVersion == "" || secret.DeletionTimestamp != nil {
		return fmt.Errorf("DSGA bootstrap config Secret identity differs")
	}
	if secret.Immutable == nil || !*secret.Immutable {
		return fmt.Errorf("DSGA original config must be immutable")
	}
	ownerMatches := 0
	for _, ref := range secret.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			if ref.Kind != "VirtualMachine" || ref.APIVersion != vmv1.SchemeGroupVersion.String() || ref.Name != vm.Name || ref.UID != vm.UID {
				return fmt.Errorf("DSGA config Secret controller belongs to another VM")
			}
			ownerMatches++
		}
	}
	if ownerMatches != 1 {
		return fmt.Errorf("DSGA config Secret requires the original VM owner")
	}
	for _, label := range []string{"neon-control/project-id", "neon-control/endpoint-id"} {
		if vm.Labels[label] != secret.Labels[label] {
			return fmt.Errorf("DSGA bootstrap config ownership differs")
		}
	}
	var config struct {
		Spec struct {
			Dynamic *struct {
				ProtocolVersion   uint32 `json:"protocol_version"`
				VMUID             string `json:"vm_uid"`
				ComputeInstanceID string `json:"compute_instance_id"`
			} `json:"dynamic_shared_buffers"`
		} `json:"spec"`
	}
	raw := secret.Data["config.json"]
	if len(raw) == 0 || len(raw) > 1<<20 || json.Unmarshal(raw, &config) != nil || config.Spec.Dynamic == nil || config.Spec.Dynamic.ProtocolVersion != 2 || config.Spec.Dynamic.VMUID != string(vm.UID) || config.Spec.Dynamic.ComputeInstanceID != b.ComputeInstanceID {
		return fmt.Errorf("DSGA bootstrap native config identity differs")
	}
	return nil
}
