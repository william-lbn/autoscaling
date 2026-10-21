package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmfake "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/fake"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// fake仅用于取得生产Store生成的完整日志/身份，不证明apiserver CAS；真实
// RFC6902/UID/RV/丢回复在journal HTTP测试覆盖。配置夹具只覆盖原生身份子集，
// 不冒称完整预算、可启动PG或物理回收。Controller还须自己直接读取Secret。
func readyInputs(t *testing.T) (*vmv1.VirtualMachine, *corev1.Secret) {
	t.Helper()
	vm := new(vmv1.VirtualMachine)
	vm.Name, vm.Namespace, vm.UID, vm.ResourceVersion = "vm", "testing", "vm-uid", "1"
	vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ComputeInstanceID: "compute-1", VMUID: "", ConfigSecretUID: "", Ready: false}
	clients := vmfake.NewSimpleClientset(vm)
	store, err := journal.NewStore(clients.NeonvmV1().VirtualMachines(vm.Namespace), journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation})
	require.NoError(t, err)
	snapshot, err := store.Initialize(context.Background(), "1", "bootstrap-owner")
	require.NoError(t, err)
	snapshot, err = store.ReleaseBootstrap(context.Background(), snapshot.ResourceVersion(), "config-uid")
	require.NoError(t, err)
	controller := true
	secret := new(corev1.Secret)
	secret.Name, secret.Namespace, secret.UID, secret.ResourceVersion = "vm-config", "testing", "config-uid", "1"
	secret.OwnerReferences = []metav1.OwnerReference{{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine", Name: vm.Name, UID: vm.UID, Controller: &controller, BlockOwnerDeletion: nil}}
	secret.Data = map[string][]byte{"config.json": []byte(`{"spec":{"dynamic_shared_buffers":{"protocol_version":2,"vm_uid":"vm-uid","compute_instance_id":"compute-1"}}}`)}
	return snapshot.VirtualMachine(), secret
}

func TestReadyRequiresActualOriginalSecretAndFullPristineJournal(t *testing.T) {
	vm, secret := readyInputs(t)
	require.NoError(t, CheckReady(vm, secret, true))
	for _, scenario := range []string{"no_secret", "secret_uid", "secret_owner", "secret_namespace", "secret_name", "secret_deleted", "secret_labels", "config_ciid", "config_uid", "config_version", "bad_journal", "no_journal", "hold", "no_bootstrap"} {
		t.Run(scenario, func(t *testing.T) {
			next, s := vm.DeepCopy(), secret.DeepCopy()
			switch scenario {
			case "no_secret":
				s = nil
			case "secret_uid":
				s.UID = "recreated-secret"
			case "secret_owner":
				s.OwnerReferences[0].UID = "recreated-vm"
			case "secret_namespace":
				s.Namespace = "foreign"
			case "secret_name":
				s.Name = "other-config"
			case "secret_deleted":
				now := metav1.Now()
				s.DeletionTimestamp = &now
			case "secret_labels":
				s.Labels = map[string]string{"neon-control/project-id": "foreign"}
			case "config_ciid":
				s.Data["config.json"] = []byte(`{"spec":{"dynamic_shared_buffers":{"protocol_version":2,"vm_uid":"vm-uid","compute_instance_id":"different"}}}`)
			case "config_uid":
				next.Spec.DSGABootstrap.VMUID = "different"
			case "config_version":
				s.Data["config.json"] = []byte(`{"spec":{"dynamic_shared_buffers":{"protocol_version":1,"vm_uid":"vm-uid","compute_instance_id":"compute-1"}}}`)
			case "bad_journal":
				next.Annotations[control.JournalAnnotation] = "malformed"
			case "no_journal":
				delete(next.Annotations, control.JournalAnnotation)
			case "hold":
				next.Spec.DSGABootstrap.Ready = false
				next.Spec.DSGABootstrap.VMUID = ""
				next.Spec.DSGABootstrap.ConfigSecretUID = ""
			case "no_bootstrap":
				next.Spec.DSGABootstrap = nil
			}
			require.Error(t, CheckReady(next, s, true))
		})
	}
}
