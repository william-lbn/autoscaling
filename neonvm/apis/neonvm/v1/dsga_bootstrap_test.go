package v1

import (
	"testing"

	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func heldBootstrapVM() *VirtualMachine {
	vm := new(VirtualMachine)
	vm.Name, vm.Namespace, vm.UID, vm.ResourceVersion = "vm", "testing", "vm-uid", "rv-1"
	vm.Spec.DSGABootstrap = &DSGABootstrap{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ComputeInstanceID: "compute-1", VMUID: "", ConfigSecretUID: "", Ready: false}
	return vm
}

func TestDSGABootstrapIsOneShotAndCannotBeAdoptedOnline(t *testing.T) {
	old := heldBootstrapVM()
	legacy := old.DeepCopy()
	legacy.Spec.DSGABootstrap = nil
	require.Error(t, old.ValidateDSGAUpdate(legacy), "existing legacy VM cannot acquire a hold")
	ready := old.DeepCopy()
	ready.Annotations = map[string]string{control.ModeAnnotation: "1:" + old.Spec.DSGABootstrap.Incarnation, control.JournalAnnotation: "shape-only-full-codec-tested-in-journal"}
	ready.Spec.DSGABootstrap.Ready = true
	ready.Spec.DSGABootstrap.VMUID = string(old.UID)
	ready.Spec.DSGABootstrap.ConfigSecretUID = "config-uid"
	require.NoError(t, ready.ValidateDSGAUpdate(old))
	held, err := ready.DSGABootstrapHeld()
	require.NoError(t, err)
	require.False(t, held)
	for _, change := range []func(*VirtualMachine){
		func(vm *VirtualMachine) { vm.Spec.DSGABootstrap = nil },
		func(vm *VirtualMachine) { vm.Spec.DSGABootstrap.Ready = false },
		func(vm *VirtualMachine) { vm.Spec.DSGABootstrap.VMUID = "replacement" },
		func(vm *VirtualMachine) { vm.Spec.DSGABootstrap.ConfigSecretUID = "other-config" },
		func(vm *VirtualMachine) { vm.Spec.DSGABootstrap.Incarnation = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
		func(vm *VirtualMachine) { vm.Spec.DSGABootstrap.ComputeInstanceID = "other-compute" },
		func(vm *VirtualMachine) { vm.Spec.DSGABootstrap.ProtocolVersion++ },
	} {
		next := ready.DeepCopy()
		change(next)
		require.Error(t, next.ValidateDSGAUpdate(ready))
	}
	_, err = ready.ValidateCreate()
	require.Error(t, err, "ready may not be injected on CREATE")
}

func TestDSGABootstrapRefusesEveryDispatchHistoryAtRelease(t *testing.T) {
	old := heldBootstrapVM()
	for _, change := range []func(*VirtualMachine){
		func(vm *VirtualMachine) { vm.Status.Node = "node" },
		func(vm *VirtualMachine) { vm.Status.PodName = "runner" },
		func(vm *VirtualMachine) { vm.Status.RestartCount = 1 },
		func(vm *VirtualMachine) { vm.Status.SSHSecretName = "ssh" },
		func(vm *VirtualMachine) { vm.Status.TLSSecretName = "tls" },
		func(vm *VirtualMachine) { vm.Status.ExtraNetIP = "10.0.0.1" },
		func(vm *VirtualMachine) { vm.Status.CurrentRevision = new(RevisionWithTime) },
		func(vm *VirtualMachine) { vm.Status.Phase = VmRunning },
	} {
		before := old.DeepCopy()
		change(before)
		next := before.DeepCopy()
		next.Spec.DSGABootstrap.Ready = true
		next.Spec.DSGABootstrap.VMUID = string(next.UID)
		next.Spec.DSGABootstrap.ConfigSecretUID = "config-uid"
		next.Annotations = map[string]string{control.ModeAnnotation: "1:" + old.Spec.DSGABootstrap.Incarnation, control.JournalAnnotation: "shape-only"}
		require.Error(t, next.ValidateDSGAUpdate(before))
		held, err := before.DSGABootstrapHeld()
		require.True(t, held)
		require.Error(t, err)
	}
}

func TestDSGACorruptHeldVMCanOnlyCleanMetadataDuringDeletion(t *testing.T) {
	old := heldBootstrapVM()
	old.Spec.DSGABootstrap.ProtocolVersion = 99
	now := metav1.Now()
	old.DeletionTimestamp = &now
	old.Finalizers = []string{"controller-finalizer"}
	next := old.DeepCopy()
	next.Finalizers = nil
	require.NoError(t, next.ValidateDSGAUpdate(old), "must not strand an undispatched corrupt held object")
	next.Spec.DSGABootstrap.Ready = true
	require.Error(t, next.ValidateDSGAUpdate(old), "deletion cleanup must not release bootstrap")
}

func TestDSGABootstrapDeepCopyHasIndependentIdentity(t *testing.T) {
	old := heldBootstrapVM()
	next := old.DeepCopy()
	next.Spec.DSGABootstrap.ComputeInstanceID = "changed"
	require.Equal(t, "compute-1", old.Spec.DSGABootstrap.ComputeInstanceID)
}
