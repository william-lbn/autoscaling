package v1

import (
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func TestDSGAIncarnationAndJournalCannotDisappearOrChange(t *testing.T) {
	var old VirtualMachine
	old.UID = "vm-uid"
	old.Annotations = map[string]string{control.ModeAnnotation: "1:1234567890abcdef1234567890abcdef", control.JournalAnnotation: "ledger"}
	for _, annotations := range []map[string]string{
		nil,
		{control.ModeAnnotation: old.Annotations[control.ModeAnnotation]},
		{control.JournalAnnotation: "ledger"},
		{control.ModeAnnotation: "1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", control.JournalAnnotation: "ledger"},
		{control.ModeAnnotation: old.Annotations[control.ModeAnnotation], control.JournalAnnotation: ""},
	} {
		next := old.DeepCopy()
		next.Annotations = annotations
		require.Error(t, next.ValidateDSGAUpdate(&old))
	}
	next := old.DeepCopy()
	next.Annotations[control.JournalAnnotation] = "updated-ledger"
	require.NoError(t, next.ValidateDSGAUpdate(&old))
}

func TestDSGAInitializesOnlyAfterUIDAndBeforeRunner(t *testing.T) {
	var old VirtualMachine
	old.UID = "vm-uid"
	next := old.DeepCopy()
	next.Annotations = map[string]string{control.ModeAnnotation: "1:1234567890abcdef1234567890abcdef", control.JournalAnnotation: "ledger"}
	require.NoError(t, next.ValidateDSGAUpdate(&old))
	for _, change := range []func(*VirtualMachine){
		func(vm *VirtualMachine) { vm.UID = "" }, func(vm *VirtualMachine) { vm.Status.Phase = VmRunning },
		func(vm *VirtualMachine) { vm.Status.PodName = "runner" }, func(vm *VirtualMachine) { vm.Status.RestartCount = 1 },
	} {
		before := old.DeepCopy()
		change(before)
		require.Error(t, next.ValidateDSGAUpdate(before))
	}
	_, err := next.ValidateCreate()
	require.ErrorContains(t, err, "UID is assigned")
}
