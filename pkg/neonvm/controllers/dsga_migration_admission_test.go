package controllers

import (
	"context"
	"errors"
	"reflect"
	"testing"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type unavailableMigrationReader struct{ client.Reader }

func (unavailableMigrationReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("original VM read unknown")
}

func TestDSGAMigrationAdmissionRejectsImmutableAndResidualManagedIdentity(t *testing.T) {
	for _, mode := range []string{"bootstrap", "mode", "journal", "broken-journal", "unknown-read", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			params := newMigrationTestParams(t)
			vm := defaultVm()
			vm.Status.Phase = vmv1.VmRunning
			vm.Annotations = map[string]string{}
			switch mode {
			case "bootstrap":
				vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{}
			case "mode":
				vm.Annotations[control.ModeAnnotation] = "1:broken"
			case "journal":
				vm.Annotations[control.JournalAnnotation] = "retained"
			case "broken-journal":
				vm.Annotations[control.JournalAnnotation] = ""
			}
			params.createVM(vm)
			migration := &vmv1.VirtualMachineMigration{ObjectMeta: metav1.ObjectMeta{Name: "request", Namespace: vm.Namespace},
				Spec: vmv1.VirtualMachineMigrationSpec{VmName: vm.Name}}
			hook := VMMigrationWebhook{APIReader: params.client, Config: params.r.Config}
			if mode == "unknown-read" {
				hook.APIReader = unavailableMigrationReader{params.client}
			}
			_, err := hook.ValidateCreate(params.ctx, migration)
			require.Equal(t, mode == "legacy", err == nil)
			if mode != "legacy" {
				return
			}
			replacement := migration.DeepCopy()
			_, err = hook.ValidateUpdate(params.ctx, migration, replacement)
			require.NoError(t, err)
		})
	}
}

func TestDSGAMigrationControllerRetainsOriginalBeforeAnyLegacySideEffects(t *testing.T) {
	for _, mode := range []string{"new", "active", "deleting", "replacement-vm", "unknown-read"} {
		t.Run(mode, func(t *testing.T) {
			params := newMigrationTestParams(t)
			vm := defaultVm()
			vm.Status.Phase = vmv1.VmRunning
			vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{}
			params.createVM(vm)
			migration := &vmv1.VirtualMachineMigration{ObjectMeta: metav1.ObjectMeta{Name: "original", Namespace: vm.Namespace},
				Spec: vmv1.VirtualMachineMigrationSpec{VmName: vm.Name}}
			if mode == "active" || mode == "deleting" {
				migration.Finalizers = []string{virtualmachinemigrationFinalizer}
				migration.Status.Phase = vmv1.VmmRunning
				migration.Status.SourcePodName = "original-source"
				migration.Status.TargetPodName = "original-target"
			}
			if mode == "deleting" {
				now := metav1.Now()
				migration.DeletionTimestamp = &now
			}
			if mode == "replacement-vm" {
				owner := true
				migration.OwnerReferences = []metav1.OwnerReference{{APIVersion: vmv1.SchemeGroupVersion.String(),
					Kind: "VirtualMachine", Name: vm.Name, UID: "older-vm", Controller: &owner}}
			}
			params.createMigration(migration)
			source := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "original-source", Namespace: vm.Namespace, UID: "source-uid"}}
			target := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "original-target", Namespace: vm.Namespace, UID: "target-uid"}}
			require.NoError(t, params.client.Create(params.ctx, source))
			require.NoError(t, params.client.Create(params.ctx, target))
			originalVM := vm.DeepCopy()
			oldMigration := migration.DeepCopy()
			if mode == "unknown-read" {
				params.r.APIReader = unavailableMigrationReader{params.client}
			}
			_, err := params.r.Reconcile(params.ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(migration)})
			if mode == "replacement-vm" || mode == "unknown-read" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			params.refetchVM(vm)
			require.True(t, reflect.DeepEqual(originalVM, vm), "migration rejection changed original VM")
			params.refetchMigration(migration)
			require.Equal(t, oldMigration.Finalizers, migration.Finalizers)
			require.Equal(t, oldMigration.OwnerReferences, migration.OwnerReferences)
			require.Equal(t, oldMigration.Status.Phase, migration.Status.Phase)
			require.Equal(t, oldMigration.Status.SourcePodName, migration.Status.SourcePodName)
			require.Equal(t, oldMigration.Status.TargetPodName, migration.Status.TargetPodName)
			for _, before := range []*core.Pod{source, target} {
				after := new(core.Pod)
				require.NoError(t, params.client.Get(params.ctx, client.ObjectKeyFromObject(before), after))
				require.Equal(t, before, after, "migration rejection mutated or deleted original Pod")
			}
			if err == nil {
				condition := meta.FindStatusCondition(migration.Status.Conditions, typeAvailableVirtualMachineMigration)
				require.NotNil(t, condition)
				require.Equal(t, "AwaitingOriginalDSGAHandoff", condition.Reason)
			}
		})
	}
}
