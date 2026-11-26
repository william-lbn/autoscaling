package controllers

import (
	"context"
	"fmt"
	"time"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// 既有标记按存在性判断，DSGA启动身份另为不可变事实；删改一个annotation不能进legacy。
// 此拒绝不是迁移能力：双端预算、源禁止恢复、目标授权与重绑定仍须原协议实现。
func requiresControlledDSGAMigration(vm *vmv1.VirtualMachine) bool {
	return vm != nil && (vm.Spec.DSGABootstrap != nil || control.Managed(vm.Annotations))
}

func originalMigrationVM(ctx context.Context, reader client.Reader, migration *vmv1.VirtualMachineMigration) (*vmv1.VirtualMachine, error) {
	if reader == nil || migration == nil || migration.Namespace == "" || migration.Spec.VmName == "" {
		return nil, fmt.Errorf("migration requires a direct reader and original VM selection")
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	vm := new(vmv1.VirtualMachine)
	if err := reader.Get(bounded, client.ObjectKey{Namespace: migration.Namespace, Name: migration.Spec.VmName}, vm); err != nil {
		return nil, err
	}
	if vm.UID == "" || vm.ResourceVersion == "" || vm.DeletionTimestamp != nil {
		return nil, fmt.Errorf("original migration VM identity or lifecycle unavailable")
	}
	if owner := metav1.GetControllerOf(migration); owner != nil {
		if owner.APIVersion != vmv1.SchemeGroupVersion.String() || owner.Kind != "VirtualMachine" ||
			owner.Name != vm.Name || owner.UID != vm.UID {
			return nil, fmt.Errorf("migration does not refer to its original VM UID")
		}
	}
	return vm, nil
}

func (w *VMMigrationWebhook) validateOriginalMigration(ctx context.Context, migration *vmv1.VirtualMachineMigration) error {
	vm, err := originalMigrationVM(ctx, w.APIReader, migration)
	if err != nil {
		return err
	}
	if requiresControlledDSGAMigration(vm) {
		return fmt.Errorf("DSGA migration requires the original controlled dual-Host handoff protocol")
	}
	return nil
}

// 只给原迁移对象增加解释性状态，不修改VM、源/目标Pod、QMP、owner或任何预算。
// 已有原阶段/目标/终态保留，以原UID继续恢复；不能先标Failed再让legacy清理猜测源是否可恢复。
func (r *VirtualMachineMigrationReconciler) retainControlledDSGAMigration(ctx context.Context, migration *vmv1.VirtualMachineMigration) (ctrl.Result, error) {
	const reason = "AwaitingOriginalDSGAHandoff"
	existing := meta.FindStatusCondition(migration.Status.Conditions, typeAvailableVirtualMachineMigration)
	if existing == nil || existing.Status != metav1.ConditionFalse || existing.Reason != reason ||
		existing.ObservedGeneration != migration.Generation {
		meta.SetStatusCondition(&migration.Status.Conditions, metav1.Condition{
			Type: typeAvailableVirtualMachineMigration, Status: metav1.ConditionFalse,
			ObservedGeneration: migration.Generation, Reason: reason,
			Message: "Retaining original migration state; controlled dual-Host handoff is required before any VM, Pod or QMP action"})
		if err := r.Status().Update(ctx, migration); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}
