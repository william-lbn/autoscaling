package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/neondatabase/autoscaling/pkg/util/patch"
)

func (r *VMReconciler) stopManagedMemoryPod(ctx context.Context, vm *vmv1.VirtualMachine) error {
	if r.APIReader == nil || vm.Status.PodName == "" {
		return fmt.Errorf("managed stop requires original Pod and direct API reader")
	}
	pod := new(core.Pod)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: vm.Status.PodName}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		} // 只保留终态VM；不宣称404释放了容量。
		return err
	}
	return r.deleteManagedMemoryPod(ctx, vm, pod)
}

func (r *VMReconciler) deleteManagedMemoryPod(ctx context.Context, vm *vmv1.VirtualMachine, pod *core.Pod) error {
	owner := metav1.GetControllerOf(pod)
	if owner == nil || len(pod.OwnerReferences) != 1 || owner.UID != vm.UID || owner.Name != vm.Name || owner.Kind != "VirtualMachine" || owner.APIVersion != vmv1.SchemeGroupVersion.String() {
		return fmt.Errorf("managed stop original Pod owner changed")
	}
	if !slices.Contains(pod.Finalizers, hostguard.PodProtectionFinalizer) {
		retirement, err := hostguard.InspectRetirement(pod)
		if err != nil {
			return err
		}
		node := new(core.Node)
		if err := r.APIReader.Get(ctx, client.ObjectKey{Name: retirement.Binding.NodeName, Namespace: ""}, node); err != nil {
			return err
		}
		if err := hostguard.InspectRetiredPod(node, pod); err != nil {
			return err
		}
	}
	if pod.DeletionTimestamp != nil {
		return nil
	}
	// Delete仅触发容器正常停止；不移除owner或Host保护。Kubelet终态和
	// 同一QEMU Wait原来源由Scheduler结清，原VM不可先Cleanup/自动重生。
	err := r.Delete(ctx, pod, client.Preconditions{UID: &pod.UID, ResourceVersion: &pod.ResourceVersion})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// finalizeManagedMemory先确认整个VM已停且全部Node承诺结清，才释放本VM
// 来源保护。Scheduler先把关闭锚点写原VM，再允许Pod GC；因此即使Pod已经
// 消失，也可恢复同一原事实。源清理的UID/RV写丢回复由下轮实际LIST/GET恢复，
// 绝不为清理清除VM操作WAL、重开旧请求或收养另一VM/boot的来源。
func (r *VMReconciler) finalizeManagedMemory(ctx context.Context, vm *vmv1.VirtualMachine) error {
	if r.APIReader == nil || vm.Spec.DSGABootstrap == nil || vm.DeletionTimestamp == nil {
		return fmt.Errorf("managed finalization requires deleting original VM and direct reader")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	pods := new(core.PodList)
	if err := r.APIReader.List(ctx, pods, client.InNamespace(vm.Namespace)); err != nil {
		return err
	}
	remaining := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		owner := metav1.GetControllerOf(pod)
		if owner == nil || owner.UID != vm.UID {
			continue
		}
		if err := r.deleteManagedMemoryPod(ctx, vm, pod); err != nil {
			return err
		}
		remaining = true
	}
	if remaining {
		return fmt.Errorf("original managed Pods are stopping or awaiting Host retirement")
	}
	var closed hostguard.Retirement
	marker := vm.Annotations[hostguard.VMRetirementAnnotation]
	if marker != "" {
		if json.Unmarshal([]byte(marker), &closed) != nil || !closed.Placement.Managed || closed.Placement.Namespace != vm.Namespace || closed.Placement.VMName != vm.Name || closed.Placement.VMUID != string(vm.UID) || closed.Placement.Incarnation != vm.Spec.DSGABootstrap.Incarnation {
			return fmt.Errorf("original VM retirement anchor changed")
		}
		node := new(core.Node)
		if err := r.APIReader.Get(ctx, client.ObjectKey{Name: closed.Binding.NodeName, Namespace: ""}, node); err != nil {
			return err
		}
		if err := hostguard.InspectRetirementCompletion(node, closed); err != nil {
			return err
		}
	} else {
		// 尚未开放bootstrap且从未创建Pod的冷VM可以撤销；运行过的
		// VM缺锚点则是未知，不能仅把Ready重新置false猜补从未运行。
		binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation}
		snapshot, err := journal.Inspect(vm, binding)
		held, holdErr := vm.DSGABootstrapHeld()
		if err != nil || holdErr != nil || !held || vm.Spec.DSGABootstrap.Ready || vm.Spec.DSGABootstrap.VMUID != "" || vm.Spec.DSGABootstrap.ConfigSecretUID != "" || !vm.DSGAUndispatched() || !journal.PristineBootstrap(snapshot.State()) {
			return fmt.Errorf("original warm VM has no persistent retirement anchor")
		}
	}
	// 必须核全部实际Node，不能只看VM.Status.Node而漏掉旧调度尝试/
	// 另一Pod的持有。这里仅在删除时分页读取，不进入每秒运行中热路径。
	nodes := new(core.NodeList)
	options := []client.ListOption{client.Limit(64)}
	for {
		if err := r.APIReader.List(ctx, nodes, options...); err != nil {
			return err
		}
		for i := range nodes.Items {
			if err := hostguard.InspectVMUnheld(&nodes.Items[i], string(vm.UID)); err != nil {
				return err
			}
		}
		if nodes.Continue == "" {
			break
		}
		options = []client.ListOption{client.Limit(64), client.Continue(nodes.Continue)}
	}
	return r.collectManagedMemorySources(ctx, vm)
}

// 只处理本协议拥有、唯一原VM controller名/UID一致的CM。任意其他标签、
// Secret或别人的finalizer均不接管。此时原VM仍有正常删除finalizer，所有
// Guest/Pod已停止且容量已归还；源不再承担活动许可，清理不等同创建新来源。
func memorySourceFinalizer(cm *core.ConfigMap, vm *vmv1.VirtualMachine) (string, error) {
	owner := metav1.GetControllerOf(cm)
	if owner == nil || len(cm.OwnerReferences) != 1 || owner.UID != vm.UID || owner.Name != vm.Name || owner.Kind != "VirtualMachine" || owner.APIVersion != vmv1.SchemeGroupVersion.String() || cm.Namespace != vm.Namespace {
		return "", nil
	}
	if kind := cm.Labels[vmguard.ClaimLabel]; kind != "" {
		switch kind {
		case "binding-v1", "state-v1", "receipt-v1", "process-exit-v1":
			return vmguard.ProtectionFinalizer, nil
		default:
			return "", fmt.Errorf("unknown original Runner source version blocks finalization")
		}
	}
	if kind := cm.Labels[budgetclaims.ClaimLabel]; kind != "" {
		switch kind {
		case budgetclaims.OutsideKind, budgetclaims.HostKind, budgetclaims.QualificationKind:
			// 平台资格与Outside共用原VM保护协议。活动VM期间它仍是
			// 可信配置/当前预算的根，不能随历史淘汰；只有上面的原
			// Pod退出、VM关闭锚点和全部Node结清同时成立才能清理。
			return budgetclaims.ProtectionFinalizer, nil
		default:
			return "", fmt.Errorf("unknown original budget source version blocks finalization")
		}
	}
	return "", nil
}

func (r *VMReconciler) collectManagedMemorySources(ctx context.Context, vm *vmv1.VirtualMachine) error {
	options := []client.ListOption{client.InNamespace(vm.Namespace), client.Limit(64)}
	for {
		list := new(core.ConfigMapList)
		if err := r.APIReader.List(ctx, list, options...); err != nil {
			return err
		}
		for i := range list.Items {
			expected := &list.Items[i]
			f, err := memorySourceFinalizer(expected, vm)
			if err != nil {
				return err
			}
			if f == "" {
				continue
			}
			cm := new(core.ConfigMap)
			if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), cm); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return err
			}
			got, err := memorySourceFinalizer(cm, vm)
			if err != nil || got != f || cm.UID == "" || cm.UID != expected.UID || cm.ResourceVersion == "" {
				return fmt.Errorf("original memory source changed during collection")
			}
			if slices.Contains(cm.Finalizers, f) {
				finalizers := slices.DeleteFunc(slices.Clone(cm.Finalizers), func(v string) bool { return v == f })
				ops := patch.JSONPatch{{Op: patch.OpTest, Path: "/metadata/uid", Value: cm.UID}, {Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: cm.ResourceVersion}, {Op: patch.OpReplace, Path: "/metadata/finalizers", Value: finalizers}}
				raw, err := json.Marshal(ops)
				if err != nil {
					return err
				}
				if err := r.Patch(ctx, cm, client.RawPatch(types.JSONPatchType, raw)); err != nil {
					return err
				}
			}
			if err := r.Delete(ctx, cm, client.Preconditions{UID: &cm.UID, ResourceVersion: &cm.ResourceVersion}); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
		if list.Continue == "" {
			return nil
		}
		options = []client.ListOption{client.InNamespace(vm.Namespace), client.Limit(64), client.Continue(list.Continue)}
	}
}
