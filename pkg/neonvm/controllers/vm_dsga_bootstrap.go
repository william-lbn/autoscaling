package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootstrap"
)

// freshBootstrapVM封闭初始cache读取后同名重建/ready变化的常见竞态。只对
// 初始或DSGA生命周期增加直接GET；nil普通Running VM仍由官方旧流程处理。
// 读取后仍可能变更，故最终Runner/QMP/预算域的UID与单调fence仍不能省略。
func (r *VMReconciler) freshBootstrapVM(ctx context.Context, vm *vmv1.VirtualMachine) error {
	if vm.Spec.DSGABootstrap == nil && !control.Managed(vm.Annotations) && vm.Status.Phase != "" && vm.Status.Phase != vmv1.VmPending {
		return nil
	}
	if r.APIReader == nil {
		return fmt.Errorf("DSGA/initial VM requires a direct API reader")
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	fresh := new(vmv1.VirtualMachine)
	if err := r.APIReader.Get(readCtx, client.ObjectKeyFromObject(vm), fresh); err != nil {
		return err
	}
	if fresh.UID != vm.UID || fresh.Name != vm.Name || fresh.Namespace != vm.Namespace {
		return fmt.Errorf("VM bootstrap identity changed after cached read")
	}
	*vm = *fresh
	return nil
}

// resourceBootstrapRuntime在Pod创建前直接重读原VM和immutable Secret，避免用
// cached ready/同名对象生成特权Guest输入。不跨Kube等待持锁，任意漂移拒绝重试
// 同一旧快照；没有resource.json的历史配置不能在此补写或用当前env兜底。
func (r *VMReconciler) resourceBootstrapRuntime(ctx context.Context, vm *vmv1.VirtualMachine) ([]byte, error) {
	if r.APIReader == nil {
		return nil, fmt.Errorf("resource startup requires direct API reader")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	fresh := new(vmv1.VirtualMachine)
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(vm), fresh); err != nil {
		return nil, err
	}
	if fresh.UID != vm.UID || fresh.DeletionTimestamp != nil || !apiequality.Semantic.DeepEqual(fresh.Spec, vm.Spec) {
		return nil, fmt.Errorf("VM changed before resource source binding")
	}
	secret := new(corev1.Secret)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: vm.Name + "-config"}, secret); err != nil {
		return nil, err
	}
	d, err := bootstrap.Runtime(fresh, secret)
	if err != nil {
		return nil, err
	}
	return json.Marshal(d)
}

// readyBootstrapInputs使用中心/Controller自己的可信reader，不由VM请求传入
// Secret数据。网络不持本地锁；缺reader/Secret、读取超时和不明日志均失败关闭。
func readyBootstrapInputs(ctx context.Context, reader client.Reader, vm *vmv1.VirtualMachine, initial bool) error {
	if reader == nil {
		return fmt.Errorf("DSGA bootstrap requires a direct API reader")
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	secret := new(corev1.Secret)
	if err := reader.Get(readCtx, client.ObjectKey{Namespace: vm.Namespace, Name: vm.Name + "-config"}, secret); err != nil {
		return fmt.Errorf("DSGA bootstrap config unavailable")
	}
	return bootstrap.CheckReady(vm, secret, initial)
}
