package controllers

import (
	"context"
	"encoding/json"
	"fmt"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api/runnerdiagnostic"
)

const typeRunnerFailure = "RunnerFailure"

type runnerFailureSnapshot struct {
	Version        int                    `json:"version"`
	PodUID         string                 `json:"pod_uid"`
	Container      string                 `json:"container"`
	ContainerIndex int                    `json:"container_index"`
	ExitCode       int32                  `json:"exit_code"`
	Signal         int32                  `json:"signal"`
	Stage          runnerdiagnostic.Stage `json:"stage"`
	Code           runnerdiagnostic.Code  `json:"code"`
}

// runnerFailureCondition只从原Pod的可信元数据/容器终态取身份和退出码。
// 终止消息仅接受独立的有限枚举协议；任意Pod Reason/Message、日志、容器ID、
// 命令参数和未认证字符串均不进入公开VM状态。未知诊断仍可持久化后正常停止。
func runnerFailureCondition(vm *vmv1.VirtualMachine, pod *core.Pod) (metav1.Condition, error) {
	var none metav1.Condition
	owner := metav1.GetControllerOf(pod)
	if vm.Spec.DSGABootstrap == nil || vm.UID == "" || pod.UID == "" || len(pod.UID) > 128 || pod.ResourceVersion == "" ||
		pod.Namespace != vm.Namespace || pod.Name != vm.Status.PodName || len(pod.OwnerReferences) != 1 || owner == nil ||
		owner.UID != vm.UID || owner.Name != vm.Name || owner.Kind != "VirtualMachine" || owner.APIVersion != vmv1.SchemeGroupVersion.String() {
		return none, fmt.Errorf("failure diagnostic requires original managed Pod identity")
	}
	out := runnerFailureSnapshot{Version: 1, PodUID: string(pod.UID), Container: "unknown", ContainerIndex: -1,
		Stage: runnerdiagnostic.StagePodFailure, Code: runnerdiagnostic.CodeUnknown}
	// Runner的固定角色优先，init仅取首个失败序号，不复制可任意命名的容器名。
	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		if status.Name == runnerContainerName && status.State.Terminated != nil {
			term := status.State.Terminated
			out.Container, out.ContainerIndex, out.ExitCode, out.Signal = "runner", 0, term.ExitCode, term.Signal
			out.Code = runnerdiagnostic.CodeContainerExited
			if term.Reason == "OOMKilled" {
				out.Code = runnerdiagnostic.CodeOOMKilled
			} else if report, err := runnerdiagnostic.Parse([]byte(term.Message)); err == nil {
				out.Stage, out.Code = report.Stage, report.Code
			}
			break
		}
	}
	if out.Container == "unknown" {
		for i, status := range pod.Status.InitContainerStatuses {
			if i >= 32 {
				break
			}
			if term := status.State.Terminated; term != nil && (term.ExitCode != 0 || term.Signal != 0) {
				out.Container, out.ContainerIndex, out.ExitCode, out.Signal = "init", i, term.ExitCode, term.Signal
				out.Stage, out.Code = runnerdiagnostic.StageInitContainer, runnerdiagnostic.CodeContainerExited
				if term.Reason == "OOMKilled" {
					out.Code = runnerdiagnostic.CodeOOMKilled
				}
				break
			}
		}
	}
	raw, err := json.Marshal(out)
	if err != nil || len(raw) > runnerdiagnostic.MaxBytes {
		return none, fmt.Errorf("failure diagnostic exceeds fixed size")
	}
	return metav1.Condition{Type: typeRunnerFailure, Status: metav1.ConditionTrue, ObservedGeneration: vm.Generation,
		Reason: "RunnerFailed", Message: string(raw)}, nil
}

func recordManagedRunnerFailure(vm *vmv1.VirtualMachine, pod *core.Pod) error {
	if vm.Spec.DSGABootstrap == nil {
		return nil
	}
	condition, err := runnerFailureCondition(vm, pod)
	if err != nil {
		return err
	}
	meta.SetStatusCondition(&vm.Status.Conditions, condition)
	return nil
}

// stopFailedManagedRunner先保存诊断，再经直接API读确认原VM已持久化，最后
// 才调用既有Delete/Host保护路径。没有新增finalizer、TTL或退款；诊断不证明
// QEMU退出，也不替代birth/NoStart/原账本。显式VM删除仍走原finalize流程，
// 不因诊断存储故障阻塞关闭。状态写丢回复由下一次直接读取原对象恢复。
func (r *VMReconciler) stopFailedManagedRunner(ctx context.Context, vm *vmv1.VirtualMachine) error {
	if r.APIReader == nil || vm.Status.PodName == "" {
		return fmt.Errorf("managed failure diagnostic requires direct reader and original Pod")
	}
	pod := new(core.Pod)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: vm.Namespace, Name: vm.Status.PodName}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // 已丢失的Pod不能伪造终止详情；保留现有VM失败状态。
		}
		return err
	}
	condition, err := runnerFailureCondition(vm, pod)
	if err != nil {
		return err
	}
	fresh := new(vmv1.VirtualMachine)
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(vm), fresh); err != nil {
		return err
	}
	if fresh.UID != vm.UID || fresh.ResourceVersion == "" || fresh.Generation != vm.Generation || fresh.DeletionTimestamp != nil ||
		fresh.Status.Phase != vmv1.VmFailed || fresh.Status.PodName != vm.Status.PodName || fresh.Spec.DSGABootstrap == nil ||
		fresh.Spec.DSGABootstrap.Incarnation != vm.Spec.DSGABootstrap.Incarnation {
		return fmt.Errorf("original VM changed before failure diagnostic persistence")
	}
	stored := meta.FindStatusCondition(fresh.Status.Conditions, typeRunnerFailure)
	if stored == nil || stored.Status != condition.Status || stored.Reason != condition.Reason || stored.Message != condition.Message || stored.ObservedGeneration != condition.ObservedGeneration {
		meta.SetStatusCondition(&vm.Status.Conditions, condition)
		return nil // 外层Reconcile唯一Status.Update成功后才会有下一轮Delete。
	}
	return r.deleteManagedMemoryPod(ctx, vm, pod)
}
