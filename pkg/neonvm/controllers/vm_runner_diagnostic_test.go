package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api/runnerdiagnostic"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

type diagnosticClient struct {
	client.Client
	failStatus, loseReply bool
	deletes               int
}

type diagnosticStatusWriter struct {
	client.SubResourceWriter
	parent *diagnosticClient
}

func (c *diagnosticClient) Status() client.SubResourceWriter {
	return &diagnosticStatusWriter{SubResourceWriter: c.Client.Status(), parent: c}
}
func (w *diagnosticStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if w.parent.failStatus {
		return errors.New("fixture status unavailable")
	}
	if err := w.SubResourceWriter.Update(ctx, obj, opts...); err != nil {
		return err
	}
	if w.parent.loseReply {
		return errors.New("fixture status reply lost")
	}
	return nil
}
func (c *diagnosticClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.deletes++
	return c.Client.Delete(ctx, obj, opts...)
}

func runnerDiagnosticFixture(t *testing.T) (*testParams, *vmv1.VirtualMachine, *core.Pod, *diagnosticClient) {
	t.Helper()
	p := newTestParams(t)
	_, vm, secret := controllerBootstrapInputs(t)
	mode := vmv1.CpuScalingModeSysfs
	vm.Spec.CpuScalingMode = &mode
	vm.Generation, vm.Finalizers = 2, []string{virtualmachineFinalizer}
	vm.Status.Phase, vm.Status.PodName = vmv1.VmFailed, "runner"
	controller := true
	raw, err := runnerdiagnostic.Encode(runnerdiagnostic.Report{Version: 1, Stage: runnerdiagnostic.StageInitialFreeze, Code: runnerdiagnostic.CodeAPIForbidden})
	require.NoError(t, err)
	pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: vm.Namespace, UID: "original-pod", ResourceVersion: "1", Finalizers: []string{hostguard.PodProtectionFinalizer},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine", Name: vm.Name, UID: vm.UID, Controller: &controller}}},
		Spec: core.PodSpec{RestartPolicy: core.RestartPolicyNever, Containers: []core.Container{{Name: runnerContainerName}}},
		Status: core.PodStatus{Phase: core.PodFailed, Message: "private-token", ContainerStatuses: []core.ContainerStatus{{Name: runnerContainerName,
			State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ExitCode: 1, Message: string(raw), Reason: "private-token"}}}}}}
	base := fake.NewClientBuilder().WithScheme(p.r.Scheme).WithStatusSubresource(&vmv1.VirtualMachine{}).WithObjects(vm, secret, pod).Build()
	c := &diagnosticClient{Client: base}
	p.client, p.r.Client, p.r.APIReader, p.origVM = c, c, base, vm
	return p, vm, pod, c
}

// 通过真实Reconcile及Status().Update而非辅助函数内存结果，独立证明诊断持久化
// 前绝不Delete，成功/写丢回复之后继续原Host finalizer路径，不提前退账或去保护。
func TestManagedFailureDiagnosticPersistsBeforeActualPodDelete(t *testing.T) {
	for _, mode := range []string{"normal", "status-unavailable", "status-reply-lost"} {
		t.Run(mode, func(t *testing.T) {
			p, vm, pod, c := runnerDiagnosticFixture(t)
			c.failStatus, c.loseReply = mode == "status-unavailable", mode == "status-reply-lost"
			request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(vm)}
			_, err := p.r.Reconcile(p.ctx, request)
			if mode == "normal" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Zero(t, c.deletes)
			current := new(core.Pod)
			require.NoError(t, c.Get(p.ctx, client.ObjectKeyFromObject(pod), current))
			require.Nil(t, current.DeletionTimestamp)
			if c.failStatus {
				require.Nil(t, meta.FindStatusCondition(p.getVM().Status.Conditions, typeRunnerFailure))
				c.failStatus = false
				_, err = p.r.Reconcile(p.ctx, request)
				require.NoError(t, err)
				require.Zero(t, c.deletes)
			}
			stored := meta.FindStatusCondition(p.getVM().Status.Conditions, typeRunnerFailure)
			require.NotNil(t, stored)
			require.NotContains(t, stored.Message, "private-token")
			var snapshot runnerFailureSnapshot
			require.NoError(t, json.Unmarshal([]byte(stored.Message), &snapshot))
			require.Equal(t, "original-pod", snapshot.PodUID)
			require.Equal(t, runnerdiagnostic.StageInitialFreeze, snapshot.Stage)
			require.Equal(t, runnerdiagnostic.CodeAPIForbidden, snapshot.Code)
			require.EqualValues(t, 1, snapshot.ExitCode)
			c.loseReply = false
			_, err = p.r.Reconcile(p.ctx, request)
			require.NoError(t, err)
			require.Equal(t, 1, c.deletes)
			require.NoError(t, c.Get(p.ctx, client.ObjectKeyFromObject(pod), current))
			require.NotNil(t, current.DeletionTimestamp)
			require.Contains(t, current.Finalizers, hostguard.PodProtectionFinalizer)
			require.Len(t, current.OwnerReferences, 1)
			require.Empty(t, current.Annotations[hostguard.RetirementAnnotation])
		})
	}
}

func TestManagedFailureDiagnosticUnknownTextDoesNotLeakOrBlockStop(t *testing.T) {
	for _, raw := range []string{"private-token", strings.Repeat("x", runnerdiagnostic.MaxBytes+1), `{"version":1,"stage":"initialFreeze","code":"failed","secret":"private-token"}`} {
		p, vm, pod, c := runnerDiagnosticFixture(t)
		pod.Status.ContainerStatuses[0].State.Terminated.Message = raw
		// 模拟Kubelet经原Pod status子资源写终态，不借spec更新覆盖状态。
		require.NoError(t, c.Status().Update(p.ctx, pod))
		for range 2 {
			_, err := p.r.Reconcile(p.ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(vm)})
			require.NoError(t, err)
		}
		require.Equal(t, 1, c.deletes)
		condition := meta.FindStatusCondition(p.getVM().Status.Conditions, typeRunnerFailure)
		require.NotNil(t, condition)
		require.NotContains(t, condition.Message, "private-token")
		require.Less(t, len(condition.Message), runnerdiagnostic.MaxBytes)
	}
}

func TestManagedFailureDiagnosticRejectsOtherPodIdentity(t *testing.T) {
	_, vm, pod, _ := runnerDiagnosticFixture(t)
	for _, mutate := range []func(*core.Pod){
		func(p *core.Pod) { p.OwnerReferences[0].UID = "replacement-vm" },
		func(p *core.Pod) { p.Namespace = "different" },
		func(p *core.Pod) { p.Name = "different" },
		func(p *core.Pod) { p.UID = "" },
	} {
		changed := pod.DeepCopy()
		mutate(changed)
		_, err := runnerFailureCondition(vm, changed)
		require.Error(t, err)
	}
}

func TestManagedFailureFirstObservationIsDurableBeforeNextReconcileDelete(t *testing.T) {
	p, vm, pod, c := runnerDiagnosticFixture(t)
	currentVM := p.getVM()
	currentVM.Status.Phase = vmv1.VmPending
	require.NoError(t, c.Status().Update(p.ctx, currentVM))
	request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(vm)}
	_, err := p.r.Reconcile(p.ctx, request)
	require.NoError(t, err)
	require.Zero(t, c.deletes)
	stored := p.getVM()
	require.Equal(t, vmv1.VmFailed, stored.Status.Phase)
	require.NotNil(t, meta.FindStatusCondition(stored.Status.Conditions, typeRunnerFailure))
	currentPod := new(core.Pod)
	require.NoError(t, c.Get(p.ctx, client.ObjectKeyFromObject(pod), currentPod))
	require.Nil(t, currentPod.DeletionTimestamp)
	_, err = p.r.Reconcile(p.ctx, request)
	require.NoError(t, err)
	require.Equal(t, 1, c.deletes)
}
