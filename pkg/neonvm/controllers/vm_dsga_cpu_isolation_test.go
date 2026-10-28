package controllers

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 执行正式Reconcile及其Status().Update，而不只检查辅助函数内存副本。
// Runner CPU为真实HTTP peer，Kube/Runner内存发布为显式协议夹具；没有QEMU、
// sysfs或CPU/物理内存认证。来源坏时必须在任何CPU HTTP之前整体拒绝。
func TestManagedCPUFailureDoesNotDiscardVerifiedMemoryStatus(t *testing.T) {
	for _, scenario := range []struct {
		name  string
		phase vmv1.VmPhase
		mode  string
	}{
		{"scaling-read-error", vmv1.VmScaling, "read-error"},
		{"running-read-error", vmv1.VmRunning, "read-error"},
		{"running-oversized-cpu", vmv1.VmRunning, "oversized"},
		{"scaling-write-error", vmv1.VmScaling, "write-error"},
		{"running-write-error", vmv1.VmRunning, "write-error"},
		{"scaling-write-accepted", vmv1.VmScaling, "write-accepted"},
		{"running-ready", vmv1.VmRunning, "ready"},
		{"scaling-bad-source", vmv1.VmScaling, "bad-source"},
		{"running-bad-source", vmv1.VmRunning, "bad-source"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var reads, writes, current atomic.Int64
			current.Store(1000)
			if scenario.mode == "ready" || scenario.mode == "oversized" {
				current.Store(1500)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/cpu_current":
					reads.Add(1)
					if scenario.mode == "read-error" {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					_ = json.NewEncoder(w).Encode(api.VCPUCgroup{VCPUs: vmv1.MilliCPU(current.Load())})
					if scenario.mode == "oversized" {
						// 合法JSON加空白旧解码可接受；新契约必须按体积拒绝。
						_, _ = w.Write([]byte(strings.Repeat(" ", 5000)))
					}
				case "/cpu_change":
					writes.Add(1)
					if scenario.mode == "write-error" {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					current.Store(1500)
					w.WriteHeader(http.StatusOK)
				default:
					t.Errorf("CPU对账不应进入其他Runner入口：%s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			p, vm, state := managedCPUStatusFixture(t, server.Listener.Addr().String(), scenario.phase)
			if scenario.mode == "bad-source" {
				state.UID = "replacement-state"
			}
			require.NoError(t, p.client.Create(p.ctx, state))
			originalJournal := vm.Annotations[control.JournalAnnotation]
			request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(vm)}
			result, err := p.r.Reconcile(p.ctx, request)
			got := p.getVM()
			require.Equal(t, originalJournal, got.Annotations[control.JournalAnnotation], "状态展示不得改内存WAL/预算")
			if scenario.mode == "bad-source" {
				require.Error(t, err)
				require.Equal(t, int64(1<<30), got.Status.MemorySize.Value())
				require.Zero(t, reads.Load())
				require.Zero(t, writes.Load())
				return
			}
			require.NoError(t, err, "已确认内存必须能通过真实外层Status写回")
			require.Equal(t, int64(2<<30), got.Status.MemorySize.Value())
			require.EqualValues(t, 1, reads.Load())
			condition := meta.FindStatusCondition(got.Status.Conditions, typeDSGACPUReady)
			require.NotNil(t, condition)
			require.Equal(t, vm.Generation, condition.ObservedGeneration)
			if scenario.mode == "ready" {
				require.Equal(t, metav1.ConditionTrue, condition.Status)
				require.Equal(t, vmv1.VmRunning, got.Status.Phase)
				require.Equal(t, 15*time.Second, result.RequeueAfter)
				require.Zero(t, writes.Load())
				return
			}
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Equal(t, vmv1.VmScaling, got.Status.Phase)
			require.Equal(t, time.Second, result.RequeueAfter)
			if scenario.mode == "read-error" || scenario.mode == "oversized" {
				require.Zero(t, writes.Load())
			} else {
				require.EqualValues(t, 1, writes.Load())
				require.EqualValues(t, 1000, *got.Status.CPUs, "展示读回，不能提前写入POST目标")
			}
			if scenario.mode == "write-accepted" {
				require.Equal(t, "Reconciling", condition.Reason)
				// POST接受后还须下一轮实际GET；只有此时才可标Ready。
				_, err = p.r.Reconcile(p.ctx, request)
				require.NoError(t, err)
				got = p.getVM()
				require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(got.Status.Conditions, typeDSGACPUReady).Status)
				require.Equal(t, vmv1.VmRunning, got.Status.Phase)
				require.EqualValues(t, 2, reads.Load())
				require.EqualValues(t, 1, writes.Load())
			} else {
				require.Equal(t, "CPURequestFailed", condition.Reason)
			}
		})
	}
}

func managedCPUStatusFixture(t *testing.T, address string, phase vmv1.VmPhase) (*testParams, *vmv1.VirtualMachine, *core.ConfigMap) {
	t.Helper()
	p := newTestParams(t)
	require.NoError(t, core.AddToScheme(p.r.Scheme))
	_, vm, secret := controllerBootstrapInputs(t)
	host, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	portNumber, err := strconv.Atoi(port)
	require.NoError(t, err)
	mode := vmv1.CpuScalingModeSysfs
	vm.Spec.CpuScalingMode, vm.Spec.RunnerPort = &mode, int32(portNumber)
	vm.Generation, vm.Finalizers = 7, append(vm.Finalizers, virtualmachineFinalizer)
	vm.Status.PodName, vm.Status.Node, vm.Status.PodIP, vm.Status.Phase = "runner", "node", host, phase
	vm.Status.MemorySize = resource.NewQuantity(1<<30, resource.BinarySI)
	oldCPU := vmv1.MilliCPU(1000)
	vm.Status.CPUs = &oldCPU
	binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation}
	snapshot, err := journal.Inspect(vm, binding)
	require.NoError(t, err)
	id := vmguard.Identity{VMUID: string(vm.UID), Incarnation: binding.Incarnation, RunnerPodUID: "runner-uid", RunnerBootID: "0123456789abcdef0123456789abcdef", NodeUID: "node-uid", MinimumBytes: 1 << 30, MaximumBytes: 4 << 30, BlockBytes: 8 << 20}
	birthRef, stateRef := budget.ObjectRef{Name: vmguard.BindingName(binding), UID: "birth-uid"}, budget.ObjectRef{Name: "runner-state", UID: "state-uid"}
	birth := vmguard.Birth{ProtocolVersion: 1, Identity: id, StateClaim: stateRef}
	view := vmguard.View{ProtocolVersion: 1, Identity: id, BindingClaim: birthRef, Sequence: 1, Floor: snapshot.State().Owner, LeaseUID: "lease-uid", HighWater: nil, Pending: nil, Last: nil, LastPublished: nil}
	publication := vmguard.Publication{ProtocolVersion: 1, Identity: id, BindingClaim: &birthRef, State: &view, Actual: &vmguard.Observation{RequestedBytes: 2 << 30, ActualBytes: 2 << 30}}
	controller, block, immutable, mutable := true, true, true, false
	owner := []metav1.OwnerReference{{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine", Name: vm.Name, UID: vm.UID, Controller: &controller, BlockOwnerDeletion: &block}}
	birthRaw, err := json.Marshal(birth)
	require.NoError(t, err)
	stateRaw, err := json.Marshal(publication)
	require.NoError(t, err)
	birthCM := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: birthRef.Name, Namespace: vm.Namespace, UID: "birth-uid", ResourceVersion: "1", Labels: map[string]string{vmguard.ClaimLabel: "binding-v1"}, Finalizers: []string{vmguard.ProtectionFinalizer}, OwnerReferences: owner}, Immutable: &immutable, Data: map[string]string{vmguard.ClaimKey: string(birthRaw)}}
	stateCM := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: stateRef.Name, Namespace: vm.Namespace, UID: "state-uid", Labels: map[string]string{vmguard.ClaimLabel: "state-v1"}, Finalizers: []string{vmguard.ProtectionFinalizer}, OwnerReferences: owner}, Immutable: &mutable, Data: map[string]string{vmguard.ClaimKey: string(stateRaw)}}
	pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: vm.Namespace, UID: "runner-uid", ResourceVersion: "1", OwnerReferences: owner}, Spec: core.PodSpec{NodeName: "node"}, Status: core.PodStatus{Phase: core.PodRunning, PodIP: host, ContainerStatuses: []core.ContainerStatus{{Name: runnerContainerName, Ready: true}}}}
	node := &core.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid", ResourceVersion: "1"}}
	p.client = fake.NewClientBuilder().WithScheme(p.r.Scheme).WithStatusSubresource(&vmv1.VirtualMachine{}).WithObjects(vm, secret, birthCM, pod, node).Build()
	p.r.Client, p.r.APIReader, p.origVM = p.client, p.client, vm
	return p, vm, stateCM
}
