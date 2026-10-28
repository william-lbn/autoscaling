package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// VM关闭锚点是Scheduler产出的明确协议夹具；此处独立验证实际Controller的
// 直接读取/GC边界、自己的finalizer与UID/RV写及未知恢复，不认证真实Kubelet。
func controllerRetirementFixture(t *testing.T) (*testParams, *vmv1.VirtualMachine, *core.Node, []*core.ConfigMap) {
	t.Helper()
	p := newTestParams(t)
	require.NoError(t, core.AddToScheme(p.r.Scheme))
	_, vm, _ := controllerBootstrapInputs(t)
	vm.Spec.DSGABootstrap.Ready = true
	stamp := metav1.Now()
	vm.DeletionTimestamp = &stamp
	vm.Finalizers = []string{virtualmachineFinalizer}
	profile := hostguard.Profile{ID: "qualified-test-only", SchedulerName: "neonvm-scheduler", VMOverheadBytes: 64 << 20, PodOverheadBytes: 16 << 20}
	binding := hostguard.Binding{NodeName: "node-one", NodeUID: "node-uid", Namespace: "system", LeaseName: "host-lease", LeaseUID: "host-lease-uid"}
	placements := []hostguard.Placement{}
	m := hostguard.Model{ProtocolVersion: 5, Binding: binding, Origin: strings.Repeat("a", 32), LeaseFloor: journal.Owner{HolderID: "host-owner", Epoch: 1}, Sequence: 2, OutsideBytes: 1 << 30, Holds: []hostguard.Hold{}, Profile: &profile, Placements: &placements}
	row := hostguard.Placement{Namespace: vm.Namespace, PodName: "runner", PodUID: "runner-uid", VMName: vm.Name, VMUID: string(vm.UID), Incarnation: vm.Spec.DSGABootstrap.Incarnation, Managed: true, MemoryBytes: 1 << 30, HostOverheadBytes: 64 << 20, Settled: nil}
	closed := hostguard.Retirement{ProtocolVersion: 1, Binding: binding, Origin: m.Origin, BeforeSequence: 1, Placement: row, TerminalSHA256: strings.Repeat("b", 64), RunnerBirth: &budget.ObjectRef{Name: "birth", UID: "birth-uid"}, RunnerExit: &budget.ObjectRef{Name: "exit", UID: "exit-uid"}, RunnerExitSHA: strings.Repeat("c", 64)}
	raw, err := json.Marshal(closed)
	require.NoError(t, err)
	vm.Annotations[hostguard.VMRetirementAnnotation] = string(raw)
	raw, err = json.Marshal(m)
	require.NoError(t, err)
	node := &core.Node{ObjectMeta: metav1.ObjectMeta{Name: binding.NodeName, UID: binding.NodeUID, ResourceVersion: "2", Finalizers: []string{hostguard.ProtectionFinalizer}, Annotations: map[string]string{hostguard.LedgerAnnotation: string(raw), hostguard.OriginAnnotation: "1:" + m.Origin}}, Status: core.NodeStatus{Allocatable: core.ResourceList{core.ResourceMemory: resource.MustParse("8Gi")}}}
	controller, block, immutable := true, true, true
	owner := []metav1.OwnerReference{{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine", Name: vm.Name, UID: vm.UID, Controller: &controller, BlockOwnerDeletion: &block}}
	sources := []*core.ConfigMap{
		{ObjectMeta: metav1.ObjectMeta{Name: "birth", Namespace: vm.Namespace, UID: "birth-uid", ResourceVersion: "1", OwnerReferences: owner, Labels: map[string]string{vmguard.ClaimLabel: "binding-v1"}, Finalizers: []string{vmguard.ProtectionFinalizer}}, Immutable: &immutable},
		{ObjectMeta: metav1.ObjectMeta{Name: "host", Namespace: vm.Namespace, UID: "host-uid", ResourceVersion: "1", OwnerReferences: owner, Labels: map[string]string{budgetclaims.ClaimLabel: budgetclaims.HostKind}, Finalizers: []string{budgetclaims.ProtectionFinalizer}}, Immutable: &immutable},
		{ObjectMeta: metav1.ObjectMeta{Name: "outside", Namespace: vm.Namespace, UID: "outside-uid", ResourceVersion: "1", OwnerReferences: owner, Labels: map[string]string{budgetclaims.ClaimLabel: budgetclaims.OutsideKind}, Finalizers: []string{budgetclaims.ProtectionFinalizer}}, Immutable: &immutable},
		{ObjectMeta: metav1.ObjectMeta{Name: "qualification", Namespace: vm.Namespace, UID: "qualification-uid", ResourceVersion: "1", OwnerReferences: owner, Labels: map[string]string{budgetclaims.ClaimLabel: budgetclaims.QualificationKind}, Finalizers: []string{budgetclaims.ProtectionFinalizer}}, Immutable: &immutable},
	}
	return p, vm, node, sources
}

func TestManagedVMFinalizationCollectsOnlyClosedOriginalSources(t *testing.T) {
	for _, what := range []string{"complete", "patch-reply-lost", "delete-reply-lost", "cold-bootstrap"} {
		t.Run(what, func(t *testing.T) {
			p, vm, node, sources := controllerRetirementFixture(t)
			if what == "cold-bootstrap" {
				cold, _, _ := controllerBootstrapInputs(t)
				cold.DeletionTimestamp, cold.Finalizers = vm.DeletionTimestamp, vm.Finalizers
				vm = cold
			}
			other := sources[0].DeepCopy()
			other.Name, other.UID = "other-vm", "other-source-uid"
			other.OwnerReferences[0].UID = "other-vm-uid"
			unrelated := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: vm.Namespace, UID: "unrelated-uid", OwnerReferences: sources[0].OwnerReferences}}
			objects := []client.Object{vm, node, other, unrelated}
			for _, cm := range sources {
				objects = append(objects, cm)
			}
			lost := false
			c := fake.NewClientBuilder().WithScheme(p.r.Scheme).WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{
				Get: nil, List: nil, Create: nil, DeleteAllOf: nil, Update: nil, Watch: nil, SubResource: nil, SubResourceGet: nil, SubResourceCreate: nil, SubResourceUpdate: nil, SubResourcePatch: nil,
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if err := c.Patch(ctx, obj, patch, opts...); err != nil {
						return err
					}
					if what == "patch-reply-lost" && !lost {
						lost = true
						return errors.New("fixture PATCH reply lost after commit")
					}
					return nil
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if err := c.Delete(ctx, obj, opts...); err != nil {
						return err
					}
					if what == "delete-reply-lost" && !lost {
						lost = true
						return errors.New("fixture DELETE reply lost after commit")
					}
					return nil
				},
			}).Build()
			p.r.Client, p.r.APIReader = c, c
			err := p.r.finalizeManagedMemory(context.Background(), vm)
			if what == "patch-reply-lost" || what == "delete-reply-lost" {
				require.Error(t, err)
				require.True(t, lost)
				require.NoError(t, p.r.finalizeManagedMemory(context.Background(), vm))
			} else {
				require.NoError(t, err)
			}
			for _, cm := range sources {
				require.True(t, apierrors.IsNotFound(c.Get(context.Background(), client.ObjectKeyFromObject(cm), new(core.ConfigMap))))
			}
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(other), new(core.ConfigMap)))
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(unrelated), new(core.ConfigMap)))
			require.Contains(t, vm.Finalizers, virtualmachineFinalizer, "本函数不提前移除VM正常保护，由原Reconcile完成")
		})
	}
}

func TestManagedVMFinalizationKeepsUnknownAndStillHeldSources(t *testing.T) {
	for _, what := range []string{"missing-anchor", "ready-flag-reverted", "replacement-node", "held-row", "unknown-source", "live-pod"} {
		t.Run(what, func(t *testing.T) {
			p, vm, node, sources := controllerRetirementFixture(t)
			objects := []client.Object{vm, node}
			for _, cm := range sources {
				objects = append(objects, cm)
			}
			switch what {
			case "missing-anchor":
				delete(vm.Annotations, hostguard.VMRetirementAnnotation)
			case "ready-flag-reverted":
				delete(vm.Annotations, hostguard.VMRetirementAnnotation)
				vm.Spec.DSGABootstrap.Ready = false
			case "replacement-node":
				node.UID = "new-node-uid"
			case "held-row":
				var closed hostguard.Retirement
				require.NoError(t, json.Unmarshal([]byte(vm.Annotations[hostguard.VMRetirementAnnotation]), &closed))
				var m hostguard.Model
				require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &m))
				*m.Placements = append(*m.Placements, closed.Placement)
				raw, err := json.Marshal(m)
				require.NoError(t, err)
				node.Annotations[hostguard.LedgerAnnotation] = string(raw)
			case "unknown-source":
				sources[0].Labels[vmguard.ClaimLabel] = "future-v99"
			case "live-pod":
				objects = append(objects, &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: vm.Namespace, UID: "runner-uid", OwnerReferences: sources[0].OwnerReferences, Finalizers: []string{hostguard.PodProtectionFinalizer}}, Spec: core.PodSpec{NodeName: "node-one"}, Status: core.PodStatus{Phase: core.PodRunning}})
			}
			c := fake.NewClientBuilder().WithScheme(p.r.Scheme).WithObjects(objects...).Build()
			p.r.Client, p.r.APIReader = c, c
			require.Error(t, p.r.finalizeManagedMemory(context.Background(), vm))
			// 未知source版本可能排在已清旧版本之后；这里检查原关键
			// birth始终未被误删，其余清理允许下轮以相同关闭锚点恢复。
			var retained core.ConfigMap
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(sources[0]), &retained))
			require.Contains(t, retained.Finalizers, vmguard.ProtectionFinalizer)
		})
	}
}
