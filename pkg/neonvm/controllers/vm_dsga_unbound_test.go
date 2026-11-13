package controllers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestManagedVMUnboundClosureRequiresOriginalCompleteAnchor(t *testing.T) {
	for _, mode := range []string{"complete", "pod-gc", "missing-domain", "replacement-node", "held-row", "wrong-pod", "wrong-vm", "changed-boot", "missing-barrier", "mixed-closure", "birth-source", "state-source"} {
		t.Run(mode, func(t *testing.T) {
			p, vm, node, _ := controllerRetirementFixture(t)
			delete(vm.Annotations, hostguard.VMRetirementAnnotation)
			vm.Status.PodName = "runner"
			owner, block := true, true
			pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: vm.Namespace, Name: "runner", UID: "runner-uid", ResourceVersion: "1", DeletionTimestamp: vm.DeletionTimestamp, Finalizers: []string{"unrelated"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: vm.Name, UID: vm.UID, Controller: &owner, BlockOwnerDeletion: &block}}}, Spec: core.PodSpec{RestartPolicy: core.RestartPolicyNever, Containers: []core.Container{{Name: "neonvm-runner"}}}, Status: core.PodStatus{Phase: core.PodFailed}}
			digest, err := hostguard.UnboundPodDigest(pod)
			require.NoError(t, err)
			var m hostguard.Model
			require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &m))
			closed := hostguard.UnboundRetirement{ProtocolVersion: 1, Namespace: vm.Namespace, PodName: pod.Name, PodUID: string(pod.UID), PodSHA256: digest, VMName: vm.Name, VMUID: string(vm.UID), Incarnation: vm.Spec.DSGABootstrap.Incarnation, Domains: []hostguard.Binding{m.Binding}, Confirmed: []hostguard.UnboundNode{{Binding: m.Binding, Origin: m.Origin, BeforeSequence: 1}}}
			require.NoError(t, closed.InspectVM(vm))
			switch mode {
			case "missing-domain":
				closed.Confirmed = []hostguard.UnboundNode{}
			case "replacement-node":
				node.UID = "replacement"
			case "held-row":
				*m.Placements = append(*m.Placements, hostguard.Placement{Namespace: vm.Namespace, PodName: pod.Name, PodUID: string(pod.UID), VMName: vm.Name, VMUID: string(vm.UID), MemoryBytes: 1 << 30, HostOverheadBytes: m.Profile.VMOverheadBytes})
				raw, err := json.Marshal(m)
				require.NoError(t, err)
				node.Annotations[hostguard.LedgerAnnotation] = string(raw)
			case "wrong-pod":
				pod.UID = "replacement"
			case "wrong-vm":
				closed.VMUID = "replacement"
			case "changed-boot":
				vm.Spec.DSGABootstrap.Incarnation = strings.Repeat("f", 32)
			case "missing-barrier":
				closed.Confirmed[0].BeforeSequence = m.Sequence
			case "mixed-closure":
				vm.Annotations[hostguard.VMRetirementAnnotation] = "other-original-retirement"
			}
			raw, err := json.Marshal(closed)
			require.NoError(t, err)
			vm.Annotations[hostguard.UnboundRetirementAnnotation] = string(raw)
			pod.Annotations = map[string]string{hostguard.UnboundRetirementAnnotation: string(raw)}
			objects := []client.Object{vm, node}
			if mode == "birth-source" || mode == "state-source" {
				binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation}
				names := vmguard.BirthSourceNames(binding)
				index := 0
				if mode == "state-source" {
					index = 1
				}
				objects = append(objects, &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: vm.Namespace, Name: names[index], UID: "unexpected-source"}})
			}
			if mode != "pod-gc" {
				objects = append(objects, pod)
			}
			c := fake.NewClientBuilder().WithScheme(p.r.Scheme).WithObjects(objects...).Build()
			p.r.Client, p.r.APIReader = c, c
			err = p.r.inspectUnboundClosure(context.Background(), vm, pod)
			if mode == "complete" || mode == "pod-gc" {
				require.NoError(t, err)
				require.NoError(t, p.r.inspectUnboundClosure(context.Background(), vm, pod))
				if mode == "pod-gc" {
					require.NoError(t, p.r.finalizeManagedMemory(context.Background(), vm))
					require.NoError(t, p.r.finalizeManagedMemory(context.Background(), vm))
				} else {
					require.NoError(t, p.r.deleteManagedMemoryPod(context.Background(), vm, pod))
				}
			} else {
				require.Error(t, err)
			}
		})
	}
}
