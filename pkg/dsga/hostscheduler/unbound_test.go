package hostscheduler

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func unboundFixture(t *testing.T) (*Service, *dsgatest.SchedulerAPI, NodeConfig, *core.Pod) {
	t.Helper()
	ctx := context.Background()
	s, f, n := schedulerFixture(t)
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	pod := legacyVM(t, f)
	inc := strings.Repeat("a", 32)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		vm := f.VMs["testing/vm"]
		vm.Status = vmv1.VirtualMachineStatus{}
		vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: inc, ComputeInstanceID: "original-compute"}
		pod = f.Pods["testing/runner"]
		pod.Spec.NodeName = ""
		pod.Spec.RestartPolicy = core.RestartPolicyNever
		pod.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("9Gi")
	})
	store, err := journal.NewStore(f.VM.NeonvmV1().VirtualMachines("testing"), journal.Binding{Namespace: "testing", Name: "vm", VMUID: "vm-uid", Incarnation: inc})
	require.NoError(t, err)
	st, err := store.Initialize(ctx, "1", "original-vm-owner")
	require.NoError(t, err)
	_, err = store.ReleaseBootstrap(ctx, st.ResourceVersion(), "original-config")
	require.NoError(t, err)
	_, _, err = s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.ErrorContains(t, err, "capacity")
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		p := f.Pods["testing/runner"]
		require.Contains(t, p.Finalizers, hostguard.PodProtectionFinalizer)
		stamp := metav1.Now()
		p.DeletionTimestamp = &stamp
		p.Status.Phase = core.PodFailed
		pod = p.DeepCopy()
		vm := f.VMs["testing/vm"]
		vm.DeletionTimestamp = &stamp
		vm.Status.Phase = vmv1.VmPending
		vm.Status.PodName = pod.Name
	})
	claims := kubefake.NewSimpleClientset()
	s.core = retirementCore{CoreV1Interface: f.Core.CoreV1(), claims: claims.CoreV1()}
	return s, f, n, pod
}

func TestSchedulerUnboundRetirementSealsEveryNodeWithoutRefund(t *testing.T) {
	for _, mode := range []string{"complete", "node-reply", "pod-reply", "vm-reply"} {
		t.Run(mode, func(t *testing.T) {
			s, f, n, pod := unboundFixture(t)
			ctx := context.Background()
			d := s.domains[n.Binding.NodeName]
			before, err := d.store.Load(ctx)
			require.NoError(t, err)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				f.LoseNodeReply = mode == "node-reply"
				f.LosePodReply = mode == "pod-reply"
				f.LoseVMReply = mode == "vm-reply"
			})
			err = s.reconcile(ctx, d)
			if mode == "node-reply" {
				require.Error(t, err)
				f.Mutate(func(f *dsgatest.SchedulerAPI) {
					require.Contains(t, f.Pods["testing/runner"].Finalizers, hostguard.PodProtectionFinalizer)
					require.Empty(t, f.VMs["testing/vm"].Annotations[hostguard.UnboundRetirementAnnotation])
				})
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, s.reconcile(ctx, d))
			after, err := d.store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, before.State().Placements, after.State().Placements)
			require.Equal(t, before.State().Holds, after.State().Holds)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				p := f.Pods["testing/runner"]
				require.Equal(t, pod.UID, p.UID)
				require.NotContains(t, p.Finalizers, hostguard.PodProtectionFinalizer)
				raw := p.Annotations[hostguard.UnboundRetirementAnnotation]
				require.Equal(t, raw, f.VMs["testing/vm"].Annotations[hostguard.UnboundRetirementAnnotation])
				var closed hostguard.UnboundRetirement
				require.NoError(t, json.Unmarshal([]byte(raw), &closed))
				require.NoError(t, closed.Validate(true))
				require.NoError(t, closed.InspectPod(p))
				require.NoError(t, hostguard.InspectUnboundNode(&f.Node, closed, closed.Confirmed[0]))
			})
			sequence := after.State().Sequence
			require.NoError(t, s.reconcile(ctx, d))
			again, err := d.store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, sequence, again.State().Sequence)
		})
	}
}

func TestSchedulerUnboundRetirementRetainsUnknownAndReservedResponsibilities(t *testing.T) {
	for _, mode := range []string{"bound", "container", "start-time", "scheduled", "not-deleting", "pod-replaced", "vm-replaced", "vm-node", "journal-used", "held-row", "missing-node-domain", "birth-source"} {
		t.Run(mode, func(t *testing.T) {
			s, f, n, pod := unboundFixture(t)
			ctx := context.Background()
			d := s.domains[n.Binding.NodeName]
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				p, vm := f.Pods["testing/runner"], f.VMs["testing/vm"]
				switch mode {
				case "bound":
					p.Spec.NodeName = n.Binding.NodeName
				case "container":
					p.Status.ContainerStatuses = []core.ContainerStatus{{Name: "app"}}
				case "start-time":
					stamp := metav1.Now()
					p.Status.StartTime = &stamp
				case "scheduled":
					p.Status.Conditions = []core.PodCondition{{Type: core.PodScheduled, Status: core.ConditionTrue}}
				case "not-deleting":
					p.DeletionTimestamp = nil
				case "pod-replaced":
					p.UID = "replacement"
				case "vm-replaced":
					vm.UID = "replacement"
				case "vm-node":
					vm.Status.Node = n.Binding.NodeName
				case "journal-used":
					vm.Annotations[journalAnnotationForUnboundTest()] = "broken"
				case "held-row":
					var m hostguard.Model
					require.NoError(t, json.Unmarshal([]byte(f.Node.Annotations[hostguard.LedgerAnnotation]), &m))
					*m.Placements = append(*m.Placements, hostguard.Placement{Namespace: p.Namespace, PodName: p.Name, PodUID: string(p.UID), VMName: vm.Name, VMUID: string(vm.UID), MemoryBytes: 1 << 30, HostOverheadBytes: n.Profile.VMOverheadBytes})
					raw, err := json.Marshal(m)
					require.NoError(t, err)
					f.Node.Annotations[hostguard.LedgerAnnotation] = string(raw)
				case "missing-node-domain":
					f.Node.UID = "replacement"
				}
			})
			if mode == "birth-source" {
				binding := journal.Binding{Namespace: "testing", Name: "vm", VMUID: "vm-uid", Incarnation: strings.Repeat("a", 32)}
				_, err := s.core.ConfigMaps("testing").Create(ctx, &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: vmguard.BindingName(binding), Namespace: "testing"}}, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			require.Error(t, s.retireUnbound(ctx, d, pod))
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Contains(t, f.Pods["testing/runner"].Finalizers, hostguard.PodProtectionFinalizer)
				require.Empty(t, f.VMs["testing/vm"].Annotations[hostguard.UnboundRetirementAnnotation])
			})
		})
	}
}

func journalAnnotationForUnboundTest() string {
	return "internal.autoscaling.neon.tech/dsga-journal-v1"
}

func TestSchedulerUnboundRequiresAllConfiguredDomainsBeforePodGC(t *testing.T) {
	s, f, n, pod := unboundFixture(t)
	ctx := context.Background()
	d := s.domains[n.Binding.NodeName]
	second := d
	second.config = n
	second.config.Binding.NodeName = "node-two"
	second.config.Binding.NodeUID = "node-two-uid"
	second.config.Binding.LeaseName = "node-two-lease"
	second.config.Binding.LeaseUID = "node-two-lease-uid"
	s.domains[second.config.Binding.NodeName] = second
	require.NoError(t, s.retireUnbound(ctx, d, pod))
	before, err := d.store.Load(ctx)
	require.NoError(t, err)
	require.NoError(t, s.retireUnbound(ctx, d, pod))
	after, err := d.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), after.State())
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		p := f.Pods["testing/runner"]
		require.Contains(t, p.Finalizers, hostguard.PodProtectionFinalizer)
		require.Empty(t, f.VMs["testing/vm"].Annotations[hostguard.UnboundRetirementAnnotation])
		var r hostguard.UnboundRetirement
		require.NoError(t, json.Unmarshal([]byte(p.Annotations[hostguard.UnboundRetirementAnnotation]), &r))
		require.Len(t, r.Domains, 2)
		require.Len(t, r.Confirmed, 1)
		require.Error(t, r.Validate(true))
	})
	second.config.Binding.NodeUID = "replacement-node-uid"
	s.domains[second.config.Binding.NodeName] = second
	require.ErrorContains(t, s.retireUnbound(ctx, d, pod), "domains changed")
}
