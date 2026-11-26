package hostscheduler

import (
	"context"
	"github.com/neondatabase/autoscaling/internal/dsgatest"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"strings"
	"testing"
)

func TestStrictCPUOriginalHostBirthDoesNotWaitForPGInitialization(t *testing.T) {
	_, f, n := schedulerFixture(t)
	pod := legacyVM(t, f)
	n.Profile.CPUAccountingVersion, n.Profile.SystemCPUMillicores = 1, 250
	n.InitializeAtResourceVersion = ""
	n.InitializeMode = KubernetesSnapshotBootstrap
	digest, err := hostguard.ProfileSHA256(n.Profile)
	require.NoError(t, err)
	n.KubernetesBudget = &hostguard.KubernetesBudgetPolicy{ProtocolVersion: 2, Scope: hostguard.KubernetesBudgetScope,
		NodeName: n.Binding.NodeName, NodeUID: string(n.Binding.NodeUID), CapacityBytes: 8 << 30, AllocatableBytes: 8 << 30,
		SystemReservationBytes: n.SystemBytes, ProfileSHA256: digest, CapacityCPUMillicores: 8000, AllocatableCPUMillicores: 8000}
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		f.Node.Status.Capacity = core.ResourceList{core.ResourceMemory: resource.MustParse("8Gi"), core.ResourceCPU: resource.MustParse("8")}
		f.Node.Status.Allocatable = f.Node.Status.Capacity.DeepCopy()
		vm := f.VMs["testing/vm"]
		mode := vmv1.CpuScalingModeSysfs
		vm.Spec.CpuScalingMode = &mode
		vm.Spec.Guest.CPUs = vmv1.CPUs{Min: 500, Use: 500, Max: 2000}
		vm.Spec.PodResources = core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("2")},
			Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("2"), core.ResourceMemory: resource.MustParse("4160Mi")}}
		vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, RuntimeContractVersion: 1,
			Incarnation: strings.Repeat("a", 32), ComputeInstanceID: "compute"}
		vm.Status.MemorySize = nil
		p := f.Pods["testing/runner"]
		p.Spec.Containers[0].Name = "neonvm-runner"
		p.Spec.Containers[0].Resources = *vm.Spec.PodResources.DeepCopy()
	})
	service, err := New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Lease.Spec.HolderIdentity = &service.holder })
	binding := journal.Binding{Namespace: "testing", Name: "vm", VMUID: "vm-uid", Incarnation: strings.Repeat("a", 32)}
	store, err := journal.NewStore(f.VM.NeonvmV1().VirtualMachines("testing"), binding)
	require.NoError(t, err)
	ctx := context.Background()
	created, err := store.Initialize(ctx, "1", "original-owner")
	require.NoError(t, err)
	released, err := store.ReleaseBootstrap(ctx, created.ResourceVersion(), "original-secret-uid")
	require.NoError(t, err)
	require.Nil(t, released.State().PGInstance)
	require.Nil(t, released.State().ResourceIdentity)
	require.NoError(t, service.reconcile(ctx, service.domains[n.Binding.NodeName]))
	_, held, err := service.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.NoError(t, err)
	require.EqualValues(t, 2000, held.CommittedCPUMillicores)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		fact, err := hostguard.InspectCPUCommitment(&f.Node, binding, pod.Name, string(pod.UID), 2000, 2000)
		require.NoError(t, err)
		require.EqualValues(t, 2000, fact.CommittedMillicores)
	})
	untouched, err := store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, untouched.State().PGInstance, "Node CPU出生不写PG identity或初始化")
}
