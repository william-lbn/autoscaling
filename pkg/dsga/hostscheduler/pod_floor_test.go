package hostscheduler

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 原typed API夹具，只测试生产allocation/Reserve/CAS；不是实际VM或平台证明。
func schedulerPodFloorFixture(t *testing.T) (*Service, *dsgatest.SchedulerAPI, NodeConfig, *core.Pod) {
	t.Helper()
	_, f, n := schedulerFixture(t)
	n.Profile.VMOverheadBytes = 3 << 30
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		f.Node.Status.Allocatable[core.ResourceMemory] = resource.MustParse("24Gi")
	})
	s, err := New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Lease.Spec.HolderIdentity = &s.holder })
	pod := legacyVM(t, f)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		p := f.Pods["testing/runner"]
		p.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("11Gi")
		vm := f.VMs["testing/vm"]
		vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: strings.Repeat("a", 32), ComputeInstanceID: "compute"}
		vm.Spec.Guest.MemorySlots = vmv1.MemorySlots{Min: 7, Use: 7, Max: 8}
		// 原新建held状态先初始化journal，禁止借已派发状态绕过生产门。
		vm.Status = vmv1.VirtualMachineStatus{}
	})
	binding := journal.Binding{Namespace: "testing", Name: "vm", VMUID: "vm-uid", Incarnation: strings.Repeat("a", 32)}
	store, err := journal.NewStore(f.VM.NeonvmV1().VirtualMachines("testing"), binding)
	require.NoError(t, err)
	initial, err := store.Initialize(context.Background(), "1", "original-floor-owner")
	require.NoError(t, err)
	_, err = store.ReleaseBootstrap(context.Background(), initial.ResourceVersion(), "original-floor-config")
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		vm := f.VMs["testing/vm"]
		vm.Status.Phase, vm.Status.PodName, vm.Status.Node = vmv1.VmRunning, "runner", n.Binding.NodeName
		vm.Status.MemorySize = resource.NewQuantity(7<<30, resource.BinarySI)
	})
	return s, f, n, pod
}

func TestSchedulerManagedPodFloorKeepsElevenGiAcrossSevenEight(t *testing.T) {
	s, f, n, pod := schedulerPodFloorFixture(t)
	ctx := context.Background()
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	for _, actual := range []int64{7, 8, 7, 8} {
		f.Mutate(func(f *dsgatest.SchedulerAPI) {
			f.VMs["testing/vm"].Status.MemorySize = resource.NewQuantity(actual<<30, resource.BinarySI)
		})
		_, allocation, _, err := s.allocation(ctx, n, pod, 0)
		require.NoError(t, err)
		require.EqualValues(t, actual<<30, allocation.MemoryBytes, "物理Guest观测保持7/8，不能改成Pod floor")
		require.EqualValues(t, 11<<30, allocation.PodMemoryLimitBytes)
		_, held, err := s.Reserve(ctx, n.Binding.NodeName, pod, 0)
		require.NoError(t, err)
		charged, err := held.Bytes()
		require.NoError(t, err)
		require.EqualValues(t, 11<<30, charged, "同一Pod降至7Gi也不能提前归还11Gi硬限floor")
		snapshot, err := s.domains[n.Binding.NodeName].store.Load(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 10, snapshot.State().ProtocolVersion)
		used, err := snapshot.State().UsedBytes()
		require.NoError(t, err)
		require.EqualValues(t, 12<<30, used, "Outside1Gi加唯一Pod11Gi，不重复叠加Guest")
	}
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		require.Positive(t, f.NodeWrites, "必须走原Node CAS而非只算本地数值")
	})
}

func TestSchedulerManagedPodFloorRejectsShortCapAndOriginalPodDrift(t *testing.T) {
	for _, mode := range []string{"below-maximum-plus-overhead", "same-UID-cap-drift", "replacement-Pod-UID"} {
		t.Run(mode, func(t *testing.T) {
			s, f, n, pod := schedulerPodFloorFixture(t)
			ctx := context.Background()
			require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
			_, _, err := s.Reserve(ctx, n.Binding.NodeName, pod, 0)
			require.NoError(t, err)
			writes := 0
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				writes = f.NodeWrites
				p := f.Pods["testing/runner"]
				switch mode {
				case "below-maximum-plus-overhead":
					p.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("10Gi")
				case "same-UID-cap-drift":
					p.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("12Gi")
				case "replacement-Pod-UID":
					p.UID = "replacement-floor-pod"
				}
			})
			_, _, err = s.Reserve(ctx, n.Binding.NodeName, pod, 0)
			require.Error(t, err)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Equal(t, writes, f.NodeWrites, "身份/原硬限漂移不可写入新Node承诺")
			})
		})
	}
}

func TestSchedulerPodFloorLeavesOrdinaryAndLegacyAtZero(t *testing.T) {
	s, f, n := schedulerFixture(t)
	legacy := legacyVM(t, f)
	ordinary := boundedPod("ordinary-floor", n.Binding.NodeName, "2Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/ordinary-floor"] = ordinary })
	for _, p := range []*core.Pod{legacy, ordinary} {
		_, row, _, err := s.allocation(context.Background(), n, p, 0)
		require.NoError(t, err)
		require.False(t, row.Managed)
		require.Zero(t, row.PodMemoryLimitBytes)
		charged, err := row.Bytes()
		require.NoError(t, err)
		require.Equal(t, row.MemoryBytes+row.HostOverheadBytes, charged)
	}
}
