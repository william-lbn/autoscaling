package hostscheduler

import (
	"context"
	"testing"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/stretchr/testify/require"
)

// 默认调度器已绑定的普通Pod也必须按完整hard limit计入初始账本。
func TestSchedulerInitialLedgerIncludesAssignedForeignOrdinaryPod(t *testing.T) {
	s, f, n := schedulerFixture(t)
	pod := boundedPod("existing-default", n.Binding.NodeName, "2Gi")
	pod.Spec.SchedulerName = "default-scheduler"
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/existing-default"] = pod.DeepCopy() })
	ctx := context.Background()
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	snapshot, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	placement, err := snapshot.State().Placement(string(pod.UID))
	require.NoError(t, err)
	require.EqualValues(t, 2<<30, placement.MemoryBytes)
	require.Equal(t, n.Profile.PodOverheadBytes, placement.HostOverheadBytes)
	require.Len(t, *snapshot.State().Placements, 1)
}

// 现有普通Pod的兼容不能变成接收外部调度候选或忽略真实内存上限的旁路。
func TestSchedulerAssignedForeignPodStillRequiresOriginalDomainAndHardLimit(t *testing.T) {
	for _, what := range []string{"pending", "other-node", "no-limit", "scheduler-drift", "vm-owner"} {
		t.Run(what, func(t *testing.T) {
			s, f, n := schedulerFixture(t)
			pod := boundedPod("foreign", n.Binding.NodeName, "2Gi")
			pod.Spec.SchedulerName = "default-scheduler"
			expected := pod.DeepCopy()
			switch what {
			case "pending":
				pod.Spec.NodeName = ""
				expected = pod.DeepCopy()
			case "other-node":
				pod.Spec.NodeName = "other-node"
				expected = pod.DeepCopy()
			case "no-limit":
				pod.Spec.Containers[0].Resources.Limits = nil
				expected = pod.DeepCopy()
			case "scheduler-drift":
				pod.Spec.SchedulerName = "changed-scheduler"
			case "vm-owner":
				pod = legacyVM(t, f)
				pod.Spec.SchedulerName = "default-scheduler"
				expected = pod.DeepCopy()
			}
			f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[pod.Namespace+"/"+pod.Name] = pod.DeepCopy() })
			_, _, _, err := s.allocation(context.Background(), n, expected, 0)
			require.Error(t, err)
			_, err = s.domains[n.Binding.NodeName].store.Load(context.Background())
			require.ErrorIs(t, err, hostguard.ErrUninitialized)
		})
	}
}
