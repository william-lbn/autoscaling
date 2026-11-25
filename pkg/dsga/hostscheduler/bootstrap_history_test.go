package hostscheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

func historicalOrdinaryPod(name, node string, phase core.PodPhase) *core.Pod {
	p := boundedPod(name, node, "1Gi")
	p.Spec.SchedulerName = core.DefaultSchedulerName
	// 旧 Job 无运行容器，不依赖其历史资源限制是否符合当前活动准入规则。
	p.Spec.Containers[0].Resources.Limits = nil
	restart := core.ContainerRestartPolicyAlways
	p.Spec.InitContainers = []core.Container{{Name: "sidecar", RestartPolicy: &restart}, {Name: "init"}}
	p.Status.Phase = phase
	start := metav1.NewTime(time.Unix(1700000000, 0))
	end := metav1.NewTime(start.Add(time.Minute))
	status := func(name string) core.ContainerStatus {
		id := "containerd://" + name + "-" + string(p.UID)
		return core.ContainerStatus{Name: name, ContainerID: id, State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: id, StartedAt: start, FinishedAt: end}}}
	}
	p.Status.ContainerStatuses = []core.ContainerStatus{status("app")}
	p.Status.InitContainerStatuses = []core.ContainerStatus{status("sidecar"), status("init")}
	return p
}

// 复现共享集群保留历史成功/失败 Job：两种终态及原生 sidecar 全部确认
// 退出后既不占首次活动行，也不阻断后续 Owner 和插件只读观察。
func TestHistoricalOrdinaryPodsDoNotBlockFreshHostOrRecurringWork(t *testing.T) {
	s, f, n := snapshotBootstrapFixture(t)
	ctx := context.Background()
	for _, phase := range []core.PodPhase{core.PodSucceeded, core.PodFailed} {
		p := historicalOrdinaryPod(string(phase), n.Binding.NodeName, phase)
		f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[p.Namespace+"/"+p.Name] = p.DeepCopy() })
	}
	live := boundedPod("current-work", n.Binding.NodeName, "1Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[live.Namespace+"/"+live.Name] = live.DeepCopy() })
	for range 3 {
		require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	}
	snapshot, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	require.Len(t, *snapshot.State().Placements, 1)
	_, err = snapshot.State().Placement(string(live.UID))
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		for _, phase := range []core.PodPhase{core.PodSucceeded, core.PodFailed} {
			p := f.Pods["testing/"+string(phase)]
			require.Empty(t, p.Finalizers)
			require.Empty(t, p.Annotations)
		}
	})
	for _, phase := range []core.PodPhase{core.PodSucceeded, core.PodFailed} {
		p, err := f.Core.CoreV1().Pods("testing").Get(ctx, string(phase), metav1.GetOptions{})
		require.NoError(t, err)
		require.NoError(t, s.ObserveUntrackedTerminalOrdinary(ctx, n.Binding.NodeName, p))
	}
}

func TestHistoricalOrdinaryIncompleteOrOwnedPodCannotBypassBootstrap(t *testing.T) {
	for _, field := range []string{"missing-status", "running-sidecar", "waiting-init", "container-id", "ephemeral", "vm", "migration", "protected", "source", "retirement", "deleting"} {
		t.Run(field, func(t *testing.T) {
			s, f, n := snapshotBootstrapFixture(t)
			p := historicalOrdinaryPod("unsafe", n.Binding.NodeName, core.PodFailed)
			switch field {
			case "missing-status":
				p.Status.ContainerStatuses = nil
			case "running-sidecar":
				p.Status.InitContainerStatuses[0].State = core.ContainerState{Running: &core.ContainerStateRunning{}}
			case "waiting-init":
				p.Status.InitContainerStatuses[1].State = core.ContainerState{Waiting: &core.ContainerStateWaiting{}}
			case "container-id":
				p.Status.ContainerStatuses[0].State.Terminated.ContainerID = "containerd://replacement"
			case "ephemeral":
				p.Spec.EphemeralContainers = []core.EphemeralContainer{{EphemeralContainerCommon: core.EphemeralContainerCommon{Name: "debug"}}}
			case "vm", "migration":
				kind := "VirtualMachine"
				if field == "migration" {
					kind = "VirtualMachineMigration"
				}
				p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: kind, Name: "original", UID: "original-uid"}}
			case "protected":
				p.Finalizers = []string{hostguard.PodProtectionFinalizer}
			case "source":
				p.Annotations = map[string]string{hostguard.OrdinaryStopSourceAnnotation: "original-intent"}
			case "retirement":
				p.Annotations = map[string]string{hostguard.RetirementAnnotation: "original-intent"}
			case "deleting":
				stamp := metav1.Now()
				p.DeletionTimestamp = &stamp
			}
			f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/unsafe"] = p })
			require.False(t, hostguard.UnprotectedTerminalOrdinary(p))
			require.Error(t, s.reconcile(context.Background(), s.domains[n.Binding.NodeName]))
			f.Mutate(func(f *dsgatest.SchedulerAPI) { require.Zero(t, f.NodeWrites); require.Zero(t, f.PodWrites) })
		})
	}
}

func TestHistoricalOrdinaryChangedListIdentityIsRejectedWithoutWrites(t *testing.T) {
	for _, field := range []string{"uid", "version", "scheduler", "node", "termination", "protection", "gone"} {
		t.Run(field, func(t *testing.T) {
			s, f, n := snapshotBootstrapFixture(t)
			expected := historicalOrdinaryPod("changed", n.Binding.NodeName, core.PodSucceeded)
			actual := expected.DeepCopy()
			switch field {
			case "uid":
				actual.UID = "replacement-uid"
			case "version":
				actual.ResourceVersion = "new-observation"
			case "scheduler":
				actual.Spec.SchedulerName = "another-scheduler"
			case "node":
				actual.Spec.NodeName = "another-node"
			case "termination":
				actual.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.Now()
			case "protection":
				actual.Finalizers = []string{hostguard.PodProtectionFinalizer}
			}
			if field != "gone" {
				f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/changed"] = actual })
			}
			useWorkPages(s, map[string]*core.PodList{"": {Items: []core.Pod{*expected}}})
			require.Error(t, s.reconcile(context.Background(), s.domains[n.Binding.NodeName]))
			f.Mutate(func(f *dsgatest.SchedulerAPI) { require.Zero(t, f.NodeWrites); require.Zero(t, f.PodWrites) })
		})
	}
}

func TestHistoricalOrdinaryPagesDoNotExhaustActiveLedger(t *testing.T) {
	s, f, n := snapshotBootstrapFixture(t)
	pages := map[string]*core.PodList{"": {}, "next-256": {}, "next-512": {}}
	for i := range 512 {
		p := historicalOrdinaryPod(fmt.Sprintf("history-%03d", i), n.Binding.NodeName, core.PodSucceeded)
		p.UID = types.UID(p.Name + "-uid")
		key := ""
		if i >= 256 {
			key = "next-256"
		}
		pages[key].Items = append(pages[key].Items, *p)
		f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[p.Namespace+"/"+p.Name] = p })
	}
	pages[""].Continue = "next-256"
	pages["next-256"].Continue = "next-512"
	live := boundedPod("live-after-history", n.Binding.NodeName, "1Gi")
	pages["next-512"].Items = []core.Pod{*live}
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[live.Namespace+"/"+live.Name] = live })
	useWorkPages(s, pages)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	snapshot, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	require.Len(t, *snapshot.State().Placements, 1)
	_, err = snapshot.State().Placement(string(live.UID))
	require.NoError(t, err)
}

func TestHistoricalOrdinaryObservationNeverRemovesExistingCharge(t *testing.T) {
	s, f, n := snapshotBootstrapFixture(t)
	p := boundedPod("previously-charged", n.Binding.NodeName, "1Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[p.Namespace+"/"+p.Name] = p })
	ctx := context.Background()
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	terminal := historicalOrdinaryPod(p.Name, n.Binding.NodeName, core.PodSucceeded)
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[p.Namespace+"/"+p.Name] = terminal })
	before, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	require.ErrorContains(t, s.ObserveUntrackedTerminalOrdinary(ctx, n.Binding.NodeName, terminal), "original placement")
	after, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), after.State(), "历史观察没有任何释放或改写账本的权限")
}
