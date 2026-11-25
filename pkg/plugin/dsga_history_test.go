package plugin

import (
	"context"
	"testing"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/plugin/initevents"
	"github.com/neondatabase/autoscaling/pkg/plugin/reconcile"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// 真实插件初始事件中间件必须接受无占账的完整历史 Job，同时保留其他
// VM 的原持有。事件成功与新的 Framework Reserve 授权是不同的边界。
func TestHistoricalOrdinaryInitialEventsCompleteWithoutGrantOrWrites(t *testing.T) {
	for _, phase := range []core.PodPhase{core.PodSucceeded, core.PodFailed} {
		t.Run(string(phase), func(t *testing.T) {
			s, f, original, _, legacyCalls := retainedManagedPluginFixture(t, false)
			pod := original.DeepCopy()
			pod.Name, pod.UID, pod.ResourceVersion = "historical-job", "historical-job-uid", "original-history-rv"
			pod.OwnerReferences, pod.Annotations, pod.Labels, pod.Finalizers, pod.DeletionTimestamp = nil, nil, nil, nil, nil
			pod.Spec.SchedulerName = core.DefaultSchedulerName
			pod.Spec.Containers[0].Name = "job"
			pod.Status.Phase = phase
			stamp := metav1.Now()
			id := "containerd://historical-job"
			pod.Status.ContainerStatuses = []core.ContainerStatus{{Name: "job", ContainerID: id, State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: id, StartedAt: stamp, FinishedAt: stamp}}}}
			f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[pod.Namespace+"/"+pod.Name] = pod.DeepCopy() })
			pod, err := f.Core.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			var ledger string
			var writes [3]int
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				ledger = f.Node.Annotations[hostguard.LedgerAnnotation]
				writes = [3]int{f.NodeWrites, f.PodWrites, f.VMWrites}
			})
			pod.SetGroupVersionKind(core.SchemeGroupVersion.WithKind("Pod"))
			init := initevents.NewInitEventsMiddleware()
			init.AddRequired(pod)
			done := init.Done()
			params := reconcile.ObjectParams{GVK: core.SchemeGroupVersion.WithKind("Pod"), UID: pod.UID, Name: pod.Name, Namespace: pod.Namespace, EventKind: reconcile.EventKindAdded, Obj: pod}
			_, err = init.Call(zap.NewNop(), params, func(logger *zap.Logger, params reconcile.ObjectParams) (reconcile.Result, error) {
				result, err := s.HandlePodEvent(logger, params.EventKind, params.Obj.(*core.Pod))
				if result == nil {
					return reconcile.Result{}, err
				}
				return *result, err
			})
			require.NoError(t, err)
			select {
			case <-done:
			default:
				t.Fatal("完整历史普通 Job 不得锁死插件初始化")
			}
			require.Empty(t, init.Remaining())
			require.Zero(t, *legacyCalls)
			enforcer := &AutoscaleEnforcer{logger: zap.NewNop(), state: s, metrics: &s.metrics.Framework}
			require.False(t, enforcer.Reserve(context.Background(), framework.NewCycleState(), pod, pod.Spec.NodeName).IsSuccess())
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Equal(t, writes, [3]int{f.NodeWrites, f.PodWrites, f.VMWrites})
				require.Equal(t, ledger, f.Node.Annotations[hostguard.LedgerAnnotation])
			})
		})
	}
}
