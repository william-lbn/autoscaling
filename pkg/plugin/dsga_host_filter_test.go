package plugin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostscheduler"
	"github.com/neondatabase/autoscaling/pkg/plugin/metrics"
)

func TestActualFrameworkFilterRejectsHostCapacityBeforeLocalPlanning(t *testing.T) {
	f, b := dsgatest.NewSchedulerAPI(t, "node-holder")
	ctx := context.Background()
	profile := hostguard.Profile{ID: "filter-test-only", SchedulerName: "neonvm-scheduler", VMOverheadBytes: 3 << 30, PodOverheadBytes: 16 << 20}
	store, err := hostguard.NewStore(f.Core.CoreV1(), f.Core.CoordinationV1(), b, "node-holder")
	require.NoError(t, err)
	_, err = store.InitializeAllocations(ctx, "1", 1<<30, profile, []hostguard.Placement{})
	require.NoError(t, err)
	host, err := hostscheduler.New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, hostscheduler.Config{Nodes: []hostscheduler.NodeConfig{{Binding: b, Profile: profile, SystemBytes: 1 << 30}}}, profile.SchedulerName)
	require.NoError(t, err)
	pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "candidate", UID: "candidate-uid", ResourceVersion: "1"}, Spec: core.PodSpec{SchedulerName: profile.SchedulerName, Containers: []core.Container{{Name: "app", Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("11Gi")}}}}}}
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/candidate"] = pod.DeepCopy() })
	metric := metrics.BuildPluginMetrics(prometheus.NewRegistry(), nil)
	s := &PluginState{config: Config{SchedulerName: profile.SchedulerName, K8sCRUDTimeoutSeconds: 3}, host: host, hostContext: ctx}
	e := &AutoscaleEnforcer{logger: zap.NewNop(), state: s, metrics: &metric.Framework}
	node := framework.NewNodeInfo()
	node.SetNode(&core.Node{ObjectMeta: metav1.ObjectMeta{Name: b.NodeName}})
	var before []byte
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		before, _ = json.Marshal([]any{f.Node, f.Lease, f.Pods, f.NodeWrites, f.PodWrites, f.VMWrites})
	})
	status := e.Filter(ctx, framework.NewCycleState(), pod, node)
	require.Equal(t, framework.Unschedulable, status.Code())
	require.Contains(t, status.Message(), "original Host capacity not confirmed")
	require.Contains(t, status.Message(), "capacity")
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		after, err := json.Marshal([]any{f.Node, f.Lease, f.Pods, f.NodeWrites, f.PodWrites, f.VMWrites})
		require.NoError(t, err)
		require.Equal(t, string(before), string(after))
	})
	// 不配置Host/候选在域外时，不引入新的API/容量拒绝；旧规划自己决策。
	require.NoError(t, (&PluginState{}).checkHost(ctx, pod, b.NodeName))
	require.NoError(t, s.checkHost(ctx, pod, "foreign-node"))
}
