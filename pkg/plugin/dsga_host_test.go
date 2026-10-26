package plugin

import (
	"context"
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
	"github.com/neondatabase/autoscaling/pkg/plugin/state"
)

func TestActualFrameworkReserveCannotIgnoreHostCASAndUnknownReply(t *testing.T) {
	f, binding := dsgatest.NewSchedulerAPI(t, "node-holder")
	ctx := context.Background()
	profile := hostguard.Profile{ID: "qualified-test-only", SchedulerName: "neonvm-scheduler", VMOverheadBytes: 64 << 20, PodOverheadBytes: 16 << 20}
	store, err := hostguard.NewStore(f.Core.CoreV1(), f.Core.CoordinationV1(), binding, "node-holder")
	require.NoError(t, err)
	_, err = store.InitializeAllocations(ctx, "1", 1<<30, profile, []hostguard.Placement{})
	require.NoError(t, err)
	config := hostscheduler.Config{Nodes: []hostscheduler.NodeConfig{{Binding: binding, Profile: profile, SystemBytes: 1 << 30, InitializeAtResourceVersion: ""}}}
	host, err := hostscheduler.New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, config, profile.SchedulerName)
	require.NoError(t, err)
	metric := metrics.BuildPluginMetrics(prometheus.NewRegistry(), nil)
	// 只使用Reserve/Unreserve与ignored namespace路径，不启动无关watch/端口。
	//nolint:exhaustruct // 其余闭包/CPU规划字段不被本用例实际入口访问。
	s := &PluginState{config: Config{SchedulerName: profile.SchedulerName, IgnoredNamespaces: []string{"testing"}, K8sCRUDTimeoutSeconds: 3}, host: host, hostContext: ctx, metrics: metric}
	enforcer := &AutoscaleEnforcer{logger: zap.NewNop(), state: s, metrics: &metric.Framework}
	pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "bounded", UID: "bounded-uid", ResourceVersion: "1"}, Spec: core.PodSpec{SchedulerName: profile.SchedulerName, Containers: []core.Container{{Name: "app", Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("2Gi")}}}}}}
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/bounded"], f.LoseNodeReply = pod.DeepCopy(), true })
	status := enforcer.Reserve(ctx, framework.NewCycleState(), pod, binding.NodeName)
	require.NotNil(t, status)
	require.False(t, status.IsSuccess(), "ignored namespace也不得把未知容量写当调度成功")
	snapshot, err := store.Load(ctx)
	require.NoError(t, err)
	held, err := snapshot.State().Placement(string(pod.UID))
	require.NoError(t, err)
	require.EqualValues(t, 2<<30, held.MemoryBytes)
	version := snapshot.ResourceVersion()
	status = enforcer.Reserve(ctx, framework.NewCycleState(), pod, binding.NodeName)
	require.True(t, status.IsSuccess())
	enforcer.Unreserve(ctx, framework.NewCycleState(), pod, binding.NodeName)
	snapshot, err = store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, version, snapshot.ResourceVersion(), "框架Unreserve不自动删除未知/可能已绑定持有")
	large := pod.DeepCopy()
	large.Name, large.UID = "large", "large-uid"
	large.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("6Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/large"] = large })
	require.False(t, enforcer.Reserve(ctx, framework.NewCycleState(), large, binding.NodeName).IsSuccess())
}

func TestHostPlanningNeverDiscountsMemoryOrMigratesManagedVM(t *testing.T) {
	var p state.Pod
	p.Mem.Overcommit = resource.NewQuantity(3, resource.DecimalSI)
	p.Migratable, p.Migrating, p.AlwaysMigrate = true, true, true
	allocation := hostguard.Placement{Namespace: "testing", PodName: "runner", PodUID: "runner-uid", VMName: "vm", VMUID: "vm-uid", Incarnation: "1234567890abcdef1234567890abcdef", Managed: true, MemoryBytes: 1 << 30, HostOverheadBytes: 64 << 20}
	applyHostAllocation(&p, allocation)
	require.EqualValues(t, 1<<30, p.Mem.Reserved)
	require.Equal(t, p.Mem.Reserved, p.Mem.Requested)
	require.EqualValues(t, 1, p.Mem.Overcommit.Value())
	require.False(t, p.Migratable || p.Migrating || p.AlwaysMigrate)
}
