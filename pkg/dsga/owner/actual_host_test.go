package owner

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"

	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostscheduler"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// Agent/Runner已经真正执行原VM扩容，PG/Root/平台上界仍为原组合的明确
// 协议夹具。alreadyClosed为false的旧用例人工组装typed Cleanup，独立检验
// 消费链；为true的新用例须先由实际自动执行器关闭。两者都禁止注入永久头。
func verifyActualHostSettlement(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, store *journal.Store, session *Session, worker *PGWorker, aggregate budget.RootAggregate, shared control.SharedCostResponse, command budget.ReconciledAbortCommand, alreadyClosed bool) {
	t.Helper()
	coord, err := coordclient.NewForConfig(api.config)
	require.NoError(t, err)
	node, err := client.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	var initial hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &initial))
	allocator, err := hostguard.NewStore(client.CoreV1(), client.CoordinationV1(), initial.Binding, "host-fixture-holder")
	require.NoError(t, err)
	publisher, err := hostexec.New(client.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	s, err := store.Load(ctx)
	require.NoError(t, err)
	var changed bool
	if !alreadyClosed {
		changed, err = publisher.SettleCurrent(ctx)
		require.NoError(t, err)
		require.False(t, changed, "Cleanup未确认不得结清")
		fence, err := worker.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: command.Capacity.Request.Key.Instance, IssuerEpoch: session.Actor().Epoch})
		require.NoError(t, err)
		receipt := budget.ReconciledAbortReceipt{ProtocolVersion: 1, Command: command, Query: true, Fence: fence, Shared: shared, Response: nil, PGAggregate: aggregate, ResourceEpoch: session.Actor().Epoch}
		require.NoError(t, receipt.ValidateFor(command, session.Actor().Epoch, session.Actor().Epoch))
		step := s.State().Active.Cleanup
		body, err := json.Marshal(receipt)
		require.NoError(t, err)
		s, err = store.Record(ctx, s, session.Actor(), command.Capacity.Request.Key, journal.Observation{Key: command.Capacity.Request.Key, RequestSHA256: s.State().Active.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: step.TargetID, Data: body})
		require.NoError(t, err)
		changed, err = publisher.SettleCurrent(ctx)
		require.NoError(t, err)
		require.False(t, changed, "实际预算落账仍不等于原Active已关闭")
		s, err = store.Finish(ctx, s, session.Actor(), command.Capacity.Request.Key)
		require.NoError(t, err)
	}
	require.Nil(t, s.State().Active)
	require.NoError(t, s.State().CheckClosedCapacity(command.Capacity.Request))
	require.Equal(t, journal.AbortedAfterReconcile, s.State().History[len(s.State().History)-1].Outcome)
	closed, err := hostguard.SettlementFor(s.State())
	require.NoError(t, err)
	require.Equal(t, command.Capacity.VMMemoryBytes, closed.ActualMemoryBytes)
	require.Less(t, command.Capacity.Buffers, command.Capacity.Request.TargetBuffers, "不能把VM完成投影成PG目标成功")
	changed, err = publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	current, err := allocator.Load(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 7, current.State().ProtocolVersion)
	row, err := current.State().Placement("runner-uid")
	require.NoError(t, err)
	require.Equal(t, closed.MemoryBytes(), row.MemoryBytes)
	require.Equal(t, closed, *row.Settled)
	claims, err := budgetclaims.New(client.CoreV1(), store.Binding())
	require.NoError(t, err)
	read, err := claims.ReadCurrent(ctx, s)
	require.NoError(t, err)
	require.Equal(t, closed, read.Closed)
	require.NoError(t, claims.RecheckCurrent(ctx, s, read))
	changed, err = publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.False(t, changed, "同原结清恢复不产生新写")
	vm, err := vmclient.NewForConfig(api.config)
	require.NoError(t, err)
	config := hostscheduler.Config{Nodes: []hostscheduler.NodeConfig{{Binding: initial.Binding, Profile: *initial.Profile, SystemBytes: initial.OutsideBytes, InitializeAtResourceVersion: ""}}}
	scheduler, err := hostscheduler.New(client.CoreV1(), client.CoordinationV1(), vm, config, initial.Profile.SchedulerName)
	require.NoError(t, err)
	pod, err := client.CoreV1().Pods("testing").Get(ctx, "runner", metav1.GetOptions{})
	require.NoError(t, err)
	pod.Spec.SchedulerName = initial.Profile.SchedulerName
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
	_, allocated, err := scheduler.Reserve(ctx, initial.Binding.NodeName, pod, 0)
	require.NoError(t, err)
	require.Equal(t, closed.MemoryBytes(), allocated.MemoryBytes, "下一轮恢复必须消费实际头，不能回退到旧spec.Use")
	require.Equal(t, closed, *allocated.Settled)
	// 结清后只读持有仍核原CM真实UID；同名替换不能继续消费，也不清账。
	outside, err := client.CoreV1().ConfigMaps("testing").Get(ctx, command.Capacity.OutsideClaim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	outside.UID = "replacement-outside-uid"
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), outside, outside.Namespace))
	_, err = claims.ReadCurrent(ctx, s)
	require.Error(t, err)
	_, err = publisher.SettleCurrent(ctx)
	require.Error(t, err)
	unchanged, err := allocator.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, current.State(), unchanged.State(), "来源变化保留持有，不靠同名CM恢复许可")
}
