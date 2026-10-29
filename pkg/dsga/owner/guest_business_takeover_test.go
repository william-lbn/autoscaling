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

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// Root已实际设置但回复丢失后真实失租。新owner只观察旧命令，保持其原epoch、
// PID和限额；PG仍Planned，绝不能为了补齐正常Grow而派发旧owner的Resize。
// 随后取消按已增长的实际VM和Root成本结清，不能退回首次512MiB预算。正式
// Session/SDK/WAL/预算/Host实现执行，Root内核/PG/Kube/QMP仍为明确peer。
func verifyLostGuestTakeoverSettlement(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, runner *vmguard.Runner, qmp *vmQMPPeer, pg *initialServicePG, root *guestRootFixture, prepared budget.Prepared, transfer *businessOwnerTransfer, unreceived bool) {
	t.Helper()
	original := budgetStep(t, store, journal.GuestReady)
	require.Equal(t, journal.Unconfirmed, original.Phase)
	if !unreceived {
		require.NotNil(t, resources.client.Pending())
	}
	require.Equal(t, journal.Planned, budgetStep(t, store, journal.ResizePG).Phase)
	session, resources, worker = transfer.takeover(t, ctx, session, resources, worker, config, client, source, runner)
	if unreceived {
		require.NotNil(t, resources.client.Pending(), "未收到不能清未知；同SDK交正式恢复入口")
	} else {
		require.Nil(t, resources.client.Pending(), "启动gate只读核原命令并清SDK未知，外层仍未结清")
	}
	require.Equal(t, original, budgetStep(t, store, journal.GuestReady), "Root启动确认不能记录外层步骤")
	_, err := worker.ReplayResourceOriginal(ctx, prepared.Request.Key)
	require.Error(t, err, "新owner不能重放旧epoch Root命令")
	if unreceived {
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		_, err = store.RequestAbort(ctx, snapshot, session.Actor(), prepared.Request.Key)
		require.NoError(t, err)
		root.mu.Lock()
		root.loseCancelReply = true
		root.afterCancel = func() { api.mu.Lock(); api.loseJournalReply = true; api.mu.Unlock() }
		root.mu.Unlock()
		_, err = worker.Advance(ctx)
		var lostCancel *resourceguard.ClientError
		require.ErrorAs(t, err, &lostCancel, "Root取消屏障成功落盘后丢回复")
		require.NotNil(t, resources.client.Pending())
	} else {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	_, err = worker.Advance(ctx)
	var lostRecord *journal.UnconfirmedWrite
	require.ErrorAs(t, err, &lostRecord, "原Root只读确认后Record落盘但丢回复")
	settled := budgetStep(t, store, journal.GuestReady)
	require.Equal(t, journal.Settled, settled.Phase)
	wantResult := journal.Completed
	if unreceived {
		wantResult = journal.NoEffect
	}
	require.Equal(t, wantResult, settled.Observation.Result)
	require.Equal(t, original.CommandSHA256, settled.CommandSHA256)
	require.Equal(t, original.Command, settled.Command)
	if unreceived {
		var audit resourceexec.CancelledAudit
		require.NoError(t, json.Unmarshal(settled.Observation.Data, &audit))
		require.True(t, audit.Query, "丢回复后只能读原取消屏障")
		require.Equal(t, prepared.Request.Key.IssuerEpoch, audit.Cancellation.Command.IssuerEpoch)
		require.NoError(t, audit.Cancellation.ValidateFor(audit.Cancellation.Command, session.Actor().Epoch))
	} else {
		var audit resourceexec.Audit
		require.NoError(t, json.Unmarshal(settled.Observation.Data, &audit))
		require.True(t, audit.Query, "原Root完成证据只能来自只读Observe")
		require.Equal(t, prepared.Request.Key.IssuerEpoch, audit.Command.IssuerEpoch)
		require.Equal(t, session.Actor().Epoch, audit.Fence.RequestedEpoch)
	}
	require.Nil(t, resources.client.Pending(), "同SDK原Observe清原Pending，不靠再次重建")
	require.Equal(t, journal.Planned, budgetStep(t, store, journal.ResizePG).Phase)
	// 新owner的普通推进也不能把旧Planned当成新执行权；随后原Key取消，
	// 故障收口保留已发生Root/VM事实，并由实际预算生产者确认后完成Cleanup。
	var progress pgexec.Progress
	if !unreceived {
		progress, err = worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.ExternalStep, progress)
	}
	snapshot, err := store.Load(ctx)
	require.NoError(t, err)
	_, err = store.RequestAbort(ctx, snapshot, session.Actor(), prepared.Request.Key)
	require.NoError(t, err)
	for range 8 {
		snapshot, err = store.Load(ctx)
		require.NoError(t, err)
		if snapshot.State().Active == nil {
			break
		}
		progress, err = worker.Advance(ctx)
		require.NoError(t, err)
		require.Contains(t, []pgexec.Progress{pgexec.Recorded, pgexec.Reconciled}, progress)
	}
	snapshot, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, snapshot.State().Active)
	require.Nil(t, snapshot.State().InitialHeld)
	require.NotNil(t, snapshot.State().CapacityHeld)
	require.Equal(t, journal.AbortedAfterReconcile, snapshot.State().History[0].Outcome)
	require.NoError(t, snapshot.State().CheckClosedCapacity(prepared.Request))
	claims, err := budgetclaims.New(client.CoreV1(), binding())
	require.NoError(t, err)
	current, err := claims.ReadCurrent(ctx, snapshot)
	require.NoError(t, err)
	require.NoError(t, claims.RecheckCurrent(ctx, snapshot, current))
	allocator := pgTakeoverHostStore(t, ctx, client)
	coord, err := coordclient.NewForConfig(api.config)
	require.NoError(t, err)
	publisher, err := hostexec.New(client.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	changed, err := publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	node, err := allocator.Load(ctx)
	require.NoError(t, err)
	birth, _, _, err := vmguard.ReadPublished(ctx, client.CoreV1(), binding(), nil)
	require.NoError(t, err)
	placement, err := node.State().Placement(birth.Identity.RunnerPodUID)
	require.NoError(t, err)
	require.Equal(t, prepared.Request.TargetVMMemoryBytes, placement.MemoryBytes)
	require.Equal(t, prepared.Request.TargetVMMemoryBytes, placement.Settled.ActualMemoryBytes)
	root.mu.Lock()
	if unreceived {
		require.Zero(t, root.posts, "未接收取消不能调用任何Root setter")
		require.Equal(t, 1, root.cancellations, "丢回复恢复不能重复发送取消POST")
		require.Equal(t, 1, root.observes, "启动先尝试原Observe；随后查持久取消屏障")
	} else {
		require.Equal(t, 1, root.posts, "接管和取消不能重发Root setter")
		require.Equal(t, 2, root.observes, "启动gate和正式执行器分别只读核原命令")
	}
	root.mu.Unlock()
	_, _, resizes := pg.counts()
	require.Zero(t, resizes, "旧owner未派发PG，新owner不能补发")
	qmp.mu.Lock()
	require.Equal(t, 1, qmp.writes)
	qmp.mu.Unlock()
	// 仅验证下一次Prepare，不重复完整后继Grow；使用未Grow的实际PG代次，
	// 但VM仍为1GiB，二者不能从原目标或已关闭操作推导成一致的假快照。
	next := prepared.Request
	next.Key.IssuerEpoch = session.Actor().Epoch
	next.Key.Revision++
	next.Key.OperationID.Low++
	next.Kind, next.TargetBuffers = control.Grow, 1024
	next.BudgetCommitmentID = "post-guest-takeover-grow"
	pg.mu.Lock()
	pg.request = next
	pg.mu.Unlock()
	snapshot, err = store.Load(ctx)
	require.NoError(t, err)
	snapshot, _, err = store.Begin(ctx, snapshot, session.Actor(), next, 0)
	require.NoError(t, err)
	_, err = claims.ReadCurrent(ctx, snapshot)
	require.Error(t, err, "旧全局闭合读者仍须拒绝新Active，不放宽结清条件")
	_, err = claims.ReadActiveHeld(ctx, snapshot)
	require.NoError(t, err, "新在途读者只认证旧永久头来源，不授予旧操作执行权")
	t.Run("active-held-original-source-required", func(t *testing.T) {
		original, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, current.Closed.HostClaim.Name, metav1.GetOptions{})
		require.NoError(t, err)
		changed := original.DeepCopy()
		changed.Finalizers = nil
		require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
		defer func() {
			require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), original, original.Namespace))
		}()
		_, err = claims.ReadActiveHeld(ctx, snapshot)
		require.Error(t, err, "在途预算也必须保有原来源保护，不能因容量数值不变降级")
	})
	require.NoError(t, worker.inspectActiveService(ctx, snapshot), "后继Prepare前必须认证原永久头及合法Root，不能把安全关闭误当Prepare完成")
	progress, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	var following budget.Prepared
	preparedStep := budgetStep(t, store, journal.PrepareBudget)
	require.NotNil(t, preparedStep.Observation)
	require.NoError(t, json.Unmarshal(preparedStep.Observation.Data, &following))
	require.Equal(t, next, following.Request)
	require.Equal(t, prepared.Request.TargetVMMemoryBytes, following.VMBeforeMemoryBytes)
	require.NoError(t, claims.CheckOutside(ctx, following))
	snapshot, err = store.Load(ctx)
	require.NoError(t, err)
	require.NoError(t, worker.inspectActiveService(ctx, snapshot), "新Start已捕获原关闭链，不能因顶层边界撤销而误关合法后继")
}
