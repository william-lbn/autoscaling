package owner

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/tools/leaderelection"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 完整首次生产后，以真实Prepare/Host/WAL覆盖两个取消窗口：VM未发送时
// 封为不可重发NoEffect；已经发送时只读原Completed，不把1GiB实际值改成
// 512MiB初始值。PG/root/资格/Kube/QMP仍为显式协议夹具，不外推Linux效果。
func verifyInitialCancelledGrow(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, coord coordclient.CoordinationV1Interface, store *journal.Store, session *Session, worker *PGWorker, resources *ResourceWorker, config compute.Config, source budget.QualificationSource, runner *vmguard.Runner, qmp *vmQMPPeer, request control.Request, beforeVM bool, transfer *businessOwnerTransfer, pg *initialServicePG, root *guestRootFixture) (*PGWorker, control.Request, *hostexec.Publisher, *Session, *ResourceWorker) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	initial, err := s.State().InitialBudgetFor()
	require.NoError(t, err)
	_, disposition, err := store.Begin(ctx, s, session.Actor(), request, 1)
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	var prepared budget.Prepared
	require.NoError(t, json.Unmarshal(budgetStep(t, store, journal.PrepareBudget).Observation.Data, &prepared))
	publisher, allocator := actualHostPublisher(t, client, coord, store, session)
	keepInitialHostOwner(t, ctx, client, allocator)
	_, err = publisher.Reconcile(ctx)
	require.NoError(t, err)
	advances := 2
	if beforeVM {
		advances = 1 // 只确认Host；不Start VM，不给原Runner登记取消命令。
	}
	for range advances {
		progress, err = worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
	}
	vmEffect := transfer != nil && transfer.effectBeforeTakeover
	if vmEffect {
		// 原Runner真正越过may_have_sent屏障并发送一次QMP，peer已经
		// 改变实际容量却丢回复。外层仍Unconfirmed，不能靠相同当前
		// 数值取消原动作，也不能新造一个VM完成命令来代替原来源。
		qmp.mu.Lock()
		qmp.lose = true
		qmp.mu.Unlock()
		require.ErrorIs(t, runner.Tick(ctx), vmguard.ErrUnknown)
		require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizeVM).Phase)
	}
	if transfer != nil {
		// 切换时真实ResizeVM WAL已经Start；是否发送由原Runner记录
		// 决定。原Key/command/Initial/Host持有必须原样跨代次保存；
		// 新owner只可对账/取消，不能把旧目标重新包装为新epoch派发。
		session, resources, worker = transfer.takeover(t, ctx, session, resources, worker, config, client, source, runner)
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	_, err = store.RequestAbort(ctx, s, session.Actor(), request.Key)
	require.NoError(t, err)
	if beforeVM {
		require.Equal(t, journal.Planned, budgetStep(t, store, journal.ResizeVM).Phase)
		verifyInitialVMSourceRejection(t, ctx, client, store, session, initial)
	} else if vmEffect {
		progress, err = worker.Advance(ctx)
		require.NoError(t, err, "已发生的原VM动作只能读回Completed，不能写NoEffect")
		require.Equal(t, pgexec.Recorded, progress)
		require.Equal(t, journal.Completed, budgetStep(t, store, journal.ResizeVM).Observation.Result)
	} else {
		_, err = worker.Advance(ctx)
		require.ErrorIs(t, err, vmguard.ErrUnknown, "Abort标记本身不是VM无效果回执")
		require.NoError(t, runner.Tick(ctx))
		progress, err = worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		require.Equal(t, journal.NoEffect, budgetStep(t, store, journal.ResizeVM).Observation.Result)
	}
	qmp.mu.Lock()
	if vmEffect {
		require.Equal(t, 1, qmp.writes, "新owner没有重发已可能发送的旧QMP")
	} else {
		require.Zero(t, qmp.writes)
	}
	qmp.mu.Unlock()
	beforeCleanup, err := store.Load(ctx)
	require.NoError(t, err)
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, err = worker.Advance(ctx)
	var unknown *journal.UnconfirmedWrite
	require.ErrorAs(t, err, &unknown, "原Cleanup Start回复未知不得发任何新setter")
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, s.State().InitialHeld)
	require.Nil(t, s.State().CapacityHeld)
	originalCleanup := s.State().Active.Cleanup
	require.NotNil(t, originalCleanup)
	if !beforeVM && !vmEffect {
		require.Greater(t, len(originalCleanup.Command), journal.MaxCommandBytes, "真实完整初始合同覆盖专用扩界路径")
	}
	require.LessOrEqual(t, len(originalCleanup.Command), journal.MaxInitialCapacityCommandBytes)
	oversized := append([]byte(nil), originalCleanup.Command...)
	oversized = append(oversized, []byte(strings.Repeat(" ", journal.MaxInitialCapacityCommandBytes-len(oversized)+1))...)
	_, err = beforeCleanup.State().StartCleanup(session.Actor(), request.Key, request.Key.Instance.ComputeInstanceID, oversized)
	require.Error(t, err, "即使typed预算合法，超过专用16KiB仍拒绝")
	ordinary := []byte(`{"padding":"` + strings.Repeat("x", journal.MaxCommandBytes) + `"}`)
	_, err = beforeCleanup.State().StartCleanup(session.Actor(), request.Key, request.Key.Instance.ComputeInstanceID, ordinary)
	require.Error(t, err, "普通Cleanup不能借新合同扩界超过8KiB")
	var command budget.ReconciledAbortCommand
	require.NoError(t, json.Unmarshal(originalCleanup.Command, &command))
	if beforeVM {
		require.EqualValues(t, 3, command.Capacity.ProtocolVersion)
		require.NotNil(t, command.Capacity.InitialVM)
		require.Empty(t, command.Capacity.VM, "启动容量没有Resize/NoEffect回执")
		require.NotContains(t, string(originalCleanup.Command), "native_vm")
		require.Equal(t, journal.Planned, budgetStep(t, store, journal.ResizeVM).Phase)
	} else if vmEffect {
		require.EqualValues(t, 1, command.Capacity.ProtocolVersion)
		require.Nil(t, command.Capacity.InitialOutside, "旧512MiB初始预算不能覆盖已长到1GiB的VM")
		require.Equal(t, *prepared.OutsideClaim, command.Capacity.OutsideClaim)
		require.Equal(t, request, command.Capacity.OutsideRequest)
	} else {
		require.EqualValues(t, 2, command.Capacity.ProtocolVersion)
	}
	if !vmEffect {
		require.Equal(t, initial, *command.Capacity.InitialOutside)
		require.Empty(t, command.Capacity.OutsideClaim)
		require.Empty(t, command.Capacity.OutsideRequest)
	}
	require.Equal(t, request, command.Capacity.Request)
	require.Nil(t, command.Admission, "首次服务仍开放，不制造新开放选择")
	var substituted budget.ReconciledAbortCommand
	require.NoError(t, json.Unmarshal(originalCleanup.Command, &substituted))
	if vmEffect {
		substituted.Capacity.OutsideClaim.UID = "replacement-prepared-outside-uid"
	} else {
		substituted.Capacity.InitialOutside.HostProfileID = "replacement-initial-host-profile"
	}
	require.NoError(t, substituted.ValidateFor(session.Actor().Epoch), "数值自洽不能代替原完整预算绑定")
	if vmEffect {
		// 外部CM的实际UID不是纯Model可认证的事实。必须走真实Reader，
		// 先确认原来源可读，再证明同名换UID被拒绝；不能放松生产校验，
		// 也不能把静态格式校验错误当成Kube来源已经认证。
		reader, readErr := budgetclaims.New(client.CoreV1(), binding())
		require.NoError(t, readErr)
		_, readErr = reader.ReadReconciled(ctx, s, command.Capacity)
		require.NoError(t, readErr)
		_, readErr = reader.ReadReconciled(ctx, s, substituted.Capacity)
		require.Error(t, readErr, "外部原Outside的真实UID拒绝来源替换")
	} else {
		require.Error(t, s.State().CheckReconciledPlan(substituted), "写前原Initial摘要拒绝同数值来源替换")
	}
	rebuild := func() *PGWorker {
		worker.CloseIdleConnections()
		next, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
		require.NoError(t, err)
		t.Cleanup(next.CloseIdleConnections)
		return next
	}
	worker = rebuild()
	qualification, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, source.Claim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	beforeSourceFailure, err := store.Load(ctx)
	require.NoError(t, err)
	hostBefore, err := allocator.Load(ctx)
	require.NoError(t, err)
	postsBefore, _, resizesBefore := pg.counts()
	root.mu.Lock()
	rootPostsBefore := root.posts
	root.mu.Unlock()
	qmp.mu.Lock()
	vmWritesBefore := qmp.writes
	qmp.mu.Unlock()
	require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, source.Claim.Name))
	progress, err = worker.Advance(ctx)
	require.NoError(t, err, "来源缺失应由公开入口完成独立安全关闭")
	require.Equal(t, pgexec.Recorded, progress)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	closed := s.State().ServiceClosure
	require.NotNil(t, closed)
	require.Equal(t, "cleanup_source_unconfirmed", closed.Reason)
	require.False(t, closed.Pending())
	require.False(t, pg.shared().Observed.AdmissionOpen)
	require.Equal(t, beforeSourceFailure.State().Active, s.State().Active, "关闭不得把原Cleanup未知写成成功")
	require.Equal(t, originalCleanup, s.State().Active.Cleanup)
	require.Equal(t, beforeSourceFailure.State().InitialHeld, s.State().InitialHeld)
	require.Nil(t, s.State().CapacityHeld, "关闭不提前替换实际预算或释放峰值")
	hostClosed, err := allocator.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, hostBefore.State(), hostClosed.State(), "原Host峰值仍完整持有")
	postsClosed, _, resizesClosed := pg.counts()
	require.Equal(t, postsBefore+1, postsClosed, "仅派发一次独立关闭")
	require.Equal(t, resizesBefore, resizesClosed)
	require.NoError(t, client.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), qualification, qualification.Namespace))
	worker = rebuild()
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, ErrServiceSafety, "来源恢复和SDK重建不能自动重开或继续原Cleanup")
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, closed, s.State().ServiceClosure)
	require.Equal(t, beforeSourceFailure.State().Active, s.State().Active)
	postsAfterWait, _, resizesAfterWait := pg.counts()
	require.Equal(t, postsClosed, postsAfterWait)
	require.Equal(t, resizesBefore, resizesAfterWait)

	// 正式Run管理入口按原关闭摘要显式选择恢复；不补写旧Cleanup完成。
	// 原PG/Root/QMP仍为协议peer，预算认证、SDK、WAL与CAS均走生产实现。
	pg.mu.Lock()
	pg.reopenFixture = &serviceReopenPG{mode: "normal", api: api, root: root}
	pg.mu.Unlock()
	endpoint, stop := reopenRun(t, ctx, worker)
	_, _, _, selector := closed.Selection()
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
	require.NoError(t, err)
	selected, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, beforeSourceFailure.State().Active, selected.State().Active)
	require.Equal(t, beforeSourceFailure.State().InitialHeld, selected.State().InitialHeld)
	require.Nil(t, selected.State().CapacityHeld)
	require.NotNil(t, selected.State().ServiceClosure.CleanupRecovery)
	require.Equal(t, "actual", selected.State().ServiceClosure.CleanupRecovery.Kind)
	reopening := selected.State().ServiceClosure.Reopen
	require.NotNil(t, reopening)
	require.Equal(t, journal.Settled, reopening.Phase)
	require.True(t, pg.shared().Observed.AdmissionOpen)
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, reopening.CommandSHA256))
	require.ErrorAs(t, err, &unknown, "恢复预算、覆盖、水位与清Active同CAS，丢回复必须保留未知")
	stop()
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().InitialHeld, "只在完整恢复同CAS替换初始头")
	require.NotNil(t, s.State().CapacityHeld)
	require.NotNil(t, s.State().CapacityHeld.Recovery)
	require.Equal(t, *beforeSourceFailure.State().Active, s.State().CapacityHeld.Recovery.Original)
	require.Equal(t, journal.Unconfirmed, s.State().CapacityHeld.Recovery.Original.Cleanup.Phase)
	require.Nil(t, s.State().Active)
	require.Nil(t, s.State().ServiceClosure)
	worker = rebuild()
	endpoint, stop = reopenRun(t, ctx, worker)
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, reopening.CommandSHA256))
	require.NoError(t, err, "完成回复丢失后只确认同一恢复证据，不能再次开放")
	stop()
	s, err = store.Load(ctx)
	require.NoError(t, err)
	record := s.State().History[len(s.State().History)-1]
	require.Equal(t, journal.RecoveredAfterCancellation, record.Outcome)
	require.Equal(t, *beforeSourceFailure.State().Active, record.Operation)
	require.NotNil(t, record.ServiceCoverage)
	require.NotNil(t, record.ServiceCoverage.CleanupRecovery)
	require.NoError(t, s.State().CheckClosedCapacity(request))
	pg.mu.Lock()
	reopenPosts, reopenCloses, resizePosts := pg.reopenFixture.posts, pg.reopenFixture.closes, pg.resizePosts
	pg.reopenFixture = nil
	pg.mu.Unlock()
	require.Equal(t, 1, reopenPosts, "显式恢复只开放一次；完成重试不重发")
	require.Zero(t, reopenCloses)
	require.Equal(t, resizesBefore, resizePosts, "恢复不重发PG变配")
	root.mu.Lock()
	rootPostsAfter := root.posts
	root.mu.Unlock()
	require.Equal(t, rootPostsBefore, rootPostsAfter, "恢复不新增Root设置")
	qmp.mu.Lock()
	vmWritesAfter := qmp.writes
	qmp.mu.Unlock()
	require.Equal(t, vmWritesBefore, vmWritesAfter, "恢复不发送反向或重复QMP")
	hostRecovered, err := allocator.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, hostBefore.State(), hostRecovered.State(), "完整恢复后仍须原Host发布者显式结清")
	retained, err := s.State().RetainedInitialOutside()
	if vmEffect {
		require.Error(t, err, "新实际预算使用新容量资格，不能保留旧Initial当完整证明")
	} else {
		require.NoError(t, err)
		require.Equal(t, initial, retained)
	}
	claims, err := budgetclaims.New(client.CoreV1(), binding())
	require.NoError(t, err)
	current, err := claims.ReadCurrent(ctx, s)
	require.NoError(t, err)
	require.NoError(t, claims.RecheckCurrent(ctx, s, current))
	releasing := qualification.DeepCopy()
	stamp := metav1.Now()
	releasing.DeletionTimestamp = &stamp
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), releasing, releasing.Namespace))
	_, err = claims.ReadCurrent(ctx, s)
	require.Error(t, err, "原Initial头替换后也不能忽略资格已进入释放")
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), qualification, qualification.Namespace))
	// 用真实原Host发布者结清取消峰值；原native receipt继续保存。
	// 未发送取before=512MiB，已发送取原Completed=1GiB；不能按原目标
	// 推断未发生的VM效果，也不能因PG尚未变化把已发生效果回写成初始值。
	changed, err := publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	n, err := allocator.Load(ctx)
	require.NoError(t, err)
	placement, err := n.State().Placement(initial.RunnerPodUID)
	require.NoError(t, err)
	actualMemory := initial.VMMemoryBytes
	if vmEffect {
		actualMemory = request.TargetVMMemoryBytes
	}
	require.Equal(t, actualMemory, placement.MemoryBytes)
	require.Equal(t, request.TargetVMMemoryBytes, placement.Settled.Request.TargetVMMemoryBytes)
	require.Equal(t, actualMemory, placement.Settled.ActualMemoryBytes)
	if beforeVM {
		require.EqualValues(t, 8, n.State().ProtocolVersion)
		require.Equal(t, *command.Capacity.InitialVM, placement.Settled.InitialVM)
		require.Empty(t, placement.Settled.Runner)
		// 结清后同一读者仍必须认证原无命令状态，不能只依赖Node v8。
		verifyInitialVMSourceRejection(t, ctx, client, store, session, initial)
	}
	_, err = claims.ReadCurrent(ctx, s)
	require.NoError(t, err, "初始头已被替换且Host已结清，原资格仍可持续核验")
	// 后继普通成功用新真实Key，原取消请求和实际预算不重命名。
	request.Key.Revision++
	request.Key.OperationID.Low++
	request.Key.IssuerEpoch = session.Actor().Epoch
	request.BudgetCommitmentID = "initial-cancel-successor"
	return worker, request, publisher, session, resources
}

// 多操作组合超过原单操作夹具的60秒静态Lease寿命。直接复用正式Host
// LeaseLock/client-go选主续约，固定原UID/holder/Node，不反复创建同名Lease
// 或延长生产有效期。Kube写入仍是明确fixture；真实RBAC/etcd后验。
func keepInitialHostOwner(t *testing.T, ctx context.Context, client *kubefake.Clientset, allocator *hostguard.Store) func() {
	t.Helper()
	lock, err := hostguard.NewLeaseLock(client.CoordinationV1(), allocator.Binding(), "host-fixture-holder")
	require.NoError(t, err)
	ready := make(chan struct{})
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{Lock: lock, LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second, Callbacks: leaderelection.LeaderCallbacks{OnStartedLeading: func(owned context.Context) { close(ready); <-owned.Done() }, OnStoppedLeading: func() {}}, ReleaseOnCancel: false, Name: "initial-cancel-original-host"})
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); elector.Run(runCtx) }()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("original Host fixture owner did not stop")
		}
	}
	t.Cleanup(stop)
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("original Host fixture owner was not acquired")
	}
	return stop
}
