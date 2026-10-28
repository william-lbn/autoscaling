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

// 完整首次生产后，用真实Prepare/Host/WAL派发VM，再由原Runner在QMP
// 发送前不可重发取消。目标1GiB、真实仍512MiB，不能改写原目标或注入旧
// Resize成功。PG/root/资格/Kube/QMP仍为显式协议夹具，不外推Linux效果。
func verifyInitialCancelledGrow(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, coord coordclient.CoordinationV1Interface, store *journal.Store, session *Session, worker *PGWorker, resources *ResourceWorker, config compute.Config, source budget.QualificationSource, runner *vmguard.Runner, qmp *vmQMPPeer, request control.Request, beforeVM bool, transfer *businessOwnerTransfer) (*PGWorker, control.Request, *hostexec.Publisher, *Session, *ResourceWorker) {
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
	if transfer != nil {
		// 切换发生在真实ResizeVM WAL已经Start、Runner尚未发送QMP的
		// 窗口。原Key/command/Initial/Host持有必须原样跨代次保存；
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
	require.Zero(t, qmp.writes)
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
	if !beforeVM {
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
	} else {
		require.EqualValues(t, 2, command.Capacity.ProtocolVersion)
	}
	require.Equal(t, initial, *command.Capacity.InitialOutside)
	require.Equal(t, request, command.Capacity.Request)
	require.Empty(t, command.Capacity.OutsideClaim)
	require.Empty(t, command.Capacity.OutsideRequest)
	require.Nil(t, command.Admission, "首次服务仍开放，不制造新开放选择")
	var substituted budget.ReconciledAbortCommand
	require.NoError(t, json.Unmarshal(originalCleanup.Command, &substituted))
	substituted.Capacity.InitialOutside.HostProfileID = "replacement-initial-host-profile"
	require.NoError(t, substituted.ValidateFor(session.Actor().Epoch), "数值自洽不能代替原完整预算绑定")
	require.Error(t, s.State().CheckReconciledPlan(substituted), "写前原Initial摘要拒绝同数值来源替换")
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
	require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, source.Claim.Name))
	_, err = worker.Advance(ctx)
	require.Error(t, err, "初始资格暂缺必须保留原Cleanup未知与峰值")
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, originalCleanup, s.State().Active.Cleanup)
	require.NoError(t, client.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), qualification, qualification.Namespace))
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, err = worker.Advance(ctx)
	require.ErrorAs(t, err, &unknown, "完整实际预算Record已落盘但回复丢失")
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().InitialHeld, "只在完整实际预算同CAS替换初始头")
	require.NotNil(t, s.State().CapacityHeld)
	worker = rebuild()
	progress, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active)
	require.Equal(t, journal.AbortedAfterReconcile, s.State().History[0].Outcome)
	require.NoError(t, s.State().CheckClosedCapacity(request))
	retained, err := s.State().RetainedInitialOutside()
	require.NoError(t, err)
	require.Equal(t, initial, retained)
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
	// 用真实原Host发布者结清取消峰值；原native receipt继续保存，
	// 512MiB来自取消命令before，绝不能用1GiB原目标当完成。
	changed, err := publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	n, err := allocator.Load(ctx)
	require.NoError(t, err)
	placement, err := n.State().Placement(initial.RunnerPodUID)
	require.NoError(t, err)
	require.Equal(t, initial.VMMemoryBytes, placement.MemoryBytes)
	require.Equal(t, request.TargetVMMemoryBytes, placement.Settled.Request.TargetVMMemoryBytes)
	require.Equal(t, initial.VMMemoryBytes, placement.Settled.ActualMemoryBytes)
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
func keepInitialHostOwner(t *testing.T, ctx context.Context, client *kubefake.Clientset, allocator *hostguard.Store) {
	t.Helper()
	lock, err := hostguard.NewLeaseLock(client.CoordinationV1(), allocator.Binding(), "host-fixture-holder")
	require.NoError(t, err)
	ready := make(chan struct{})
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{Lock: lock, LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second, Callbacks: leaderelection.LeaderCallbacks{OnStartedLeading: func(owned context.Context) { close(ready); <-owned.Done() }, OnStoppedLeading: func() {}}, ReleaseOnCancel: false, Name: "initial-cancel-original-host"})
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); elector.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("original Host fixture owner did not stop")
		}
	})
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("original Host fixture owner was not acquired")
	}
}
