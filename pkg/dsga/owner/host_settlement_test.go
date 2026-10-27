package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	kt "k8s.io/client-go/testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 复用实际Agent/Runner缩容组合后的事实，新增真正Publisher结清/当前预算只读/
// 垃圾源恢复以及实际Scheduler循环。PG/root/资格及Kube/QMP peer仍是原用例
// 明确夹具，尤其不能把承诺归还写成Mac已证明Linux物理内存回收。
func verifyClosedHostSettlement(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, coord coordclient.CoordinationV1Interface, store *journal.Store, session *Session, worker *PGWorker, runner *vmguard.Runner, peer *vmQMPPeer, mode string) {
	t.Helper()
	latest, err := store.Load(ctx)
	require.NoError(t, err)
	closed, err := hostguard.SettlementFor(latest.State())
	require.NoError(t, err)
	var prepared budget.Prepared
	for _, step := range latest.State().Active.Steps {
		if step.Action == journal.PrepareBudget {
			require.NoError(t, json.Unmarshal(step.Observation.Data, &prepared))
		}
	}
	node, err := client.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	var initial hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &initial))
	allocator, err := hostguard.NewStore(client.CoreV1(), client.CoordinationV1(), initial.Binding, "host-fixture-holder")
	require.NoError(t, err)
	publisher, err := hostexec.New(client.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	changed, err := publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.False(t, changed, "Resume已完成但原Active未闭合，仍不能结清")
	// 把Node容量收紧到4GiB：旧峰值2GiB+系统1GiB+开销后不足另一1GiB。
	node.Status.Allocatable[core.ResourceMemory] = resource.MustParse("4Gi")
	node.ResourceVersion = "host-capacity-for-settlement"
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), node, ""))
	snapshot, err := allocator.Load(ctx)
	require.NoError(t, err)
	waiting := hostguard.Placement{Namespace: "testing", PodName: "waiting", PodUID: "waiting-uid", VMName: "", VMUID: "", Incarnation: "", Managed: false, MemoryBytes: 1 << 30, HostOverheadBytes: 16 << 20, Settled: nil}
	// 新Node包含v5的终态回收约束，授予前须核真实原Pod/保护标记；不能让
	// 旧v4夹具里根本不存在的Pod越过该验证，也不为通过测试放宽生产检查。
	_, err = client.CoreV1().Pods("testing").Create(ctx, &core.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "waiting", UID: "waiting-uid", ResourceVersion: "waiting-rv-1", Finalizers: []string{hostguard.PodProtectionFinalizer}}, Status: core.PodStatus{Phase: core.PodPending}}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, _, err = allocator.HoldPlacement(ctx, snapshot, waiting)
	require.Error(t, err, "峰值未结清前不能授予等待Pod")
	before, err := snapshot.State().UsedBytes()
	require.NoError(t, err)
	currentCM, err := client.CoreV1().ConfigMaps("testing").Get(ctx, closed.HostClaim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var host budget.HostReservation
	require.NoError(t, json.Unmarshal([]byte(currentCM.Data[budgetclaims.ClaimKey]), &host))
	old := host
	old.Request.Key.Revision--
	old.Request.Key.OperationID.Low++
	old.SourceID = hostguard.SourceID(initial.Origin, hostguard.Plan{Namespace: "testing", VMName: binding().Name, Incarnation: binding().Incarnation, RunnerPodUID: host.RunnerPodUID, Request: old.Request, BeforeBytes: 2 << 30, HostOverheadBytes: 64 << 20})
	oldName, err := budgetclaims.HostClaimName(binding(), old.Request)
	require.NoError(t, err)
	oldRef := vmReservation(t, client, oldName, budgetclaims.HostKind, old)
	other := old
	other.RunnerPodUID = "other-pod-uid"
	otherRef := vmReservation(t, client, "other-pod-host", budgetclaims.HostKind, other)
	if mode == "host-settle-gc-lost" {
		lost := false
		client.PrependReactor("delete", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
			a := action.(kt.DeleteAction)
			if a.GetName() != oldRef.Name || lost {
				return false, nil, nil
			}
			lost = true
			require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), "testing", oldRef.Name))
			return true, nil, errors.New("fixture lost old Host DELETE reply after commit")
		})
	}
	if mode == "abort-committed" {
		finishCommittedAbort(t, ctx, api, store, worker, mode)
		latest, err = store.Load(ctx)
		require.NoError(t, err)
	} else {
		latest, err = store.Finish(ctx, latest, session.Actor(), closed.Request.Key)
		require.NoError(t, err)
	}
	require.Nil(t, latest.State().Active)
	var cancelledPlan *hostguard.Plan
	var cancelledSource *budget.ObjectRef
	if mode == "host-settle-after-cancel-history" {
		originalBudget := latest.State().BudgetHeld
		// 数量上限/18次取消已由真实Model/codec测试覆盖；此处用无关注释
		// 占用真实256KiB预算，一次Begin就迫使生产codec淘汰旧成功结果。
		// 不手填历史、不注入关闭水位；随后实际worker取消、读回及Publisher
		// 认证仍全走生产路径，避免18轮重复整套HTTP/CAS拖慢组合验证。
		annotations := latest.VirtualMachine().Annotations
		room := 256*1024 - len(control.JournalAnnotation)
		for key, value := range annotations {
			if key != control.JournalAnnotation {
				room -= len(key) + len(value)
			}
		}
		paddingKey := "test-only.example/annotation-budget"
		// Active需预留4096字节；净预算比原完整历史少1字节，迫使淘汰。
		paddingBytes := room - len(paddingKey) - (len(annotations[control.JournalAnnotation]) + 4095)
		require.Positive(t, paddingBytes)
		api.mu.Lock()
		api.vm.Annotations[paddingKey] = strings.Repeat("x", paddingBytes)
		api.vmVersion++
		api.vm.ResourceVersion = fmt.Sprintf("vm-rv-%d", api.vmVersion)
		api.mu.Unlock()
		latest, err = store.Load(ctx)
		require.NoError(t, err)
		next := closed.Request
		next.Kind = control.Grow
		next.Key.Revision = latest.State().HighWater.Revision + 1
		next.Key.OperationID.Low += next.Key.Revision
		next.BudgetCommitmentID += "-unstarted-cancel"
		var disposition journal.Disposition
		latest, disposition, err = store.Begin(ctx, latest, session.Actor(), next, 0)
		require.NoError(t, err)
		require.Equal(t, journal.Accepted, disposition)
		_, err = store.RequestAbort(ctx, latest, session.Actor(), next.Key)
		require.NoError(t, err)
		progress, err := worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		latest, err = store.Load(ctx)
		require.NoError(t, err)
		require.NotZero(t, latest.State().ExpiredResults)
		require.Equal(t, originalBudget, latest.State().BudgetHeld)
		require.EqualValues(t, 5, latest.State().ProtocolVersion)
		require.NoError(t, latest.State().CheckClosedBudget(closed.Request))
		for _, record := range latest.State().History {
			require.NotEqual(t, closed.Request.Key, record.Operation.Request.Key, "原成功已真正淘汰，后续结清不能借短期历史")
		}
		closed, err = hostguard.SettlementFor(latest.State())
		require.NoError(t, err)
		require.Equal(t, next.Key, closed.ThroughKey, "Host必须接收永久关闭水位，不能只持旧预算Key")
		cancelledPlan = &hostguard.Plan{Namespace: "testing", VMName: binding().Name, Incarnation: binding().Incarnation, RunnerPodUID: host.RunnerPodUID, Request: next, BeforeBytes: closed.Request.TargetVMMemoryBytes, HostOverheadBytes: host.HostOverheadBytes}
		// 模拟旧分配者已经创建、但尚无Node执行授权的迟到Host CM。原VM
		// 已证明该Key关闭，GC须涵盖Through本身，同时保留当前预算来源。
		cancelledHost := host
		cancelledHost.Request, cancelledHost.SourceID = next, hostguard.SourceID(initial.Origin, *cancelledPlan)
		cancelledHost.ReservedMemoryBytes, err = cancelledPlan.Bytes()
		require.NoError(t, err)
		cancelledName, err := budgetclaims.HostClaimName(binding(), next)
		require.NoError(t, err)
		ref := vmReservation(t, client, cancelledName, budgetclaims.HostKind, cancelledHost)
		cancelledSource = &ref
		require.Equal(t, paddingBytes, len(latest.VirtualMachine().Annotations[paddingKey]), "journal CAS must preserve unrelated annotations")
	}
	if mode == "host-settle-source-gone" {
		full, err := latest.State().BudgetFor(closed.Request)
		require.NoError(t, err)
		require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), "testing", full.Sources.OutsideClaim.Name))
	}
	if mode == "host-settle-runner-drift" {
		peer.mu.Lock()
		peer.actual = 2 << 30
		peer.mu.Unlock()
		require.NoError(t, runner.Tick(ctx))
	}
	if mode == "host-settle-scheduler-loop" {
		require.Equal(t, closed.HostClaim, schedulerPublishesOriginalHost(t, ctx, api, client, coord, allocator, oldRef.Name))
	} else {
		changed, err = publisher.SettleCurrent(ctx)
		if mode == "host-settle-source-gone" || mode == "host-settle-runner-drift" {
			require.Error(t, err)
			after, loadErr := allocator.Load(ctx)
			require.NoError(t, loadErr)
			used, usedErr := after.State().UsedBytes()
			require.NoError(t, usedErr)
			require.Equal(t, before, used, "坏来源/实际值偏离不得释放")
			require.EqualValues(t, 3, after.State().ProtocolVersion)
			return
		}
		if mode == "host-settle-lost" || mode == "host-settle-gc-lost" {
			require.Error(t, err)
			require.False(t, changed)
			changed, err = publisher.SettleCurrent(ctx)
			require.NoError(t, err)
			require.False(t, changed, "原结清CAS或垃圾删除未知后只读恢复，不重发VM动作")
		} else {
			require.NoError(t, err)
			require.True(t, changed)
		}
	}
	snapshot, err = allocator.Load(ctx)
	require.NoError(t, err)
	if cancelledPlan == nil {
		require.EqualValues(t, 4, snapshot.State().ProtocolVersion)
	} else {
		require.EqualValues(t, 6, snapshot.State().ProtocolVersion)
		_, _, err = allocator.Reserve(ctx, snapshot, *cancelledPlan)
		require.Error(t, err, "历史已淘汰的较新取消Key不能重新占用Host容量")
	}
	row, err := snapshot.State().Placement(host.RunnerPodUID)
	require.NoError(t, err)
	require.Equal(t, &closed, row.Settled)
	used, err := snapshot.State().UsedBytes()
	require.NoError(t, err)
	require.Less(t, used, before)
	require.Equal(t, closed.Request.TargetVMMemoryBytes, row.MemoryBytes)
	claims, err := budgetclaims.New(client.CoreV1(), binding())
	require.NoError(t, err)
	_, err = claims.ReadCurrent(ctx, latest)
	require.NoError(t, err, "已结清来源继续维持永久当前预算")
	_, err = claims.Read(ctx, latest, prepared, closed.HostClaim)
	require.ErrorIs(t, err, hostguard.ErrSettled, "同一Host旧回执不再提供新的执行授权")
	_, _, err = allocator.Reserve(ctx, snapshot, snapshot.State().Holds[0].Plan)
	require.Error(t, err)
	_, _, err = allocator.HoldPlacement(ctx, snapshot, waiting)
	require.NoError(t, err, "明确结清后的容量可供新Pod使用")
	_, err = client.CoreV1().ConfigMaps("testing").Get(ctx, oldRef.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "同原Pod的旧失效Host来源可回收")
	_, err = client.CoreV1().ConfigMaps("testing").Get(ctx, otherRef.Name, metav1.GetOptions{})
	require.NoError(t, err, "不能删除另一Pod/迁移域的Host来源")
	if cancelledSource != nil {
		_, err = client.CoreV1().ConfigMaps("testing").Get(ctx, cancelledSource.Name, metav1.GetOptions{})
		require.True(t, apierrors.IsNotFound(err), "关闭水位本身的迟到Host CM可清理，当前原预算CM仍持有")
	}
	peer.mu.Lock()
	writes := peer.writes
	peer.mu.Unlock()
	require.Equal(t, 1, writes, "结清/GC和重试均不发送额外QMP写入")
}
