package owner

import (
	"context"
	"errors"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/stretchr/testify/require"
)

// 原Elector、完整初始预算/Runner/TLS/Run均实际执行；API和Native为显式peer。
// observe屏障只更新同一VM的status/RV，不改Owner、WAL或PG期限，不冒物理验收。
func TestProductionAdmissionRenewalStatusRevisionKeepsOwner(t *testing.T) {
	productionInitialService(t, "admission-revision-status")
}

func TestProductionAdmissionRenewalIdentityDriftStillStops(t *testing.T) {
	for _, mode := range []string{"vm-uid", "lease-uid", "lease-holder", "vm-not-dispatchable", "wal-corrupt", "cancel"} {
		t.Run(mode, func(t *testing.T) { t.Parallel(); productionInitialService(t, "admission-revision-"+mode) })
	}
}

func verifyAdmissionRevisionRace(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, worker *PGWorker, pg *initialServicePG) {
	t.Helper()
	before, err := store.Load(ctx)
	require.NoError(t, err)
	actor := worker.session.Actor()
	posts, _, resizes := pg.counts()
	// 首次只读observe建立原Native期限基线；不能把peer初始化误报为renew。
	_, err = worker.observeAdmissionLease(ctx)
	require.NoError(t, err)
	pg.mu.Lock()
	renewals, deadline := pg.leaseRenewals, pg.leaseDeadline
	pg.mu.Unlock()
	loopCtx, stop := context.WithCancel(ctx)
	defer stop()
	observes, ticks, rejectedSharedReads := 0, 0, 0
	var firstErr, firstLoadErr, firstSessionErr error
	var firstState journal.Model
	var firstRenewals int
	var firstDeadline control.Uint64
	// Run协商期限的第一次observe不修改来源；第二次位于独立完整续租审计之后。
	pg.beforeLeaseObserve = func() {
		observes++
		if observes != 2 {
			return
		}
		api.mu.Lock()
		defer api.mu.Unlock()
		switch mode {
		case "status":
			// Controller同UID状态更新保留全部资源身份，只增加普通状态条件。
			api.vm.Status.Conditions = append(api.vm.Status.Conditions, metav1.Condition{Type: "RevisionAuditFixture", Status: metav1.ConditionTrue, Reason: "StatusOnly", Message: "test status update", LastTransitionTime: metav1.Now()})
		case "vm-uid":
			api.vm.UID = "replacement-vm-uid"
		case "lease-uid":
			api.lease.UID = "replacement-owner-lease-uid"
		case "lease-holder":
			holder := "different-owner-process"
			api.lease.Spec.HolderIdentity = &holder
		case "vm-not-dispatchable":
			api.vm.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		case "wal-corrupt":
			api.vm.Annotations[control.JournalAnnotation] = "{broken"
		case "cancel":
			stop()
		default:
			t.Fatalf("unknown admission revision fixture %q", mode)
		}
		api.vmVersion++
		api.vm.ResourceVersion = fmt.Sprintf("renew-status-rv-%d", api.vmVersion)
	}
	err = worker.Run(loopCtx, PGLoopOptions{PollInterval: 10 * time.Millisecond, Observe: func(tick Tick) {
		ticks++
		if mode != "status" {
			return
		}
		if ticks == 1 {
			// 只记录首轮事实；不能FailNow先于旧Run自身的致命错误分支。
			firstErr = tick.Err
			pg.mu.Lock()
			firstRenewals, firstDeadline = pg.leaseRenewals, pg.leaseDeadline
			rejectedSharedReads = pg.leaseAuditSharedReads
			pg.mu.Unlock()
			same, loadErr := store.Load(ctx)
			firstLoadErr = loadErr
			if loadErr == nil {
				firstState = same.State()
			}
			firstSessionErr = worker.session.Check(ctx)
			return
		}
		require.NoError(t, tick.Err, "下一轮必须由完整原审计恢复续租")
		require.Equal(t, actor, worker.session.Actor())
		pg.mu.Lock()
		require.Greater(t, pg.leaseAuditSharedReads, rejectedSharedReads, "下一轮重新查询完整Native共享成本来源")
		require.Equal(t, renewals+1, pg.leaseRenewals)
		require.Greater(t, pg.leaseDeadline, deadline)
		pg.mu.Unlock()
		stop()
	}})
	pg.beforeLeaseObserve = nil
	require.GreaterOrEqual(t, observes, 2, "竞态必须真实到达续租末次observe")
	afterPosts, _, afterResizes := pg.counts()
	require.Equal(t, posts, afterPosts, "不重发旧门禁或业务动作")
	require.Equal(t, resizes, afterResizes, "不重发旧PG resize")
	if mode == "status" {
		t.Logf("original Run returned: ticks=%d error_is_service_safety=%t canceled=%t", ticks, errors.Is(err, ErrServiceSafety), errors.Is(err, context.Canceled))
		require.ErrorIs(t, err, context.Canceled, "原RV竞态必须保持Owner而非致命退出")
		require.Equal(t, 2, ticks, "同一次Run保留同一Owner并完成下一轮")
		require.ErrorIs(t, firstErr, ErrServiceSafety)
		require.Contains(t, firstErr.Error(), "original audited VM revision")
		require.Equal(t, renewals, firstRenewals, "失效审计绝不发送renew")
		require.Equal(t, deadline, firstDeadline)
		require.NoError(t, firstLoadErr)
		require.Equal(t, before.State(), firstState, "拒绝续租不写WAL或修改旧未知")
		require.NoError(t, firstSessionErr)
		after, loadErr := store.Load(ctx)
		require.NoError(t, loadErr)
		require.Equal(t, before.State(), after.State())
	} else {
		require.Error(t, err)
		require.False(t, errors.Is(err, ErrServiceSafety), "身份/取消/坏日志仍是原拒绝，不泛化为安全等待")
		pg.mu.Lock()
		require.Equal(t, renewals, pg.leaseRenewals)
		require.Equal(t, deadline, pg.leaseDeadline)
		pg.mu.Unlock()
		// 仅恢复夹具外部输入供原Elector自然退出；不重试Run或产生第二次renew。
		api.mu.Lock()
		if mode == "vm-uid" {
			api.vm.UID = before.VirtualMachine().UID
		}
		if mode == "lease-uid" {
			api.lease.UID = worker.session.leaseUID
		}
		if mode == "lease-holder" {
			holder := actor.HolderID
			api.lease.Spec.HolderIdentity = &holder
		}
		if mode == "vm-not-dispatchable" {
			api.vm.DeletionTimestamp = nil
		}
		if mode == "wal-corrupt" {
			api.vm.Annotations[control.JournalAnnotation] = before.VirtualMachine().Annotations[control.JournalAnnotation]
		}
		api.mu.Unlock()
	}
}
