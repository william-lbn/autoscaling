package owner

import (
	"encoding/json"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 只用原完整前驱和协议peer测试真实worker.Advance接线；不直接调用
// Guest AdvanceWithAudit，不手填任何Completed，也不代替物理或K8s验收。
func enableGuestWorkerDispatchAudit(t *testing.T, f *guestAuditFixture) {
	t.Helper()
	require.False(t, f.worker.executor.InFlightEnabled())
	parent, cancel := f.session.coupledContext(f.ctx)
	stop, err := f.worker.executor.EnableInFlight(parent)
	require.NoError(t, err)
	require.True(t, f.worker.executor.InFlightEnabled())
	t.Cleanup(func() { stop(); cancel() })
}

func TestProductionGuestWorkerDispatchUsesCompleteAudit(t *testing.T) {
	for _, kind := range []control.Kind{control.Grow, control.Shrink} {
		t.Run(string(kind), func(t *testing.T) {
			withGuestAuditFixture(t, kind, func(f *guestAuditFixture) {
				armGuestAuditClock(f, "healthy")
				enableGuestWorkerDispatchAudit(t, f)
				before, err := f.store.Load(f.ctx)
				require.NoError(t, err)
				require.NoError(t, before.State().Validate())
				require.Equal(t, journal.Planned, budgetStep(t, f.store, journal.GuestReady).Phase)
				f.root.mu.Lock()
				applies := f.root.posts
				f.root.mu.Unlock()
				f.pg.mu.Lock()
				deadline := f.pg.leaseDeadline
				f.pg.mu.Unlock()

				// 唯一被测推进入口必须经过Owner原安全门和Guest调用选择。
				progress, err := f.worker.Advance(f.ctx)
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
				after, err := f.store.Load(f.ctx)
				require.NoError(t, err)
				require.NoError(t, after.State().Validate())
				guest := budgetStep(t, f.store, journal.GuestReady)
				require.Equal(t, journal.Settled, guest.Phase)
				require.NotNil(t, guest.Observation)
				require.Equal(t, journal.Completed, guest.Observation.Result)
				require.NotEmpty(t, guest.Command)
				require.NotNil(t, guest.DispatchedBy)
				require.Equal(t, f.session.Actor(), *guest.DispatchedBy)
				f.root.mu.Lock()
				actualApplies := f.root.posts
				f.root.mu.Unlock()
				require.Equal(t, applies+1, actualApplies, "必须真正执行同一原Guest Apply")
				f.pg.mu.Lock()
				renewals, deadlineAfter, expired := f.pg.leaseRenewals, f.pg.leaseDeadline, f.pg.leaseExpired
				f.pg.mu.Unlock()
				require.Equal(t, 2, renewals, "真实worker入口须命中完整pre/post两个fresh Native CAS")
				require.Greater(t, deadlineAfter, deadline)
				require.False(t, expired)
				require.Nil(t, after.State().ServiceClosure)
				require.Equal(t, before.State().Active.Request, after.State().Active.Request)
				require.Equal(t, before.State().InitialHeld, after.State().InitialHeld)
				require.Equal(t, before.State().BudgetHeld, after.State().BudgetHeld)
				next := journal.ResizePG
				if kind == control.Shrink {
					next = journal.ResizeVM
				}
				require.Equal(t, journal.Planned, budgetStep(t, f.store, next).Phase, "本轮不推进任何Guest后继")
			})
		})
	}
}

func TestProductionGuestWorkerDispatchRejectsLostHeldSource(t *testing.T) {
	for _, mode := range []string{"missing", "replaced"} {
		t.Run(mode, func(t *testing.T) {
			withGuestAuditFixture(t, control.Shrink, func(f *guestAuditFixture) {
				armGuestAuditClock(f, "healthy")
				enableGuestWorkerDispatchAudit(t, f)
				before, err := f.store.Load(f.ctx)
				require.NoError(t, err)
				require.NoError(t, before.State().Validate())
				guestBefore := budgetStep(t, f.store, journal.GuestReady)
				require.Equal(t, journal.Planned, guestBefore.Phase)
				require.NotNil(t, before.State().BudgetHeld)
				// 先证明故障前原旧来源真实有效；不借错误前门得到假绿。
				_, err = f.worker.safety.claims.ReadActiveHeld(f.ctx, before)
				require.NoError(t, err)
				var original budget.FinalizationCommand
				require.NoError(t, json.Unmarshal(before.State().BudgetHeld.Finalization.Command, &original))
				require.NotNil(t, original.Finalized.Sources)
				ref := original.Finalized.Sources.HostClaim
				cm, err := f.core.CoreV1().ConfigMaps(binding().Namespace).Get(f.ctx, ref.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, ref.UID, string(cm.UID))
				f.root.mu.Lock()
				applies := f.root.posts
				f.root.mu.Unlock()
				f.qmp.mu.Lock()
				vmWrites := f.qmp.writes
				f.qmp.mu.Unlock()
				if mode == "missing" {
					require.NoError(t, f.core.CoreV1().ConfigMaps(binding().Namespace).Delete(f.ctx, cm.Name, metav1.DeleteOptions{}))
				} else {
					// 只替换只读peer的对象身份；真实Kube Update不能改UID。
					bad := cm.DeepCopy()
					bad.UID += "-replaced"
					require.NoError(t, f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), bad, binding().Namespace))
				}

				_, advanceErr := f.worker.Advance(f.ctx)
				after, err := f.store.Load(f.ctx)
				require.NoError(t, err)
				require.NoError(t, after.State().Validate())
				// 原安全关闭可以Record并返回nil；它绝不等于Guest完成。
				require.True(t, advanceErr != nil || after.State().ServiceClosure != nil, "来源拒绝必须报错或保留原安全关闭WAL")
				require.Equal(t, guestBefore, budgetStep(t, f.store, journal.GuestReady), "旧来源失效不能Guest Start/Record")
				f.root.mu.Lock()
				actualApplies := f.root.posts
				f.root.mu.Unlock()
				require.Equal(t, applies, actualApplies)
				f.qmp.mu.Lock()
				actualVMWrites := f.qmp.writes
				f.qmp.mu.Unlock()
				require.Equal(t, vmWrites, actualVMWrites)
				f.pg.mu.Lock()
				renewals := f.pg.leaseRenewals
				f.pg.mu.Unlock()
				require.Zero(t, renewals, "显式on缺旧来源不能退回legacy续租")
				require.Equal(t, before.State().Active.Request, after.State().Active.Request)
				require.Equal(t, before.State().InitialHeld, after.State().InitialHeld)
				require.Equal(t, before.State().BudgetHeld, after.State().BudgetHeld)
			})
		})
	}
}
