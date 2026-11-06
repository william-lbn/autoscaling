package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

func enableVMServiceAuditTest(t *testing.T, f *guestAuditFixture) {
	t.Helper()
	require.False(t, f.worker.executor.InFlightEnabled())
	parent, cancel := f.session.coupledContext(f.ctx)
	stop, err := f.worker.executor.EnableInFlight(parent)
	require.NoError(t, err)
	require.True(t, f.worker.executor.InFlightEnabled(), "新VM直测必须显式进入同Session审计模式")
	t.Cleanup(func() { stop(); cancel() })
}

// 复用B同包原生产Grow→Shrink前驱和真实Guest完成。只提供显式协议peer，
// 不填任何Completed、不把回调或本机测试当Linux/Source/K8s许可。
func finishGuestForVMServiceAudit(t *testing.T, f *guestAuditFixture) {
	t.Helper()
	progress, err := advanceGuestAudit(f, f.worker.renewGuestServiceAudit)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	require.Equal(t, journal.Settled, budgetStep(t, f.store, journal.GuestReady).Phase)
	require.Equal(t, journal.Planned, budgetStep(t, f.store, journal.ResizeVM).Phase)
}

func TestProductionVMServiceLeaseShrinkResizeAndObserveCompleteSources(t *testing.T) {
	withGuestAuditFixture(t, control.Shrink, func(f *guestAuditFixture) {
		finishGuestForVMServiceAudit(t, f)
		enableVMServiceAuditTest(t, f)
		f.pg.mu.Lock()
		before := f.pg.leaseRenewals
		f.pg.mu.Unlock()
		trace := []string{}
		audit := func(ctx context.Context, s *journal.Snapshot, shared control.SharedCostResponse) error {
			var cursor journal.Step
			for _, step := range s.State().Active.Steps {
				if step.Phase != journal.Settled {
					cursor = step
					break
				}
			}
			trace = append(trace, fmt.Sprintf("%s/%s", cursor.Action, cursor.Phase))
			f.pg.mu.Lock()
			old := f.pg.leaseRenewals
			f.pg.mu.Unlock()
			err := f.worker.renewVMServiceAudit(ctx, s, shared)
			require.NoError(t, err)
			f.pg.mu.Lock()
			require.Equal(t, 1, f.pg.leaseRenewals-old, "合法Shrink/Observe完整审计出口必须真实一次Native renew")
			f.pg.mu.Unlock()
			return err
		}
		progress, err := f.worker.vm.AdvanceWithAudit(f.ctx, audit)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		require.Equal(t, journal.Unconfirmed, budgetStep(t, f.store, journal.ResizeVM).Phase)
		require.NoError(t, f.runner.Tick(f.ctx))
		progress, err = f.worker.vm.AdvanceWithAudit(f.ctx, audit)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		require.Equal(t, journal.Settled, budgetStep(t, f.store, journal.ResizeVM).Phase)
		for i := 0; i < 2 && budgetStep(t, f.store, journal.ObserveVM).Phase != journal.Settled; i++ {
			progress, err = f.worker.vm.AdvanceWithAudit(f.ctx, audit)
			require.NoError(t, err)
			require.Equal(t, pgexec.Recorded, progress)
		}
		require.Equal(t, journal.Settled, budgetStep(t, f.store, journal.ObserveVM).Phase)
		require.Equal(t, []string{"resize_vm/planned", "resize_vm/unconfirmed", "observe_vm/planned", "observe_vm/unconfirmed"}, trace)
		f.pg.mu.Lock()
		require.Equal(t, 4, f.pg.leaseRenewals-before)
		f.pg.mu.Unlock()
		require.Equal(t, journal.Planned, budgetStep(t, f.store, journal.FinalizeBudget).Phase)
		require.Equal(t, journal.Planned, budgetStep(t, f.store, journal.ResumeAdmission).Phase)
	})
}

func TestProductionVMServiceLeaseShrinkRetainsOldSourceAndAuthority(t *testing.T) {
	for _, mode := range []string{"wrong-cursor", "old-held-host-UID", "old-held-host-missing", "expired", "VM-RV", "different-actor"} {
		t.Run(mode, func(t *testing.T) {
			withGuestAuditFixture(t, control.Shrink, func(f *guestAuditFixture) {
				if mode != "wrong-cursor" {
					finishGuestForVMServiceAudit(t, f)
				}
				enableVMServiceAuditTest(t, f)
				snapshot, err := f.store.Load(f.ctx)
				require.NoError(t, err)
				shared, err := f.worker.client.SharedCost(f.ctx)
				require.NoError(t, err)
				f.pg.mu.Lock()
				old := f.pg.leaseRenewals
				f.pg.mu.Unlock()
				f.qmp.mu.Lock()
				writes := f.qmp.writes
				f.qmp.mu.Unlock()
				switch mode {
				case "old-held-host-UID", "old-held-host-missing":
					var held budget.Finalized
					require.NotNil(t, snapshot.State().BudgetHeld)
					require.NoError(t, json.Unmarshal(snapshot.State().BudgetHeld.Finalization.Observation.Data, &held))
					require.NotNil(t, held.Sources)
					cm, err := f.core.CoreV1().ConfigMaps(f.session.binding.Namespace).Get(f.ctx, held.Sources.HostClaim.Name, metav1.GetOptions{})
					require.NoError(t, err)
					if mode == "old-held-host-missing" {
						require.NoError(t, f.core.CoreV1().ConfigMaps(f.session.binding.Namespace).Delete(f.ctx, cm.Name, metav1.DeleteOptions{}))
					} else {
						bad := cm.DeepCopy()
						bad.UID = types.UID(string(cm.UID) + "-lost")
						require.NoError(t, f.core.Tracker().Update(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, bad, f.session.binding.Namespace))
					}
				case "expired":
					f.pg.mu.Lock()
					f.pg.leaseExpired = true
					f.pg.mu.Unlock()
				case "VM-RV":
					f.pg.beforeLeaseObserve = func() {
						f.api.mu.Lock()
						f.api.vmVersion++
						f.api.vm.ResourceVersion = fmt.Sprintf("vm-service-rv-%d", f.api.vmVersion)
						f.api.mu.Unlock()
					}
				case "different-actor":
					saved := f.session.actor
					f.session.actor.Epoch++
					defer func() { f.session.actor = saved }()
				}
				err = f.worker.renewVMServiceAudit(f.ctx, snapshot, shared)
				if mode == "wrong-cursor" || mode == "different-actor" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				f.pg.mu.Lock()
				require.Equal(t, old, f.pg.leaseRenewals)
				f.pg.mu.Unlock()
				f.qmp.mu.Lock()
				require.Equal(t, writes, f.qmp.writes)
				f.qmp.mu.Unlock()
				current, err := f.store.Load(f.ctx)
				require.NoError(t, err)
				require.Equal(t, snapshot.State(), current.State(), "拒绝续租不得写WAL/清原未知")
			})
		})
	}
}
