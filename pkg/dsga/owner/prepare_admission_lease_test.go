package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// 完整生产Prepare/Owner/Root/Runner/TLS/原WAL执行，Native用显式单调时钟peer。
// 每次完整审计各10秒、串行累计40秒；不是缩短30秒期限或物理Linux验收。
func TestProductionPrepareAuditsKeepOriginalLeaseAndCommand(t *testing.T) {
	productionInitialService(t, "prepare-audited-slow")
}

func TestProductionPrepareAuditCannotReviveExpiredLease(t *testing.T) {
	for _, mode := range []string{"expired-first", "expired-after-wal"} {
		t.Run(mode, func(t *testing.T) { t.Parallel(); productionInitialService(t, "prepare-audited-"+mode) })
	}
}

func TestProductionPrepareAuditKeepsUnknownOutsideOriginal(t *testing.T) {
	productionInitialService(t, "prepare-audited-outside-unknown")
}

func TestProductionPrepareAuditRejectsOriginalAuthorityDrift(t *testing.T) {
	for _, mode := range []string{"old-host-missing", "root-floor", "vm-rv", "lease-uid", "native-gate", "renew-reply-lost"} {
		t.Run(mode, func(t *testing.T) { t.Parallel(); productionInitialService(t, "prepare-audited-"+mode) })
	}
}

func verifyPrepareAuditedLease(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, core *kubefake.Clientset, session *Session, worker *PGWorker, pg *initialServicePG, root *guestRootFixture) {
	t.Helper()
	var now, audits atomic.Uint64
	now.Store(uint64(1000 * time.Second))
	pg.mu.Lock()
	pg.leaseClock = func() control.Uint64 { return control.Uint64(now.Load()) }
	pg.leaseWindow = control.Uint64(30 * time.Second)
	pg.leaseDeadline = control.Uint64(now.Load()) + pg.leaseWindow
	pg.leaseExpired, pg.leaseRenewals = false, 0
	pg.rearmFixture = &serviceRearmPG{epoch: session.Actor().Epoch, api: api, loseRenewReply: mode == "renew-reply-lost"}
	pg.mu.Unlock()
	root.mu.Lock()
	root.afterProcess = func() {
		n := audits.Add(1)
		step := uint64(10 * time.Second)
		if mode == "expired-first" && n == 1 || mode == "expired-after-wal" && n == 3 {
			step = uint64(31 * time.Second)
		}
		now.Add(step)
	}
	if mode == "root-floor" {
		root.view.IssuerFloorEpoch--
	}
	root.mu.Unlock()
	if mode == "old-host-missing" {
		// 新immutable资格仍存在；只移除仍占住原承诺的Host来源peer。
		require.NoError(t, core.CoreV1().Nodes().Delete(ctx, "node-one", metav1.DeleteOptions{}))
		_, err := worker.safety.claims.Qualification(ctx, *worker.qualification)
		require.NoError(t, err)
	}
	api.mu.Lock()
	originalLeaseUID := api.lease.UID
	api.mu.Unlock()
	if mode == "vm-rv" || mode == "lease-uid" || mode == "native-gate" {
		pg.mu.Lock()
		pg.rearmFixture.afterObserve = func() {
			switch mode {
			case "native-gate":
				pg.gate.Revision++
			case "lease-uid":
				api.mu.Lock()
				api.lease.UID = "replaced-prepare-lease-uid"
				api.mu.Unlock()
			case "vm-rv":
				api.mu.Lock()
				api.vmVersion++
				api.vm.ResourceVersion = fmt.Sprintf("prepare-drift-%d", api.vmVersion)
				api.mu.Unlock()
			}
		}
		pg.mu.Unlock()
	}
	var createCalls atomic.Int32
	core.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
		if cm.Labels[budgetclaims.ClaimLabel] != budgetclaims.OutsideKind {
			return false, nil, nil
		}
		createCalls.Add(1)
		if mode == "outside-unknown" {
			return true, nil, apierrors.NewTimeoutError("explicit lost outside reply peer", 1)
		}
		return false, nil, nil
	})
	before, err := store.Load(ctx)
	require.NoError(t, err)
	request := pg.request
	_, _, err = store.Begin(ctx, before, session.Actor(), request, 1)
	require.NoError(t, err)
	baseline := now.Load()
	progress, callErr := worker.Advance(ctx)
	if mode == "lease-uid" {
		api.mu.Lock()
		api.lease.UID = originalLeaseUID
		api.mu.Unlock()
	}
	after, err := store.Load(ctx)
	require.NoError(t, err)
	pg.mu.Lock()
	renewals, expired := pg.leaseRenewals, pg.leaseExpired
	pg.mu.Unlock()
	require.Equal(t, before.State().InitialHeld, after.State().InitialHeld, "旧永久头不能被续租改写")
	require.NotNil(t, after.State().Active)
	for _, step := range after.State().Active.Steps[1:] {
		require.Equal(t, journal.Planned, step.Phase, "续租不派发PG/Guest/VM")
	}
	if mode == "slow" {
		require.NoError(t, callErr)
		require.Equal(t, pgexec.Recorded, progress)
		require.Greater(t, now.Load()-baseline, uint64(30*time.Second))
		require.False(t, expired)
		require.GreaterOrEqual(t, renewals, 4, "进入Prepare前及三次完整审计均须有限接续")
		step := after.State().Active.Steps[0]
		require.Equal(t, journal.Settled, step.Phase)
		var original budget.PrepareCommand
		require.NoError(t, json.Unmarshal(step.Command, &original))
		require.NoError(t, original.Validate())
		require.True(t, original.Prepared.Shared.Observed.AdmissionOpen)
		require.EqualValues(t, 1, original.Prepared.Shared.Observed.AdmissionRevision)
		require.EqualValues(t, 1, createCalls.Load())
		return
	}
	if callErr == nil {
		// 原Advance可在拒绝继续Prepare后完成独立安全关闭并返回Recorded。
		// 这不是业务成功：必须有真实关闭WAL及Native闭门，不能只吞错误。
		require.Equal(t, pgexec.Recorded, progress)
		require.NotNil(t, after.State().ServiceClosure)
		require.Equal(t, journal.Settled, after.State().ServiceClosure.Phase)
		require.False(t, pg.shared().Observed.AdmissionOpen)
	}
	if mode == "outside-unknown" {
		require.Error(t, callErr)
		require.Equal(t, journal.Unconfirmed, after.State().Active.Steps[0].Phase)
		original := after.State().Active.Steps[0]
		_, err = worker.Advance(ctx)
		require.Error(t, err)
		again, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, original, again.State().Active.Steps[0], "原创建未知必须保留完整WAL")
		require.EqualValues(t, 1, createCalls.Load(), "普通下一轮只能GET原对象，不能重发Create")
		return
	}
	require.Zero(t, createCalls.Load(), "无有效原审计不创建新Outside")
	if mode == "expired-after-wal" {
		require.Equal(t, journal.Unconfirmed, after.State().Active.Steps[0].Phase)
		require.Equal(t, 2, renewals)
		require.True(t, expired)
	} else if mode == "renew-reply-lost" {
		require.Equal(t, 1, renewals, "Unknown可能已续，但同一次调用绝不重发")
	} else {
		require.Zero(t, renewals, "漂移或单次过期不得发送续租")
	}
}
