package owner

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// 原完整首次预算/Root/Runner先关闭，再由真实Elector接管。只替换同状态
// Native TLS peer的计数外壳，不伪造Session、Owner、WAL或生产来源审计。
// peer组合只证明协议及调用边界，不能外推Linux硬限或K8s端到端验收。
func TestProductionAuditedLeaseUnknownReplyStopsOriginalCall(t *testing.T) {
	for _, mode := range []string{"renew-reply-lost", "rearm-reply-lost"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			productionInitialService(t, "lifecycle-lease-guard-"+mode)
		})
	}
}

func TestProductionAuditedLeaseRequiresFreshAuthorityAndSnapshot(t *testing.T) {
	for _, mode := range []string{"owner-uid-drift", "vm-rv-drift"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			productionInitialService(t, "lifecycle-lease-guard-"+mode)
		})
	}
}

func TestProductionAuditedLeaseRequiresExactNativeGate(t *testing.T) {
	t.Parallel()
	productionInitialService(t, "lifecycle-lease-guard-native-gate-drift")
}

func TestProductionAuditedLeaseNeverRevivesExpiredDeadline(t *testing.T) {
	t.Parallel()
	productionInitialService(t, "lifecycle-lease-guard-expired-after-rearm")
}

func verifyAuditedLeaseGuard(t *testing.T, ctx context.Context, mode string, transfer *businessOwnerTransfer, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner) {
	t.Helper()
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()

	closed, err := transfer.store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, closed.State().ServiceClosure)
	require.Equal(t, journal.Settled, closed.State().ServiceClosure.Phase)
	oldEpoch := session.Actor().Epoch
	var now atomic.Uint64
	now.Store(uint64(1000 * time.Second))
	pg.mu.Lock()
	pg.leaseExpired, pg.leaseDeadline = true, control.Uint64(now.Load()-1)
	pg.leaseClock = func() control.Uint64 { return control.Uint64(now.Load()) }
	pg.leaseWindow = control.Uint64(30 * time.Second)
	pg.rearmFixture = &serviceRearmPG{epoch: oldEpoch, api: transfer.api, loseRenewReply: mode == "renew-reply-lost", loseAfter: mode == "rearm-reply-lost"}
	pg.reopenFixture = &serviceReopenPG{mode: "normal", api: transfer.api, root: root}
	pg.mu.Unlock()
	session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
	require.NoError(t, runner.Tick(ctx))
	require.Less(t, pg.shared().Observed.IssuerFloorEpoch, session.Actor().Epoch, "必须由原HTTP ownerFence推进Native floor")

	transfer.api.mu.Lock()
	originalLeaseUID := transfer.api.lease.UID
	transfer.api.mu.Unlock()
	// 请求数在Native入口计数；成功应用计数不能识别同一旧deadline被重发。
	// 这里只包住原handler，所有请求仍由原SDK/TLS/Native协议peer核验。
	var renewRequests atomic.Int64
	var injected atomic.Bool
	native := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dsg/v1/admission/lease" {
			raw, readErr := io.ReadAll(r.Body)
			require.NoError(t, readErr)
			require.NoError(t, r.Body.Close())
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var q control.AdmissionLeaseRequest
			require.NoError(t, json.Unmarshal(raw, &q))
			if q.Action == "renew" {
				renewRequests.Add(1)
			}
			pg.mu.Lock()
			rearmed := pg.rearmFixture.posts != 0
			pg.mu.Unlock()
			if q.Action == "observe" && rearmed && injected.CompareAndSwap(false, true) {
				pg.mu.Lock()
				switch mode {
				case "native-gate-drift":
					// 完整资源审计已通过；此次Native鲜读必须仍匹配原关闭锚。
					pg.gate.Revision++
				case "expired-after-rearm":
					// 只推进Native时钟到真实deadline，不能靠布尔恢复期限。
					now.Store(uint64(pg.leaseDeadline))
				case "owner-uid-drift", "vm-rv-drift":
					// 原Native快照已生成后才改权威来源，要求发送前再次Check/CAS。
					pg.rearmFixture.afterObserve = func() {
						transfer.api.mu.Lock()
						defer transfer.api.mu.Unlock()
						if mode == "owner-uid-drift" {
							transfer.api.lease.UID = "lease-guard-replaced-owner-uid"
						} else {
							transfer.api.vmVersion++
							transfer.api.vm.ResourceVersion = "lease-guard-changed-vm-rv"
						}
					}
				}
				pg.mu.Unlock()
			}
		}
		pg.serve(t, w, r)
	}
	config = pgConfigWithFence(t, pg.request, native, pg.ownerFence(t), func() control.Snapshot { return pg.shared().Observed })
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(worker.CloseIdleConnections)
	before, err := transfer.store.Load(ctx)
	require.NoError(t, err)
	_, _, _, selector := before.State().ServiceClosure.Selection()
	// 否例同步进入生产复合入口；不启后台Run，避免普通Observe污染未知计数。
	command := closureControl(session, config.Instance, control.CoordinationAction("recover_service_lifecycle"), selector)
	_, callErr := worker.executeControl(ctx, command)
	if mode == "owner-uid-drift" {
		// 恢复的仅是peer故障开关；生产Owner及原Session从未手工替换。
		transfer.api.mu.Lock()
		transfer.api.lease.UID = originalLeaseUID
		transfer.api.mu.Unlock()
	}
	require.Error(t, callErr)
	after, err := transfer.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State().Owner, after.State().Owner)
	require.Equal(t, before.State().InitialHeld, after.State().InitialHeld)
	require.Equal(t, before.State().BudgetHeld, after.State().BudgetHeld)
	require.Equal(t, before.State().Active, after.State().Active)
	require.Equal(t, before.State().History, after.State().History)
	require.Equal(t, before.State().ServiceLifecycle, after.State().ServiceLifecycle)
	require.NotNil(t, after.State().ServiceClosure)
	require.Nil(t, after.State().ServiceClosure.Reopen, "续租门失败不能选择新开放WAL")
	require.NotNil(t, after.State().ServiceClosure.Rearm)
	retained := *after.State().ServiceClosure
	retained.Rearm = nil
	require.Equal(t, *before.State().ServiceClosure, retained, "原关闭锚逐字保留")
	pg.mu.Lock()
	rearmPosts, rearmQueries := pg.rearmFixture.posts, pg.rearmFixture.queries
	reopenPosts, reopenCloses := pg.reopenFixture.posts, pg.reopenFixture.closes
	renewals, expired := pg.leaseRenewals, pg.leaseExpired
	pg.mu.Unlock()
	require.Equal(t, 1, rearmPosts)
	require.Zero(t, reopenPosts)
	require.Zero(t, reopenCloses)
	if mode == "renew-reply-lost" {
		require.Equal(t, int64(1), renewRequests.Load(), "renew已应用丢回复后本调用不能自动重发")
		require.Equal(t, 1, renewals)
		require.Equal(t, journal.Settled, after.State().ServiceClosure.Rearm.Phase)
	} else {
		require.Zero(t, renewRequests.Load(), "失权、快照漂移、错门禁或再次过期均不得发送renew")
		require.Zero(t, renewals)
	}
	if mode == "rearm-reply-lost" {
		require.Equal(t, journal.Unconfirmed, after.State().ServiceClosure.Rearm.Phase)
		require.Zero(t, rearmQueries)
		// 原未知不能被当前Native数值升格；再次显式复合选择也不观察/重发它。
		_, again := worker.executeControl(ctx, command)
		require.Error(t, again)
		unchanged, loadErr := transfer.store.Load(ctx)
		require.NoError(t, loadErr)
		require.Equal(t, after.State(), unchanged.State())
		pg.mu.Lock()
		require.Equal(t, rearmPosts, pg.rearmFixture.posts)
		require.Equal(t, rearmQueries, pg.rearmFixture.queries)
		pg.mu.Unlock()
		require.Zero(t, renewRequests.Load())
	} else {
		require.True(t, injected.Load(), "必须命中rearm之后的鲜读边界，不能在前置条件假通过")
		require.Equal(t, journal.Settled, after.State().ServiceClosure.Rearm.Phase)
	}
	if mode == "expired-after-rearm" {
		require.True(t, expired)
		require.False(t, pg.shared().Observed.AdmissionOpen)
	}
}
