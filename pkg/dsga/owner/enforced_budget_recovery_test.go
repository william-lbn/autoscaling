package owner

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func TestProductionEnforcedBudgetFormalRecovery(t *testing.T) {
	for _, mode := range []string{"restored", "takeover", "source-drift", "wrong-selector", "open-takeover", "open-takeover-source-drift"} {
		t.Run(mode, func(t *testing.T) {
			productionInitialService(t, "enforced-budget-recovery-"+mode)
		})
	}
}

// 使用同一正式恢复前驱；PG gate 是协议 peer，真实 K8s 后继伸缩另行验收。
func TestProductionEnforcedBudgetRecoveryHistoryAllowsNewGate(t *testing.T) {
	productionInitialService(t, "enforced-budget-recovery-new-gate")
}

func verifyEnforcedBudgetRecovery(t *testing.T, ctx context.Context, mode string, f *businessOwnerTransfer,
	session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset,
	source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner) {
	t.Helper()
	require.True(t, pg.shared().Observed.AdmissionOpen)
	if mode == "open-takeover" || mode == "open-takeover-source-drift" {
		verifyOpenBudgetTakeover(t, ctx, mode == "open-takeover-source-drift", f, session, config, core, source, pg, root, runner)
		return
	}
	// 在原运行入口观察到实际 Guest 硬限漂移，先完成保护关闭。
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	closed, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, closed.State().ServiceClosure)
	require.False(t, pg.shared().Observed.AdmissionOpen)
	if mode != "source-drift" {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
	}
	if mode == "takeover" {
		session, _, worker = f.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
		require.NoError(t, runner.Tick(ctx))
	}
	before, err := f.store.Load(ctx)
	require.NoError(t, err)
	_, _, _, selector := before.State().ServiceClosure.Selection()
	if mode == "wrong-selector" {
		selector = strings.Repeat("ab", 32)
	}
	pg.mu.Lock()
	pg.reopenFixture = &serviceReopenPG{mode: "normal", api: f.api, root: root}
	pg.mu.Unlock()
	result, callErr := worker.executeControl(ctx, closureControl(session, config.Instance, lifecycleSequenceAction, selector))
	after, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State().Owner, after.State().Owner)
	require.Equal(t, before.State().InitialHeld, after.State().InitialHeld)
	require.Equal(t, before.State().HighWater, after.State().HighWater)
	require.Equal(t, before.State().History, after.State().History)
	pg.mu.Lock()
	opens, renewals := pg.reopenFixture.posts, pg.leaseRenewals
	pg.mu.Unlock()
	require.Zero(t, renewals)
	if mode == "source-drift" || mode == "wrong-selector" {
		require.Error(t, callErr)
		require.Equal(t, before.State(), after.State())
		require.False(t, pg.shared().Observed.AdmissionOpen)
		require.Zero(t, opens)
		return
	}
	require.NoError(t, callErr)
	require.Equal(t, pgexec.Recorded, result.Progress)
	require.Nil(t, after.State().ServiceClosure)
	life := after.State().ServiceLifecycle
	require.NotNil(t, life)
	require.Equal(t, "serving", life.SafetyStage)
	require.True(t, life.Current.BudgetBound())
	require.Equal(t, after.State().Owner.Epoch, life.Current.Snapshot().IssuerFloorEpoch)
	require.True(t, pg.shared().Observed.AdmissionOpen)
	require.Equal(t, 1, opens)
	var archive struct {
		Closed *journal.ServiceClosure `json:"service_closure"`
	}
	require.NoError(t, json.Unmarshal(life.Context, &archive))
	require.NotNil(t, archive.Closed)
	require.Nil(t, archive.Closed.LatestRearm())
	require.Equal(t, before.State().ServiceClosure.Request, archive.Closed.Request)
	// 完成答复丢失后再次确认只读已结清选择和真实 gate，不重复 setter。
	_, err = worker.completeServiceReopen(ctx, archive.Closed.Reopen.CommandSHA256)
	require.NoError(t, err)
	pg.mu.Lock()
	count := pg.reopenFixture.posts
	pg.mu.Unlock()
	require.Equal(t, opens, count)
	if mode == "new-gate" {
		// 原恢复已完成；后继合法门禁代次只改变当前 peer，历史完成证据保留。
		// 当前容量、Root/Host/Runner 和原始预算均由完整生产读者重新核验。
		pg.mu.Lock()
		pg.reopenFixture = nil // 原恢复 peer 已完成，后继关闭使用正常准入处理器。
		pg.gate.Revision++
		pg.mu.Unlock()
		require.NotEqual(t, life.Current.Snapshot().AdmissionRevision, pg.shared().Observed.AdmissionRevision)
		require.NoError(t, worker.inspectIdleService(ctx, after))
		unchanged, err := f.store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, after.State(), unchanged.State(), "闲置审计不能重写恢复历史")
		pg.mu.Lock()
		renewals = pg.leaseRenewals
		pg.mu.Unlock()
		require.Zero(t, renewals)

		// 历史记录不参与当前续租，当前真实硬限漂移仍由原来源审计关闭服务。
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
		progress, err = worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		closedAgain, err := f.store.Load(ctx)
		require.NoError(t, err)
		require.NotNil(t, closedAgain.State().ServiceClosure)
		require.Equal(t, after.State().InitialHeld, closedAgain.State().InitialHeld)
		require.False(t, pg.shared().Observed.AdmissionOpen)
	}
}

func verifyOpenBudgetTakeover(t *testing.T, ctx context.Context, drift bool, f *businessOwnerTransfer,
	old *Session, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource,
	pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner) {
	t.Helper()
	before := pg.shared().Observed
	require.True(t, before.AdmissionOpen)
	posts, _, resizes := pg.counts()
	// 两个真实选主会话使用同一 VM、PG peer、预算和 Runner。原 Owner 已退出。
	f.stop(f.active)
	f.active = f.start(ctx)
	next := f.await(ctx, f.active)
	require.Greater(t, next.Actor().Epoch, old.Actor().Epoch)
	resources, err := next.NewResourceWorker(ctx, f.resourceConfig)
	require.NoError(t, err)
	t.Cleanup(resources.CloseIdleConnections)
	worker, err := next.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(worker.CloseIdleConnections)
	require.NoError(t, runner.Tick(ctx))
	s, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.NoError(t, next.Check(ctx))
	require.Equal(t, before, pg.shared().Observed)
	var lag *idlePGFenceNeeded
	require.ErrorAs(t, worker.inspectIdleService(ctx, s), &lag)
	require.Equal(t, before, pg.shared().Observed, "只读核验不能提高写者水位")
	if drift {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
		handled, _, err := worker.advanceServiceSafety(ctx, "")
		require.True(t, handled)
		require.Error(t, err)
		require.Equal(t, before, pg.shared().Observed, "硬限漂移时不可沿用旧预算接管")
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
	}
	handled, _, err := worker.advanceServiceSafety(ctx, "")
	require.NoError(t, err)
	require.False(t, handled, "本轮接管完成后应允许正常协调继续")
	before.IssuerFloorEpoch = next.Actor().Epoch
	require.Equal(t, before, pg.shared().Observed)
	after, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, s.State(), after.State(), "接管不能改写操作、预算或服务历史")
	require.NoError(t, worker.inspectIdleService(ctx, after))
	gatePosts, _, resizePosts := pg.counts()
	require.Equal(t, posts, gatePosts)
	require.Equal(t, resizes, resizePosts)
}
