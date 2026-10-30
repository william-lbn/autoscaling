package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/policy"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

func automaticConfig(targets ...policy.Target) policy.Config {
	return policy.Config{Targets: targets, SampleIntervalSeconds: 1, MaximumSampleGapSeconds: 5, GrowWindowSeconds: 1, ShrinkWindowSeconds: 2, CooldownSeconds: 3, RequestDeadlineSeconds: 120, ShrinkThresholdPermille: 200, GrowThresholdPermille: 800, EmergencyThresholdPermille: 950}
}

// Work2由当前有状态PG peer返回；它只模拟原生计数，绝不是实际PG内核。
// 自动缩容要求ClientProcesses=0而非仅ForegroundIdle，闭门实例不自动开放。
func serveAutomaticWork(w http.ResponseWriter, pg *initialServicePG) {
	s := pg.shared().Observed
	pgReply(w, control.WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 2, VMShrinkPermit: false, ClientProcesses: 0, StartupClients: 0, IdleClients: 0, FrontendReads: 0, FrontendExecutes: 0, BoundedInputs: 0, RetainedTransactions: 0, BackgroundProcesses: 1, AuxiliaryProcesses: 1, ObserverExcluded: true, ForegroundIdle: !s.AdmissionOpen, Observed: s})
}

func TestProductionAutomaticMemoryPolicyUsesOriginalOwnerAndBudgetChain(t *testing.T) {
	productionInitialService(t, "automatic-policy")
}

func verifyProductionAutomaticBegin(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, store *journal.Store, worker *PGWorker, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture) {
	t.Helper()
	current := pg.shared().Observed
	c := automaticConfig(policy.Target{VMMemoryBytes: 512 << 20, Buffers: current.HighBuffers}, policy.Target{VMMemoryBytes: pg.request.TargetVMMemoryBytes, Buffers: 1024})
	require.NoError(t, worker.ConfigureAutomaticMemory(c, client.CoreV1()))
	require.Error(t, worker.ConfigureAutomaticMemory(c, client.CoreV1()), "不能另开策略或换原资格")
	s, err := store.Load(ctx)
	require.NoError(t, err)
	root.mu.Lock()
	root.view.Actual.MemoryCurrentBytes = 63 << 20
	root.mu.Unlock()
	sample, err := worker.sampleAutomatic(ctx, s)
	require.NoError(t, err)
	require.EqualValues(t, 64<<20, sample.NormalMax)
	require.EqualValues(t, 63<<20, sample.NormalCurrent)
	require.Equal(t, []bool{true, true}, sample.Eligible)
	unchanged := func(t *testing.T) {
		t.Helper()
		_, err := worker.sampleAutomatic(ctx, s)
		require.Error(t, err)
		latest, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, s.State(), latest.State(), "坏采样不能产生Begin、门禁或预算写入")
	}
	require.True(t, t.Run("root-limit-drift-no-begin", func(t *testing.T) {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
		defer func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes--; root.mu.Unlock() }()
		unchanged(t)
	}))
	require.True(t, t.Run("qualification-releasing-no-begin", func(t *testing.T) {
		cm, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, source.Claim.Name, metav1.GetOptions{})
		require.NoError(t, err)
		changed := cm.DeepCopy()
		stamp := metav1.Now()
		changed.DeletionTimestamp = &stamp
		require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
		defer func() {
			require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
		}()
		unchanged(t)
	}))
	require.True(t, t.Run("unqualified-target-no-begin", func(t *testing.T) {
		worker.automatic.config.Targets[1].VMMemoryBytes = 768 << 20
		defer func() { worker.automatic.config.Targets[1].VMMemoryBytes = c.Targets[1].VMMemoryBytes }()
		unchanged(t)
	}))
	require.True(t, t.Run("expired-unaccepted-begin-lost-fence-reply", func(t *testing.T) {
		request := pg.request
		// 本前置是当前owner尚未提交的新Begin，不能复用首次服务前的
		// issuer。期限恢复组合经过真实选主接管；沿用旧epoch会正确地被
		// 生产屏障拒绝，根本没有覆盖此处要验证的CAS丢回复窗口。
		request.Key.IssuerEpoch = s.State().Owner.Epoch
		request.DeadlineUnixMillis = control.Uint64(time.Now().Add(-time.Second).UnixMilli())
		worker.automatic.pending, worker.automatic.pendingRV = &request, s.ResourceVersion()
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
		_, err := worker.advanceAutomatic(ctx)
		var unknown *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &unknown)
		require.Equal(t, request, *worker.automatic.pending, "丢屏障回复保留完整原请求")
		progress, err := worker.advanceAutomatic(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Reconciled, progress)
		require.Nil(t, worker.automatic.pending)
		latest, err := store.Load(ctx)
		require.NoError(t, err)
		want := s.State()
		want.Sequence++
		require.Equal(t, want, latest.State())
	}))
	// 真实Run生成完整请求、Begin成功后丢回复，再以同一WAL读回；不手动
	// Begin，不补Key/期限/领域Completed，后半链接着用原生产执行器推进。
	worker.automatic.next = time.Time{}
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	loopCtx, stop := context.WithCancel(ctx)
	defer stop()
	var ticks []Tick
	var original *control.Request
	err = worker.Run(loopCtx, PGLoopOptions{Control: nil, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, PollInterval: 5 * time.Millisecond, Observe: func(tick Tick) {
		ticks = append(ticks, tick)
		if worker.automatic.pending != nil {
			copyRequest := *worker.automatic.pending
			original = &copyRequest
		}
		if tick.Err == nil && tick.Progress == pgexec.Reconciled {
			stop()
		}
	}})
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, ticks, 2)
	var unknown *journal.UnconfirmedWrite
	require.ErrorAs(t, ticks[0].Err, &unknown)
	require.NoError(t, ticks[1].Err)
	require.NotNil(t, original)
	latest, err := store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, latest.State().Active)
	request := latest.State().Active.Request
	require.Equal(t, *original, request)
	require.Equal(t, control.Grow, request.Kind)
	require.Equal(t, c.Targets[1].VMMemoryBytes, request.TargetVMMemoryBytes)
	require.EqualValues(t, 1024, request.TargetBuffers)
	require.Equal(t, current.CapacityGeneration, request.ExpectedCapacityGeneration)
	require.Nil(t, worker.automatic.pending)
	_, err = worker.sampleAutomatic(ctx, latest)
	require.Error(t, err, "已有Active时不能继续采样或发新Begin")
	pg.mu.Lock()
	pg.request = request
	pg.after.Observed = pgResponse(t, request).Observed
	pg.mu.Unlock()
}

// 正式Finish和Host结清后，重建策略必须重新累计缩容窗口。后继选择原资格
// 明确支持的2GiB→1GiB，保持normal硬上界；安全降normal仍是独立缺口。
// PG/Root/QMP只提供协议事实，实际Prepare/Quiesce/Runner/预算/准入/Finish
// 必须由生产代码顺序推进，不手填领域Completed，也不手动Store.Finish。
func verifyProductionAutomaticSuccessor(t *testing.T, ctx context.Context, client *kubefake.Clientset, coord coordclient.CoordinationV1Interface, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, runner *vmguard.Runner, qmp *vmQMPPeer) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	var progress pgexec.Progress
	if s.State().Active != nil {
		progress, err = worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		s, err = store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, journal.Succeeded, s.State().History[0].Outcome)
	} else {
		// 新恢复组合已在独立完成CAS结案，原Resume结果保持未知。
		// 不调用普通Finish再把它投影成Succeeded；后继Shrink与Host
		// 仍走相同生产协议，不能通过手填资源Completed继续。
		require.Len(t, s.State().History, 1)
		require.Contains(t, []journal.Outcome{journal.RecoveredAfterResourceCommit, journal.RecoveredAfterCancellation}, s.State().History[0].Outcome)
		require.NotNil(t, s.State().History[0].ServiceCoverage)
	}
	require.Nil(t, s.State().Active)
	require.Len(t, s.State().History, 1)
	node, err := client.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	var ledger hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger))
	stopHost := keepUnchangedHostLease(t, ctx, client, ledger.Binding)
	defer stopHost()
	allocator, err := hostguard.NewStore(client.CoreV1(), client.CoordinationV1(), ledger.Binding, "host-fixture-holder")
	require.NoError(t, err)
	publisher, err := hostexec.New(client.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	settled, err := publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, settled)
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	c := automaticConfig(policy.Target{VMMemoryBytes: 1 << 30, Buffers: 512}, policy.Target{VMMemoryBytes: 2 << 30, Buffers: 1024})
	require.NoError(t, worker.ConfigureAutomaticMemory(c, client.CoreV1()))
	s, err = store.Load(ctx)
	require.NoError(t, err)
	sample, err := worker.sampleAutomatic(ctx, s)
	require.NoError(t, err)
	t.Logf("自动Shrink首样本：age=%s current=%+v normal=%d/%d clients=%d open=%t eligible=%v", time.Since(sample.At), sample.Current, sample.NormalCurrent, sample.NormalMax, sample.ClientProcesses, sample.AdmissionOpen, sample.Eligible)
	loopCtx, stop := context.WithCancel(ctx)
	defer stop()
	var sampleTick int
	lastTick := time.Now()
	err = worker.Run(loopCtx, PGLoopOptions{Control: nil, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, PollInterval: 20 * time.Millisecond, Observe: func(tick Tick) {
		sampleTick++
		if sampleTick <= 4 || sampleTick%10 == 0 {
			t.Logf("自动Shrink采样：tick=%d progress=%s interval=%s", sampleTick, tick.Progress, time.Since(lastTick))
		}
		lastTick = time.Now()
		if tick.Err != nil {
			// 失败时保留准确领域阶段和只读审计原因，不能把安全关闭
			// 误报成策略超时。只输出状态类别/摘要，不输出凭据或领域原文。
			snapshot, loadErr := store.Load(ctx)
			if loadErr == nil {
				m := snapshot.State()
				reason := ""
				if m.ServiceClosure != nil {
					reason = m.ServiceClosure.Reason
				}
				t.Logf("自动Shrink失败边界：sequence=%d closure_version=%d reason=%s active=%t idle_audit=%v", m.Sequence, m.ClosureFormatVersion(), reason, m.Active != nil, worker.inspectIdleService(ctx, snapshot))
			}
		}
		require.NoError(t, tick.Err)
		if tick.Progress == pgexec.Recorded {
			stop()
		}
	}})
	require.ErrorIs(t, err, context.Canceled)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, s.State().Active)
	r := s.State().Active.Request
	require.Equal(t, control.Shrink, r.Kind)
	require.EqualValues(t, 1<<30, r.TargetVMMemoryBytes)
	require.EqualValues(t, 512, r.TargetBuffers)
	require.Equal(t, pg.shared().Observed.CapacityGeneration, r.ExpectedCapacityGeneration)
	require.Equal(t, s.State().History[0].Operation.Request.Key.Revision+1, r.Key.Revision)
	progress, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	var prepared budget.Prepared
	require.NoError(t, json.Unmarshal(budgetStep(t, store, journal.PrepareBudget).Observation.Data, &prepared))
	require.Equal(t, r, prepared.Request)
	require.EqualValues(t, 512<<20, prepared.Limits.MemoryMaxBytes, "自动缩容不得偷偷降低normal上界")
	// 下一轮切换同一PG实例的有状态协议peer，只替代原生响应；SDK重建
	// 必须从原完整WAL继续。Prepare已经是生产者真实生成的原预算/资格，
	// 不能重新注入它的Completed来跳过本轮预算或Host持有。
	pg.mu.Lock()
	opening := *pg.gate
	pg.mu.Unlock()
	var shrinkPG partialPGPeer
	shrinkPG.admission = opening
	shrinkPG.configure(t, prepared)
	shrinkConfig := pgConfig(t, r, func(w http.ResponseWriter, req *http.Request) {
		shrinkPG.serve(t, w, req)
	}, func() control.Snapshot { return shrinkPG.shared().Observed })
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, shrinkConfig, resources, client.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	_, err = publisher.Reconcile(ctx)
	require.NoError(t, err)
	t.Log("自动Shrink原Prepare与Host保留已确认，继续排空/PG/Guest/Runner")
	for _, action := range []journal.Action{journal.Quiesce, journal.ResizePG, journal.ReleasePG, journal.GuestReady} {
		advancePartialStep(t, ctx, store, worker, action, journal.Settled)
	}
	progress, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizeVM).Phase)
	require.NoError(t, runner.Tick(ctx))
	for _, action := range []journal.Action{journal.ResizeVM, journal.ObserveVM, journal.FinalizeBudget, journal.ResumeAdmission} {
		advancePartialStep(t, ctx, store, worker, action, journal.Settled)
	}
	var vmResult budget.VMSettlement
	require.NoError(t, json.Unmarshal(budgetStep(t, store, journal.ObserveVM).Observation.Data, &vmResult))
	require.EqualValues(t, 1<<30, vmResult.ActualMemoryBytes)
	require.EqualValues(t, 1<<30, vmResult.QMPObservedMemoryBytes)
	progress, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress, "Shrink必须由生产执行器正常Finish")
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active)
	require.Len(t, s.State().History, 2)
	require.Equal(t, journal.Succeeded, s.State().History[1].Outcome)
	require.Equal(t, r, s.State().History[1].Operation.Request)
	finalized, err := s.State().BudgetFor(r)
	require.NoError(t, err)
	require.EqualValues(t, 1<<30, finalized.Request.TargetVMMemoryBytes)
	require.True(t, shrinkPG.shared().Observed.AdmissionOpen)
	settled, err = publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, settled)
	qmp.mu.Lock()
	require.Equal(t, uint64(1<<30), qmp.actual)
	require.Equal(t, 2, qmp.writes, "整组扩一次、缩一次，不重复QMP写")
	qmp.mu.Unlock()
	shrinkPG.mu.Lock()
	require.Equal(t, 1, shrinkPG.resizePosts)
	require.Equal(t, 2, shrinkPG.admissionPosts, "Shrink只关闭和重新开放各一次")
	shrinkPG.mu.Unlock()
	t.Log("自动Shrink全领域、正常Finish和Host结清已确认，QMP实际读回1GiB")
}

// Pending恢复不能借持久owner字段尚未变而绕过原Session失效。用已取消
// Session在任何SDK/Store访问前拒绝，保证恢复分支也执行和Advance相同的门禁。
func TestAutomaticMemoryPendingRejectsLostSessionBeforeAnyIO(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var session Session
	session.ctx = ctx
	var worker PGWorker
	worker.session = &session
	progress, err := worker.advanceAutomatic(context.Background())
	require.ErrorIs(t, err, ErrLeadershipLost)
	require.Equal(t, pgexec.AwaitingFact, progress)
}

// 正式Prepare/Host/Guest/Runner/最终预算/准入全链携带原AS，且自动Grow后再
// 自动Shrink。内核仍是明确peer；不把这一控制组合称为Linux RLIMIT_AS认证。
func TestProductionAutomaticMemoryKeepsOriginalAddressSpaceAcrossGrowShrink(t *testing.T) {
	productionInitialService(t, "automatic-policy-as")
}
