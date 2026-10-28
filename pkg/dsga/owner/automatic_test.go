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
	c := automaticConfig(policy.Target{VMMemoryBytes: 512 << 20, Buffers: current.HighBuffers}, policy.Target{VMMemoryBytes: 1 << 30, Buffers: 1024})
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
		worker.automatic.config.Targets[1].VMMemoryBytes = 2 << 30
		defer func() { worker.automatic.config.Targets[1].VMMemoryBytes = 1 << 30 }()
		unchanged(t)
	}))
	require.True(t, t.Run("expired-unaccepted-begin-lost-fence-reply", func(t *testing.T) {
		request := pg.request
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
	require.EqualValues(t, 1<<30, request.TargetVMMemoryBytes)
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

// 正式Finish和Host结清后，重建策略必须重新累计缩容窗口。后继选择同VM
// 较小Buffer且保持normal硬上界的真实支持目标；安全降normal仍是独立缺口，
// 不在peer中改小约束来绕过Prepare拒绝。这里核到正式Prepare，后续仍Planned。
func verifyProductionAutomaticSuccessor(t *testing.T, ctx context.Context, client *kubefake.Clientset, coord coordclient.CoordinationV1Interface, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG) {
	t.Helper()
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	s, err := store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active)
	require.Len(t, s.State().History, 1)
	require.Equal(t, journal.Succeeded, s.State().History[0].Outcome)
	node, err := client.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	var ledger hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger))
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
	c := automaticConfig(policy.Target{VMMemoryBytes: 1 << 30, Buffers: 512}, policy.Target{VMMemoryBytes: 1 << 30, Buffers: 1024})
	require.NoError(t, worker.ConfigureAutomaticMemory(c, client.CoreV1()))
	loopCtx, stop := context.WithCancel(ctx)
	defer stop()
	err = worker.Run(loopCtx, PGLoopOptions{Control: nil, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, PollInterval: 20 * time.Millisecond, Observe: func(tick Tick) {
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
	s, err = store.Load(ctx)
	require.NoError(t, err)
	for _, step := range s.State().Active.Steps[1:] {
		require.Equal(t, journal.Planned, step.Phase)
	}
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
