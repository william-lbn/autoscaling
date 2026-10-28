package owner

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 一次正式首次开放→Prepare/Host/VM/Guest/PG/预算/准入之后，串联所有
// 新结案窗口，不重复旧失租/长超时矩阵。来源故障只改显式peer输入；WAL、
// 生产SDK、owner循环、Finish CAS、Host结清与下一Prepare均走真实代码。
// peer并不是Linux PG/Root、K8s API或真实QEMU，不能外推物理/权限认证。
func verifyAutomaticNormalFinish(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture) *PGWorker {
	t.Helper()
	before, err := store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, before.State().Active)
	for _, step := range before.State().Active.Steps {
		require.Equal(t, journal.Settled, step.Phase)
		require.Equal(t, journal.Completed, step.Observation.Result)
	}
	postsBefore, _, resizesBefore := pg.counts()
	root.mu.Lock()
	rootPosts := root.posts
	root.mu.Unlock()
	blocked := func(t *testing.T) {
		t.Helper()
		progress, err := worker.Advance(ctx)
		require.Equal(t, pgexec.AwaitingFact, progress, "结案前必须重新认证原来源，不能静默停ExternalStep或Finish")
		if err != nil {
			t.Logf("保留原Active：%v", err)
		}
		after, readErr := store.Load(ctx)
		require.NoError(t, readErr)
		require.Equal(t, before.State(), after.State(), "来源未确认不得改原回执、预算头、关闭边界或历史")
	}
	require.True(t, t.Run("root-limit-drift", func(t *testing.T) {
		root.mu.Lock()
		original := root.view.Actual.MemoryMaxBytes
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
		defer func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes = original; root.mu.Unlock() }()
		blocked(t)
	}))
	require.True(t, t.Run("qualification-releasing", func(t *testing.T) {
		cm, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, source.Claim.Name, metav1.GetOptions{})
		require.NoError(t, err)
		changed := cm.DeepCopy()
		stamp := metav1.Now()
		changed.DeletionTimestamp = &stamp
		require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
		defer func() {
			require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
		}()
		blocked(t)
	}))
	require.True(t, t.Run("postmaster-replaced", func(t *testing.T) {
		root.mu.Lock()
		birth := *root.view.Birth
		birth.PostmasterStartTicks++
		root.processOverride = &birth
		root.mu.Unlock()
		defer func() { root.mu.Lock(); root.processOverride = nil; root.mu.Unlock() }()
		blocked(t)
	}))
	require.True(t, t.Run("admission-superseded", func(t *testing.T) {
		pg.mu.Lock()
		original := pg.gate
		closed := *original
		closed.Open, closed.Revision = false, closed.Revision+1
		pg.gate = &closed
		pg.mu.Unlock()
		defer func() { pg.mu.Lock(); pg.gate = original; pg.mu.Unlock() }()
		blocked(t)
	}))
	require.True(t, t.Run("vm-rv-changed-during-check", func(t *testing.T) {
		// 只改变resourceVersion，保持完整原journal及预算/Pod数值，证明
		// 不是靠目标不同被其他门禁偶然挡住。下次必须使用新RV重新核验。
		root.mu.Lock()
		root.afterProcess = func() {
			api.mu.Lock()
			api.vmVersion++
			api.vm.ResourceVersion = fmt.Sprintf("finish-interleaved-%d", api.vmVersion)
			api.mu.Unlock()
		}
		root.mu.Unlock()
		defer func() { root.mu.Lock(); root.afterProcess = nil; root.mu.Unlock() }()
		blocked(t)
	}))
	// 真正Run而非测试手动Store.Finish；只有Finish这一写丢回复。循环必须
	// 保留未知，再从原WAL读到已结案，不能重开、重发setter或生成新操作。
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	loopCtx, stop := context.WithCancel(ctx)
	defer stop()
	var ticks []Tick
	err = worker.Run(loopCtx, PGLoopOptions{PollInterval: 5 * time.Millisecond, Control: nil, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: func(tick Tick) {
		ticks = append(ticks, tick)
		if tick.Err == nil && tick.Progress == pgexec.Idle {
			stop()
		}
	}})
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, ticks, 2)
	var unknown *journal.UnconfirmedWrite
	require.True(t, errors.As(ticks[0].Err, &unknown), "必须真实到达Finish CAS提交后丢回复")
	require.Equal(t, pgexec.AwaitingFact, ticks[0].Progress)
	require.NoError(t, ticks[1].Err)
	require.Equal(t, pgexec.Idle, ticks[1].Progress)
	after, err := store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, after.State().Active)
	require.Len(t, after.State().History, 1)
	require.Equal(t, journal.Succeeded, after.State().History[0].Outcome)
	require.Equal(t, *before.State().Active, after.State().History[0].Operation)
	require.Equal(t, before.State().BudgetHeld, after.State().BudgetHeld)
	require.Equal(t, before.State().Sequence+1, after.State().Sequence)
	require.NoError(t, after.State().CheckClosedBudget(before.State().Active.Request))
	postsAfter, _, resizesAfter := pg.counts()
	require.Equal(t, postsBefore, postsAfter, "全部结案/故障只读原开放，不重发setter")
	require.Equal(t, resizesBefore, resizesAfter)
	root.mu.Lock()
	require.Equal(t, rootPosts, root.posts)
	root.mu.Unlock()
	worker.CloseIdleConnections()
	rebuilt, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(rebuilt.CloseIdleConnections)
	progress, err := rebuilt.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Idle, progress)
	again, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, after.State(), again.State(), "重建和空闲轮询不能重复结案或追加历史")
	return rebuilt
}
