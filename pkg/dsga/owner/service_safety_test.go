package owner

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

func TestProductionIdleServiceClosureUsesOriginalWALAndRecovery(t *testing.T) {
	for _, mode := range []string{"root-drift", "qualification", "birth", "wal-before-send", "setter-before", "setter-after"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "service-safety-"+mode) })
	}
}

// 只复用正式首次服务前驱，不重跑Grow/Shrink或已有故障矩阵。坏来源
// 之后走真实Session、SDK、TLS、完整WAL及typed UID/RV；内核/PG/平台仍为
// 显式peer。验证关门，不把原连接或实际物理内存当作已经排空/释放。
func verifyIdleServiceClosure(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, client *kubefake.Clientset, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture) {
	t.Helper()
	mode = strings.TrimPrefix(mode, "service-safety-")
	before, err := store.Load(ctx)
	require.NoError(t, err)
	require.True(t, pg.shared().Observed.AdmissionOpen)
	// 健康空闲轮次不能增加门禁或WAL，也不能影响原首次服务预算。
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Idle, progress)
	healthy, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), healthy.State())
	var restoreSource func()
	switch mode {
	case "qualification":
		cm, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, source.Claim.Name, metav1.GetOptions{})
		require.NoError(t, err)
		changed := cm.DeepCopy()
		stamp := metav1.Now()
		changed.DeletionTimestamp = &stamp
		require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
		restoreSource = func() {
			require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
		}
	case "birth":
		root.mu.Lock()
		process := *root.view.Birth
		process.PostmasterStartTicks++
		root.processOverride = &process
		root.mu.Unlock()
		restoreSource = func() { root.mu.Lock(); root.processOverride = nil; root.mu.Unlock() }
	default:
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
		restoreSource = func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes--; root.mu.Unlock() }
	}
	var restoreOnce sync.Once
	restore := func() { restoreOnce.Do(restoreSource) }
	defer restore()
	if mode == "wal-before-send" {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	pg.faults(mode == "setter-before", mode == "setter-after")
	progress, err = worker.Advance(ctx)
	switch mode {
	case "wal-before-send":
		var unknown *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &unknown)
	case "setter-before", "setter-after":
		require.ErrorIs(t, err, ErrServiceSafety)
	default:
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
	}
	s, err := store.Load(ctx)
	require.NoError(t, err)
	c := s.State().ServiceClosure
	require.NotNil(t, c)
	require.False(t, c.Request.Open)
	require.Equal(t, before.State().InitialHeld, s.State().InitialHeld)
	require.Nil(t, s.State().Active)
	require.Empty(t, s.State().History)
	posts, _, _ := pg.counts()
	wantPosts := 2
	if mode == "wal-before-send" {
		wantPosts = 1
	}
	require.Equal(t, wantPosts, posts, "未获写前票据不能发PG关闭")
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	if c.Phase == journal.Unconfirmed {
		pending, closing := worker.client.PendingAdmission()
		require.Equal(t, &c.Request, pending, "重建必须恢复同一SDK原关闭未知")
		require.Nil(t, closing)
		progress, err = worker.Advance(ctx)
		if mode == "setter-after" {
			require.NoError(t, err)
			require.Equal(t, pgexec.Recorded, progress)
		} else {
			require.ErrorIs(t, err, ErrServiceSafety)
			afterQuery, _, _ := pg.counts()
			require.Equal(t, posts, afterQuery, "NotFound不能在普通轮询中重发")
			wrong := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(c.Request.Instance), ExpectedOwnerEpoch: session.Actor().Epoch, Action: control.CoordinationReplayServiceClosure, Selector: strings.Repeat("ab", 32), Request: nil, Key: nil}
			_, err := worker.executeControl(ctx, wrong)
			require.Error(t, err)
			wrong.Selector = c.CommandSHA256
			result, err := worker.executeControl(ctx, wrong)
			require.NoError(t, err)
			require.Equal(t, pgexec.Recorded, result.Progress)
			wantPosts++
		}
	}
	restore()
	// 来源恢复只影响观察，不授权重新开放或新的Begin。显式旧首次重放
	// 也必须被持久服务关闭拒绝，不能从管理入口绕过它。
	for range 2 {
		_, err := worker.Advance(ctx)
		require.ErrorIs(t, err, ErrServiceSafety)
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.Settled, s.State().ServiceClosure.Phase)
	require.False(t, pg.shared().Observed.AdmissionOpen)
	posts, _, resizePosts := pg.counts()
	require.Equal(t, wantPosts, posts)
	require.Zero(t, resizePosts)
	require.Equal(t, before.State().InitialHeld, s.State().InitialHeld)
	for _, action := range []control.CoordinationAction{control.CoordinationBegin, control.CoordinationReplayInitial} {
		command := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(c.Request.Instance), ExpectedOwnerEpoch: session.Actor().Epoch, Action: action, Request: nil, Key: nil, Selector: ""}
		if action == control.CoordinationBegin {
			command.Request = &pg.request
		} else {
			command.Selector = s.State().InitialHeld.Admission.CommandSHA256
		}
		_, err := worker.executeControl(ctx, command)
		require.ErrorIs(t, err, ErrServiceSafety)
	}
	var fact control.AdmissionResponse
	raw, err := json.Marshal(s.State().ServiceClosure.Receipt)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &fact))
	require.NoError(t, fact.ValidateFor(c.Request, c.Request.Instance, s.State().ServiceClosure.Query))
}
