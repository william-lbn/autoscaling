package owner

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/admissionexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 同一条真实Agent/Runner容量链串联服务独立授权、Start丢回复、setter执行前/
// 后丢回复、Record提交后丢回复及worker重建。PG/Root/资格仍为协议夹具；
// 不手工登记Cleanup/原准入、不增加PG/Guest/VM反向动作，不把重建说成换主。
func verifySelectedActualCancellation(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, store *journal.Store, session *Session, worker *PGWorker, resources *ResourceWorker, config compute.Config, aggregate budget.RootAggregate, shared control.SharedCostResponse, pg *committedAbortPG) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	req := s.State().Active.Request
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, admissionexec.ErrRecoverySelectionRequired, "Grow取消不能自动覆盖外部关闭")
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active.Cleanup)
	wrong := req.Key
	wrong.OperationID.Low++
	_, err = worker.RecoverCapacityAdmission(ctx, wrong)
	require.Error(t, err)
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	selections := make(chan control.Key, 1)
	selections <- req.Key
	loopCtx, stop := context.WithCancel(ctx)
	defer stop()
	err = worker.Run(loopCtx, PGLoopOptions{PollInterval: time.Second, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: selections, Observe: func(tick Tick) {
		var unknown *journal.UnconfirmedWrite
		if errors.As(tick.Err, &unknown) {
			stop()
		}
	}})
	require.ErrorIs(t, err, context.Canceled)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.Unconfirmed, s.State().Active.Cleanup.Phase)
	require.Nil(t, s.State().CapacityHeld)
	var original budget.ReconciledAbortCommand
	require.NoError(t, json.Unmarshal(s.State().Active.Cleanup.Command, &original))
	require.NotNil(t, original.Admission)
	require.Equal(t, req.ExpectedCapacityGeneration, original.Admission.ExpectedCapacityGeneration, "PG未扩，不能使用目标成功的下一代次")
	pg.mu.Lock()
	require.Zero(t, pg.posts, "Start回复未知没有setter票据")
	pg.mu.Unlock()
	rebuild := func() {
		worker.CloseIdleConnections()
		rebuilt, rebuildErr := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
		require.NoError(t, rebuildErr)
		worker = rebuilt
		pending, closing := worker.client.PendingAdmission()
		require.Nil(t, closing)
		require.NotNil(t, pending)
		require.Equal(t, *original.Admission, *pending)
	}
	defer func() { worker.CloseIdleConnections() }()
	rebuild()
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.AwaitingFact, progress, "not-found只读等待，不自动重发")
	pg.mu.Lock()
	require.Zero(t, pg.posts)
	pg.loseBefore, pg.loseAfter = true, true
	pg.afterSet = func() {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	pg.mu.Unlock()
	_, err = worker.ReplayCommittedAbortOriginal(ctx, wrong)
	require.Error(t, err)
	_, err = worker.ReplayCommittedAbortOriginal(ctx, req.Key)
	require.Error(t, err, "setter执行前丢回复")
	rebuild()
	progress, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.AwaitingFact, progress)
	pg.mu.Lock()
	require.Equal(t, 1, pg.posts, "普通轮询没有补发第一次失败的POST")
	pg.mu.Unlock()
	_, err = worker.ReplayCommittedAbortOriginal(ctx, req.Key)
	require.Error(t, err, "setter已执行但回复丢失")
	rebuild()
	_, err = worker.Advance(ctx)
	var unknown *journal.UnconfirmedWrite
	require.ErrorAs(t, err, &unknown, "查询原开放完成，随后Record已提交但回复丢失")
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, s.State().CapacityHeld, "实际预算和原审计须随Record原子保留")
	require.Equal(t, journal.Settled, s.State().Active.Cleanup.Phase)
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
	require.NoError(t, err)
	progress, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active)
	require.NoError(t, s.State().CheckClosedCapacity(req))
	var retained budget.ReconciledAbortCommand
	require.NoError(t, json.Unmarshal(s.State().CapacityHeld.Cleanup.Command, &retained))
	require.Equal(t, original, retained, "所有重建/丢回复均保留原预算和准入，不能重算替换")
	pg.mu.Lock()
	require.Equal(t, 2, pg.posts)
	require.Equal(t, *original.Admission, *pg.last)
	pg.mu.Unlock()
	verifyActualHostSettlement(t, ctx, api, client, store, session, worker, aggregate, shared, retained, true)
}
