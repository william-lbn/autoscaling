package owner

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 复用正式首次预算/开放前驱及真实Elector Session，不手改owner、缩短
// 租约或填生产终态。HTTP/RFC6902、TLS、管理串行入口、SDK和Journal为
// 真实代码；PG/Root/Kube/平台仍为明确peer，不冒充Linux/K8s端到端认证。
func TestProductionServiceContinuationAcrossOwnersAndUnknownReplies(t *testing.T) {
	for _, mode := range []string{"unknown-windows", "original-applied"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "closure-transfer-"+mode) })
	}
}

func (p *initialServicePG) serveClosureContinuation(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	var request control.AdmissionContinuationRequest
	require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
	s, err := p.store.Load(r.Context())
	require.NoError(t, err)
	c := s.State().ServiceClosure
	require.NotNil(t, c)
	require.NotNil(t, c.Continuation)
	require.Equal(t, request, c.Continuation.Request, "完整写前CAS必须先于任何后继派发")
	query := strings.HasSuffix(r.URL.Path, "/status")
	p.mu.Lock()
	defer p.mu.Unlock()
	if query {
		p.queries++
		if p.gate == nil || *p.gate != request.Close {
			w.WriteHeader(http.StatusConflict)
			return
		}
	} else {
		require.Equal(t, journal.Unconfirmed, c.Continuation.Phase)
		require.Equal(t, s.State().Owner.Epoch, request.Close.IssuerEpoch)
		require.Equal(t, request.Close.IssuerEpoch, p.current().Observed.IssuerFloorEpoch, "必须先fence再派发后继关闭")
		p.posts++
		require.Equal(t, request.Close.ExpectedCapacityGeneration, p.current().Observed.CapacityGeneration)
		if p.loseBefore {
			p.loseBefore = false
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if p.gate == nil || *p.gate != request.Close {
			require.Greater(t, request.Close.Revision, p.current().Observed.AdmissionRevision)
			closeRequest := request.Close
			p.gate = &closeRequest
		}
		if p.loseAfter {
			p.loseAfter = false
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	}
	digest, err := request.ContextSHA256()
	require.NoError(t, err)
	closeDigest, err := request.Close.ContextSHA256()
	require.NoError(t, err)
	disposition := control.AdmissionApplied
	if query {
		disposition = control.AdmissionReplayed
	}
	closed := control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: request.Close, Disposition: disposition, ContextSHA256: closeDigest, Observed: p.current().Observed}
	pgReply(w, control.AdmissionContinuationResponse{ProtocolVersion: request.ProtocolVersion, VMShrinkPermit: false, Request: request, ContextSHA256: digest, ClosedBy: closed, PriorMayHaveApplied: true})
}

// 独立调度关闭/恢复期间的真实失租接管窗口，包含13/14已结算资源的
// 冻结Active。原关闭、Active和全部预算逐字保留，CAS丢回复仅重读确认。
func (f *businessOwnerTransfer) takeoverClosure(t *testing.T, ctx context.Context, old *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource) (*Session, *ResourceWorker, *PGWorker) {
	t.Helper()
	before, err := f.store.Load(ctx)
	require.NoError(t, err)
	c := before.State().ServiceClosure
	require.True(t, c.Pending() || c.Reopen != nil, "接管前必须存在原关闭或独立恢复WAL")
	nextRun := f.start(ctx)
	select {
	case <-nextRun.ready:
		t.Fatal("后继不能越过仍续约的关闭owner")
	case <-time.After(200 * time.Millisecond):
	}
	f.api.mu.Lock()
	f.api.denyLeaseHolder = old.Actor().HolderID
	f.api.mu.Unlock()
	select {
	case err := <-f.active.done:
		f.active.joined = true
		require.ErrorIs(t, err, ErrLeadershipLost)
		f.active.cancel()
	case <-ctx.Done():
		t.Fatal("原关闭owner失租未退出")
	}
	f.api.mu.Lock()
	f.api.loseJournalReply = true
	f.api.mu.Unlock()
	f.active = nextRun
	next := f.await(ctx, nextRun)
	require.Equal(t, old.Actor().Epoch+1, next.Actor().Epoch)
	require.Equal(t, old.Ownership().LeaseUID, next.Ownership().LeaseUID)
	after, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State().Sequence+1, after.State().Sequence)
	require.Equal(t, before.State().ServiceClosure, after.State().ServiceClosure)
	require.Equal(t, before.State().InitialHeld, after.State().InitialHeld)
	require.Equal(t, before.State().Active, after.State().Active)
	require.Equal(t, before.State().BudgetHeld, after.State().BudgetHeld)
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, ErrLeadershipLost)
	_, err = resources.Inspect(ctx)
	require.ErrorIs(t, err, ErrLeadershipLost)
	_, _, _, digest := before.State().ServiceClosure.Selection()
	_, err = worker.executeControl(ctx, closureControl(old, config.Instance, control.CoordinationContinueServiceClosure, digest))
	// executeControl先耦合Session ctx，失租可能先在该处报告Canceled；
	// Advance/Inspect则先报告LeadershipLost。两者都必须零派发，不能
	// 为测试统一错误文字而放宽生产的最早取消边界。
	require.True(t, errors.Is(err, ErrLeadershipLost) || errors.Is(err, context.Canceled), "旧Session不能迟到派发后继关闭: %v", err)
	worker.CloseIdleConnections()
	resources.CloseIdleConnections()
	nextResources, err := next.NewResourceWorker(ctx, f.resourceConfig)
	require.NoError(t, err)
	t.Cleanup(nextResources.CloseIdleConnections)
	nextWorker, err := next.NewProductionPGWorker(ctx, config, nextResources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(nextWorker.CloseIdleConnections)
	return next, nextResources, nextWorker
}

func closureControl(session *Session, instance control.Instance, action control.CoordinationAction, selector string) control.CoordinationCommand {
	return control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(instance), ExpectedOwnerEpoch: session.Actor().Epoch, Action: action, Request: nil, Key: nil, Selector: selector}
}

func verifyServiceClosureContinuation(t *testing.T, ctx context.Context, mode string, transfer *businessOwnerTransfer, core *kubefake.Clientset, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner) {
	t.Helper()
	applied := mode == "closure-transfer-original-applied" || strings.HasPrefix(mode, "closure-transfer-rearm-")
	before, err := transfer.store.Load(ctx)
	require.NoError(t, err)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	var restored bool
	restore := func() {
		if !restored {
			root.mu.Lock()
			root.view.Actual.MemoryMaxBytes--
			root.mu.Unlock()
			restored = true
		}
	}
	defer restore()
	if applied {
		pg.faults(false, true)
	} else {
		transfer.api.mu.Lock()
		transfer.api.loseJournalReply = true
		transfer.api.mu.Unlock()
	}
	_, err = worker.Advance(ctx)
	require.Error(t, err)
	s, err := transfer.store.Load(ctx)
	require.NoError(t, err)
	original := *s.State().ServiceClosure
	require.Equal(t, journal.Unconfirmed, original.Phase)
	require.Equal(t, !applied, pg.shared().Observed.AdmissionOpen)
	session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
	command := closureControl(session, config.Instance, control.CoordinationContinueServiceClosure, original.CommandSHA256)
	posts, _, _ := pg.counts()
	wrong := command.Clone()
	wrong.Selector = strings.Repeat("ab", 32)
	_, err = worker.executeControl(ctx, wrong)
	require.Error(t, err)
	afterWrong, _, _ := pg.counts()
	require.Equal(t, posts, afterWrong)
	if !applied {
		transfer.api.mu.Lock()
		transfer.api.loseJournalReply = true
		transfer.api.mu.Unlock()
		_, err = worker.executeControl(ctx, command)
		var unknown *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &unknown)
		afterWAL, _, _ := pg.counts()
		require.Equal(t, posts, afterWAL, "后继WAL丢回复不得派发")
		s, err = transfer.store.Load(ctx)
		require.NoError(t, err)
		first := *s.State().ServiceClosure.Continuation
		require.Equal(t, original.Request.Revision, first.Request.Close.Revision)
		worker.CloseIdleConnections()
		worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
		require.NoError(t, err)
		t.Cleanup(worker.CloseIdleConnections)
		pending, closing := worker.client.PendingAdmission()
		require.Equal(t, &original.Request, pending)
		require.Equal(t, &first.Request.Close, closing)
		require.Equal(t, &first.Request, worker.client.PendingAdmissionContinuation())
		_, err = worker.Advance(ctx)
		require.ErrorIs(t, err, ErrServiceSafety)
		afterQuery, _, _ := pg.counts()
		require.Equal(t, posts, afterQuery, "普通轮询不得隐式接续或重放")
		_, err = worker.executeControl(ctx, command)
		require.Error(t, err, "同owner不得重新选择revision")
		pg.faults(true, false)
		replay := closureControl(session, config.Instance, control.CoordinationReplayServiceClosure, first.CommandSHA256)
		_, err = worker.executeControl(ctx, replay)
		require.ErrorIs(t, err, ErrServiceSafety)
		posts++
		// 再次接管不能跳过第一次后继。旧关闭仍未到达时，新的owner
		// 可在原revision继续关闭，但要完整保留直接前驱及三份选择。
		session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
		command = closureControl(session, config.Instance, control.CoordinationContinueServiceClosure, original.CommandSHA256)
		_, err = worker.executeControl(ctx, command)
		require.Error(t, err)
		command.Selector = first.CommandSHA256
		pg.faults(false, true)
		_, err = worker.executeControl(ctx, command)
		require.ErrorIs(t, err, ErrServiceSafety)
		posts++
		s, err = transfer.store.Load(ctx)
		require.NoError(t, err)
		require.True(t, s.State().ServiceClosure.Pending())
		require.Equal(t, first.Request.Close, s.State().ServiceClosure.Continuation.Request.PreviousClose)
		worker.CloseIdleConnections()
		worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
		require.NoError(t, err)
		t.Cleanup(worker.CloseIdleConnections)
		transfer.api.mu.Lock()
		transfer.api.loseJournalReply = true
		transfer.api.mu.Unlock()
		_, err = worker.Advance(ctx)
		require.ErrorAs(t, err, &unknown, "查询后落账丢回复仍须保留原WAL")
	} else {
		result, err := worker.executeControl(ctx, command)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, result.Progress)
		posts++
		require.Equal(t, original.Request.Revision+1, result.State.ServiceClosure.Continuation.Request.Close.Revision)
	}
	s, err = transfer.store.Load(ctx)
	require.NoError(t, err)
	c := s.State().ServiceClosure
	require.False(t, c.Pending())
	require.Equal(t, journal.Settled, c.Continuation.Phase)
	retained := *c
	retained.Continuation = nil
	require.Equal(t, original, retained)
	require.Equal(t, before.State().InitialHeld, s.State().InitialHeld)
	require.Empty(t, s.State().History)
	require.Nil(t, s.State().Active)
	require.False(t, pg.shared().Observed.AdmissionOpen)
	actualPosts, _, resizes := pg.counts()
	require.Equal(t, posts, actualPosts)
	require.Zero(t, resizes)
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	pending, closing := worker.client.PendingAdmission()
	require.Nil(t, pending)
	require.Nil(t, closing)
	require.Nil(t, worker.client.PendingAdmissionContinuation())
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, ErrServiceSafety, "来源恢复或重建不能自动重开服务")
	t.Logf("关闭接续完成：mode=%s owner=%d 原revision=%d 后继revision=%d，旧意图和永久预算保留", mode, session.Actor().Epoch, original.Request.Revision, c.Continuation.Request.Close.Revision)
	if strings.HasPrefix(mode, "closure-transfer-rearm-") {
		restore()
		// 本组合此前只运行关闭协调器。恢复期限前必须让真实Runner轮次
		// 确认新owner floor并发布来源，不能手改publication冒充已确认。
		require.NoError(t, runner.Tick(ctx))
		verifyServiceRearm(t, ctx, strings.TrimPrefix(mode, "closure-transfer-rearm-"), transfer.api, transfer.store, session, resources, worker, config, core, source, pg, root)
	}
}
