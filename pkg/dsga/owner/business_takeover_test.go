package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 本夹具只调度正式Elector产生的Session，不构造Session、手改Owner、删除Lease
// 或缩短其过期时间。注册回调持有Session直至失租，业务调用在测试goroutine顺序
// 执行；跨回调join本身由原owner测试负责，这里补完整生产领域接线和保留事实。
// 真实apiserver/RBAC/进程分区仍须Linux验收，HTTP/Kube/root/PG/QMP都是显式peer。
type businessOwnerTransfer struct {
	t              *testing.T
	api            *ownerAPI
	coord          coordclient.CoordinationV1Interface
	store          *journal.Store
	resourceConfig resourceguard.ClientConfig
	active         *businessOwnerRun
	// 仅第二个窗口：QMP已产生效果且回复丢失，不能走未发送NoEffect。
	effectBeforeTakeover bool
	// 同一选主故障入口可在不同真实已派发领域停住；默认仍是既有VM窗口。
	takeoverAction journal.Action
}

type businessOwnerRun struct {
	ready  chan *Session
	done   chan error
	cancel context.CancelFunc
	joined bool
}

func newBusinessOwnerTransfer(t *testing.T, api *ownerAPI, coord coordclient.CoordinationV1Interface, store *journal.Store, resources resourceguard.ClientConfig) *businessOwnerTransfer {
	t.Helper()
	return &businessOwnerTransfer{t: t, api: api, coord: coord, store: store, resourceConfig: resources, active: nil, effectBeforeTakeover: false, takeoverAction: journal.ResizeVM}
}

func (f *businessOwnerTransfer) start(ctx context.Context) *businessOwnerRun {
	f.t.Helper()
	elector, err := New(f.coord, f.store, DefaultOptions())
	require.NoError(f.t, err)
	runCtx, cancel := context.WithCancel(ctx)
	run := &businessOwnerRun{ready: make(chan *Session, 1), done: make(chan error, 1), cancel: cancel, joined: false}
	go func() {
		run.done <- elector.Run(runCtx, func(ctx context.Context, session *Session) error {
			select {
			case run.ready <- session:
			case <-ctx.Done():
				return nil
			}
			<-ctx.Done()
			return nil
		})
	}()
	f.t.Cleanup(func() { f.stop(run) })
	return run
}

func (f *businessOwnerTransfer) stop(run *businessOwnerRun) {
	f.t.Helper()
	if run.joined {
		return
	}
	run.cancel()
	select {
	case err := <-run.done:
		run.joined = true
		require.ErrorIs(f.t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		f.t.Fatal("business fixture owner did not join after cancellation")
	}
}

func (f *businessOwnerTransfer) await(ctx context.Context, run *businessOwnerRun) *Session {
	f.t.Helper()
	select {
	case session := <-run.ready:
		require.NoError(f.t, session.Check(ctx))
		return session
	case err := <-run.done:
		run.joined = true
		f.t.Fatalf("business fixture owner stopped before acquisition: %v", err)
	case <-ctx.Done():
		f.t.Fatal("business fixture owner was not acquired within test deadline")
	}
	return nil
}

func (f *businessOwnerTransfer) run(ctx context.Context, business func(context.Context, *Session) error) error {
	f.active = f.start(ctx)
	session := f.await(ctx, f.active)
	defer func() { f.stop(f.active) }()
	// 业务总ctx独立于旧Session；每次正式API仍会耦合各自Session ctx。
	// 接管后继续用旧Session必须失败，不能用已取消ctx让所有调用一起假成功。
	return business(ctx, session)
}

func (f *businessOwnerTransfer) takeover(t *testing.T, ctx context.Context, old *Session, resources *ResourceWorker, worker *PGWorker, pgConfig compute.Config, core *kubefake.Clientset, source budget.QualificationSource, runner *vmguard.Runner) (*Session, *ResourceWorker, *PGWorker) {
	t.Helper()
	before, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.Unconfirmed, budgetStep(t, f.store, f.takeoverAction).Phase)
	nextRun := f.start(ctx)
	// 两Elector先真实竞争同一租约；旧owner仍续约时后继不能拿到Session。
	select {
	case <-nextRun.ready:
		t.Fatal("successor bypassed current renewing business owner")
	case <-time.After(200 * time.Millisecond):
	}
	// 不主动取消旧owner：只拒绝其续约，等待正式client-go的原
	// RenewDeadline触发失租并join回调。未放宽30/20/2秒生产期限。
	f.api.mu.Lock()
	f.api.denyLeaseHolder = old.Actor().HolderID
	f.api.mu.Unlock()
	select {
	case err := <-f.active.done:
		f.active.joined = true
		require.ErrorIs(t, err, ErrLeadershipLost)
		f.active.cancel()
	case <-ctx.Done():
		t.Fatal("original business owner did not lose failed renewals")
	}
	// 下一次VM接管CAS的响应故意丢失；claim只能重读确认同一次决定，
	// 不能因此再增加epoch或重建未决操作/预算。
	f.api.mu.Lock()
	f.api.loseJournalReply = true
	f.api.mu.Unlock()
	f.active = nextRun
	next := f.await(ctx, nextRun)
	f.api.mu.Lock()
	lostReplyConsumed := !f.api.loseJournalReply
	f.api.mu.Unlock()
	require.True(t, lostReplyConsumed, "后继接管CAS必须实际经历写成功丢回复")
	require.Equal(t, old.Ownership().LeaseUID, next.Ownership().LeaseUID)
	require.NotEqual(t, old.Actor().HolderID, next.Actor().HolderID)
	require.Equal(t, old.Actor().Epoch+1, next.Actor().Epoch)
	after, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State().Sequence+1, after.State().Sequence, "接管只有一项原UID/RV持久变化")
	require.Equal(t, before.State().Active, after.State().Active)
	require.Equal(t, before.State().InitialHeld, after.State().InitialHeld)
	require.Equal(t, before.State().HighWater, after.State().HighWater)
	require.Equal(t, before.State().BudgetHeld, after.State().BudgetHeld)
	require.Equal(t, before.State().CapacityHeld, after.State().CapacityHeld)
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, ErrLeadershipLost)
	_, err = resources.Inspect(ctx)
	require.ErrorIs(t, err, ErrLeadershipLost)
	_, err = worker.ReplayVMOriginal(ctx, before.State().Active.Request.Key)
	require.ErrorIs(t, err, ErrLeadershipLost, "旧Session迟到重放不能写原Runner选择")
	if f.takeoverAction == journal.ResizePG {
		_, err = worker.ReplayOriginal(ctx, before.State().Active.Request.Key)
		require.ErrorIs(t, err, ErrLeadershipLost, "旧Session不能迟到重新派发原PG")
	} else if f.takeoverAction == journal.GuestReady {
		_, err = worker.ReplayResourceOriginal(ctx, before.State().Active.Request.Key)
		require.ErrorIs(t, err, ErrLeadershipLost, "旧Session不能迟到重新派发原Root设置")
	}
	resources.CloseIdleConnections()
	worker.CloseIdleConnections()
	var nextResources *ResourceWorker
	if f.takeoverAction == journal.GuestReady {
		// 与Agent真实启动顺序相同：先恢复Root，再建立PG执行器。Root完成
		// WAL未知时不能绕过此gate直接构造PGWorker，否则会漏掉恢复环依赖。
		nextResources, err = next.NewResourceRecoveryWorker(ctx, f.resourceConfig)
		require.NoError(t, err)
		require.NotNil(t, nextResources.client.Pending(), "新SDK必须先恢复原GuestReady未知")
		require.NoError(t, nextResources.NewRecoveryControlEndpoint().RunResourceRecovery(ctx, 5*time.Millisecond))
	} else {
		nextResources, err = next.NewResourceWorker(ctx, f.resourceConfig)
	}
	require.NoError(t, err)
	t.Cleanup(nextResources.CloseIdleConnections)
	nextWorker, err := next.NewProductionPGWorker(ctx, pgConfig, nextResources, core.CoreV1(), source)
	require.NoError(t, err)
	t.Cleanup(nextWorker.CloseIdleConnections)
	_, err = nextWorker.ReplayVMOriginal(ctx, before.State().Active.Request.Key)
	require.Error(t, err, "新owner不可把旧epoch原ResizeVM派发重放")
	// 推进正式Runner的独立floor。尚未发送的旧命令不得发送；已越过
	// may_have_sent只读回原结果。Tick返回不能独自推导NoEffect，后续
	// 外层仍须以完整原不可变来源确认Completed或封住的取消终态。
	if f.effectBeforeTakeover {
		require.NoError(t, runner.Tick(ctx), "新owner只读原may_have_sent结果，不能重发QMP")
	} else {
		require.Error(t, runner.Tick(ctx), "旧epoch命令尚未取消，不能因新owner而派发")
	}
	after, err = f.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State().Active, after.State().Active)
	return next, nextResources, nextWorker
}

// 跨owner测试的PG有状态fence：请求必须等于实际已选主journal代次，
// floor单调持有，并同步到所有后续共享成本/准入/resize快照。旧Key仍保持
// 原epoch，不把Fenced或当前快照冒充旧命令已完成。
func (p *initialServicePG) ownerFence(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		var request control.FenceRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		s, err := p.store.Load(r.Context())
		require.NoError(t, err)
		require.Equal(t, s.State().Owner.Epoch, request.IssuerEpoch)
		p.mu.Lock()
		defer p.mu.Unlock()
		require.Equal(t, p.request.Key.Instance, request.Instance)
		require.GreaterOrEqual(t, request.IssuerEpoch, p.before.Observed.IssuerFloorEpoch)
		p.before.Observed.IssuerFloorEpoch = request.IssuerEpoch
		p.after.Observed.IssuerFloorEpoch = request.IssuerEpoch
		pgReply(w, control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: request.Instance, RequestedEpoch: request.IssuerEpoch, Disposition: control.Fenced, Observed: p.current().Observed})
	}
}
