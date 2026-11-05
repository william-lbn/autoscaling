package owner

import (
	"context"
	"encoding/json"
	"errors"
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

// Native期限使用显式单调时钟peer，完整审计每次耗10秒、总计超过30秒。
// Epoch仍由真实Elector接管；这验证协议活性，不冒真实PG/宿主耗时证明。
func TestProductionAuditedLeaseSpansSlowAuditsAndRunHandoff(t *testing.T) {
	t.Parallel()
	productionInitialService(t, "lifecycle-lease-slow")
}
func TestProductionAuditedLeaseRecoversSettledClosingWithUnknownPredecessor(t *testing.T) {
	t.Parallel()
	productionInitialService(t, "lifecycle-lease-closing-slow")
}
func TestProductionAuditedLeaseRefusesOneExpiredAudit(t *testing.T) {
	t.Parallel()
	productionInitialService(t, "lifecycle-lease-too-slow")
}

func verifyAuditedLeaseSequence(t *testing.T, ctx context.Context, mode string, f *businessOwnerTransfer, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner) {
	t.Helper()
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	progress, e := worker.Advance(ctx)
	require.NoError(t, e)
	require.Equal(t, pgexec.Recorded, progress)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()
	var clock atomic.Uint64
	clock.Store(uint64(1000 * time.Second))
	pg.mu.Lock()
	pg.leaseClock = func() control.Uint64 { return control.Uint64(clock.Add(1)) }
	pg.leaseWindow = control.Uint64(30 * time.Second)
	pg.leaseDeadline = control.Uint64(clock.Load() - 1)
	pg.leaseExpired = true
	pg.rearmFixture = &serviceRearmPG{epoch: session.Actor().Epoch, api: f.api}
	pg.mu.Unlock()
	session, resources, worker = f.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
	require.NoError(t, runner.Tick(ctx))
	pg.mu.Lock()
	pg.reopenFixture = &serviceReopenPG{mode: "normal", api: f.api, root: root}
	pg.mu.Unlock()
	initial, e := f.store.Load(ctx)
	require.NoError(t, e)
	_, _, _, selector := initial.State().ServiceClosure.Selection()
	var originalUnknown *journal.ServiceReopen
	if mode == "closing-slow" {
		// 精确模拟现场：原开放丢回复Unknown，随后原独立关闭确认，不补NoEffect。
		_, e = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationRearmService, selector))
		require.NoError(t, e)
		pg.mu.Lock()
		pg.reopenFixture.mode = "setter-before"
		pg.mu.Unlock()
		_, e = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
		require.Error(t, e)
		unknown, x := f.store.Load(ctx)
		require.NoError(t, x)
		originalUnknown = unknown.State().ServiceClosure.Reopen
		require.Equal(t, journal.Unconfirmed, originalUnknown.Phase)
		_, e = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationCloseServiceReopen, originalUnknown.CommandSHA256))
		require.NoError(t, e)
		closed, x := f.store.Load(ctx)
		require.NoError(t, x)
		require.NotNil(t, closed.State().ServiceClosure.Reopen.Closing)
		require.False(t, closed.State().ServiceClosure.Reopen.Closing.Close.Pending())
		_, _, _, selector = closed.State().ServiceClosure.Reopen.Closing.Close.Selection()
		pg.mu.Lock()
		clock.Store(uint64(pg.leaseDeadline) + 1)
		pg.leaseExpired = true
		pg.reopenFixture = &serviceReopenPG{mode: "normal", api: f.api, root: root}
		pg.mu.Unlock()
		session, resources, worker = f.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
		require.NoError(t, runner.Tick(ctx))
	}
	before, e := f.store.Load(ctx)
	require.NoError(t, e)
	baselineClock := clock.Load()
	step := uint64(10 * time.Second)
	if mode == "too-slow" {
		step = uint64(31 * time.Second)
	}
	var audits atomic.Uint64
	root.mu.Lock()
	root.afterProcess = func() { clock.Add(step); audits.Add(1) }
	root.mu.Unlock()
	command := closureControl(session, config.Instance, lifecycleSequenceAction, selector)
	if mode == "too-slow" {
		_, e = worker.executeControl(ctx, command)
		require.Error(t, e)
		after, x := f.store.Load(ctx)
		require.NoError(t, x)
		require.NotNil(t, after.State().ServiceClosure)
		require.Nil(t, after.State().ServiceClosure.Reopen)
		pg.mu.Lock()
		require.Zero(t, pg.leaseRenewals, "单次完整审计已过期，绝不续活")
		require.True(t, pg.leaseExpired)
		pg.mu.Unlock()
		require.Equal(t, before.State().Active, after.State().Active)
		require.Equal(t, before.State().History, after.State().History)
		return
	}
	endpoint := worker.NewControlEndpoint()
	loopCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	ready := make(chan struct{}, 1)
	healthy := make(chan struct{}, 1)
	postComplete := atomic.Bool{}
	postTick := make(chan Tick, 1)
	go func() {
		done <- worker.Run(loopCtx, PGLoopOptions{PollInterval: 20 * time.Millisecond, Control: endpoint, Observe: func(tick Tick) {
			if postComplete.Load() {
				select {
				case postTick <- tick:
				default:
				}
			}
			select {
			case ready <- struct{}{}:
			default:
			}
			if tick.Err == nil {
				select {
				case healthy <- struct{}{}:
				default:
				}
			}
		}})
	}()
	select {
	case <-ready:
	case x := <-done:
		t.Fatalf("Run stopped before selected recovery: %v", x)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	result, e := endpoint.Submit(ctx, command)
	require.NoError(t, e, "full audits each below30s must allow aggregate recovery above30s")
	require.NotNil(t, result.State)
	require.Nil(t, result.State.ServiceClosure)
	require.NotNil(t, result.State.ServiceLifecycle)
	require.Equal(t, "serving", result.State.ServiceLifecycle.SafetyStage)
	postComplete.Store(true)
	// 保留原nil成功条件，只把第一轮完成后错误投影出来，避免无叶等待到总期限。
	select {
	case tick := <-postTick:
		pg.mu.Lock()
		fact, deadline, expired := pg.current().Observed, pg.leaseDeadline, pg.leaseExpired
		expected, returned, httpCode := pg.rearmFixture.lastRenewExpected, pg.rearmFixture.lastRenewReturned, pg.rearmFixture.lastRenewHTTP
		pg.mu.Unlock()
		pending, closing := worker.client.PendingAdmission()
		var nativeCall *compute.CallError
		callCode := "none"
		if errors.As(tick.Err, &nativeCall) {
			callCode = nativeCall.Code
		}
		require.True(t, tick.Err == nil, "post_complete_tick type=%T safety=%t native_expired=%t leadership=%t policy=%t progress=%s idle=%+v gate_open=%t gate_rev=%d floor=%d deadline=%d clock=%d expired=%t pending_admission=%t pending_close=%t pending_resize=%t native_code=%s HTTP=%d renew_expected=%d renew_returned=%d", tick.Err, errors.Is(tick.Err, ErrServiceSafety), errors.Is(tick.Err, errNativeAdmissionExpired), errors.Is(tick.Err, ErrLeadershipLost), errors.Is(tick.Err, ErrPolicyObservation), tick.Progress, tick.IdleService, fact.AdmissionOpen, fact.AdmissionRevision, fact.IssuerFloorEpoch, deadline, clock.Load(), expired, pending != nil, closing != nil, worker.client.Pending() != nil, callCode, httpCode, expected, returned)
	case x := <-done:
		t.Fatalf("Run stopped after completed lifecycle: %T", x)
	case <-ctx.Done():
		t.Fatal("no post-complete Tick within original context")
	}
	// 不在回复后终止Run：真实原latch重新审计与首次正常renew也必须保持有效。
	select {
	case <-healthy:
	case x := <-done:
		t.Fatalf("Run lost completed lifecycle handoff: %v", x)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	pg.mu.Lock()
	renewals := pg.leaseRenewals
	live := !pg.leaseExpired
	observed := pg.current().Observed
	pg.mu.Unlock()
	require.Greater(t, renewals, 0)
	require.True(t, live)
	require.True(t, observed.AdmissionOpen)
	require.Greater(t, clock.Load()-baselineClock, uint64(30*time.Second))
	require.Greater(t, audits.Load(), uint64(3))
	if originalUnknown != nil {
		var frozen struct {
			ServiceClosure *journal.ServiceClosure `json:"service_closure"`
		}
		require.NoError(t, json.Unmarshal(result.State.ServiceLifecycle.Context, &frozen))
		require.NotNil(t, frozen.ServiceClosure.Reopen.Retired)
		require.NotEqual(t, originalUnknown.CommandSHA256, frozen.ServiceClosure.Reopen.CommandSHA256)
		require.Equal(t, journal.Unconfirmed, originalUnknown.Phase, "原Unknown不被伪改为成功")
	}
	stop()
	select {
	case x := <-done:
		require.ErrorIs(t, x, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not join")
	}
}
