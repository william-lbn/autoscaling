package owner

import (
	"context"
	"encoding/json"
	"strings"
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

// 使用动作字面值，使旧生产代码也能编译并真实拒绝不支持的行为；RED不靠undefined。
const lifecycleSequenceAction = control.CoordinationAction("recover_service_lifecycle")

func TestProductionSingleControlRecoversServiceLifecycleWithoutPeriodicGap(t *testing.T) {
	t.Parallel()
	productionInitialService(t, "lifecycle-sequence-normal")
}
func TestProductionSingleControlContinuesExpiredSettledRearm(t *testing.T) {
	t.Parallel()
	productionInitialService(t, "lifecycle-sequence-continue")
}
func TestProductionSingleControlLifecycleRetainsUnknownAndRefusesDrift(t *testing.T) {
	for _, mode := range []string{"wrong-selector", "lost-start", "setter-after", "same-owner-expired", "existing-reopen", "lease-drift", "cancel"} {
		t.Run(mode, func(t *testing.T) { t.Parallel(); productionInitialService(t, "lifecycle-sequence-"+mode) })
	}
}
func TestLifecycleSequenceCommandOnlySelectsOriginalClosure(t *testing.T) {
	c := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding{VMUID: "vm", ComputeInstanceID: "compute", PGBootID: 1}, ExpectedOwnerEpoch: 1, Action: lifecycleSequenceAction, Selector: strings.Repeat("a", 64)}
	require.NoError(t, c.Validate())
	require.False(t, c.Action.ResourceOnly())
	require.True(t, expiredAdmissionControlAllowed(c.Action))
	for _, mode := range []string{"empty", "uppercase", "request", "key", "zero-boot"} {
		bad := c
		switch mode {
		case "empty":
			bad.Selector = ""
		case "uppercase":
			bad.Selector = strings.Repeat("A", 64)
		case "request":
			bad.Request = &control.Request{}
		case "key":
			bad.Key = &control.Key{}
		case "zero-boot":
			bad.Instance.PGBootID = 0
		}
		require.Error(t, bad.Validate(), mode)
	}
}

// 原首次完整预算/Root/Runner/SDK→关闭→真实Elector更高Owner→原串行Run。
// Native、Kube、Root均为明确peer，不外推实际Linux/K8s或任一方法的30秒成本。
func verifyServiceLifecycleSequence(t *testing.T, ctx context.Context, mode string, f *businessOwnerTransfer, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner) {
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
	original, err := f.store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, original.State().ServiceClosure)
	pg.mu.Lock()
	pg.leaseExpired = true
	pg.leaseDeadline = 100
	pg.rearmFixture = &serviceRearmPG{epoch: session.Actor().Epoch, api: f.api}
	pg.mu.Unlock()
	session, resources, worker = f.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
	require.NoError(t, runner.Tick(ctx))
	before, err := f.store.Load(ctx)
	require.NoError(t, err)
	_, _, _, selector := before.State().ServiceClosure.Selection()
	var retainedOriginal *journal.ServiceRearm
	if mode == "continue" || mode == "same-owner-expired" || mode == "existing-reopen" {
		_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationRearmService, selector))
		require.NoError(t, err)
		settled, e := f.store.Load(ctx)
		require.NoError(t, e)
		retainedOriginal = settled.State().ServiceClosure.Rearm
		require.NotNil(t, retainedOriginal)
		require.Equal(t, journal.Settled, retainedOriginal.Phase)
		if mode == "existing-reopen" {
			pg.mu.Lock()
			pg.reopenFixture = &serviceReopenPG{mode: "normal", api: f.api, root: root}
			pg.mu.Unlock()
			_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
			require.NoError(t, err)
		} else {
			pg.mu.Lock()
			pg.leaseExpired = true
			pg.mu.Unlock()
		}
		if mode == "continue" {
			session, resources, worker = f.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
			require.NoError(t, runner.Tick(ctx))
		}
	}
	before, err = f.store.Load(ctx)
	require.NoError(t, err)
	oldOwner, oldActive, oldHistory := before.State().Owner, before.State().Active, before.State().History
	pg.mu.Lock()
	baseRearms := pg.rearmFixture.posts
	if pg.reopenFixture == nil {
		pg.reopenFixture = &serviceReopenPG{mode: "normal", api: f.api, root: root}
	}
	if mode == "setter-after" {
		pg.rearmFixture.loseAfter = true
	}
	if mode == "lease-drift" {
		pg.rearmFixture.afterObserve = func() { f.api.mu.Lock(); f.api.lease.UID = "changed-owner-lease"; f.api.mu.Unlock() }
	}
	pg.mu.Unlock()
	if mode == "lost-start" {
		f.api.mu.Lock()
		f.api.loseJournalReply = true
		f.api.mu.Unlock()
	}
	var ticks, armTick, openTick atomic.Int64
	armTick.Store(-1)
	openTick.Store(-1)
	pg.mu.Lock()
	pg.rearmFixture.afterApply = func() { armTick.Store(ticks.Load()) }
	pg.afterOpen = func() { openTick.Store(ticks.Load()) }
	pg.mu.Unlock()
	var submit func(context.Context, control.CoordinationCommand) (CoordinationResult, error)
	var stopRun func()
	if mode == "normal" || mode == "continue" {
		endpoint := worker.NewControlEndpoint()
		loopCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		ready := make(chan struct{}, 1)
		go func() {
			done <- worker.Run(loopCtx, PGLoopOptions{PollInterval: 20 * time.Millisecond, Control: endpoint, Observe: func(Tick) {
				ticks.Add(1)
				select {
				case ready <- struct{}{}:
				default:
				}
			}})
		}()
		select {
		case <-ready:
		case e := <-done:
			t.Fatalf("Run exited before explicit selection: %v", e)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		submit = endpoint.Submit
		stopRun = func() {
			stop()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not join")
			}
		}
		defer stopRun()
	} else {
		// 否例同步进入同一生产executeControl，避免原周期Observe混入本复合入口的计数。
		submit = worker.executeControl
	}

	command := closureControl(session, config.Instance, lifecycleSequenceAction, selector)
	if mode == "wrong-selector" {
		command.Selector = strings.Repeat("ab", 32)
	}
	callCtx, callCancel := context.WithCancel(ctx)
	defer callCancel()
	if mode == "cancel" {
		callCancel()
	}
	result, callErr := submit(callCtx, command)
	if mode == "normal" || mode == "continue" {
		require.NoError(t, callErr, "one selector-only control must complete original recovery without periodic audit gap")
		require.Equal(t, pgexec.Recorded, result.Progress)
		require.NotNil(t, result.State)
		require.Nil(t, result.State.ServiceClosure)
		require.NotNil(t, result.State.ServiceLifecycle)
		require.Equal(t, "serving", result.State.ServiceLifecycle.SafetyStage)
		require.Equal(t, oldOwner, result.State.Owner)
		require.Equal(t, oldActive, result.State.Active)
		require.Equal(t, oldHistory, result.State.History)
		require.GreaterOrEqual(t, armTick.Load(), int64(0))
		require.Equal(t, armTick.Load(), openTick.Load(), "原Native rearm与open之间没有周期Tick")
		pg.mu.Lock()
		require.Equal(t, baseRearms+1, pg.rearmFixture.posts)
		require.Equal(t, 1, pg.reopenFixture.posts)
		require.Zero(t, pg.reopenFixture.closes)
		pg.mu.Unlock()
	} else {
		require.Error(t, callErr)
		after, e := f.store.Load(ctx)
		require.NoError(t, e)
		require.Equal(t, before.State().Active, after.State().Active)
		require.Equal(t, before.State().History, after.State().History)
		if mode == "lost-start" || mode == "setter-after" {
			require.NotNil(t, after.State().ServiceClosure.LatestRearm())
			require.Equal(t, journal.Unconfirmed, after.State().ServiceClosure.LatestRearm().Phase)
			require.Nil(t, after.State().ServiceClosure.Reopen)
			pg.mu.Lock()
			posts, queries := pg.rearmFixture.posts, pg.rearmFixture.queries
			pg.mu.Unlock()
			_, again := submit(ctx, command)
			require.Error(t, again)
			pg.mu.Lock()
			require.Equal(t, posts, pg.rearmFixture.posts, "不重发未知setter")
			require.Equal(t, queries, pg.rearmFixture.queries, "复合入口不自动Observe未知")
			pg.mu.Unlock()
		} else if mode != "lease-drift" {
			require.Equal(t, before.State(), after.State())
		}
	}
	if retainedOriginal != nil && mode == "continue" {
		var frozen struct {
			ServiceClosure *journal.ServiceClosure `json:"service_closure"`
		}
		require.NoError(t, json.Unmarshal(result.State.ServiceLifecycle.Context, &frozen))
		require.Equal(t, retainedOriginal, frozen.ServiceClosure.Rearm, "完整原Rearm永久保留")
	}

}
