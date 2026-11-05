package owner

import (
	"context"
	"errors"
	"fmt"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// 复用真实生产首次服务/Prepare/Host/Runner夹具；不新增执行器或来源。
// 用接口检查使旧源码可编译后按缺失审计出口行为失败，不以编译失败作RED。
type vmAuditAdvancer interface {
	AdvanceWithAudit(context.Context, func(context.Context, *journal.Snapshot, control.SharedCostResponse) error) (pgexec.Progress, error)
}

func TestProductionVMCompleteAuditCheckpointPreservesUnknownAndCAS(t *testing.T) {
	productionInitialService(t, "vm-audit-complete-checkpoints")
}

func verifyVMExecAuditCheckpoint(t *testing.T, ctx context.Context, _ string, api *ownerAPI, store *journal.Store, _ *kubefake.Clientset, session *Session, worker *PGWorker, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner, qmp *vmQMPPeer) {
	t.Helper()
	audited, ok := any(worker.vm).(vmAuditAdvancer)
	require.True(t, ok, "原完整VM审计必须提供同步成功检查点")
	before, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.Planned, budgetStep(t, store, journal.ResizeVM).Phase)
	qmp.mu.Lock()
	initialWrites := qmp.writes
	qmp.mu.Unlock()
	sameWAL := func(want journal.Model) {
		t.Helper()
		now, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, want, now.State())
	}
	root.mu.Lock()
	initialTotal := root.view.Actual.GuestMemTotalBytes
	initialAggregate, aggregateErr := budget.RootAggregateFromView(root.view)
	root.mu.Unlock()
	require.NoError(t, aggregateErr)
	initialRootSHA, digestErr := budget.RootDigest(initialAggregate)
	require.NoError(t, digestErr)
	var phases []journal.Phase
	check := func(c context.Context, s *journal.Snapshot, shared control.SharedCostResponse) error {
		require.Equal(t, session.Actor(), s.State().Owner)
		require.True(t, s.Dispatchable())
		now, err := store.Load(c)
		require.NoError(t, err)
		require.Equal(t, s.ResourceVersion(), now.ResourceVersion())
		require.Equal(t, s.State(), now.State())
		instance, _ := worker.Binding()
		require.NoError(t, shared.ValidateFor(instance))
		require.Equal(t, pg.shared(), shared, "仅交付本次完整原生审计读到的Shared")
		phases = append(phases, budgetStep(t, store, journal.ResizeVM).Phase)
		return nil
	}
	reject := errors.New("explicit checkpoint refused")
	t.Run("nil-callback-does-not-start", func(t *testing.T) {
		progress, err := audited.AdvanceWithAudit(ctx, nil)
		require.Error(t, err)
		require.Equal(t, pgexec.AwaitingFact, progress)
		sameWAL(before.State())
	})
	t.Run("source-rv-drift-has-no-callback", func(t *testing.T) {
		root.mu.Lock()
		root.afterProcess = func() {
			api.mu.Lock()
			api.vmVersion++
			api.vm.ResourceVersion = fmt.Sprintf("vm-audit-drift-%d", api.vmVersion)
			api.mu.Unlock()
		}
		root.mu.Unlock()
		defer func() { root.mu.Lock(); root.afterProcess = nil; root.mu.Unlock() }()
		called := 0
		progress, err := audited.AdvanceWithAudit(ctx, func(context.Context, *journal.Snapshot, control.SharedCostResponse) error { called++; return nil })
		require.Error(t, err)
		require.Equal(t, pgexec.AwaitingFact, progress)
		require.Zero(t, called)
		sameWAL(before.State())
	})
	t.Run("valid-but-different-root-source-has-no-callback", func(t *testing.T) {
		root.mu.Lock()
		bootstrapLimit, actualLimit := root.view.Bootstrap.Limits.MemoryMaxBytes, root.view.Actual.MemoryMaxBytes
		root.view.Bootstrap.Limits.MemoryMaxBytes += 4 << 20
		root.view.Actual.MemoryMaxBytes += 4 << 20
		_, limitsErr := root.view.ConfirmedLimits()
		changed, aggregateErr := budget.RootAggregateFromView(root.view)
		root.mu.Unlock()
		require.NoError(t, limitsErr)
		require.NoError(t, aggregateErr)
		changedSHA, err := budget.RootDigest(changed)
		require.NoError(t, err)
		require.NotEqual(t, initialRootSHA, changedSHA)
		defer func() {
			root.mu.Lock()
			root.view.Bootstrap.Limits.MemoryMaxBytes, root.view.Actual.MemoryMaxBytes = bootstrapLimit, actualLimit
			root.mu.Unlock()
		}()
		called := 0
		progress, err := audited.AdvanceWithAudit(ctx, func(context.Context, *journal.Snapshot, control.SharedCostResponse) error { called++; return reject })
		require.Error(t, err)
		require.Equal(t, pgexec.AwaitingFact, progress)
		require.Zero(t, called, "完整但非原Prepare绑定的Root不能续接服务")
		sameWAL(before.State())
	})
	t.Run("pre-audit-rejection-does-not-start", func(t *testing.T) {
		progress, err := audited.AdvanceWithAudit(ctx, func(c context.Context, s *journal.Snapshot, shared control.SharedCostResponse) error {
			require.NoError(t, check(c, s, shared))
			return reject
		})
		require.ErrorIs(t, err, reject)
		require.Equal(t, pgexec.AwaitingFact, progress)
		sameWAL(before.State())
		qmp.mu.Lock()
		require.Equal(t, initialWrites, qmp.writes)
		qmp.mu.Unlock()
	})
	// 正常前检查点只在完整审计后同步返回，然后才持久Start。
	progress, err := audited.AdvanceWithAudit(ctx, check)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	started, err := store.Load(ctx)
	require.NoError(t, err)
	original := budgetStep(t, store, journal.ResizeVM)
	require.Equal(t, journal.Unconfirmed, original.Phase)
	t.Run("original-advance-keeps-unknown-without-callback", func(t *testing.T) {
		priorCalls := len(phases)
		progress, err := worker.vm.Advance(ctx)
		require.ErrorIs(t, err, vmguard.ErrUnknown)
		require.Equal(t, pgexec.AwaitingFact, progress)
		require.Len(t, phases, priorCalls)
		sameWAL(started.State())
	})
	t.Run("runner-unknown-has-no-callback-or-replay", func(t *testing.T) {
		called := 0
		progress, err := audited.AdvanceWithAudit(ctx, func(context.Context, *journal.Snapshot, control.SharedCostResponse) error { called++; return nil })
		require.ErrorIs(t, err, vmguard.ErrUnknown)
		require.Equal(t, pgexec.AwaitingFact, progress)
		require.Zero(t, called)
		sameWAL(started.State())
		qmp.mu.Lock()
		require.Equal(t, initialWrites, qmp.writes)
		qmp.mu.Unlock()
	})
	require.NoError(t, runner.Tick(ctx))
	root.mu.Lock()
	actualTotal := root.view.Actual.GuestMemTotalBytes
	afterAggregate, aggregateErr := budget.RootAggregateFromView(root.view)
	root.mu.Unlock()
	require.NoError(t, aggregateErr)
	afterRootSHA, digestErr := budget.RootDigest(afterAggregate)
	require.NoError(t, digestErr)
	require.Greater(t, actualTotal, initialTotal, "真实Runner完成后Guest容量已经增加")
	require.Equal(t, initialRootSHA, afterRootSHA, "原Root摘要不包含动态Guest容量")

	qmp.mu.Lock()
	require.Equal(t, initialWrites+1, qmp.writes)
	qmp.mu.Unlock()
	t.Run("complete-result-rejection-does-not-record", func(t *testing.T) {
		progress, err := audited.AdvanceWithAudit(ctx, func(c context.Context, s *journal.Snapshot, shared control.SharedCostResponse) error {
			require.NoError(t, check(c, s, shared))
			return reject
		})
		require.ErrorIs(t, err, reject)
		require.Equal(t, pgexec.AwaitingFact, progress)
		sameWAL(started.State())
		require.Equal(t, original.CommandSHA256, budgetStep(t, store, journal.ResizeVM).CommandSHA256)
	})
	progress, err = audited.AdvanceWithAudit(ctx, check)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	settled := budgetStep(t, store, journal.ResizeVM)
	require.Equal(t, journal.Settled, settled.Phase)
	require.Equal(t, journal.Completed, settled.Observation.Result)
	require.Equal(t, original.CommandSHA256, settled.CommandSHA256)
	require.Equal(t, []journal.Phase{journal.Planned, journal.Planned, journal.Unconfirmed, journal.Unconfirmed}, phases)
	qmp.mu.Lock()
	require.Equal(t, initialWrites+1, qmp.writes)
	qmp.mu.Unlock()
}
