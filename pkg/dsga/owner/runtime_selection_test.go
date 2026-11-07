package owner

import (
	"context"
	"errors"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/policy"
	"github.com/neondatabase/autoscaling/pkg/dsga/runtimeselection"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
)

// 复用原首次开户/资格/PG/Root/Runner 有状态协议夹具；不把协议 peer 当真实 Linux 验收。
func TestRuntimeSelectionUsesSameOwnerAndPreservesUnknownBegin(t *testing.T) {
	productionInitialServiceWithFixtureHook(t, "runtime-selection", func(t *testing.T, ctx context.Context, api *ownerAPI, store *journal.Store, client *kubefake.Clientset, coord coordclient.CoordinationV1Interface, session *Session, resources *ResourceWorker, old *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner, qmp *vmQMPPeer, request control.Request) bool {
		old.CloseIdleConnections()
		w, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
		require.NoError(t, err)
		defer w.CloseIdleConnections()
		instance, epoch := w.Binding()
		q, err := w.safety.claims.Qualification(ctx, source)
		require.NoError(t, err)
		require.Len(t, q.Targets, 2)
		c := automaticConfig(policy.Target{VMMemoryBytes: q.Targets[0].VMMemoryBytes, Buffers: pg.before.Observed.HighBuffers}, policy.Target{VMMemoryBytes: q.Targets[1].VMMemoryBytes, Buffers: pg.after.Observed.HighBuffers})
		selected := runtimeselection.Selection{Namespace: binding().Namespace, Name: binding().Name, ProtocolVersion: 1, Instance: instance, Incarnation: binding().Incarnation, Generation: 1, Qualification: source, Automatic: &c}
		var readErr error
		reads := 0
		reader := func(context.Context) (*runtimeselection.Selection, error) {
			reads++
			if reads < 2 {
				require.False(t, pg.shared().Observed.AdmissionOpen)
				wrong := selected
				wrong.Instance.PGBootID++
				return &wrong, nil
			}
			copy := selected
			return &copy, readErr
		}
		originalPG, originalRoot := w.client, w.resourceOwner.client
		endpoint := w.NewControlEndpoint()
		loopCtx, stop := context.WithCancel(ctx)
		defer stop()
		ticks := 0
		err = w.Run(loopCtx, PGLoopOptions{PollInterval: 10 * time.Millisecond, Control: endpoint, RuntimeSelection: reader, EnableInFlightGrow: true, Observe: func(tick Tick) {
			require.NoError(t, tick.Err)
			ticks++
			if pg.shared().Observed.AdmissionOpen {
				stop()
			}
		}})
		require.ErrorIs(t, err, context.Canceled)
		require.Positive(t, ticks)
		require.True(t, pg.shared().Observed.AdmissionOpen)
		require.Same(t, originalPG, w.client)
		require.Same(t, originalRoot, w.resourceOwner.client)
		require.Equal(t, source, *w.qualification)
		_, gotEpoch := w.Binding()
		require.Equal(t, epoch, gotEpoch, "在线装载不能换 Owner")
		require.EqualValues(t, 1, w.runtimeStatus.AppliedGeneration)
		refresh := func() bool { w.runtimeNext = time.Time{}; return w.refreshRuntimeSelection(ctx) }
		current := w.automatic
		readErr = errors.New("fixture unavailable")
		require.False(t, refresh())
		require.Same(t, current, w.automatic)
		require.EqualValues(t, 1, w.runtimeStatus.AppliedGeneration)
		progress, err := w.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Idle, progress)
		require.True(t, pg.shared().Observed.AdmissionOpen, "选择暂时不可读不能关闭原安全服务")
		readErr = nil
		changed := c
		changed.CooldownSeconds++
		selected.Automatic = &changed
		require.False(t, refresh(), "同代改内容拒绝")
		require.Same(t, current, w.automatic)
		selected.Generation = 2
		require.True(t, refresh())
		require.NotSame(t, current, w.automatic)
		require.Equal(t, changed, w.automatic.config)
		selected.Generation = 3
		selected.Automatic = nil
		require.True(t, refresh())
		require.Nil(t, w.automatic)
		selected.Generation = 4
		selected.Automatic = &c
		require.True(t, refresh())
		selected.Generation = 3
		require.False(t, refresh(), "旧代不能回滚")
		require.EqualValues(t, 4, w.runtimeStatus.AppliedGeneration)
		selected.Generation = 5
		selected.Qualification.Claim.UID = "wrong-original"
		require.False(t, refresh())
		require.Equal(t, source, *w.qualification)
		selected.Generation = 6
		selected.Qualification = source
		require.True(t, refresh())
		// 真实原 Begin CAS 已接收但故意丢回复。策略更新不能清本地 Pending 或原 WAL。
		root.mu.Lock()
		root.view.Actual.MemoryCurrentBytes = 63 << 20
		root.mu.Unlock()
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
		_, err = w.advanceAutomatic(ctx)
		var unknown *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &unknown)
		require.NotNil(t, w.automatic.pending)
		pending := *w.automatic.pending
		selected.Generation = 7
		selected.Automatic = nil
		require.False(t, refresh())
		require.Equal(t, pending, *w.automatic.pending)
		require.EqualValues(t, 6, w.runtimeStatus.AppliedGeneration)
		progress, err = w.advanceAutomatic(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Reconciled, progress)
		require.Nil(t, w.automatic.pending)
		require.False(t, refresh(), "已接受的原动作仍须完成，不能把清本地 Pending 当作动作结束")
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, pending, snapshot.State().Active.Request)
		require.Equal(t, "waiting_for_original_operation", w.runtimeStatus.State)
		result, err := w.executeControl(ctx, control.CoordinationCommand{ProtocolVersion: 1, Action: control.CoordinationState, Instance: control.CoordinationBinding(instance), ExpectedOwnerEpoch: epoch})
		require.NoError(t, err)
		require.NotNil(t, result.RuntimeSelection)
		require.EqualValues(t, 7, result.RuntimeSelection.DesiredGeneration)
		require.EqualValues(t, 6, result.RuntimeSelection.AppliedGeneration)
		return true
	})
}
