package owner

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

func TestOwnedControlRepairsOriginalHeldAndObservesLostReply(t *testing.T) {
	_, store, coord := newAPI(t)
	root, rootConfig := newResourceOwnerFixture(t, store)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceWorker(ctx, rootConfig)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		request := pgRequest(t, session.Actor())
		worker, err := session.NewPGWorker(ctx, pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
			t.Error("Root repair reached PG business route", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}), resources)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		before, err := store.Load(ctx)
		require.NoError(t, err)
		endpoint := worker.NewControlEndpoint()
		loopCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- worker.Run(loopCtx, PGLoopOptions{PollInterval: 5 * time.Millisecond, Control: endpoint, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: nil})
		}()
		defer func() { stop(); require.ErrorIs(t, <-done, context.Canceled) }()
		command := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(request.Key.Instance), ExpectedOwnerEpoch: session.Actor().Epoch, Action: control.CoordinationResourceState, Request: nil, Key: nil, Selector: ""}
		initial, err := endpoint.Submit(ctx, command)
		require.NoError(t, err)
		require.NotNil(t, initial.ResourceState)
		selector, err := resourceguard.HeldSourceSHA256(*initial.ResourceState)
		require.NoError(t, err)
		require.Equal(t, selector, initial.HeldSourceSHA256)
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes /= 2
		root.dropRepair = true
		root.mu.Unlock()
		// 状态检查保持可达，但绝不从漂移本身自动发修复；选择摘要来自
		// 原Bootstrap/Held/出生，不能取当前较小值重新生产预算或换来源。
		value, err := endpoint.Submit(ctx, command)
		require.NoError(t, err)
		require.Equal(t, selector, value.HeldSourceSHA256)
		require.Less(t, value.ResourceState.Actual.MemoryMaxBytes, value.ResourceState.Bootstrap.Limits.MemoryMaxBytes)
		command.Action, command.Selector = control.CoordinationObserveHeld, selector
		_, err = endpoint.Submit(ctx, command)
		require.ErrorIs(t, err, resourceguard.ErrUnknown)
		command.Action, command.Selector = control.CoordinationRepairHeld, strings.Repeat("a", 64)
		_, err = endpoint.Submit(ctx, command)
		require.Error(t, err)
		root.mu.Lock()
		require.Zero(t, root.repairs)
		root.mu.Unlock()
		command.Selector = selector
		_, err = endpoint.Submit(ctx, command)
		var unknown *resourceguard.ClientError
		require.ErrorAs(t, err, &unknown)
		require.True(t, unknown.MayHaveExecuted)
		require.NotNil(t, resources.client.PendingHeldRepair())
		command.Action = control.CoordinationObserveHeld
		value, err = endpoint.Submit(ctx, command)
		require.NoError(t, err)
		require.Equal(t, pgexec.Reconciled, value.Progress)
		require.NotNil(t, value.ResourceEvidence)
		require.False(t, value.ResourceEvidence.VMShrinkPermit)
		require.Equal(t, initial.ResourceState.Bootstrap.Limits.MemoryMaxBytes, value.ResourceEvidence.MemoryMaxBytes)
		require.Nil(t, resources.client.PendingHeldRepair(), "readback clears only original repair fence")
		require.Equal(t, before.State(), *value.State, "repair does not change budget/GuestReady/VM/Host journal")
		root.mu.Lock()
		require.Equal(t, 1, root.repairs, "observe and ordinary polling cannot replay repair POST")
		root.mu.Unlock()
		return nil
	})
	require.NoError(t, err)
}
