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

func TestResourceRecoveryBeforePGPreservesLostReplyAndHandsOff(t *testing.T) {
	_, store, coord := newAPI(t)
	root, config := newResourceOwnerFixture(t, store)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes /= 2
	root.dropRepair = true
	root.mu.Unlock()
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		// 原严格构造仍拒绝漂移；专用构造只把原Guest绑定/fence，未接PG或开放。
		_, err := session.NewResourceWorker(ctx, config)
		require.ErrorIs(t, err, resourceguard.ErrUnknown)
		resources, err := session.NewResourceRecoveryWorker(ctx, config)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		before, err := store.Load(ctx)
		require.NoError(t, err)
		require.Nil(t, before.State().PGInstance)
		endpoint := resources.NewRecoveryControlEndpoint()
		loopCtx, stop := context.WithCancel(ctx)
		defer stop()
		done := make(chan error, 1)
		go func() { done <- endpoint.RunResourceRecovery(loopCtx, 5*time.Millisecond) }()
		instance, epoch := endpoint.Binding()
		require.Zero(t, instance.PGBootID)
		command := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(instance), ExpectedOwnerEpoch: epoch, Action: control.CoordinationResourceState, Request: nil, Key: nil, Selector: ""}
		initial, err := endpoint.Submit(ctx, command)
		require.NoError(t, err)
		require.NotEmpty(t, initial.HeldSourceSHA256)
		other := resources.NewRecoveryControlEndpoint()
		require.ErrorContains(t, other.RunResourceRecovery(ctx, time.Second), "already running")
		request := pgRequest(t, session.Actor())
		pgConfig := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
			t.Error("startup Root recovery dispatched PG business", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		})
		_, err = session.NewPGWorker(ctx, pgConfig, resources)
		require.ErrorContains(t, err, "already running", "不能在原Root恢复循环中并发创建PG写者")
		command.Action, command.Selector = control.CoordinationRepairHeld, strings.Repeat("a", 64)
		_, err = endpoint.Submit(ctx, command)
		require.Error(t, err)
		root.mu.Lock()
		require.Zero(t, root.repairs)
		root.mu.Unlock()
		command.Selector = initial.HeldSourceSHA256
		_, err = endpoint.Submit(ctx, command)
		var unknown *resourceguard.ClientError
		require.ErrorAs(t, err, &unknown)
		require.True(t, unknown.MayHaveExecuted)
		require.NotNil(t, resources.client.PendingHeldRepair())
		// 内核夹具已经恢复，但普通轮询不得清SDK的原修复未知，也不能交给PG。
		command.Action, command.Selector = control.CoordinationResourceState, ""
		current, err := endpoint.Submit(ctx, command)
		require.NoError(t, err)
		require.Equal(t, initial.ResourceState.Bootstrap.Limits.MemoryMaxBytes, current.ResourceState.Actual.MemoryMaxBytes)
		require.NotNil(t, resources.client.PendingHeldRepair())
		select {
		case err := <-done:
			t.Fatal("recovery loop cleared unknown without explicit observe", err)
		default:
		}
		command.Action, command.Selector = control.CoordinationObserveHeld, initial.HeldSourceSHA256
		_, err = endpoint.Submit(ctx, command)
		// 返回前端点可能同时Done；该传输歧义仍只准查原状态，不按NoEffect重发。
		if err != nil {
			require.ErrorIs(t, err, ErrControlUnavailable)
		}
		require.NoError(t, <-done)
		require.Nil(t, resources.client.PendingHeldRepair())
		_, err = endpoint.Submit(ctx, command)
		require.ErrorIs(t, err, ErrControlUnavailable)
		after, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), after.State(), "Root恢复不能产生PG绑定/预算/外层终态")
		root.mu.Lock()
		require.Equal(t, 1, root.repairs)
		root.mu.Unlock()
		// 恢复完成才允许正式能力协商，实际boot必须非零；旧0 boot不能复用。
		worker, err := session.NewPGWorker(ctx, pgConfig, resources)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		pgEndpoint := worker.NewControlEndpoint()
		pgInstance, _ := pgEndpoint.Binding()
		require.NotZero(t, pgInstance.PGBootID)
		_, err = pgEndpoint.Submit(ctx, command)
		require.ErrorIs(t, err, ErrControlUnavailable)
		return nil
	})
	require.NoError(t, err)
}

func TestResourceRecoveryCancellationKeepsOriginalBindingAndNeverRepairs(t *testing.T) {
	_, store, coord := newAPI(t)
	root, config := newResourceOwnerFixture(t, store)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes /= 2
	root.mu.Unlock()
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceRecoveryWorker(ctx, config)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		before, err := store.Load(ctx)
		require.NoError(t, err)
		endpoint := resources.NewRecoveryControlEndpoint()
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- endpoint.RunResourceRecovery(runCtx, 5*time.Millisecond) }()
		instance, epoch := endpoint.Binding()
		value, err := endpoint.Submit(ctx, control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(instance), ExpectedOwnerEpoch: epoch, Action: control.CoordinationResourceState, Request: nil, Key: nil, Selector: ""})
		require.NoError(t, err)
		require.Equal(t, pgexec.Idle, value.Progress)
		stop()
		require.ErrorIs(t, <-done, context.Canceled)
		<-endpoint.Done()
		require.ErrorContains(t, endpoint.RunResourceRecovery(ctx, time.Second), "cannot be reused")
		after, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), after.State())
		root.mu.Lock()
		require.Zero(t, root.repairs)
		root.mu.Unlock()
		return nil
	})
	require.NoError(t, err)
}

func TestResourceRecoveryCannotAdoptUnknownOrOrphanBootstrap(t *testing.T) {
	for _, scenario := range []string{"unknown-bootstrap", "orphan-command"} {
		t.Run(scenario, func(t *testing.T) {
			_, store, coord := newAPI(t)
			root, config := newResourceOwnerFixture(t, store)
			if scenario == "unknown-bootstrap" {
				root.view.BootstrapApplied = false
			} else {
				command := resourceguard.Command{ProtocolVersion: 1, Identity: root.view.Identity, IssuerEpoch: 1, Revision: 1, BudgetCommitmentID: "orphan", PostmasterPID: 123, PostmasterStartTicks: 456, MemoryMaxBytes: 64 << 20, PidsMaxTasks: 0, SystemMemoryMaxBytes: 0, OutsideUpperBytes: 32 << 20, HugePools: root.view.Bootstrap.Limits.HugePools}
				root.view.Original = &command
			}
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				_, err := session.NewResourceRecoveryWorker(ctx, config)
				require.Error(t, err)
				snapshot, err := store.Load(ctx)
				require.NoError(t, err)
				require.Nil(t, snapshot.State().ResourceIdentity)
				require.Nil(t, snapshot.State().PGInstance)
				root.mu.Lock()
				require.Zero(t, root.fences)
				require.Zero(t, root.repairs)
				root.mu.Unlock()
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestResourceRecoveryRunStopCancelsActiveManagementAndRetainsUnknown(t *testing.T) {
	_, store, coord := newAPI(t)
	root, config := newResourceOwnerFixture(t, store)
	root.view.Actual.MemoryMaxBytes /= 2
	root.repairStarted = make(chan struct{})
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceRecoveryWorker(ctx, config)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		before, err := store.Load(ctx)
		require.NoError(t, err)
		endpoint := resources.NewRecoveryControlEndpoint()
		runCtx, stop := context.WithCancel(ctx)
		defer stop()
		done := make(chan error, 1)
		go func() { done <- endpoint.RunResourceRecovery(runCtx, 5*time.Millisecond) }()
		instance, epoch := endpoint.Binding()
		command := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(instance), ExpectedOwnerEpoch: epoch, Action: control.CoordinationResourceState, Request: nil, Key: nil, Selector: ""}
		value, err := endpoint.Submit(ctx, command)
		require.NoError(t, err)
		command.Action, command.Selector = control.CoordinationRepairHeld, value.HeldSourceSHA256
		reply := make(chan error, 1)
		go func() { _, err := endpoint.Submit(ctx, command); reply <- err }()
		select {
		case <-root.repairStarted:
		case <-ctx.Done():
			t.Fatal("original repair did not reach blocking transport")
		}
		stop()
		require.ErrorIs(t, <-done, context.Canceled)
		require.Error(t, <-reply)
		require.NoError(t, session.Context().Err(), "Run停止不能靠杀Session制造通过")
		require.NoError(t, ctx.Err(), "管理调用者仍有效")
		require.NotNil(t, resources.client.PendingHeldRepair(), "取消不能清原修复未知")
		after, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), after.State())
		root.mu.Lock()
		require.Zero(t, root.repairs)
		root.mu.Unlock()
		return nil
	})
	require.NoError(t, err)
}

// WithResourceRecoveryEndpointTestFixture仅向本包外部传输测试暴露正式构造的端点。
// Root/Lease为明确协议夹具，真实Session/SDK/原WAL均复用生产实现。
func WithResourceRecoveryEndpointTestFixture(t *testing.T, callback func(context.Context, *ControlEndpoint)) {
	t.Helper()
	_, store, coord := newAPI(t)
	root, config := newResourceOwnerFixture(t, store)
	root.view.Actual.MemoryMaxBytes /= 2
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceRecoveryWorker(ctx, config)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		before, err := store.Load(ctx)
		require.NoError(t, err)
		endpoint := resources.NewRecoveryControlEndpoint()
		runCtx, stop := context.WithCancel(ctx)
		defer stop()
		done := make(chan error, 1)
		go func() { done <- endpoint.RunResourceRecovery(runCtx, 5*time.Millisecond) }()
		callback(ctx, endpoint)
		require.NoError(t, <-done)
		after, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), after.State())
		root.mu.Lock()
		require.Equal(t, 1, root.repairs)
		root.mu.Unlock()
		return nil
	})
	require.NoError(t, err)
}
