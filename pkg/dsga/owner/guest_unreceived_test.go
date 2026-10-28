package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/api/resource"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceexec"
)

// 真实Start CAS写成功丢回复，Root零setter；按Agent相同启动选择恢复同一
// Pending并交正式PGWorker。只验证新窗口；PG/root/Kube均为协议peer，
// 前驱Prepare/Host/VM使用beginGuest显式夹具，不声称完整产品E2E。
func TestUnreceivedGuestStartupHandoffReplaysOrCancelsOriginal(t *testing.T) {
	for _, mode := range []string{"replay", "cancel", "cancel-reply-lost", "cancel-record-lost"} {
		t.Run(mode, func(t *testing.T) {
			api, store, coord := newAPI(t)
			root, resourceConfig := guestRoot(t, store)
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				resources, err := session.NewResourceWorker(ctx, resourceConfig)
				require.NoError(t, err)
				defer resources.CloseIdleConnections()
				request := pgRequest(t, session.Actor())
				prepared, shared := preparedGuest(t, request, root.view.Identity)
				config := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, "/dsg/v1/budget/shared", r.URL.Path, "本窗口不可派发PG resize")
					pgReply(w, shared)
				})
				worker, err := session.NewPGWorker(ctx, config, resources)
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				api.mu.Lock()
				api.vm.Status.MemorySize = resource.NewQuantity(int64(request.TargetVMMemoryBytes), resource.BinarySI)
				api.mu.Unlock()
				beginGuest(t, store, request, prepared)
				api.mu.Lock()
				api.loseJournalReply = true
				api.mu.Unlock()
				_, err = worker.Advance(ctx)
				var lost *journal.UnconfirmedWrite
				require.ErrorAs(t, err, &lost)
				original := guestPhase(t, store)
				require.Equal(t, journal.Unconfirmed, original.Phase)
				before, err := store.Load(ctx)
				require.NoError(t, err)
				restored, err := session.NewResourceRecoveryWorker(ctx, resourceConfig)
				require.NoError(t, err)
				defer restored.CloseIdleConnections()
				pending := restored.client.Pending()
				require.NotNil(t, pending)
				handoff, err := restored.PrepareRetainedPGRecovery(ctx)
				require.NoError(t, err)
				require.True(t, handoff, "Root未收到时必须到达正式原重放/取消入口")
				require.Equal(t, pending, restored.client.Pending(), "交接不能清SDK未知")
				after, err := store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, before.State(), after.State(), "交接不写外层日志或确认预算")
				next, err := session.NewPGWorker(ctx, config, restored)
				require.NoError(t, err)
				defer next.CloseIdleConnections()
				command := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(request.Key.Instance), ExpectedOwnerEpoch: session.Actor().Epoch, Action: control.CoordinationReplayResource, Request: nil, Key: &request.Key, Selector: ""}
				if mode == "replay" {
					result, err := next.executeControl(ctx, command)
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, result.Progress)
					require.Equal(t, journal.Completed, guestPhase(t, store).Observation.Result)
					root.mu.Lock()
					require.Equal(t, 1, root.posts)
					require.Zero(t, root.cancellations)
					root.mu.Unlock()
					return nil
				}
				root.mu.Lock()
				root.loseCancelReply = mode == "cancel-reply-lost"
				if mode == "cancel-record-lost" {
					root.afterCancel = func() { api.mu.Lock(); api.loseJournalReply = true; api.mu.Unlock() }
				}
				root.mu.Unlock()
				endpoint := next.NewControlEndpoint()
				loopCtx, stop := context.WithCancel(ctx)
				defer stop()
				observed := make(chan struct{})
				var once sync.Once
				done := make(chan error, 1)
				go func() {
					done <- next.Run(loopCtx, PGLoopOptions{PollInterval: 5 * time.Millisecond, Control: endpoint, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: func(Tick) {
						snapshot, err := store.Load(ctx)
						if err == nil && snapshot.State().Active != nil {
							for _, step := range snapshot.State().Active.Steps {
								if step.Action == journal.GuestReady && step.Phase == journal.Settled {
									stop()
									return
								}
							}
						}
						once.Do(func() { close(observed) })
					}})
				}()
				select {
				case <-observed:
				case <-ctx.Done():
					t.Fatal("原Observe未进入正式循环")
				}
				root.mu.Lock()
				require.Zero(t, root.posts)
				require.Zero(t, root.cancellations, "普通启动未知不能自行选择取消")
				root.mu.Unlock()
				command.Action = control.CoordinationCancel
				_, err = endpoint.Submit(ctx, command)
				require.NoError(t, err)
				require.ErrorIs(t, <-done, context.Canceled)
				settled := guestPhase(t, store)
				require.Equal(t, original.Command, settled.Command)
				require.Equal(t, journal.NoEffect, settled.Observation.Result)
				var audit resourceexec.CancelledAudit
				require.NoError(t, json.Unmarshal(settled.Observation.Data, &audit))
				require.NoError(t, audit.Cancellation.ValidateFor(*pending, session.Actor().Epoch))
				require.Equal(t, mode == "cancel-reply-lost", audit.Query)
				require.Nil(t, restored.client.Pending())
				require.Equal(t, journal.Planned, budgetStep(t, store, journal.ResizePG).Phase)
				root.mu.Lock()
				require.Zero(t, root.posts)
				require.Equal(t, 1, root.cancellations)
				require.Nil(t, root.view.Original)
				require.Nil(t, root.view.Held)
				root.mu.Unlock()
				return nil
			})
			require.NoError(t, err)
		})
	}
}
