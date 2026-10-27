package pgexec

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 本文件验证真实Store/执行器与HTTP UID/RV CAS；Kube和PG服务为协议夹具。
// 零派发取消只关闭外层意图，不伪造原生Cancel/NoEffect或任何释放回执。
func TestUnstartedAbortClosesOriginalAcrossLostReplyAndTakeover(t *testing.T) {
	for _, mode := range []string{"grow", "shrink", "lost-finish-reply", "takeover"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			intent := request(t, control.Grow)
			if mode == "shrink" {
				intent.Kind = control.Shrink
			}
			api, store := prepareUnstartedStore(t, intent)
			snapshot, err := store.Load(ctx)
			require.NoError(t, err)
			snapshot, err = store.RequestAbort(ctx, snapshot, snapshot.State().Owner, intent.Key)
			require.NoError(t, err)
			clientIntent := intent
			if mode == "takeover" {
				snapshot, err = store.Takeover(ctx, snapshot, snapshot.State().Owner, "owner-b")
				require.NoError(t, err)
				clientIntent.Key.IssuerEpoch = snapshot.State().Owner.Epoch
			}
			original := snapshot.State()
			client, calls := computeClient(t, clientIntent, func(w http.ResponseWriter, r *http.Request) {
				t.Error("zero-dispatch cancellation sent native request", r.URL.Path)
				w.WriteHeader(http.StatusInternalServerError)
			})
			executor, err := New(store, client, original.Owner)
			require.NoError(t, err)
			beforeRPC := calls.Load()
			if mode == "lost-finish-reply" {
				api.mu.Lock()
				api.loseNext = true
				api.mu.Unlock()
			}
			progress, err := executor.Advance(ctx)
			if mode == "lost-finish-reply" {
				var unknown *journal.UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				require.Equal(t, AwaitingFact, progress, "lost reply cannot be reported as confirmed completion")
			} else {
				require.NoError(t, err)
				require.Equal(t, Recorded, progress)
			}
			closed, err := store.Load(ctx)
			require.NoError(t, err)
			state := closed.State()
			require.Nil(t, state.Active)
			require.Equal(t, original.Sequence+1, state.Sequence)
			require.Equal(t, original.HighWater, state.HighWater)
			require.Equal(t, original.BudgetHeld, state.BudgetHeld)
			require.Len(t, state.History, 1)
			require.Equal(t, *original.Active, state.History[0].Operation, "original request/owner/all Planned steps must remain byte-equivalent")
			require.Equal(t, journal.AbortedBeforeDispatch, state.History[0].Outcome)
			require.Nil(t, state.History[0].Operation.Cleanup)
			progress, err = executor.Advance(ctx)
			require.NoError(t, err)
			require.Equal(t, Idle, progress)
			require.Equal(t, beforeRPC, calls.Load(), "no Fence/PG cancel/root/budget/admission traffic")
			again, err := store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, state, again.State(), "retry reads original closure instead of appending history again")
		})
	}
}

func TestUnstartedAbortKeepsChangedBindingAndLocalUnknown(t *testing.T) {
	for _, mode := range []string{"deleting", "wrong-pg-boot", "pg-unknown", "admission-unknown", "close-original-unknown"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			intent := request(t, control.Grow)
			api, store := prepareUnstartedStore(t, intent)
			snapshot, err := store.Load(ctx)
			require.NoError(t, err)
			snapshot, err = store.RequestAbort(ctx, snapshot, snapshot.State().Owner, intent.Key)
			require.NoError(t, err)
			if mode == "deleting" {
				api.mu.Lock()
				stamp := metav1.Now()
				api.vm.DeletionTimestamp = &stamp
				api.mu.Unlock()
			}
			clientIntent := intent
			if mode == "wrong-pg-boot" {
				clientIntent.Key.Instance.PGBootID--
			}
			client, calls := computeClient(t, clientIntent, func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "fixture reply lost after possible native effect", http.StatusServiceUnavailable)
			})
			admission := control.AdmissionRequest{ProtocolVersion: 1, Instance: intent.Key.Instance, IssuerEpoch: intent.Key.IssuerEpoch, Revision: 1, ExpectedCapacityGeneration: intent.ExpectedCapacityGeneration, Open: true, BudgetCommitmentID: intent.BudgetCommitmentID}
			switch mode {
			case "pg-unknown":
				_, err = client.Resize(ctx, intent)
				require.Error(t, err)
				require.Equal(t, &intent, client.Pending())
			case "admission-unknown":
				_, err = client.SetAdmission(ctx, admission)
				require.Error(t, err)
			case "close-original-unknown":
				closeIntent := admission
				closeIntent.Open, closeIntent.Revision = false, 2
				_, err = client.CloseAdmissionOriginal(ctx, control.AdmissionCloseRequest{ProtocolVersion: 1, Original: admission, Close: closeIntent})
				require.Error(t, err)
			}
			beforeRPC := calls.Load()
			pending := client.Pending()
			pendingAdmission, pendingClose := client.PendingAdmission()
			executor, err := New(store, client, snapshot.State().Owner)
			require.NoError(t, err)
			progress, err := executor.Advance(ctx)
			require.Error(t, err)
			require.Equal(t, AwaitingFact, progress)
			latest, err := store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, snapshot.State(), latest.State())
			require.Equal(t, pending, client.Pending())
			afterAdmission, afterClose := client.PendingAdmission()
			require.Equal(t, pendingAdmission, afterAdmission)
			require.Equal(t, pendingClose, afterClose)
			require.Equal(t, beforeRPC, calls.Load(), "contradiction must not trigger guessed Cancel or clear SDK state")
		})
	}
}

func TestStartedAbortNeverUsesUnstartedShortcut(t *testing.T) {
	for _, mode := range []string{"unconfirmed", "completed", "rejected", "cleanup"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			intent := request(t, control.Grow)
			_, store := prepareUnstartedStore(t, intent)
			snapshot, err := store.Load(ctx)
			require.NoError(t, err)
			actor := snapshot.State().Owner
			action := snapshot.State().Active.Steps[0].Action
			snapshot, dispatch, err := store.Start(ctx, snapshot, actor, intent.Key, action, "test-only-external-source", []byte(`{"test_only":true}`))
			require.NoError(t, err)
			if mode != "unconfirmed" {
				result := journal.Completed
				if mode == "rejected" {
					result = journal.Rejected
				}
				snapshot, err = store.Record(ctx, snapshot, actor, intent.Key, journal.Observation{Key: intent.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256, Ordinal: dispatch.Step.Ordinal, Action: action, Result: result, SourceID: dispatch.Step.TargetID, Data: []byte(`{"test_only":true}`)})
				require.NoError(t, err)
			}
			snapshot, err = store.RequestAbort(ctx, snapshot, actor, intent.Key)
			require.NoError(t, err)
			if mode == "cleanup" {
				snapshot, _, err = store.StartCleanup(ctx, snapshot, actor, intent.Key, "test-only-cleanup", []byte(`{"test_only":true}`))
				require.NoError(t, err)
			}
			client, calls := computeClient(t, intent, func(w http.ResponseWriter, r *http.Request) {
				t.Error("unexpected native request", r.URL.Path)
				w.WriteHeader(http.StatusInternalServerError)
			})
			executor, err := New(store, client, actor)
			require.NoError(t, err)
			progress, err := executor.Advance(ctx)
			require.NoError(t, err)
			require.Equal(t, ExternalStep, progress, "started/settled/rejected/cleanup effects still need domain reconciliation")
			latest, err := store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, snapshot.State(), latest.State())
			require.EqualValues(t, 1, calls.Load(), "capability discovery only")
		})
	}
}

func TestUnstartedAbortDoesNotEraseCompetingStartOrTakeover(t *testing.T) {
	for _, mode := range []string{"start-wins", "abort-wins", "takeover-invalidates-finish"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			intent := request(t, control.Grow)
			_, store := prepareUnstartedStore(t, intent)
			original, err := store.Load(ctx)
			require.NoError(t, err)
			actor := original.State().Owner
			action := original.State().Active.Steps[0].Action
			client, _ := computeClient(t, intent, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) })
			executor, err := New(store, client, actor)
			require.NoError(t, err)
			if mode == "start-wins" {
				_, dispatch, err := store.Start(ctx, original, actor, intent.Key, action, "test-only-original-domain", []byte(`{"test_only":true}`))
				require.NoError(t, err)
				require.NotNil(t, dispatch)
				_, err = store.RequestAbort(ctx, original, actor, intent.Key)
				require.Error(t, err)
				latest, err := store.Load(ctx)
				require.NoError(t, err)
				latest, err = store.RequestAbort(ctx, latest, actor, intent.Key)
				require.NoError(t, err)
				handled, err := executor.finishUnstartedAbort(ctx, latest)
				require.NoError(t, err)
				require.False(t, handled)
				require.Equal(t, journal.Unconfirmed, latest.State().Active.Steps[0].Phase)
				return
			}
			aborted, err := store.RequestAbort(ctx, original, actor, intent.Key)
			require.NoError(t, err)
			if mode == "abort-wins" {
				_, dispatch, err := store.Start(ctx, original, actor, intent.Key, action, "test-only-original-domain", []byte(`{"test_only":true}`))
				require.Error(t, err)
				require.Nil(t, dispatch, "lost UID/RV race must not grant a dispatch ticket")
				handled, err := executor.finishUnstartedAbort(ctx, aborted)
				require.NoError(t, err)
				require.True(t, handled)
				return
			}
			current, err := store.Takeover(ctx, aborted, actor, "owner-b")
			require.NoError(t, err)
			handled, err := executor.finishUnstartedAbort(ctx, aborted)
			require.Error(t, err, "stale all-Planned snapshot cannot close after a competing owner CAS")
			require.True(t, handled)
			latest, err := store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, current.State(), latest.State())
		})
	}
}
