package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

// 复用既有Lease/RFC6902协议夹具和真实TLS消费者。前序预算/VM事实是明确的
// 领域夹具，不伪装成实际K8s、完整预算生产者或Linux物理内存验收。
func TestPGWorkerFinalAdmissionUsesDurableOriginalAndFullBudget(t *testing.T) {
	for _, mode := range []string{"reply-lost-restart", "dispatch-write-lost", "incomplete-budget"} {
		t.Run(mode, func(t *testing.T) {
			api, store, coord := newAPI(t)
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				intent := pgRequest(t, session.Actor())
				base := pgResponse(t, intent).Observed
				base.AdmissionOpen = false
				base.AdmissionRevision = 0
				startup := base.StartupSharedUpperBytes
				shared := control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: startup / 2, StartupArraysUpperBytes: startup - startup/2, Observed: base}
				shared.CurrentMainAndArraysUpperBytes = shared.StartupMainUpperBytes
				for _, a := range base.Arrays {
					shared.CurrentMainAndArraysUpperBytes += a.CommittedUpperBytes
				}
				var sent atomic.Int32
				var resizes, terminalReads atomic.Int32
				var original atomic.Pointer[control.AdmissionRequest]
				config := pgConfig(t, intent, func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/dsg/v1/resize" || r.URL.Path == "/dsg/v1/resize/status" {
						if r.URL.Path == "/dsg/v1/resize" {
							resizes.Add(1)
						} else {
							terminalReads.Add(1)
						}
						pgReply(w, pgResponse(t, intent))
						return
					}
					if r.URL.Path == "/dsg/v1/budget/shared" {
						pgReply(w, shared)
						return
					}
					var request control.AdmissionRequest
					require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
					if r.URL.Path == "/dsg/v1/admission" {
						copy := request
						original.Store(&copy)
						sent.Add(1)
						w.WriteHeader(503)
						return
					}
					require.Equal(t, "/dsg/v1/admission/status", r.URL.Path)
					digest, err := request.ContextSHA256()
					require.NoError(t, err)
					response := control.AdmissionResponse{ProtocolVersion: 1, Request: request, ContextSHA256: digest, Disposition: control.AdmissionNotFound, Observed: base}
					if actual := original.Load(); actual != nil {
						require.Equal(t, *actual, request)
						response.Disposition = control.AdmissionReplayed
						response.Observed.AdmissionOpen = true
						response.Observed.AdmissionRevision = request.Revision
					}
					pgReply(w, response)
				})
				worker, err := session.NewPGWorker(ctx, config)
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				api.mu.Lock()
				api.vm.Status.MemorySize = resource.NewQuantity(int64(intent.TargetVMMemoryBytes), resource.BinarySI)
				api.mu.Unlock()
				bound := func(id string) *budget.Bound {
					return &budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 1 << 20, SourceID: id}
				}
				full := budget.Finalized{ProtocolVersion: 1, Request: intent, IssuerEpoch: session.Actor().Epoch, RunnerPodName: "runner", HostReservationID: "host-original", GuestUsableMemoryBytes: intent.TargetVMMemoryBytes, Shared: shared, SystemOutsidePG: *bound("os"), PrivateAndDSM: bound("private-dsm"), LFCOutsideShared: bound("lfc"), HugePoolOutsideShared: bound("pool"), Transition: *bound("transition"), Safety: *bound("safety")}
				if mode == "incomplete-budget" {
					full.PrivateAndDSM.Kind = "memory_high_observation"
				}
				snapshot, err := store.Load(ctx)
				require.NoError(t, err)
				snapshot, _, err = store.Begin(ctx, snapshot, session.Actor(), intent, 1)
				require.NoError(t, err)
				for _, s := range snapshot.State().Active.Steps {
					if s.Action == journal.ResumeAdmission {
						break
					}
					if s.Action == journal.ResizePG {
						// PG步骤由生产executor/客户端真实写前CAS和落终态，
						// 不以普通前驱fixture伪造重建worker需要的原生审计。
						progress, err := worker.Advance(ctx)
						require.NoError(t, err)
						require.Equal(t, pgexec.Recorded, progress)
						snapshot, err = store.Load(ctx)
						require.NoError(t, err)
						continue
					}
					data := []byte(`{"test_only_prior_domain":true}`)
					target := "prior-domain"
					if s.Action == journal.FinalizeBudget {
						data, err = json.Marshal(full)
						require.NoError(t, err)
						target = intent.Key.Instance.ComputeInstanceID
					}
					var dispatch *journal.Dispatch
					snapshot, dispatch, err = store.Start(ctx, snapshot, session.Actor(), intent.Key, s.Action, target, []byte(`{"test_only_prior_domain":true}`))
					require.NoError(t, err)
					snapshot, err = store.Record(ctx, snapshot, session.Actor(), intent.Key, journal.Observation{Key: intent.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256, Ordinal: s.Ordinal, Action: s.Action, Result: journal.Completed, SourceID: target, Data: data})
					require.NoError(t, err)
				}
				if mode == "dispatch-write-lost" {
					api.mu.Lock()
					api.loseJournalReply = true
					api.mu.Unlock()
				}
				progress, err := worker.Advance(ctx)
				require.Equal(t, pgexec.AwaitingFact, progress)
				require.Error(t, err)
				after, loadErr := store.Load(ctx)
				require.NoError(t, loadErr)
				step := after.State().Active.Steps[len(after.State().Active.Steps)-1]
				if mode == "incomplete-budget" {
					require.Equal(t, journal.Planned, step.Phase)
					require.Zero(t, sent.Load())
					cancel()
					return ctx.Err()
				}
				require.Equal(t, journal.Unconfirmed, step.Phase, "dispatch error: %v", err)
				if mode == "reply-lost-restart" {
					require.EqualValues(t, 1, sent.Load())
					restarted, err := session.NewPGWorker(ctx, config)
					require.NoError(t, err)
					defer restarted.CloseIdleConnections()
					require.Equal(t, &intent, restarted.client.Pending())
					sequence := after.State().Sequence
					progress, err = restarted.Advance(ctx)
					require.NoError(t, err)
					require.Equal(t, pgexec.Reconciled, progress)
					require.Nil(t, restarted.client.Pending())
					after, err = store.Load(ctx)
					require.NoError(t, err)
					require.Equal(t, sequence, after.State().Sequence, "只读原终态核对不重写账本")
					require.EqualValues(t, 1, terminalReads.Load())
					require.EqualValues(t, 1, resizes.Load(), "恢复不能重发已完成Resize")
					progress, err = restarted.Advance(ctx)
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
					after, err = store.Load(ctx)
					require.NoError(t, err)
					require.Equal(t, journal.Settled, after.State().Active.Steps[len(after.State().Active.Steps)-1].Phase)
					require.EqualValues(t, 1, sent.Load(), "恢复仅查询，不重发setter")
				} else {
					require.Zero(t, sent.Load(), "写前回复丢失不能产生发送票据")
					progress, err = worker.Advance(ctx)
					require.NoError(t, err)
					require.Equal(t, pgexec.AwaitingFact, progress)
					after, err = store.Load(ctx)
					require.NoError(t, err)
					require.Equal(t, journal.Unconfirmed, after.State().Active.Steps[len(after.State().Active.Steps)-1].Phase)
					require.Zero(t, sent.Load(), "not_found不是自动重发许可")
					// 只有明确选择原意图重放才发送，且不改revision/预算/boot。
					progress, err = worker.ReplayAdmissionOriginal(ctx, intent.Key)
					require.Error(t, err, "此夹具继续模拟setter回复丢失")
					require.Equal(t, pgexec.AwaitingFact, progress)
					require.EqualValues(t, 1, sent.Load())
					var saved control.AdmissionRequest
					require.NoError(t, json.Unmarshal(step.Command, &saved))
					require.Equal(t, saved, *original.Load())
					progress, err = worker.Advance(ctx)
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
				}
				cancel()
				return ctx.Err()
			})
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}
