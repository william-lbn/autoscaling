package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

// 实际Session/TLS/双客户端/写前UID-RV CAS与生产Quiesce/后续Shrink消费者；
// Kube、PG原生事实和root内核为明确协议模型，不冒充Linux或原生PG端到端。
func TestPGWorkerContinuousQuiesceUsesOriginalCloseAndRetainsAllOtherCosts(t *testing.T) {
	for _, mode := range []string{"retained", "reply-lost-restart", "dispatch-lost-replay", "busy", "ack-pending", "root-drift", "runner-drift", "lower-normal", "post-quiesce-drift", "drain-dispatch", "concurrent", "incomplete-prepared"} {
		t.Run(mode, func(t *testing.T) {
			api, store, coord := newAPI(t)
			root, resourceConfig := guestRoot(t, store)
			// 在首次资源绑定之前冻结测试normal512MB。不是运行期改原Bootstrap；
			// 旧source保持同一512额度，验证非零保留状态仍由整体覆盖而非补零。
			root.view.Bootstrap.Limits.MemoryMaxBytes = 512 << 20
			root.view.Actual.MemoryMaxBytes = 512 << 20
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				req := pgRequest(t, session.Actor())
				req.Kind, req.TargetBuffers, req.TargetVMMemoryBytes = control.Shrink, 512, 768<<20
				resources, err := session.NewResourceWorker(ctx, resourceConfig)
				require.NoError(t, err)
				defer resources.CloseIdleConnections()
				p, _ := preparedGuest(t, req, root.view.Identity)
				if mode == "lower-normal" {
					p.Limits.MemoryMaxBytes = 256 << 20
				}
				require.NoError(t, p.ValidateFor(req, root.view.Identity))
				if mode == "incomplete-prepared" {
					p.SystemOutsidePG.Kind = "observed"
				}
				api.mu.Lock()
				api.vm.Status.MemorySize = resource.NewQuantity(int64(p.VMBeforeMemoryBytes), resource.BinarySI)
				api.mu.Unlock()
				var mu sync.Mutex
				var original *control.AdmissionRequest
				posts, queries, resizes := 0, 0, 0
				busy, ackPending, reopened := mode == "busy", mode == "ack-pending", false
				base := req
				base.Kind, base.TargetBuffers = control.Grow, 1024
				config := pgConfig(t, base, func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					switch r.URL.Path {
					case "/dsg/v1/admission", "/dsg/v1/admission/status":
						var closeIntent control.AdmissionRequest
						require.NoError(t, json.NewDecoder(r.Body).Decode(&closeIntent))
						require.False(t, closeIntent.Open)
						if r.URL.Path == "/dsg/v1/admission" {
							// 在任何模拟native效果之前重读真实typed Store原命令。
							s, err := store.Load(r.Context())
							require.NoError(t, err)
							step := s.State().Active.Steps[1]
							require.Equal(t, journal.Quiesce, step.Action)
							require.Equal(t, journal.Unconfirmed, step.Phase)
							var saved control.AdmissionRequest
							require.NoError(t, json.Unmarshal(step.Command, &saved))
							require.Equal(t, saved, closeIntent)
							posts++
							copy := closeIntent
							original = &copy
							if mode == "root-drift" {
								root.mu.Lock()
								root.view.Actual.MemoryMaxBytes++
								root.mu.Unlock()
							}
							if mode == "runner-drift" {
								api.mu.Lock()
								api.vm.Status.RestartCount++
								api.mu.Unlock()
							}
							if mode == "reply-lost-restart" {
								w.WriteHeader(503)
								return
							}
						} else {
							queries++
						}
						observed := p.Shared.Observed
						observed.Arrays = append([]control.ArrayFact(nil), observed.Arrays...)
						disposition := control.AdmissionNotFound
						if original != nil {
							require.Equal(t, *original, closeIntent)
							observed.AdmissionOpen, observed.AdmissionRevision = false, closeIntent.Revision
							disposition = control.AdmissionReplayed
							if r.URL.Path == "/dsg/v1/admission" {
								disposition = control.AdmissionApplied
							}
						}
						digest, err := closeIntent.ContextSHA256()
						require.NoError(t, err)
						pgReply(w, control.AdmissionResponse{ProtocolVersion: 1, Request: closeIntent, Disposition: disposition, ContextSHA256: digest, Observed: observed})
					case "/dsg/v1/work/dsm":
						require.NotNil(t, original)
						s := p.Shared.Observed
						s.Arrays = append([]control.ArrayFact(nil), s.Arrays...)
						s.AdmissionOpen, s.AdmissionRevision = false, original.Revision
						work := control.WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 2, ClientProcesses: 1, IdleClients: 1, BackgroundProcesses: 3, AuxiliaryProcesses: 2, ObserverExcluded: true, ForegroundIdle: true, Observed: s}
						if busy {
							work.IdleClients, work.FrontendExecutes, work.ForegroundIdle = 0, 1, false
						}
						conn := control.ConnectionResponse{ProtocolVersion: 1, ConnectionAPIVersion: 1, Enabled: true, RequestedSerial: 2, AppliedSerial: 2, Acknowledged: true, PrivateManagementReady: true, PublicConnectionsBlocked: true, Work: work}
						if ackPending {
							conn.AppliedSerial, conn.AppliedOpen, conn.Acknowledged, conn.PublicConnectionsBlocked = 1, true, false, false
						}
						if reopened {
							conn.Work.Observed.AdmissionOpen = true
							conn.Work.Observed.AdmissionRevision = original.Revision + 1
							conn.Work.ForegroundIdle = false
							conn.RequestedSerial, conn.AppliedSerial, conn.RequestedOpen, conn.AppliedOpen, conn.PublicConnectionsBlocked = 3, 3, true, true, false
						}
						dsm := control.DSMResponse{ProtocolVersion: 1, DSMAPIVersion: 1, CurrentBootOnly: true, AccountingValid: true, ReservedUpperBytes: 4096, ControlUpperBytes: 4096, ExternalSegments: 1, UnknownSegments: 1, PageBytes: 4096, Prepared: control.PreparedResponse{ProtocolVersion: 1, PreparedAPIVersion: 1, PreparedSlots: 1, MaximumPreparedSlots: 4, RuntimePreparesBlocked: !reopened, Connections: conn}}
						require.NoError(t, dsm.ValidateFor(req.Key.Instance))
						pgReply(w, dsm)
					case "/dsg/v1/resize":
						resizes++
						require.Equal(t, "drain-dispatch", mode, "unverified resize must not dispatch")
						var sent control.Request
						require.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
						require.Equal(t, req, sent)
						fact := pgResponse(t, base)
						hash, err := req.ContextSHA256()
						require.NoError(t, err)
						for _, s := range []*control.Snapshot{fact.Receipt.Result, &fact.Observed} {
							s.LowBuffers, s.HighBuffers, s.CapacityGeneration = req.TargetBuffers, req.TargetBuffers, req.ExpectedCapacityGeneration+1
							s.AdmissionOpen, s.AdmissionRevision = false, original.Revision
							s.Operation.Kind, s.Operation.ContextSHA256, s.Operation.TargetBuffers = req.Kind, hash, req.TargetBuffers
						}
						require.NoError(t, fact.ValidateResize(req, req.Key.Instance))
						pgReply(w, fact)
					default:
						t.Error("unexpected quiesce route", r.URL.Path)
						w.WriteHeader(404)
					}
				})
				worker, err := session.NewPGWorker(ctx, config, resources)
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				s, err := store.Load(ctx)
				require.NoError(t, err)
				s, _, err = store.Begin(ctx, s, session.Actor(), req, 0)
				require.NoError(t, err)
				data, err := json.Marshal(p)
				require.NoError(t, err)
				s, dispatch, err := store.Start(ctx, s, session.Actor(), req.Key, journal.PrepareBudget, req.Key.Instance.ComputeInstanceID, []byte(`{"test_only_prepared_producer":true}`))
				require.NoError(t, err)
				_, err = store.Record(ctx, s, session.Actor(), req.Key, journal.Observation{Key: req.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256, Ordinal: dispatch.Step.Ordinal, Action: journal.PrepareBudget, Result: journal.Completed, SourceID: req.Key.Instance.ComputeInstanceID, Data: data})
				require.NoError(t, err)
				if mode == "dispatch-lost-replay" {
					api.mu.Lock()
					api.loseJournalReply = true
					api.mu.Unlock()
				}
				var progress pgexec.Progress
				if mode == "concurrent" {
					// 两个生产Advance同时竞争同一Planned步骤。失败CAS不得获得
					// 派发票据；获票据者发送一次，另一执行者只查询或返回冲突。
					type outcome struct {
						progress pgexec.Progress
						err      error
					}
					results := make(chan outcome, 2)
					start := make(chan struct{})
					for i := 0; i < 2; i++ {
						go func() { <-start; p, e := worker.Advance(ctx); results <- outcome{p, e} }()
					}
					close(start)
					recorded := 0
					for i := 0; i < 2; i++ {
						r := <-results
						if r.err == nil && r.progress == pgexec.Recorded {
							recorded++
						}
					}
					require.Positive(t, recorded, "至少一个原CAS持有者完成，不能仅靠无效果的并发调用通过")
					progress, err = pgexec.Recorded, nil
				} else {
					progress, err = worker.Advance(ctx)
				}
				if mode == "retained" || mode == "post-quiesce-drift" || mode == "drain-dispatch" || mode == "concurrent" {
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
				} else {
					require.Error(t, err)
					require.Equal(t, pgexec.AwaitingFact, progress)
				}
				after, err := store.Load(ctx)
				require.NoError(t, err)
				if mode == "lower-normal" || mode == "incomplete-prepared" {
					require.Equal(t, journal.Planned, after.State().Active.Steps[1].Phase)
					require.Zero(t, posts)
					cancel()
					return ctx.Err()
				}
				if mode == "root-drift" || mode == "runner-drift" {
					require.Equal(t, journal.Unconfirmed, after.State().Active.Steps[1].Phase)
					require.Equal(t, 1, posts)
					cancel()
					return ctx.Err()
				}
				if mode == "reply-lost-restart" {
					restored, err := session.NewPGWorker(ctx, config, resources)
					require.NoError(t, err)
					defer restored.CloseIdleConnections()
					pending, closing := restored.client.PendingAdmission()
					require.NotNil(t, pending)
					require.Nil(t, closing)
					require.Equal(t, *original, *pending)
					worker = restored
				}
				if mode == "busy" || mode == "ack-pending" {
					mu.Lock()
					busy, ackPending = false, false
					mu.Unlock()
				}
				if mode == "dispatch-lost-replay" {
					require.Zero(t, posts)
					progress, err = worker.Advance(ctx)
					require.NoError(t, err)
					require.Equal(t, pgexec.AwaitingFact, progress)
					require.Zero(t, posts)
					progress, err = worker.ReplayQuiesceOriginal(ctx, req.Key)
				} else if mode != "retained" && mode != "post-quiesce-drift" && mode != "drain-dispatch" && mode != "concurrent" {
					progress, err = worker.Advance(ctx)
				}
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
				after, err = store.Load(ctx)
				require.NoError(t, err)
				qstep := after.State().Active.Steps[1]
				require.Equal(t, journal.Settled, qstep.Phase)
				var q budget.Quiesced
				require.NoError(t, json.Unmarshal(qstep.Observation.Data, &q))
				require.NoError(t, q.ValidateFor(p, session.Actor().Epoch))
				require.EqualValues(t, 1, q.DSM.Prepared.PreparedSlots)
				require.EqualValues(t, 1, q.DSM.UnknownSegments)
				require.EqualValues(t, 3, q.DSM.Prepared.Connections.Work.BackgroundProcesses)
				require.False(t, q.VMShrinkPermit)
				require.Equal(t, 1, posts)
				require.Zero(t, resizes)
				if mode != "retained" && mode != "post-quiesce-drift" && mode != "drain-dispatch" && mode != "concurrent" {
					require.Positive(t, queries)
				}
				if mode == "post-quiesce-drift" {
					mu.Lock()
					reopened = true
					mu.Unlock()
					progress, err = worker.Advance(ctx)
					require.Error(t, err)
					require.Equal(t, pgexec.AwaitingFact, progress)
					require.Zero(t, resizes)
					after, err = store.Load(ctx)
					require.NoError(t, err)
					require.Equal(t, journal.Planned, after.State().Active.Steps[2].Phase)
				}
				if mode == "drain-dispatch" {
					progress, err = worker.Advance(ctx)
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
					require.Equal(t, 1, resizes)
					after, err = store.Load(ctx)
					require.NoError(t, err)
					require.Equal(t, journal.Completed, after.State().Active.Steps[2].Observation.Result)
					require.Equal(t, journal.Planned, after.State().Active.Steps[3].Phase, "逻辑PG提交不能跳过物理释放步骤")
				}
				cancel()
				return ctx.Err()
			})
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}
