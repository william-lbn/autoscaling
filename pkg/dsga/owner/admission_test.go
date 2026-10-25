package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

// 复用既有Lease/RFC6902协议夹具和真实TLS消费者。前序预算/VM事实是明确的
// 领域夹具，不伪装成实际K8s、完整预算生产者或Linux物理内存验收。
func TestPGWorkerFinalAdmissionUsesDurableOriginalAndFullBudget(t *testing.T) {
	for _, mode := range []string{"reply-lost-restart", "dispatch-write-lost", "replay-source-replaced", "incomplete-budget", "success", "root-replaced-before", "root-floor-before", "root-birth-before", "root-unknown-before", "root-sdk-pending-before", "root-actual-drift-before", "root-process-before", "no-resource-before", "outside-mismatch-before", "vm-before-write", "root-after-write", "vm-after-write"} {
		t.Run(mode, func(t *testing.T) {
			api, store, coord := newAPI(t)
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				intent := pgRequest(t, session.Actor())
				root, rootConfig := guestRoot(t, store)
				var resources *ResourceWorker
				if mode != "no-resource-before" {
					resources, err = session.NewResourceWorker(ctx, rootConfig)
					require.NoError(t, err)
					defer resources.CloseIdleConnections()
				}
				root.mu.Lock()
				originalView := root.view
				root.mu.Unlock()
				aggregate, err := budget.RootAggregateFromView(originalView)
				require.NoError(t, err)
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
				var resizes, terminalReads, admissionReads atomic.Int32
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
						if mode == "success" || mode == "root-after-write" || mode == "vm-after-write" {
							if mode == "root-after-write" {
								root.mu.Lock()
								root.view.Actual.MemoryMaxBytes += 4 << 20
								root.mu.Unlock()
							}
							if mode == "vm-after-write" {
								api.mu.Lock()
								api.vm.Status.PodName = "another-runner"
								api.vmVersion++
								api.vm.ResourceVersion = "vm-rv-" + strconv.Itoa(api.vmVersion)
								api.mu.Unlock()
							}
							digest, err := request.ContextSHA256()
							require.NoError(t, err)
							observed := base
							observed.AdmissionOpen, observed.AdmissionRevision = true, request.Revision
							pgReply(w, control.AdmissionResponse{ProtocolVersion: 1, Request: request, ContextSHA256: digest, Disposition: control.AdmissionApplied, Observed: observed})
							return
						}
						w.WriteHeader(503)
						return
					}
					require.Equal(t, "/dsg/v1/admission/status", r.URL.Path)
					admissionReads.Add(1)
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
				var resourceArgs []*ResourceWorker
				if resources != nil {
					resourceArgs = append(resourceArgs, resources)
				}
				worker, err := session.NewPGWorker(ctx, config, resourceArgs...)
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				api.mu.Lock()
				api.vm.Status.MemorySize = resource.NewQuantity(int64(intent.TargetVMMemoryBytes), resource.BinarySI)
				api.mu.Unlock()
				bound := func(id string) *budget.Bound {
					return &budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 1 << 20, SourceID: id}
				}
				full := budget.Finalized{ProtocolVersion: 1, Request: intent, IssuerEpoch: session.Actor().Epoch, RunnerPodName: "runner", HostReservationID: "host-original", GuestUsableMemoryBytes: 768 << 20, Shared: shared, Aggregate: &aggregate.Bound, SystemOutsidePG: *bound("os"), Transition: *bound("transition"), Safety: *bound("safety")}
				full.SystemOutsidePG.UpperBytes, full.Transition.UpperBytes, full.Safety.UpperBytes = 24<<20, 4<<20, 4<<20
				if mode == "outside-mismatch-before" {
					// 总量仍可装入Guest，但不能冒认原root保留的Outside份额。
					full.SystemOutsidePG.UpperBytes++
				}
				if mode == "incomplete-budget" {
					full.Aggregate.Kind = "memory_high_observation"
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
				if mode == "dispatch-write-lost" || mode == "replay-source-replaced" {
					api.mu.Lock()
					api.loseJournalReply = true
					api.mu.Unlock()
				}
				// 新负向场景只改最后开放前的真实领域输入；不改变已落账预算。
				root.mu.Lock()
				switch mode {
				case "root-replaced-before", "root-unknown-before", "root-sdk-pending-before":
					c := resourceguard.Command{ProtocolVersion: 1, Identity: root.view.Identity, IssuerEpoch: session.Actor().Epoch, Revision: 1, BudgetCommitmentID: "different-original-source", PostmasterPID: root.view.Birth.PostmasterPID, PostmasterStartTicks: root.view.Birth.PostmasterStartTicks, MemoryMaxBytes: root.view.Bootstrap.Limits.MemoryMaxBytes, OutsideUpperBytes: root.view.Bootstrap.Limits.OutsideUpperBytes, HugePools: root.view.Bootstrap.Limits.HugePools}
					if mode == "root-sdk-pending-before" {
						require.NoError(t, resources.client.RetainOriginal(c))
					} else {
						root.view.Original = &c
					}
					if mode == "root-replaced-before" {
						root.view.Held, root.view.Applied = &c, true
					}
				case "root-floor-before":
					root.view.IssuerFloorEpoch++
				case "root-birth-before":
					birth := *root.view.Birth
					birth.PostmasterStartTicks++
					root.view.Birth = &birth
				case "root-process-before":
					birth := *root.view.Birth
					birth.PostmasterStartTicks++
					root.processOverride = &birth
				case "root-actual-drift-before":
					root.view.Actual.MemoryMaxBytes += 4 << 20
				case "vm-before-write":
					root.afterProcess = func() {
						api.mu.Lock()
						api.vm.Status.PodName = "another-runner"
						api.vmVersion++
						api.vm.ResourceVersion = "vm-rv-" + strconv.Itoa(api.vmVersion)
						api.mu.Unlock()
					}
				}
				root.mu.Unlock()
				progress, err := worker.Advance(ctx)
				if mode == "success" {
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
					after, err := store.Load(ctx)
					require.NoError(t, err)
					require.Equal(t, journal.Settled, after.State().Active.Steps[len(after.State().Active.Steps)-1].Phase)
					require.EqualValues(t, 1, sent.Load())
					require.Zero(t, root.posts, "最终开放不得顺带重写root限额")
					var receipt struct {
						ProtocolVersion uint32               `json:"protocol_version"`
						PGAggregate     budget.RootAggregate `json:"pg_aggregate"`
						ResourceEpoch   control.Uint64       `json:"resource_issuer_epoch"`
					}
					last := after.State().Active.Steps[len(after.State().Active.Steps)-1]
					require.NoError(t, json.Unmarshal(last.Observation.Data, &receipt))
					require.EqualValues(t, 2, receipt.ProtocolVersion)
					require.Equal(t, aggregate, receipt.PGAggregate)
					require.Equal(t, session.Actor().Epoch, receipt.ResourceEpoch)
					cancel()
					return ctx.Err()
				}
				require.Equal(t, pgexec.AwaitingFact, progress)
				require.Error(t, err)
				after, loadErr := store.Load(ctx)
				require.NoError(t, loadErr)
				step := after.State().Active.Steps[len(after.State().Active.Steps)-1]
				if mode == "incomplete-budget" || mode == "root-replaced-before" || mode == "root-floor-before" || mode == "root-birth-before" || mode == "root-unknown-before" || mode == "root-sdk-pending-before" || mode == "root-process-before" || mode == "root-actual-drift-before" || mode == "no-resource-before" || mode == "outside-mismatch-before" || mode == "vm-before-write" {
					require.Zero(t, sent.Load(), "未核验原资源来源不得向原生PG派发开放")
					require.Equal(t, journal.Planned, step.Phase)
					cancel()
					return ctx.Err()
				}
				require.Equal(t, journal.Unconfirmed, step.Phase, "dispatch error: %v", err)
				if mode == "root-after-write" || mode == "vm-after-write" {
					require.EqualValues(t, 1, sent.Load())
					progress, err = worker.Advance(ctx)
					require.Error(t, err)
					require.Equal(t, pgexec.AwaitingFact, progress)
					require.EqualValues(t, 1, admissionReads.Load(), "原只读查询不能被资源/VM漂移隐藏")
					after, loadErr = store.Load(ctx)
					require.NoError(t, loadErr)
					require.Equal(t, journal.Unconfirmed, after.State().Active.Steps[len(after.State().Active.Steps)-1].Phase)
					root.mu.Lock()
					root.view = originalView
					root.mu.Unlock()
					api.mu.Lock()
					api.vm.Status.PodName = "runner"
					api.vmVersion++
					api.vm.ResourceVersion = "vm-rv-" + strconv.Itoa(api.vmVersion)
					api.mu.Unlock()
					progress, err = worker.Advance(ctx)
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
					require.EqualValues(t, 1, sent.Load(), "恢复原来源后仍只查询，不重发setter")
					require.EqualValues(t, 2, admissionReads.Load())
					cancel()
					return ctx.Err()
				}
				if mode == "reply-lost-restart" {
					require.EqualValues(t, 1, sent.Load())
					restarted, err := session.NewPGWorker(ctx, config, resources)
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
					// 同额的新Held不能借显式重放收养为原预算；恢复原来源后才允许
					// 发送完全相同的命令。拒绝期间WAL/SDK及原预算继续保留。
					if mode == "replay-source-replaced" {
						root.mu.Lock()
						c := resourceguard.Command{ProtocolVersion: 1, Identity: root.view.Identity, IssuerEpoch: session.Actor().Epoch, Revision: 1, BudgetCommitmentID: "another-source-for-replay", PostmasterPID: root.view.Birth.PostmasterPID, PostmasterStartTicks: root.view.Birth.PostmasterStartTicks, MemoryMaxBytes: root.view.Bootstrap.Limits.MemoryMaxBytes, OutsideUpperBytes: root.view.Bootstrap.Limits.OutsideUpperBytes, HugePools: root.view.Bootstrap.Limits.HugePools}
						root.view.Original, root.view.Held, root.view.Applied = &c, &c, true
						root.mu.Unlock()
						progress, err = worker.ReplayAdmissionOriginal(ctx, intent.Key)
						require.ErrorContains(t, err, "original aggregate source was replaced")
						require.Equal(t, pgexec.AwaitingFact, progress)
						require.Zero(t, sent.Load())
						unchanged, err := store.Load(ctx)
						require.NoError(t, err)
						require.Equal(t, step, unchanged.State().Active.Steps[len(unchanged.State().Active.Steps)-1])
						root.mu.Lock()
						root.view = originalView
						root.mu.Unlock()
					}
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
