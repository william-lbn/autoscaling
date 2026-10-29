package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
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

// 本测试执行实际Session/双客户端/TLS/完整VM账本与UID/RV CAS。Kube API、
// root内核及PG成本/前驱领域均为显式协议夹具，不是Linux物理释放/完整预算验收。
// root处理器在任何模拟资源效果之前，向真实typed Store重读完整写前命令。
type guestRootFixture struct {
	mu         sync.Mutex
	view       resourceguard.View
	posts      int
	observes   int
	loseReply  bool
	afterApply func()
	// 仅用于启动恢复在Root Observe到达时同步测试，不替代外层原WAL。
	afterObserve func()
	// 取消peer只模拟协议屏障，真实文件锁/fsync/防迟到由resourceguard独立验证。
	cancellations   int
	loseCancelReply bool
	afterCancel     func()
	// 最终准入测试的独立读取/出生竞态钩子；均在fixture锁内访问。
	afterProcess    func()
	processOverride *resourceguard.ProcessIdentity
}

func guestRoot(t *testing.T, store *journal.Store) (*guestRootFixture, resourceguard.ClientConfig) {
	t.Helper()
	id := resourceguard.Identity{VMUID: string(binding().VMUID), ComputeInstanceID: "compute-1", GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
	pools := []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 8}, {PageSizeKB: 1048576, Pages: 0}}
	limits := resourceguard.Limits{MemoryMaxBytes: 64 << 20, OutsideUpperBytes: 32 << 20, HugePools: pools}
	f := &guestRootFixture{view: resourceguard.View{ProtocolVersion: 1, Identity: id, IssuerFloorEpoch: 1, Bootstrap: resourceguard.BootstrapCommand{ProtocolVersion: 1, Identity: id, IssuerEpoch: 1, BudgetCommitmentID: "bootstrap-original", Limits: limits}, BootstrapApplied: true, Birth: &resourceguard.ProcessIdentity{ProtocolVersion: 1, Identity: id, PostmasterPID: 123, PostmasterStartTicks: 456}, Actual: resourceguard.Evidence{ProtocolVersion: 1, Identity: id, MemoryMaxBytes: 64 << 20, MemoryCurrentBytes: 4 << 20, GuestMemTotalBytes: 768 << 20, HugePools: pools, WholePoolUpperBytes: 16 << 20, HugeOvercommitDisabled: true, CgroupPopulated: true}}}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer original-guest-fixture" {
			w.WriteHeader(401)
			return
		}
		if r.URL.Path == "/dsg/v1/resource/fence" {
			var req resourceguard.FenceRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			snapshot, err := store.Load(r.Context())
			require.NoError(t, err)
			require.Equal(t, &req.Identity, snapshot.State().ResourceIdentity)
			f.mu.Lock()
			f.view.IssuerFloorEpoch = req.IssuerEpoch
			f.mu.Unlock()
			pgReply(w, req.Identity)
			return
		}
		var c resourceguard.Command
		var cancellation resourceguard.CancelRequest
		write := r.URL.Path == "/dsg/v1/resource/apply" || r.URL.Path == "/dsg/v1/resource/replay-original"
		observe := r.URL.Path == "/dsg/v1/resource/observe"
		cancelOriginal := r.URL.Path == "/dsg/v1/resource/cancel-original"
		if write || observe || cancelOriginal {
			if cancelOriginal {
				require.NoError(t, json.NewDecoder(r.Body).Decode(&cancellation))
				c = cancellation.Command
			} else {
				require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
			}
			snapshot, err := store.Load(r.Context())
			require.NoError(t, err)
			op := snapshot.State().Active
			require.NotNil(t, op)
			if cancelOriginal {
				require.True(t, op.AbortRequested, "Root取消必须来自持久原取消选择")
			}
			var found bool
			for _, step := range op.Steps {
				if step.Action == journal.GuestReady && step.Phase == journal.Unconfirmed {
					var saved resourceguard.Command
					require.NoError(t, json.Unmarshal(step.Command, &saved))
					require.Equal(t, saved, c, "实际资源效果前必须有完整原WAL")
					found = true
				}
			}
			require.True(t, found, "没有真实写前CAS票据的资源命令")
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/dsg/v1/resource/identity":
			pgReply(w, f.view.Identity)
		case "/dsg/v1/resource/state":
			pgReply(w, f.view)
		case "/dsg/v1/resource/process":
			process := f.view.Birth
			if f.processOverride != nil {
				process = f.processOverride
			}
			if f.afterProcess != nil {
				f.afterProcess()
			}
			pgReply(w, process)
		case "/dsg/v1/resource/observe":
			f.observes++
			if f.afterObserve != nil {
				f.afterObserve()
			}
			if f.view.Original == nil || !reflect.DeepEqual(*f.view.Original, c) {
				w.WriteHeader(409)
				return
			}
			// 与生产Guard.Observe一致：仅实际完整匹配才确认Root自身WAL，
			// 不触发内核写；外层GuestReady仍须执行器另行核全套来源。
			if f.view.Actual.ValidateFor(c) == nil {
				f.view.Held, f.view.Applied = f.view.Original, true
			}
			pgReply(w, f.view.Actual)
		case "/dsg/v1/resource/cancel-original":
			f.cancellations++
			require.Equal(t, f.view.IssuerFloorEpoch, cancellation.IssuerEpoch)
			if f.view.Original != nil && (!f.view.Applied || f.view.Original.Revision >= c.Revision) {
				w.WriteHeader(http.StatusConflict)
				return
			}
			if f.view.Cancelled == nil || !reflect.DeepEqual(*f.view.Cancelled, c) {
				next, err := f.view.NextRevision()
				require.NoError(t, err)
				require.Equal(t, next, c.Revision)
				f.view.Cancelled = &c
			}
			receipt := resourceguard.Cancellation{ProtocolVersion: 1, VMShrinkPermit: false, Command: c, State: f.view}
			require.NoError(t, receipt.ValidateFor(c, cancellation.IssuerEpoch))
			if f.afterCancel != nil {
				f.afterCancel()
			}
			if f.loseCancelReply {
				f.loseCancelReply = false
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			pgReply(w, receipt)
		case "/dsg/v1/resource/apply", "/dsg/v1/resource/replay-original":
			if f.view.Cancelled != nil && c.Revision <= f.view.Cancelled.Revision {
				w.WriteHeader(http.StatusConflict)
				return
			}
			f.posts++
			require.Equal(t, f.view.IssuerFloorEpoch, c.IssuerEpoch)
			f.view.Original, f.view.Held, f.view.Applied = &c, &c, true
			f.view.Actual.MemoryMaxBytes = c.MemoryMaxBytes
			f.view.Actual.HugePools = append([]resourceguard.HugePool(nil), c.HugePools...)
			f.view.Actual.WholePoolUpperBytes = 0
			for _, h := range c.HugePools {
				f.view.Actual.WholePoolUpperBytes += h.PageSizeKB * 1024 * h.Pages
			}
			if f.afterApply != nil {
				f.afterApply()
			}
			if f.loseReply {
				f.loseReply = false
				w.WriteHeader(503)
				return
			}
			pgReply(w, f.view.Actual)
		default:
			t.Error("unexpected root route", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(server.Close)
	return f, resourceguard.ClientConfig{BaseURL: server.URL, Identity: resourceguard.Identity{VMUID: id.VMUID, ComputeInstanceID: id.ComputeInstanceID}, Timeout: time.Second, Transport: server.Client().Transport.(*http.Transport), Token: func(context.Context) (string, error) { return "original-guest-fixture", nil }}
}

func preparedGuest(t *testing.T, req control.Request, id resourceguard.Identity) (budget.Prepared, control.SharedCostResponse) {
	t.Helper()
	base := req
	base.Kind, base.TargetBuffers = control.Grow, 1024
	observed := pgResponse(t, base).Observed
	observed.Operation = nil
	observed.CapacityGeneration = req.ExpectedCapacityGeneration
	observed.LowBuffers, observed.HighBuffers = 512, 512
	if req.Kind == control.Shrink {
		observed.LowBuffers, observed.HighBuffers = 1024, 1024
	}
	shared := control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: 16 << 20, StartupArraysUpperBytes: 16 << 20, CurrentMainAndArraysUpperBytes: 80 << 20, Observed: observed}
	p := budget.Prepared{ProtocolVersion: 1, Request: req, Identity: id, RunnerPodName: "runner", VMBeforeMemoryBytes: 512 << 20, GuestUsableMemoryBytes: 768 << 20, Shared: shared, TargetSharedUpperBytes: 144 << 20, Limits: resourceguard.Limits{MemoryMaxBytes: 512 << 20, OutsideUpperBytes: 32 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 8}, {PageSizeKB: 1048576, Pages: 0}}}, SystemOutsidePG: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 24 << 20, SourceID: "original-system"}, Transition: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "original-transition"}, Safety: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "original-safety"}}
	if req.Kind == control.Shrink {
		p.VMBeforeMemoryBytes = 2 << 30
		p.TargetSharedUpperBytes = 80 << 20
		shared.Observed.CapacityGeneration++
		shared.Observed.LowBuffers, shared.Observed.HighBuffers = req.TargetBuffers, req.TargetBuffers
		hash, err := req.ContextSHA256()
		require.NoError(t, err)
		shared.Observed.Operation = &control.OperationFact{Key: req.Key, Kind: req.Kind, Phase: control.Committed, Reason: control.ReasonNone, TargetBuffers: req.TargetBuffers, ExpectedCapacityGeneration: req.ExpectedCapacityGeneration, ContextSHA256: hash, CancelClosed: true}
		shared.Observed.Arrays = append([]control.ArrayFact(nil), shared.Observed.Arrays...)
		for i := range shared.Observed.Arrays {
			a := &shared.Observed.Arrays[i]
			key := req.Key
			a.ReleaseKey, a.ReleaseCapacityGeneration, a.ReleaseState = &key, shared.Observed.CapacityGeneration, control.ReleaseVerified
			a.Provider = control.ProviderFact{HasResult: true, Status: control.ProviderNoop}
		}
		shared.Observed.Arrays[0].Provider = control.ProviderFact{HasResult: true, Status: control.ProviderOK, PhysicalReleaseVerified: true, RequestedOffset: 4096, AffectedOffset: 4096, RequestedBytes: 4096, AffectedBytes: 4096}
	}
	require.NoError(t, p.ValidateFor(req, id))
	require.NoError(t, p.ValidateCurrent(shared, req.Key.IssuerEpoch))
	return p, shared
}

func beginGuest(t *testing.T, store *journal.Store, req control.Request, p budget.Prepared) {
	t.Helper()
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	snapshot, _, err = store.Begin(context.Background(), snapshot, snapshot.State().Owner, req, 0)
	require.NoError(t, err)
	for _, step := range snapshot.State().Active.Steps {
		if step.Action == journal.GuestReady {
			break
		}
		data := []byte(`{"test_only_prior_domain":true}`)
		target := "prior-domain-fixture"
		if step.Action == journal.PrepareBudget {
			data, err = json.Marshal(p)
			require.NoError(t, err)
			target = req.Key.Instance.ComputeInstanceID
		}
		var dispatch *journal.Dispatch
		snapshot, dispatch, err = store.Start(context.Background(), snapshot, snapshot.State().Owner, req.Key, step.Action, target, []byte(`{"test_only_prior_domain":true}`))
		require.NoError(t, err)
		snapshot, err = store.Record(context.Background(), snapshot, snapshot.State().Owner, req.Key, journal.Observation{Key: req.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256, Ordinal: step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: target, Data: data})
		require.NoError(t, err)
	}
}

func guestPhase(t *testing.T, store *journal.Store) journal.Step {
	t.Helper()
	s, err := store.Load(context.Background())
	require.NoError(t, err)
	for _, step := range s.State().Active.Steps {
		if step.Action == journal.GuestReady {
			return step
		}
	}
	t.Fatal("missing GuestReady")
	return journal.Step{}
}

func TestGuestReadyWorkerUsesOriginalBudgetAndActualDomainOrder(t *testing.T) {
	for _, mode := range []string{"grow", "root-reply-lost", "start-reply-lost", "incomplete-budget", "runner-drift", "pg-drift", "shrink", "shrink-unsupported", "omit-hstate", "root-empty", "lower-normal-cap", "tasks-finite", "tasks-changed", "tasks-reply-lost", "system-finite", "system-changed", "system-reply-lost", "system-drift", "as-finite", "as-changed", "as-reply-lost", "as-drift"} {
		t.Run(mode, func(t *testing.T) {
			api, store, coord := newAPI(t)
			root, resourceConfig := guestRoot(t, store)
			if mode == "tasks-finite" || mode == "tasks-changed" || mode == "tasks-reply-lost" {
				root.view.Bootstrap.Limits.PidsMaxTasks = 64
				root.view.Actual.TaskLimit = &resourceguard.TaskLimitEvidence{MaxTasks: 64, CurrentTasks: 4}
			}
			// 仅本矩阵明确启用AS来源；这是Root peer事实，真实硬限由原生/目标Linux另验。
			if mode == "as-finite" || mode == "as-changed" || mode == "as-reply-lost" || mode == "as-drift" {
				root.view.Bootstrap.Limits.PidsMaxTasks = 64
				root.view.Bootstrap.Limits.AddressSpaceMaxBytes = 128 << 20
				root.view.Birth.AddressSpaceMaxBytes = 128 << 20
				root.view.Actual.TaskLimit = &resourceguard.TaskLimitEvidence{MaxTasks: 64, CurrentTasks: 4}
				root.view.Actual.AddressSpace = &resourceguard.AddressSpaceEvidence{Processes: 4, MaxHardBytes: 128 << 20, MaxVirtualBytes: 64 << 20, Irreversible: true}
			}
			if mode == "system-finite" || mode == "system-changed" || mode == "system-reply-lost" || mode == "system-drift" {
				root.view.Identity.SystemCgroupDevice = root.view.Identity.CgroupDevice
				root.view.Identity.SystemCgroupInode = 99
				root.view.Bootstrap.Identity = root.view.Identity
				root.view.Birth.Identity = root.view.Identity
				root.view.Actual.Identity = root.view.Identity
				root.view.Bootstrap.Limits.SystemMemoryMaxBytes = 16 << 20
				root.view.SystemPlacementObserved = true
				root.view.Actual.System = &resourceguard.SystemEvidence{Scope: resourceguard.SystemChargedScope, MemoryMaxBytes: 16 << 20, MemoryCurrentBytes: 2 << 20, PID1Member: true, ProcessCount: 3}
			}
			if mode == "lower-normal-cap" {
				root.view.Bootstrap.Limits.MemoryMaxBytes = 512 << 20
				root.view.Actual.MemoryMaxBytes = 512 << 20
			}
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				resources, err := session.NewResourceWorker(ctx, resourceConfig)
				require.NoError(t, err)
				defer resources.CloseIdleConnections()
				req := pgRequest(t, session.Actor())
				if mode == "shrink" || mode == "shrink-unsupported" || mode == "lower-normal-cap" {
					req.Kind, req.TargetBuffers = control.Shrink, 512
				}
				p, shared := preparedGuest(t, req, root.view.Identity)
				p.Limits.PidsMaxTasks = root.view.Bootstrap.Limits.PidsMaxTasks
				p.Limits.AddressSpaceMaxBytes = root.view.Bootstrap.Limits.AddressSpaceMaxBytes
				if mode == "as-changed" {
					p.Limits.AddressSpaceMaxBytes = 0
				}
				p.Limits.SystemMemoryMaxBytes = root.view.Bootstrap.Limits.SystemMemoryMaxBytes
				if mode == "system-changed" {
					p.Limits.SystemMemoryMaxBytes = 8 << 20
				}
				if mode == "tasks-changed" {
					p.Limits.PidsMaxTasks--
				}
				if mode == "lower-normal-cap" {
					p.Limits.MemoryMaxBytes = 256 << 20
				}
				var drift atomic.Bool
				configBase := req
				configBase.Kind, configBase.TargetBuffers = control.Grow, 1024
				config := pgConfig(t, configBase, func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, "/dsg/v1/budget/shared", r.URL.Path)
					next := shared
					if drift.Load() {
						next.Observed.CapacityGeneration++
					}
					pgReply(w, next)
				})
				worker, err := session.NewPGWorker(ctx, config, resources)
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				api.mu.Lock()
				memory := req.TargetVMMemoryBytes
				if req.Kind == control.Shrink {
					memory = p.VMBeforeMemoryBytes
				}
				api.vm.Status.MemorySize = resource.NewQuantity(int64(memory), resource.BinarySI)
				api.mu.Unlock()
				root.mu.Lock()
				// 资源序号已有7，PG请求序号仍1：新命令必须采用8。
				old := resourceguard.Command{ProtocolVersion: 1, Identity: root.view.Identity, IssuerEpoch: session.Actor().Epoch, Revision: 7, BudgetCommitmentID: "prior-resource-budget", PostmasterPID: 123, PostmasterStartTicks: 456, MemoryMaxBytes: root.view.Bootstrap.Limits.MemoryMaxBytes, PidsMaxTasks: root.view.Bootstrap.Limits.PidsMaxTasks, AddressSpaceMaxBytes: root.view.Bootstrap.Limits.AddressSpaceMaxBytes, SystemMemoryMaxBytes: root.view.Bootstrap.Limits.SystemMemoryMaxBytes, OutsideUpperBytes: root.view.Bootstrap.Limits.OutsideUpperBytes, HugePools: root.view.Bootstrap.Limits.HugePools}
				root.view.Original, root.view.Held, root.view.Applied = &old, &old, true
				root.loseReply = mode == "root-reply-lost" || mode == "tasks-reply-lost" || mode == "system-reply-lost" || mode == "as-reply-lost"
				switch mode {
				case "as-drift":
					root.afterApply = func() { root.view.Actual.AddressSpace.Irreversible = false }
				case "system-drift":
					root.afterApply = func() { root.view.Actual.System.RootUserspaceCount = 1 }
				case "runner-drift":
					root.afterApply = func() { api.mu.Lock(); api.vm.Status.RestartCount++; api.mu.Unlock() }
				case "pg-drift":
					root.afterApply = func() { drift.Store(true) }
				case "root-empty":
					root.afterApply = func() { root.view.Actual.CgroupPopulated = false }
				}
				root.mu.Unlock()
				if mode == "incomplete-budget" {
					p.SystemOutsidePG.Kind = "observed"
				} else if mode == "omit-hstate" {
					p.Limits.HugePools = p.Limits.HugePools[:1]
				} else if mode == "shrink-unsupported" {
					shared.Observed.Arrays[0].ReleaseState = control.ReleaseUnsupported
					shared.Observed.Arrays[0].Provider = control.ProviderFact{HasResult: true, Status: control.ProviderUnsupported}
				}
				beginGuest(t, store, req, p)
				if mode == "start-reply-lost" {
					api.mu.Lock()
					api.loseJournalReply = true
					api.mu.Unlock()
				}
				progress, err := worker.Advance(ctx)
				if mode == "grow" || mode == "shrink" || mode == "tasks-finite" || mode == "system-finite" || mode == "as-finite" {
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
				} else {
					require.Error(t, err)
					require.Equal(t, pgexec.AwaitingFact, progress)
				}
				step := guestPhase(t, store)
				root.mu.Lock()
				posts := root.posts
				root.mu.Unlock()
				switch mode {
				case "grow", "shrink", "tasks-finite", "system-finite", "as-finite":
					require.Equal(t, journal.Settled, step.Phase)
					require.Equal(t, journal.Completed, step.Observation.Result)
					require.Equal(t, 1, posts)
					var result struct {
						ProtocolVersion uint32               `json:"protocol_version"`
						Aggregate       budget.RootAggregate `json:"pg_aggregate"`
					}
					require.NoError(t, json.Unmarshal(step.Observation.Data, &result))
					require.EqualValues(t, 2, result.ProtocolVersion)
					require.NoError(t, result.Aggregate.Validate())
					root.mu.Lock()
					require.NoError(t, result.Aggregate.Match(root.view))
					root.mu.Unlock()
					require.Equal(t, p.Limits.MemoryMaxBytes+control.Uint64(16<<20), result.Aggregate.Bound.UpperBytes)
					require.False(t, result.Aggregate.VMShrinkPermit)
					var saved resourceguard.Command
					require.NoError(t, json.Unmarshal(step.Command, &saved))
					require.Equal(t, p.Limits.PidsMaxTasks, saved.PidsMaxTasks)
					require.Equal(t, p.Limits.AddressSpaceMaxBytes, saved.AddressSpaceMaxBytes)
					require.Equal(t, p.Limits.SystemMemoryMaxBytes, saved.SystemMemoryMaxBytes)
					require.Equal(t, control.Uint64(8), saved.Revision)
					require.NotEqual(t, req.Key.Revision, saved.Revision)
				case "incomplete-budget", "shrink-unsupported", "omit-hstate", "lower-normal-cap", "tasks-changed", "system-changed", "as-changed":
					require.Equal(t, journal.Planned, step.Phase)
					require.Zero(t, posts)
					if mode == "lower-normal-cap" {
						require.ErrorContains(t, err, "memory.max decrease")
					}
				case "root-empty":
					require.Equal(t, journal.Unconfirmed, step.Phase)
					require.Equal(t, 1, posts)
					require.ErrorContains(t, err, "populated resource domain")
					// 资源读回缺少活组时不发证；恢复后仅Observe原命令并保存原来源，
					// 不重新Apply、更换预算ID或伪造此前无效果。
					root.mu.Lock()
					root.view.Actual.CgroupPopulated = true
					root.mu.Unlock()
					progress, err = worker.Advance(ctx)
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
					var observed struct {
						ProtocolVersion uint32               `json:"protocol_version"`
						Aggregate       budget.RootAggregate `json:"pg_aggregate"`
					}
					require.NoError(t, json.Unmarshal(guestPhase(t, store).Observation.Data, &observed))
					root.mu.Lock()
					require.NoError(t, observed.Aggregate.Match(root.view))
					require.Equal(t, 1, root.posts)
					require.Equal(t, 1, root.observes)
					root.mu.Unlock()
				case "runner-drift", "pg-drift", "system-drift", "as-drift":
					require.Equal(t, journal.Unconfirmed, step.Phase)
					require.Equal(t, 1, posts)
				default:
					require.Equal(t, journal.Unconfirmed, step.Phase)
					wantPosts := 1
					if mode == "start-reply-lost" {
						wantPosts = 0
					}
					require.Equal(t, wantPosts, posts)
					if mode == "root-reply-lost" || mode == "tasks-reply-lost" || mode == "system-reply-lost" || mode == "as-reply-lost" {
						// setter回复丢失必须保留准确的执行不确定错误，
						// 不能被发送分支的局部err遮蔽成零值Evidence不符。
						var responseErr *resourceguard.ClientError
						require.ErrorAs(t, err, &responseErr)
						require.True(t, responseErr.MayHaveExecuted)
						require.Equal(t, "response_unconfirmed", responseErr.Code)
						// 完整重建实际Guest/PG worker，Pending必须从原WAL恢复，
						// 而不是复用旧对象才偶然能恢复。仍只Observe，不派发新命令。
						restoredResource, err := session.NewResourceWorker(ctx, resourceConfig)
						require.NoError(t, err)
						defer restoredResource.CloseIdleConnections()
						worker, err = session.NewPGWorker(ctx, config, restoredResource)
						require.NoError(t, err)
						defer worker.CloseIdleConnections()
					}
					// 普通推进只Observe同一原命令；Start丢回复时root未收到仍
					// 保持Unconfirmed，只有受信显式原重放才可首次派发。
					progress, err = worker.Advance(ctx)
					if mode == "start-reply-lost" {
						require.Error(t, err)
						require.Equal(t, journal.Unconfirmed, guestPhase(t, store).Phase)
						progress, err = worker.ReplayResourceOriginal(ctx, req.Key)
					}
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
					require.Equal(t, journal.Settled, guestPhase(t, store).Phase)
					root.mu.Lock()
					require.Equal(t, 1, root.posts, "普通轮询不重发资源POST")
					require.Equal(t, 1, root.observes)
					root.mu.Unlock()
				}
				cancel()
				return ctx.Err()
			})
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestGuestReadyConcurrentOwnerCallsDispatchOnlyOneCompleteCommand(t *testing.T) {
	api, store, coord := newAPI(t)
	root, resourceConfig := guestRoot(t, store)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceWorker(ctx, resourceConfig)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		req := pgRequest(t, session.Actor())
		p, shared := preparedGuest(t, req, root.view.Identity)
		var reads atomic.Int32
		bothReady := make(chan struct{})
		config := pgConfig(t, req, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/dsg/v1/budget/shared", r.URL.Path)
			n := reads.Add(1)
			if n == 2 {
				close(bothReady)
			}
			if n <= 2 {
				select {
				case <-bothReady:
				case <-r.Context().Done():
					return
				}
			}
			pgReply(w, shared)
		})
		worker, err := session.NewPGWorker(ctx, config, resources)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		// 每个PG客户端的观察池按生产设计串行；使用同owner下两个实际
		// worker覆盖跨对象并发，不在单连接池内制造测试屏障死锁。
		second, err := session.NewPGWorker(ctx, config, resources)
		require.NoError(t, err)
		defer second.CloseIdleConnections()
		api.mu.Lock()
		api.vm.Status.MemorySize = resource.NewQuantity(int64(req.TargetVMMemoryBytes), resource.BinarySI)
		api.mu.Unlock()
		beginGuest(t, store, req, p)
		results := make(chan Tick, 2)
		for _, candidate := range []*PGWorker{worker, second} {
			go func(candidate *PGWorker) {
				progress, err := candidate.Advance(ctx)
				results <- Tick{Progress: progress, Err: err}
			}(candidate)
		}
		completed := 0
		for i := 0; i < 2; i++ {
			r := <-results
			if r.Err == nil && r.Progress == pgexec.Recorded {
				completed++
			}
		}
		require.Equal(t, 1, completed, "同owner并发也只有CAS票据持有者可派发并落账")
		root.mu.Lock()
		require.Equal(t, 1, root.posts)
		root.mu.Unlock()
		require.Equal(t, journal.Settled, guestPhase(t, store).Phase)
		cancel()
		return ctx.Err()
	})
	require.ErrorIs(t, err, context.Canceled)
}
