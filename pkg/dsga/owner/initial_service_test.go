package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/api/resource"
	kubefake "k8s.io/client-go/kubernetes/fake"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 仅PG协议peer；管理客户端、原预算生产/SDK/WAL/Runner/Host执行真实代码。
// 当前门禁状态随setter变化且所有接口取同点快照，不能用静态闭门假象掩盖
// 写后预算检查。PG/root/平台及Kube/QMP仍是显式夹具，不外推Linux物理证明。
type initialServicePG struct {
	mu                          sync.Mutex
	request                     control.Request
	before, after               control.SharedCostResponse
	resized                     bool
	gate                        *control.AdmissionRequest
	posts, queries, resizePosts int
	loseBefore, loseAfter       bool
	store                       *journal.Store
	// 新PG已派发接管窗口单独启用；旧首次/取消矩阵保持原响应行为。
	loseResize    bool
	resizeReceipt *control.ControlResponse
	resizeQueries int
	// 只在新增安全组合中安排原开放已发送、审计前来源漂移。
	// hook在peer锁外运行，不伪造生产WAL/关闭结果。
	afterOpen func()
	// 仅新容量竞态组合在原关闭WAL确认后、setter到达前提交原Resize。
	// 由peer锁外调用；生产代码仍必须识别Stale并写独立接续WAL。
	beforeClose    func()
	leaseDeadline  control.Uint64
	leaseRenewals  int
	leaseReject    bool
	leaseExpired   bool
	leaseTimeoutMS int
	rearmFixture   *serviceRearmPG
	reopenFixture  *serviceReopenPG
}

func (p *initialServicePG) shared() control.SharedCostResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current()
}

func (p *initialServicePG) current() control.SharedCostResponse {
	s := p.before
	if p.resized {
		s = p.after
	}
	if p.gate != nil {
		s.Observed.AdmissionOpen, s.Observed.AdmissionRevision = p.gate.Open, p.gate.Revision
	}
	if p.leaseExpired {
		s.Observed.AdmissionOpen = false
	}
	return s
}

func (p *initialServicePG) faults(before, after bool) {
	p.mu.Lock()
	p.loseBefore, p.loseAfter = before, after
	p.mu.Unlock()
}

func (p *initialServicePG) counts() (int, int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.posts, p.queries, p.resizePosts
}

func (p *initialServicePG) serve(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	if p.serveRearmFixture(t, w, r) {
		return
	}
	if p.serveReopenFixture(t, w, r) {
		return
	}
	switch r.URL.Path {
	case "/dsg/v1/admission/lease":
		var request control.AdmissionLeaseRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.leaseReject {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if p.leaseDeadline == 0 {
			p.leaseDeadline = 1000000000
		}
		s := p.current().Observed
		renewed := !p.leaseExpired && request.Action == "renew" && request.ExpectedDeadlineNS == p.leaseDeadline && request.ExpectedCapacityGeneration == s.CapacityGeneration && request.ExpectedAdmissionRevision == s.AdmissionRevision && request.IssuerEpoch == s.IssuerFloorEpoch
		if renewed {
			p.leaseDeadline++
			p.leaseRenewals++
		}
		timeout := p.leaseTimeoutMS
		if timeout == 0 {
			timeout = 30000
		}
		pgReply(w, control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Expired: p.leaseExpired, Request: request, Armed: true, Renewed: renewed, DeadlineNS: p.leaseDeadline, LeaseEpoch: s.IssuerFloorEpoch, TimeoutMS: timeout, Observed: s})
		return
	case "/dsg/v1/budget/shared":
		pgReply(w, p.shared())
		return
	case "/dsg/v1/admission/continue-close", "/dsg/v1/admission/continue-close/status":
		p.serveClosureContinuation(t, w, r)
		return
	case "/dsg/v1/admission/close-original", "/dsg/v1/admission/close-original/status":
		p.serveInitialClosure(t, w, r)
		return
	case "/dsg/v1/resize":
		var request control.Request
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.Equal(t, p.request, request)
		require.Equal(t, journal.Unconfirmed, budgetStep(t, p.store, journal.ResizePG).Phase)
		response := pgResponse(t, request)
		p.mu.Lock()
		p.resized = true
		p.resizePosts++
		s := p.current().Observed
		p.mu.Unlock()
		response.Observed = s
		*response.Receipt.Result = s
		p.mu.Lock()
		p.resizeReceipt = &response
		lose := p.loseResize
		p.loseResize = false
		p.mu.Unlock()
		if lose {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		pgReply(w, response)
		return
	case "/dsg/v1/resize/status", "/dsg/v1/resize/cancel":
		var query control.Query
		require.NoError(t, json.NewDecoder(r.Body).Decode(&query))
		p.mu.Lock()
		defer p.mu.Unlock()
		require.Equal(t, p.request.Key, query.Key)
		require.NotNil(t, p.resizeReceipt, "查询不能凭当前数值补造原PG终态")
		response := *p.resizeReceipt
		response.Observed = p.current().Observed
		response.Receipt.Disposition = control.ReceiptReplayed
		if r.URL.Path == "/dsg/v1/resize/cancel" {
			response.Receipt.Disposition = control.ReceiptTooLate
		}
		p.resizeQueries++
		pgReply(w, response)
		return
	case "/dsg/v1/admission", "/dsg/v1/admission/status":
		var request control.AdmissionRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		query := r.URL.Path == "/dsg/v1/admission/status"
		if !query {
			s, err := p.store.Load(r.Context())
			require.NoError(t, err)
			if c := s.State().ServiceClosure; c != nil {
				require.Equal(t, journal.Unconfirmed, c.Phase)
				require.Equal(t, c.Request, request, "异常关闭setter之前须有完整原关闭WAL")
			} else if s.State().InitialHeld != nil && s.State().InitialHeld.Admission != nil && s.State().Active == nil {
				a := s.State().InitialHeld.Admission
				require.Equal(t, journal.Unconfirmed, a.Phase)
				var c budget.InitialAdmissionCommand
				require.NoError(t, json.Unmarshal(a.Command, &c))
				require.Equal(t, c.Request, request, "真实首次setter之前必须有原完整WAL")
			} else if active := s.State().Active; active != nil && active.Cleanup != nil {
				boundary, err := s.State().CleanupServiceForClosure()
				require.NoError(t, err)
				require.Equal(t, journal.Unconfirmed, active.Cleanup.Phase)
				require.Equal(t, &request, boundary.Admission, "原Cleanup独立开放必须先于setter写前持久")
			} else if s.State().Active != nil && s.State().Active.Request.Kind == control.Shrink && !request.Open {
				step := budgetStep(t, p.store, journal.Quiesce)
				require.Equal(t, journal.Unconfirmed, step.Phase)
				var command budget.QuiesceCommand
				require.NoError(t, json.Unmarshal(step.Command, &command))
				require.Equal(t, command.Close, request, "原Quiesce完整关闭必须先于setter写前持久")
			} else {
				require.Equal(t, journal.Unconfirmed, budgetStep(t, p.store, journal.ResumeAdmission).Phase)
			}
		}
		if !query && !request.Open {
			p.mu.Lock()
			hook := p.beforeClose
			p.beforeClose = nil
			p.mu.Unlock()
			if hook != nil {
				hook()
			}
		}
		p.mu.Lock()
		disposition := control.AdmissionApplied
		if query {
			p.queries++
			disposition = control.AdmissionNotFound
			if p.gate != nil && *p.gate == request {
				disposition = control.AdmissionReplayed
			} else if p.gate != nil && p.gate.Revision > request.Revision {
				disposition = control.AdmissionResultExpired
			}
		} else {
			p.posts++
			if p.loseBefore {
				p.loseBefore = false
				p.mu.Unlock()
				w.WriteHeader(503)
				return
			}
			if request.ExpectedCapacityGeneration != p.current().Observed.CapacityGeneration && (p.gate == nil || *p.gate != request) {
				disposition = control.AdmissionStale
			} else {
				if p.gate != nil && *p.gate != request {
					require.Equal(t, p.gate.Revision+1, request.Revision)
				}
				original := request
				p.gate = &original
			}
		}
		s := p.current().Observed
		lose := !query && p.loseAfter
		if !query {
			p.loseAfter = false
		}
		p.mu.Unlock()
		if !query && request.Open && p.afterOpen != nil {
			p.afterOpen()
		}
		if lose {
			w.WriteHeader(503)
			return
		}
		hash, err := request.ContextSHA256()
		require.NoError(t, err)
		pgReply(w, control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, Disposition: disposition, ContextSHA256: hash, Observed: s})
		return
	default:
		t.Errorf("unexpected initial PG route %s", r.URL.Path)
		w.WriteHeader(500)
	}
}

func initialAndGrowQualification(t *testing.T, ctx context.Context, client *kubefake.Clientset, root *guestRootFixture, p budget.Prepared, extra ...budget.QualifiedTarget) budget.QualificationSource {
	t.Helper()
	birth, ref, _, err := vmguard.ReadPublished(ctx, client.CoreV1(), binding(), nil)
	require.NoError(t, err)
	identityRaw, err := json.Marshal(birth.Identity)
	require.NoError(t, err)
	root.mu.Lock()
	bootstrap := root.view.Bootstrap
	root.mu.Unlock()
	bootstrapRaw, err := json.Marshal(bootstrap)
	require.NoError(t, err)
	q := budget.PlatformQualification{ProtocolVersion: 1, Identity: p.Identity, Incarnation: binding().Incarnation, ReportID: "explicit-initial-and-grow-platform-fixture", RootBootstrapSHA256: vmguard.SourceDigest(bootstrapRaw), RunnerBirth: ref, RunnerIdentitySHA256: vmguard.SourceDigest(identityRaw), RunnerPodName: p.RunnerPodName, RunnerPodUID: birth.Identity.RunnerPodUID, RunnerRestartCount: 0, NodeName: "node-one", NodeUID: birth.Identity.NodeUID, HoldUntilExplicitSettlement: true, Targets: []budget.QualifiedTarget{
		{VMMemoryBytes: 512 << 20, GuestUsableMemoryBytes: 480 << 20, Limits: bootstrap.Limits, SystemOutsidePGBytes: 24 << 20, TransitionBytes: 4 << 20, SafetyBytes: 4 << 20},
		{VMMemoryBytes: p.Request.TargetVMMemoryBytes, GuestUsableMemoryBytes: p.GuestUsableMemoryBytes, Limits: p.Limits, SystemOutsidePGBytes: 24 << 20, TransitionBytes: 4 << 20, SafetyBytes: 4 << 20},
	}}
	if len(extra) > 0 {
		// 仅新自动缩VM组合显式加入中间规格；这些数字仍是独立平台peer
		// 输入，不按占用/比例在运行时补资格，也不改变旧组合的目标集合。
		q.Targets = append(q.Targets, extra...)
		sort.Slice(q.Targets, func(i, j int) bool { return q.Targets[i].VMMemoryBytes < q.Targets[j].VMMemoryBytes })
	}
	require.NoError(t, q.Validate())
	raw, err := json.Marshal(q)
	require.NoError(t, err)
	qref := vmReservation(t, client, "initial-and-grow-qualified-platform", budgetclaims.QualificationKind, q)
	return budget.QualificationSource{Claim: qref, SHA256: vmguard.SourceDigest(raw)}
}

func TestProductionInitialServiceThenGrowUsesOriginalSourcesAndUnknownRecovery(t *testing.T) {
	productionInitialService(t, "grow")
}

func TestProductionNormalCompletionRechecksSourcesAndRecoversLostFinish(t *testing.T) {
	productionInitialService(t, "auto-finish")
}

func TestProductionInitialHostAndUndispatchedCancelKeepOriginalPlacement(t *testing.T) {
	productionInitialService(t, "host")
}

func TestProductionInitialCancelledGrowRecoversActualBudgetAndSuccessor(t *testing.T) {
	productionInitialService(t, "cancel")
}

// Prepare与Host已有真实持久效果，但VM从未Start。复用后续故障/重建/
// Host结清/完整Grow/下一Prepare组合，不能靠制造NoEffect来走旧恢复路径。
func TestProductionInitialCancelBeforeVMRecoversActualBudgetAndSuccessor(t *testing.T) {
	productionInitialService(t, "cancel-before-vm")
}

// 不是手改Model.Owner：旧Session必须真正退出选主循环，另一Elector经
// 同一Lease UID/RV接管；全部后续领域调用使用其正式创建的新Session。
func TestProductionInitialOwnerTransferCancelsOriginalVMAndCompletesSuccessor(t *testing.T) {
	productionInitialService(t, "owner-transfer")
}

func TestProductionInitialOwnerTransferObservesLostVMReplyAndRecoversActualCapacity(t *testing.T) {
	productionInitialService(t, "owner-transfer-after-vm")
}

func TestProductionInitialOwnerTransferObservesLostPGReplyAndSettlesActualBudget(t *testing.T) {
	productionInitialService(t, "owner-transfer-after-pg")
}

func TestProductionInitialOwnerTransferObservesLostGuestReplyWithoutSendingPG(t *testing.T) {
	productionInitialService(t, "owner-transfer-after-guest")
}

func TestProductionInitialOwnerTransferCancelsUnreceivedGuestAndSettlesActualBudget(t *testing.T) {
	productionInitialService(t, "owner-transfer-before-guest-delivery")
}

func productionInitialService(t *testing.T, mode string) {
	t.Helper()
	// 同一完整自动组合显式追加AS来源；不改变旧夹具事实或以peer冒充Linux硬限。
	finiteAddressSpace := mode == "automatic-policy-as"
	if finiteAddressSpace {
		mode = "automatic-policy"
	}
	expiredRetry := strings.HasPrefix(mode, "service-retry-expired-")
	automaticMode := mode == "service-retry-expired-automatic" || mode == "automatic-policy" || mode == "service-lifecycle-first-unknown-automatic" || mode == "service-retry-first-unknown-automatic"
	ownerTransfer := strings.HasPrefix(mode, "owner-transfer")
	closureTransfer := strings.HasPrefix(mode, "closure-transfer-") || mode == "initial-safety-transfer" || mode == "resume-safety-transfer" || mode == "resume-safety-planned-transfer" || mode == "finalize-safety-transfer" || mode == "active-safety-vm-unknown-transfer" || mode == "active-safety-pg-transfer" || mode == "cleanup-safety-transfer"
	pgSafety := strings.HasPrefix(mode, "active-safety-pg-")
	pgTakeover := mode == "owner-transfer-after-pg"
	unreceivedGuest := mode == "owner-transfer-before-guest-delivery"
	guestTakeover := mode == "owner-transfer-after-guest" || unreceivedGuest
	cancelled := mode == "cancel" || mode == "cancel-before-vm" || ownerTransfer && !pgTakeover && !guestTakeover
	api, store, coord := newAPI(t)
	client := vmCore(t)
	api.mu.Lock()
	cpuMode := vmv1.CpuScalingModeSysfs
	api.vm.Spec.CpuScalingMode = &cpuMode
	api.vm.Spec.ServiceAccountName = "dsga-runner-test"
	api.vm.Spec.Guest.MemorySlotSize = *resource.NewQuantity(256<<20, resource.BinarySI)
	api.vm.Spec.Guest.MemorySlots.Min = 2
	api.vm.Spec.Guest.MemorySlots.Max = 8
	api.vm.Spec.Guest.MemorySlots.Use = 2
	api.vm.Status.Node = "node-one"
	api.vm.Status.MemorySize = resource.NewQuantity(512<<20, resource.BinarySI)
	api.mu.Unlock()
	dir, err := os.MkdirTemp("/private/tmp", "dsga-first-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	runner, err := vmguard.Initialize(context.Background(), store, client.CoreV1(), coord, vmguard.Config{StateDir: dir, PodName: "runner", PodUID: "runner-uid", NodeName: "node-one", MinimumBytes: 512 << 20, MaximumBytes: 2 << 30})
	require.NoError(t, err)
	root, resourceConfig := guestRoot(t, store)
	qualifiedRootFixture(root)
	root.mu.Lock()
	root.view.Actual.GuestMemTotalBytes = 480 << 20
	if finiteAddressSpace {
		root.view.Bootstrap.Limits.AddressSpaceMaxBytes = 512 << 20
		root.view.Birth.AddressSpaceMaxBytes = 512 << 20
		root.view.Actual.AddressSpace = &resourceguard.AddressSpaceEvidence{Processes: 5, MaxHardBytes: 512 << 20, MaxVirtualBytes: 128 << 20, Irreversible: true}
	}
	if strings.HasPrefix(mode, "active-safety-quiesce-") {
		// 新Quiesce组合保留80MiB原PG成本，初始normal64MiB不足以
		// 形成合法Prepare。首次绑定前即固定128MiB，原bootstrap、
		// 资格、永久头和真实读取保持一致；不在运行中补大限额。
		root.view.Bootstrap.Limits.MemoryMaxBytes = 128 << 20
		root.view.Actual.MemoryMaxBytes = 128 << 20
	}
	root.mu.Unlock()
	args := runner.QMPArguments()
	qmp := &vmQMPPeer{mu: sync.Mutex{}, uuid: args[1], minimum: 512 << 20, maximum: 2 << 30, actual: 512 << 20, writes: 0, lose: false, onResize: func(actual uint64) {
		require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizeVM).Phase)
		root.mu.Lock()
		root.view.Actual.GuestMemTotalBytes = control.Uint64(actual - (256 << 20))
		root.mu.Unlock()
		api.mu.Lock()
		api.vm.Status.MemorySize = resource.NewQuantity(int64(actual), resource.BinarySI)
		api.vmVersion++
		api.vm.ResourceVersion = fmt.Sprintf("initial-grow-rv-%d", api.vmVersion)
		api.mu.Unlock()
	}}
	vmPeer(t, filepath.Join(dir, "qmp.sock"), qmp)
	elector, err := New(coord, store, DefaultOptions())
	require.NoError(t, err)
	deadline := 2 * time.Minute
	if cancelled {
		// 本组合串联首次服务、取消回收、后继Grow和下一次Prepare，
		// -race下已实测到最后Prepare才触及原两分钟总期限；扩大的是
		// 整个测试的上限，各生产请求/租约/单轮deadline均保持原值。
		deadline = 3 * time.Minute
	}
	if automaticMode || strings.HasPrefix(mode, "service-lifecycle-") || strings.HasPrefix(mode, "service-retry-") {
		// 在途永久头/Prepare/Host的正式来源预审增加了受Kube客户端
		// 限流的只读请求；完整Grow→Shrink曾在原三分钟总期限耗尽。
		// 仅扩大组合测试总期限，保留生产限流、请求期限和选主续租。
		deadline = 5 * time.Minute
	}
	if strings.HasPrefix(mode, "service-retry-") {
		// 本批组合增加原开放→异常再关闭→再次来源审计和SDK重建，
		// 仅扩大测试总期限；生产请求、Kube限流和选主期限不变。
		deadline = 7 * time.Minute
	}
	if ownerTransfer || closureTransfer || expiredRetry {
		// 实测包含原30秒Lease失效等待的完整组合为175.53秒，
		// 总期限留出race调度余量；不改各生产请求或30/20/2秒租约。
		deadline = 4 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	run := elector.Run
	var transfer *businessOwnerTransfer
	if ownerTransfer || closureTransfer || expiredRetry {
		transfer = newBusinessOwnerTransfer(t, api, coord, store, resourceConfig)
		transfer.effectBeforeTakeover = mode == "owner-transfer-after-vm" || pgTakeover || guestTakeover
		if pgTakeover {
			transfer.takeoverAction = journal.ResizePG
		} else if guestTakeover {
			transfer.takeoverAction = journal.GuestReady
		}
		run = transfer.run
	}
	err = run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceWorker(ctx, resourceConfig)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		request := pgRequest(t, session.Actor())
		if automaticMode {
			request.TargetVMMemoryBytes = 2 << 30
		}
		p, before := preparedGuest(t, request, root.view.Identity)
		p.Limits.PidsMaxTasks = 128
		p.Limits.AddressSpaceMaxBytes = root.view.Bootstrap.Limits.AddressSpaceMaxBytes
		p.Limits.SystemMemoryMaxBytes = 16 << 20
		after := before
		after.Observed = pgResponse(t, request).Observed
		after.CurrentMainAndArraysUpperBytes = after.StartupMainUpperBytes
		for _, a := range after.Observed.Arrays {
			after.CurrentMainAndArraysUpperBytes += a.CommittedUpperBytes
		}
		pg := &initialServicePG{mu: sync.Mutex{}, request: request, before: before, after: after, resized: false, gate: nil, posts: 0, queries: 0, resizePosts: 0, loseBefore: false, loseAfter: false, store: store, loseResize: false, resizeReceipt: nil, resizeQueries: 0, afterOpen: nil, beforeClose: nil, leaseDeadline: 0, leaseRenewals: 0, leaseReject: false, leaseExpired: false, leaseTimeoutMS: 0, rearmFixture: nil, reopenFixture: nil}
		var nativeFence http.HandlerFunc
		if transfer != nil {
			nativeFence = pg.ownerFence(t)
		}
		config := pgConfigWithFence(t, request, func(w http.ResponseWriter, r *http.Request) {
			if automaticMode && r.URL.Path == "/dsg/v1/work/state" {
				serveAutomaticWork(w, pg)
				return
			}
			pg.serve(t, w, r)
		}, nativeFence, func() control.Snapshot { return pg.shared().Observed })
		worker, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		require.NoError(t, runner.Tick(ctx))
		var extra []budget.QualifiedTarget
		if automaticMode {
			extra = []budget.QualifiedTarget{{VMMemoryBytes: 1 << 30, GuestUsableMemoryBytes: 768 << 20, Limits: p.Limits, SystemOutsidePGBytes: 24 << 20, TransitionBytes: 4 << 20, SafetyBytes: 4 << 20}}
		}
		source := initialAndGrowQualification(t, ctx, client, root, p, extra...)
		rebuild := func() *PGWorker {
			worker.CloseIdleConnections()
			next, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
			require.NoError(t, err)
			t.Cleanup(next.CloseIdleConnections)
			return next
		}
		worker = rebuild()
		retryMode := strings.HasPrefix(mode, "service-retry-")
		lifecycleMode := strings.HasPrefix(mode, "service-lifecycle-") || retryMode
		lifecyclePosts := 0
		if lifecycleMode {
			progress, err := worker.Advance(ctx)
			require.NoError(t, err)
			require.Equal(t, pgexec.Recorded, progress)
			unknown := strings.Contains(mode, "first-unknown")
			if unknown {
				pg.faults(false, true)
			}
			_, err = worker.Advance(ctx)
			if unknown {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			predecessor := "normal"
			if unknown {
				predecessor = "first-unknown"
			}
			if retryMode {
				verifyProductionServiceReopen(t, ctx, "source-after-unknown-setter", api, store, session, resources, worker, config, client, source, pg, root)
				if expiredRetry {
					session, resources, worker = verifyProductionReopenLeaseRecovery(t, ctx, strings.TrimPrefix(mode, "service-retry-expired-"), transfer, session, resources, worker, config, client, source, pg, root, runner)
				} else {
					worker = verifyProductionServiceReopenRetry(t, ctx, strings.TrimPrefix(mode, "service-retry-"), api, store, session, resources, worker, config, client, source, pg, root)
				}
			} else {
				verifyProductionServiceReopen(t, ctx, predecessor, api, store, session, resources, worker, config, client, source, pg, root)
				worker = verifyProductionServiceLifecycle(t, ctx, strings.TrimPrefix(mode, "service-lifecycle-"), api, store, session, resources, worker, config, client, source, pg, root)
			}
			if mode != "service-lifecycle-first-unknown-grow" && !automaticMode {
				return nil
			}
			lifecyclePosts, _, _ = pg.counts()
		}
		if strings.HasPrefix(mode, "service-reopen-") {
			progress, err := worker.Advance(ctx)
			require.NoError(t, err)
			require.Equal(t, pgexec.Recorded, progress)
			if mode == "service-reopen-first-unknown" {
				pg.faults(false, true)
			}
			_, err = worker.Advance(ctx)
			if mode == "service-reopen-first-unknown" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			verifyProductionServiceReopen(t, ctx, strings.TrimPrefix(mode, "service-reopen-"), api, store, session, resources, worker, config, client, source, pg, root)
			return nil
		}
		if strings.HasPrefix(mode, "native-lease-") {
			for range 2 {
				progress, err := worker.Advance(ctx)
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
			}
			verifyNativeAdmissionLease(t, ctx, mode, api, store, worker, pg)
			return nil
		}
		if strings.HasPrefix(mode, "initial-safety-") {
			verifyInitialServiceSafety(t, ctx, mode, api, store, client, session, resources, worker, config, source, pg, root, transfer)
			return nil
		}
		if strings.HasPrefix(mode, "closure-transfer-") {
			for range 2 {
				progress, err := worker.Advance(ctx)
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
			}
			verifyServiceClosureContinuation(t, ctx, mode, transfer, client, session, resources, worker, config, source, pg, root, runner)
			return nil
		}
		if strings.HasPrefix(mode, "service-safety-") {
			for range 2 {
				progress, err := worker.Advance(ctx)
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
			}
			verifyIdleServiceClosure(t, ctx, mode, api, store, client, session, resources, worker, config, source, pg, root)
			return nil
		}
		if mode == "host" {
			for range 2 {
				progress, err := worker.Advance(ctx)
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
			}
			verifyInitialHostAndCancel(t, ctx, api, client, coord, store, session, worker, source, pg, request)
			return nil
		}
		var successorHost *hostexec.Publisher
		if lifecycleMode {
			// 已由完整生产恢复链完成首次服务；继续原预算Grow，无重复初开。
		} else if pgTakeover || guestTakeover || mode == "auto-finish" || automaticMode || strings.HasPrefix(mode, "resume-safety-") || strings.HasPrefix(mode, "finalize-safety-") || strings.HasPrefix(mode, "active-safety-") || strings.HasPrefix(mode, "cleanup-safety-") {
			// 新窗口只复用必要的首次成功前驱，不重复既有首次丢回复矩阵。
			for range 2 {
				progress, err := worker.Advance(ctx)
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
			}
		} else if cancelled {
			for range 2 {
				progress, err := worker.Advance(ctx)
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
			}
			worker, request, successorHost, session, resources = verifyInitialCancelledGrow(t, ctx, api, client, coord, store, session, worker, resources, config, source, runner, qmp, request, mode == "cancel-before-vm", transfer)
			pg.mu.Lock()
			pg.request = request
			pg.after.Observed = pgResponse(t, request).Observed
			pg.mu.Unlock()
		} else {
			worker = verifyInitialServiceReplies(t, ctx, api, client, store, session, resources, worker, config, source, pg, root)
		}
		if strings.HasPrefix(mode, "active-safety-quiesce-") {
			// 只缩PG、VM保持原最小规格，使用已有512MiB完整资格及真实
			// 首次预算。Prepare/Quiesce仍走生产者，不注入领域Completed。
			request.Kind, request.TargetBuffers, request.TargetVMMemoryBytes = control.Shrink, 256, 512<<20
			pg.mu.Lock()
			pg.request = request
			pg.mu.Unlock()
		}
		// 后继完整Grow使用实际Prepare/Host/Guest/VM/PG/预算/准入，首次永久
		// 头只能在完整Finalize的同CAS被替换；不注入领域Completed。
		s, err := store.Load(ctx)
		require.NoError(t, err)
		if automaticMode {
			verifyProductionAutomaticBegin(t, ctx, api, client, store, worker, source, pg, root)
		} else {
			_, _, err = store.Begin(ctx, s, session.Actor(), request, 1)
			require.NoError(t, err)
		}
		if strings.HasPrefix(mode, "active-safety-prepare-") {
			verifyActiveServiceSafety(t, ctx, mode, api, store, client, session, resources, worker, config, source, pg, root, qmp, transfer)
			return nil
		}
		progress, err := worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		if strings.HasPrefix(mode, "active-safety-quiesce-") {
			unreceived := strings.Contains(mode, "unreceived")
			pg.faults(unreceived, !unreceived)
			_, err = worker.Advance(ctx)
			require.Error(t, err, "保留原Quiesce在写前已确认、native回复未知的边界")
			require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.Quiesce).Phase)
			verifyActiveServiceSafety(t, ctx, mode, api, store, client, session, resources, worker, config, source, pg, root, qmp, transfer)
			return nil
		}
		var prepared budget.Prepared
		require.NoError(t, json.Unmarshal(budgetStep(t, store, journal.PrepareBudget).Observation.Data, &prepared))
		if successorHost != nil {
			// 取消到后继操作复用同一真实Host owner/Lease/Node账本，
			// 不重复初始化夹具或重建同名Lease来掩盖生命周期。
			_, err = successorHost.Reconcile(ctx)
			require.NoError(t, err)
		} else {
			publishVMHost(t, ctx, client, coord, store, session)
			// 完整结案/自动Grow→Shrink会经过多次正式来源预审，race运行
			// 可能超过静态夹具Lease的60秒寿命。用原UID/holder的真实
			// 选主续租维持Host，不放宽生产过期校验，也不重建同名来源。
			if pgTakeover || guestTakeover || mode == "auto-finish" || automaticMode || pgSafety || strings.HasPrefix(mode, "cleanup-safety-") {
				keepPGTakeoverHostOwner(t, ctx, client)
			}
		}
		for _, action := range []journal.Action{journal.ReserveHost, journal.ResizeVM} {
			progress, err = worker.Advance(ctx)
			require.NoError(t, err, string(action))
			require.Equal(t, pgexec.Recorded, progress)
		}
		if strings.HasPrefix(mode, "active-safety-vm-unknown-") {
			verifyActiveServiceSafety(t, ctx, mode, api, store, client, session, resources, worker, config, source, pg, root, qmp, transfer)
			return nil
		}
		require.NoError(t, runner.Tick(ctx))
		progress, err = worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		for _, action := range []journal.Action{journal.GuestReady, journal.ResizePG, journal.FinalizeBudget, journal.ResumeAdmission} {
			if action == journal.ResizePG && strings.HasPrefix(mode, "active-safety-guest-complete-") {
				verifyActiveServiceSafety(t, ctx, mode, api, store, client, session, resources, worker, config, source, pg, root, qmp, transfer)
				return nil
			}
			if action == journal.FinalizeBudget && strings.HasPrefix(mode, "finalize-safety-") {
				verifyPendingFinalizationSafety(t, ctx, mode, api, store, client, session, resources, worker, config, source, pg, root, qmp, transfer)
				return nil
			}
			if action == journal.ResumeAdmission && strings.HasPrefix(mode, "resume-safety-") {
				verifyResumeServiceSafety(t, ctx, mode, api, store, client, session, resources, worker, config, source, pg, root, qmp, transfer)
				return nil
			}
			if action == journal.ResumeAdmission && strings.HasPrefix(mode, "cleanup-safety-") {
				verifyCleanupServiceSafety(t, ctx, strings.TrimPrefix(mode, "cleanup-safety-"), api, store, client, session, resources, worker, config, source, pg, root, qmp, transfer)
				return nil
			}
			s, err = store.Load(ctx)
			require.NoError(t, err)
			if action == journal.FinalizeBudget {
				if cancelled {
					require.NotNil(t, s.State().CapacityHeld)
				} else {
					require.NotNil(t, s.State().InitialHeld)
				}
			}
			if (cancelled || guestTakeover) && action == journal.GuestReady && !unreceivedGuest {
				// 后继操作再覆盖一次真实root setter丢回复；外层WAL和
				// 资源SDK必须保留未知。不能要求本机响应总在1秒内，
				// 也不能把失败改成成功；恢复只能查询同一原命令。
				root.mu.Lock()
				root.loseReply = true
				if guestTakeover {
					// 资源效果已经发生，但Root自身完成落账未确认。后继须先
					// 经正式启动gate Observe原命令，不能靠直接建PG绕过它。
					root.afterApply = func() { root.view.Applied, root.view.Held = false, nil }
				}
				root.mu.Unlock()
			}
			if pgTakeover && action == journal.ResizePG {
				pg.mu.Lock()
				pg.loseResize = true
				pg.mu.Unlock()
			}
			if pgSafety && action == journal.ResizePG {
				if strings.Contains(mode, "unreceived") || strings.Contains(mode, "race") {
					api.mu.Lock()
					api.loseJournalReply = true
					api.mu.Unlock()
				} else {
					pg.mu.Lock()
					pg.loseResize = true
					pg.mu.Unlock()
				}
			}
			if unreceivedGuest && action == journal.GuestReady {
				// 完整首次/Prepare/Host/VM均由正式执行器推进；原GuestReady
				// 写前CAS已经持久但回复丢失，Root尚未接收，不能补造NoEffect。
				api.mu.Lock()
				api.loseJournalReply = true
				api.mu.Unlock()
			}
			progress, err = worker.Advance(ctx)
			if pgSafety && action == journal.ResizePG {
				require.Error(t, err, "原PG派发WAL或实际提交回复未知必须保留")
				verifyPGActiveServiceSafety(t, ctx, mode, api, store, client, session, resources, worker, config, source, pg, root, qmp, transfer)
				return nil
			}
			if guestTakeover && action == journal.GuestReady {
				if unreceivedGuest {
					var lost *journal.UnconfirmedWrite
					require.ErrorAs(t, err, &lost)
				} else {
					var lost *resourceguard.ClientError
					require.ErrorAs(t, err, &lost)
					require.True(t, lost.MayHaveExecuted)
				}
				verifyLostGuestTakeoverSettlement(t, ctx, api, client, store, session, resources, worker, config, source, runner, qmp, pg, root, prepared, transfer, unreceivedGuest)
				return nil
			}
			if pgTakeover && action == journal.ResizePG {
				var lost *compute.CallError
				require.ErrorAs(t, err, &lost)
				require.True(t, lost.MayHaveExecuted)
				verifyLostPGTakeoverSettlement(t, ctx, api, client, store, session, resources, worker, config, source, runner, qmp, pg, prepared, transfer)
				return nil
			}
			if cancelled && action == journal.GuestReady {
				var responseErr *resourceguard.ClientError
				require.ErrorAs(t, err, &responseErr)
				require.True(t, responseErr.MayHaveExecuted)
				original := budgetStep(t, store, action)
				require.Equal(t, journal.Unconfirmed, original.Phase)
				restored, restoreErr := session.NewResourceWorker(ctx, resourceConfig)
				require.NoError(t, restoreErr)
				t.Cleanup(restored.CloseIdleConnections)
				worker.CloseIdleConnections()
				worker, restoreErr = session.NewProductionPGWorker(ctx, config, restored, client.CoreV1(), source)
				require.NoError(t, restoreErr)
				t.Cleanup(worker.CloseIdleConnections)
				progress, err = worker.Advance(ctx)
				require.Equal(t, original.CommandSHA256, budgetStep(t, store, action).CommandSHA256)
				root.mu.Lock()
				posts, observes := root.posts, root.observes
				root.mu.Unlock()
				require.Equal(t, 1, posts, "重建后只读原命令，不重发setter")
				require.Equal(t, 1, observes)
			}
			if err != nil && action == journal.GuestReady {
				// 保留严格生产校验；错误时区分夹具实际约束不符与
				// Guest可用容量不足，不通过改断言掩盖来源问题。
				var c resourceguard.Command
				require.NoError(t, json.Unmarshal(budgetStep(t, store, action).Command, &c))
				root.mu.Lock()
				t.Logf("Guest事实：actual=%+v command=%+v validation=%v qualified=%d", root.view.Actual, c, root.view.Actual.ValidateFor(c), prepared.GuestUsableMemoryBytes)
				root.mu.Unlock()
			}
			require.NoError(t, err, string(action))
			require.Equal(t, pgexec.Recorded, progress)
		}
		s, err = store.Load(ctx)
		require.NoError(t, err)
		require.Nil(t, s.State().InitialHeld)
		require.Nil(t, s.State().CapacityHeld)
		require.NotNil(t, s.State().BudgetHeld)
		if lifecycleMode {
			require.EqualValues(t, 21, s.State().ProtocolVersion)
			require.NotNil(t, s.State().ServiceLifecycle)
		} else {
			require.EqualValues(t, 9, s.State().ProtocolVersion)
		}
		posts, _, resizes := pg.counts()
		if lifecycleMode {
			require.Equal(t, lifecyclePosts+1, posts)
		} else if cancelled || mode == "auto-finish" || automaticMode {
			require.Equal(t, 2, posts, "取消时已开放不新增setter，后继正常扩容只再开放一次")
		} else {
			require.Equal(t, 3, posts, "首次setter发送前/后各丢一次，正常后继只再开放一次")
		}
		require.Equal(t, 1, resizes)
		qmp.mu.Lock()
		require.Equal(t, 1, qmp.writes)
		qmp.mu.Unlock()
		if mode == "auto-finish" {
			worker = verifyAutomaticNormalFinish(t, ctx, api, client, store, session, resources, worker, config, source, pg, root)
		}
		if automaticMode {
			t.Log("自动Grow全部领域确认，开始生产结案与自动Shrink")
			verifyProductionAutomaticSuccessor(t, ctx, client, coord, store, session, resources, worker, config, source, pg, runner, qmp)
			return nil
		}
		finishTrustedPrepareHost(t, ctx, api, client, store, session, worker, prepared, mode == "cancel-before-vm")
		return nil
	})
	require.NoError(t, err)
}

// 同一首次启动串联预算保存/Start/原setter前后/Record丢回复及SDK重建；
// 无开放WAL时Root漂移仍只拒绝新开放；已有开放未知后资格丢失会安全
// 关闭，单独由initial_service_safety_test验证，不再在本正常成功链中
// 依赖“资格恢复就可继续旧开放”。本链只验证健康来源下的丢回复恢复。
func verifyInitialServiceReplies(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture) *PGWorker {
	t.Helper()
	rebuild := func() *PGWorker {
		worker.CloseIdleConnections()
		next, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
		require.NoError(t, err)
		t.Cleanup(next.CloseIdleConnections)
		return next
	}
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, err := worker.Advance(ctx)
	var unknown *journal.UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	s, err := store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, s.State().InitialHeld)
	require.Nil(t, s.State().InitialHeld.Admission)
	posts, _, _ := pg.counts()
	require.Zero(t, posts)
	worker = rebuild()
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	_, err = worker.Advance(ctx)
	require.Error(t, err)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()
	posts, _, _ = pg.counts()
	require.Zero(t, posts)
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, err = worker.Advance(ctx)
	require.ErrorAs(t, err, &unknown)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, s.State().InitialHeld.Admission)
	selector := s.State().InitialHeld.Admission.CommandSHA256
	worker = rebuild()
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, resourceguard.ErrUnknown, "健康来源NotFound只保留未知，不自动重发")
	posts, queries, _ := pg.counts()
	require.Zero(t, posts)
	require.Positive(t, queries)
	_, err = worker.ReplayInitialAdmission(ctx, strings.Repeat("0", 64))
	require.Error(t, err)
	pg.faults(true, false)
	// 原重放经过实际PG循环的独立可信通道；普通第一次Tick仍只查原未知。
	// 首次setter前丢回复后终止本次循环，取消子ctx不取消Session或清pending。
	loopCtx, stopLoop := context.WithCancel(ctx)
	selections := make(chan string, 1)
	selections <- selector
	err = worker.Run(loopCtx, PGLoopOptions{PollInterval: 20 * time.Millisecond, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: selections, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: func(Tick) {
		posts, _, _ := pg.counts()
		if posts == 1 {
			stopLoop()
		}
	}})
	stopLoop()
	require.ErrorIs(t, err, context.Canceled)
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, resourceguard.ErrUnknown)
	pg.faults(false, true)
	_, err = worker.ReplayInitialAdmission(ctx, selector)
	require.Error(t, err)
	posts, _, _ = pg.counts()
	require.Equal(t, 2, posts)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.Unconfirmed, s.State().InitialHeld.Admission.Phase)
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, err = worker.Advance(ctx)
	require.ErrorAs(t, err, &unknown)
	worker = rebuild()
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.Settled, s.State().InitialHeld.Admission.Phase)
	require.Nil(t, s.State().Active)
	require.Nil(t, s.State().HighWater)
	pending, _ := worker.client.PendingAdmission()
	require.Nil(t, pending)
	// 已开放后的资格失效由独立service_safety组合验证，其正确结果是
	// 保留关闭；不能把故障混进本正常链再靠恢复资格自动重新开放。
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Idle, progress)
	return worker
}
