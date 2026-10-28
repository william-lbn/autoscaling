package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
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
	switch r.URL.Path {
	case "/dsg/v1/budget/shared":
		pgReply(w, p.shared())
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
		pgReply(w, response)
		return
	case "/dsg/v1/admission", "/dsg/v1/admission/status":
		var request control.AdmissionRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		query := r.URL.Path == "/dsg/v1/admission/status"
		if !query {
			s, err := p.store.Load(r.Context())
			require.NoError(t, err)
			if s.State().InitialHeld != nil && s.State().InitialHeld.Admission != nil && s.State().Active == nil {
				a := s.State().InitialHeld.Admission
				require.Equal(t, journal.Unconfirmed, a.Phase)
				var c budget.InitialAdmissionCommand
				require.NoError(t, json.Unmarshal(a.Command, &c))
				require.Equal(t, c.Request, request, "真实首次setter之前必须有原完整WAL")
			} else {
				require.Equal(t, journal.Unconfirmed, budgetStep(t, p.store, journal.ResumeAdmission).Phase)
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
			if p.gate != nil && *p.gate != request {
				require.Equal(t, p.gate.Revision+1, request.Revision)
			}
			original := request
			p.gate = &original
		}
		s := p.current().Observed
		lose := !query && p.loseAfter
		if !query {
			p.loseAfter = false
		}
		p.mu.Unlock()
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

func initialAndGrowQualification(t *testing.T, ctx context.Context, client *kubefake.Clientset, root *guestRootFixture, p budget.Prepared) budget.QualificationSource {
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
	require.NoError(t, q.Validate())
	raw, err := json.Marshal(q)
	require.NoError(t, err)
	qref := vmReservation(t, client, "initial-and-grow-qualified-platform", budgetclaims.QualificationKind, q)
	return budget.QualificationSource{Claim: qref, SHA256: vmguard.SourceDigest(raw)}
}

func TestProductionInitialServiceThenGrowUsesOriginalSourcesAndUnknownRecovery(t *testing.T) {
	api, store, coord := newAPI(t)
	client := vmCore(t)
	api.mu.Lock()
	mode := vmv1.CpuScalingModeSysfs
	api.vm.Spec.CpuScalingMode = &mode
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceWorker(ctx, resourceConfig)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		request := pgRequest(t, session.Actor())
		p, before := preparedGuest(t, request, root.view.Identity)
		p.Limits.PidsMaxTasks = 128
		p.Limits.SystemMemoryMaxBytes = 16 << 20
		after := before
		after.Observed = pgResponse(t, request).Observed
		after.CurrentMainAndArraysUpperBytes = after.StartupMainUpperBytes
		for _, a := range after.Observed.Arrays {
			after.CurrentMainAndArraysUpperBytes += a.CommittedUpperBytes
		}
		pg := &initialServicePG{mu: sync.Mutex{}, request: request, before: before, after: after, resized: false, gate: nil, posts: 0, queries: 0, resizePosts: 0, loseBefore: false, loseAfter: false, store: store}
		config := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) { pg.serve(t, w, r) }, func() control.Snapshot { return pg.shared().Observed })
		worker, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		require.NoError(t, runner.Tick(ctx))
		source := initialAndGrowQualification(t, ctx, client, root, p)
		rebuild := func() *PGWorker {
			worker.CloseIdleConnections()
			next, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
			require.NoError(t, err)
			t.Cleanup(next.CloseIdleConnections)
			return next
		}
		worker = rebuild()
		worker = verifyInitialServiceReplies(t, ctx, api, client, store, session, resources, worker, config, source, pg, root)
		// 后继完整Grow使用实际Prepare/Host/Guest/VM/PG/预算/准入，首次永久
		// 头只能在完整Finalize的同CAS被替换；不注入领域Completed。
		s, err := store.Load(ctx)
		require.NoError(t, err)
		_, _, err = store.Begin(ctx, s, session.Actor(), request, 1)
		require.NoError(t, err)
		progress, err := worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		var prepared budget.Prepared
		require.NoError(t, json.Unmarshal(budgetStep(t, store, journal.PrepareBudget).Observation.Data, &prepared))
		publishVMHost(t, ctx, client, coord, store, session)
		for _, action := range []journal.Action{journal.ReserveHost, journal.ResizeVM} {
			progress, err = worker.Advance(ctx)
			require.NoError(t, err, string(action))
			require.Equal(t, pgexec.Recorded, progress)
		}
		require.NoError(t, runner.Tick(ctx))
		progress, err = worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		for _, action := range []journal.Action{journal.GuestReady, journal.ResizePG, journal.FinalizeBudget, journal.ResumeAdmission} {
			s, err = store.Load(ctx)
			require.NoError(t, err)
			if action == journal.FinalizeBudget {
				require.NotNil(t, s.State().InitialHeld)
			}
			progress, err = worker.Advance(ctx)
			require.NoError(t, err, string(action))
			require.Equal(t, pgexec.Recorded, progress)
		}
		s, err = store.Load(ctx)
		require.NoError(t, err)
		require.Nil(t, s.State().InitialHeld)
		require.NotNil(t, s.State().BudgetHeld)
		require.EqualValues(t, 9, s.State().ProtocolVersion)
		posts, _, resizes := pg.counts()
		require.Equal(t, 3, posts, "首次setter发送前/后各丢一次，正常后继只再开放一次")
		require.Equal(t, 1, resizes)
		qmp.mu.Lock()
		require.Equal(t, 1, qmp.writes)
		qmp.mu.Unlock()
		finishTrustedPrepareHost(t, ctx, api, client, store, session, worker, prepared)
		return nil
	})
	require.NoError(t, err)
}

// 同一首次启动串联预算保存/Start/原setter前后/Record丢回复及SDK重建；
// 删除/换UID/Root漂移仅作负向故障，恢复原来源后继续，不创建替代资格。
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
	qualification, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, source.Claim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, source.Claim.Name))
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, resourceguard.ErrUnknown, "先查原NotFound，不能让丢资格掩盖原未知")
	require.NoError(t, client.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), qualification, binding().Namespace))
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
	require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, source.Claim.Name))
	_, err = worker.Advance(ctx)
	require.Error(t, err)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.Unconfirmed, s.State().InitialHeld.Admission.Phase)
	require.NoError(t, client.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), qualification, binding().Namespace))
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
	changed := qualification.DeepCopy()
	changed.UID = "replacement-qualification"
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, binding().Namespace))
	_, err = worker.Advance(ctx)
	require.Error(t, err, "首次已开放也须持续认证原资格")
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), qualification, binding().Namespace))
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Idle, progress)
	return worker
}
