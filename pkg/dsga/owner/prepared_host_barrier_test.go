package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	kt "k8s.io/client-go/testing"

	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostscheduler"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 本组为协议E2E：Session/SDK/WAL/typed VM CAS、原Prepare、Publisher、
// 独立恢复/显式重开/Complete/Host退休真实执行。Node走HTTP RFC6902；
// PG/Root/QMP与其余Kube对象是明确peer，不冒充真实K8s/RBAC或物理归还。
func TestPG17EP1InitialSealBeforeReserve(t *testing.T) {
	t.Parallel() // 各自独立HTTP peer、Tracker、临时WAL，竞态互不共享来源。
	t.Log("PG17-E-P1-001: Initial原Prepare Completed，Node Seal先赢，原预算完整恢复")
	productionInitialService(t, "retained-safety-initial-completed-seal-first")
}

func TestPG17EP1InitialReservedHostCreateLate(t *testing.T) {
	t.Parallel() // 各自独立HTTP peer、Tracker、临时WAL，竞态互不共享来源。
	t.Log("PG17-E-P1-002: Initial Reserve先赢/CREATE晚到/Seal回复丢失/前缀退休及精确GC")
	productionInitialService(t, "retained-safety-initial-completed-reserve-first")
}

func TestPG17EP1FinalizedBoundHostAndRealSuccessor(t *testing.T) {
	t.Parallel() // 各自独立HTTP peer、Tracker、临时WAL，竞态互不共享来源。
	t.Log("PG17-E-P1-003: 普通预算原Claim已绑定但外层Host Planned，旧预算恢复及1024到2048真实后继")
	productionInitialService(t, "retained-safety-finalized-completed-bound")
}

type preparedHostCore struct {
	coreclient.CoreV1Interface
	nodes coreclient.NodeInterface
}

func (c preparedHostCore) Nodes() coreclient.NodeInterface { return c.nodes }

// canonicalPreparedNode只有一个Node真状态，即同一Core Tracker。所有Node
// PATCH经HTTP锁内UID/RV RFC6902，Agent真实GET读取同一Tracker；阻塞点
// 在peer锁与fake reactor锁外，避免双视图或死锁假造出串行屏障。
type canonicalPreparedNode struct {
	mu       sync.Mutex
	core     *kubefake.Clientset
	version  int
	before   func(hostguard.Model)
	loseSeal bool
	loseBind bool
	bindKey  control.Key
}

func preparedNodeCore(t *testing.T, client *kubefake.Clientset) (coreclient.CoreV1Interface, *canonicalPreparedNode) {
	t.Helper()
	peer := &canonicalPreparedNode{core: client, version: 0}
	server := httptest.NewServer(http.HandlerFunc(peer.serve))
	t.Cleanup(server.Close)
	config := &rest.Config{Host: server.URL, QPS: 10000, Burst: 10000}
	real, err := kubernetes.NewForConfig(config)
	require.NoError(t, err)
	return preparedHostCore{CoreV1Interface: client.CoreV1(), nodes: real.CoreV1().Nodes()}, peer
}

func (p *canonicalPreparedNode) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path != "/api/v1/nodes/node-one" {
		status(w, http.StatusNotFound)
		return
	}
	var patch []byte
	if r.Method == http.MethodPatch {
		var err error
		patch, err = io.ReadAll(r.Body)
		if err != nil {
			status(w, http.StatusBadRequest)
			return
		}
		var ops []struct {
			Op    string          `json:"op"`
			Path  string          `json:"path"`
			Value json.RawMessage `json:"value"`
		}
		if json.Unmarshal(patch, &ops) != nil {
			status(w, http.StatusBadRequest)
			return
		}
		var next hostguard.Model
		for _, op := range ops {
			if op.Op == "replace" && strings.HasSuffix(op.Path, "dsga-host-ledger-v1") {
				var raw string
				if json.Unmarshal(op.Value, &raw) != nil || json.Unmarshal([]byte(raw), &next) != nil {
					status(w, http.StatusBadRequest)
					return
				}
			}
		}
		p.mu.Lock()
		hook := p.before
		p.before = nil
		p.mu.Unlock()
		if hook != nil {
			hook(next)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	obj, err := p.core.Tracker().Get(core.SchemeGroupVersion.WithResource("nodes"), "", "node-one")
	if err != nil {
		status(w, http.StatusNotFound)
		return
	}
	current := obj.(*core.Node).DeepCopy()
	if r.Method == http.MethodPatch {
		ops, err := jsonpatch.DecodePatch(patch)
		if err != nil {
			status(w, http.StatusBadRequest)
			return
		}
		raw, _ := json.Marshal(current)
		updated, err := ops.Apply(raw)
		if err != nil {
			status(w, http.StatusConflict)
			return
		}
		var node core.Node
		if json.Unmarshal(updated, &node) != nil {
			status(w, http.StatusBadRequest)
			return
		}
		p.version++
		node.ResourceVersion = fmt.Sprintf("p1-canonical-node-%d", p.version)
		if p.core.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), &node, "") != nil {
			status(w, http.StatusInternalServerError)
			return
		}
		current = &node
		var ledger hostguard.Model
		if json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger) != nil {
			status(w, http.StatusBadRequest)
			return
		}
		sealed, bound := false, false
		if ledger.Placements != nil {
			for _, row := range *ledger.Placements {
				sealed = sealed || row.Barrier != nil
			}
		}
		for _, hold := range ledger.Holds {
			bound = bound || hold.Plan.Request.Key == p.bindKey && hold.Claim != nil
		}
		if p.loseSeal && sealed {
			p.loseSeal = false
			status(w, http.StatusServiceUnavailable)
			return
		}
		if p.loseBind && bound {
			p.loseBind = false
			status(w, http.StatusServiceUnavailable)
			return
		}
	} else if r.Method != http.MethodGet {
		status(w, http.StatusMethodNotAllowed)
		return
	}
	_ = json.NewEncoder(w).Encode(current)
}

type preparedHostBusiness struct {
	api *ownerAPI

	nodeCore coreclient.CoreV1Interface
	vmCoord  coordclient.CoordinationV1Interface
	stopHost func()

	mode      string
	core      *kubefake.Clientset
	publisher *hostexec.Publisher
	allocator *hostguard.Store
	peer      *canonicalPreparedNode
	plan      hostguard.Plan
	before    *hostguard.Snapshot
	barrier   budget.HostReservationBarrier
	blocked   chan struct{}
	release   chan struct{}
	once      sync.Once
	result    chan error
	late      *core.ConfigMap
	lateDone  chan error
}

func (h *preparedHostBusiness) unblock() { h.once.Do(func() { close(h.release) }) }

func prepareHostBusiness(t *testing.T, ctx context.Context, mode string, api *ownerAPI, client *kubefake.Clientset, coord coordclient.CoordinationV1Interface, store *journal.Store, session *Session) *preparedHostBusiness {
	t.Helper()
	var allocator *hostguard.Store
	api.mu.Lock()
	stopHost := api.p1HostStop
	api.mu.Unlock()
	if strings.Contains(mode, "initial") {
		_, allocator = actualHostPublisher(t, client, coord, store, session)
		stopHost = keepPGTakeoverHostOwner(t, ctx, client)
	} else {
		_, allocator = retainedHostPublisher(t, ctx, client, coord, store, session)
	}
	original, err := allocator.Load(ctx)
	require.NoError(t, err)
	wrapped, peer := preparedNodeCore(t, client)
	allocator, err = hostguard.NewStore(wrapped, client.CoordinationV1(), original.State().Binding, "host-fixture-holder")
	require.NoError(t, err)
	publisher, err := hostexec.New(wrapped, coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	s, err := store.Load(ctx)
	require.NoError(t, err)
	step := budgetStep(t, store, journal.PrepareBudget)
	require.Equal(t, journal.Completed, step.Observation.Result)
	var prepared budget.Prepared
	require.NoError(t, json.Unmarshal(step.Observation.Data, &prepared))
	h := &preparedHostBusiness{api: api, nodeCore: wrapped, vmCoord: coord, stopHost: stopHost, mode: mode, core: client, publisher: publisher, allocator: allocator, peer: peer,
		plan:   hostguard.Plan{Namespace: store.Binding().Namespace, VMName: store.Binding().Name, Incarnation: store.Binding().Incarnation, RunnerPodUID: "runner-uid", Request: s.State().Active.Request, BeforeBytes: prepared.VMBeforeMemoryBytes, HostOverheadBytes: 64 << 20},
		before: original, blocked: make(chan struct{}), release: make(chan struct{}), result: make(chan error, 1), lateDone: make(chan error, 1)}
	t.Cleanup(h.unblock)
	if strings.Contains(mode, "seal-first") {
		peer.mu.Lock()
		peer.before = func(candidate hostguard.Model) { close(h.blocked); <-h.release }
		peer.mu.Unlock()
		go func() { _, err := publisher.Reconcile(ctx); h.result <- err }()
		select {
		case <-h.blocked:
		case <-ctx.Done():
			t.Fatal("原Reserve未进入真实Node HTTP CAS阻塞点")
		}
	} else if strings.Contains(mode, "reserve-first") {
		client.PrependReactor("create", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
			cm := action.(kt.CreateAction).GetObject().(*core.ConfigMap)
			if cm.Labels[budgetclaims.ClaimLabel] != budgetclaims.HostKind || h.late != nil {
				return false, nil, nil
			}
			h.late = cm.DeepCopy()
			h.late.UID = "p1-original-late-host-uid"
			h.late.ResourceVersion = "p1-original-late-host-rv"
			payload := h.late.DeepCopy()
			go func() {
				<-h.release
				h.lateDone <- client.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), payload, payload.Namespace)
			}()
			return true, nil, fmt.Errorf("explicit network peer: original Host CREATE remains in flight after lost reply")
		})
		_, err := publisher.Reconcile(ctx)
		require.Error(t, err)
		require.NotNil(t, h.late)
		h.before, err = allocator.Load(ctx)
		require.NoError(t, err)
		held, err := h.before.Hold(h.plan)
		require.NoError(t, err)
		require.Nil(t, held.Claim)
	} else {
		// 精确绑定新Active Key，旧预算已有Claim不触发Bind丢回复故障。
		peer.mu.Lock()
		peer.bindKey = h.plan.Request.Key
		peer.loseBind = true
		peer.mu.Unlock()
		_, err := publisher.Reconcile(ctx)
		var lost *hostguard.UnconfirmedWrite
		require.ErrorAs(t, err, &lost)
		h.before, err = allocator.Load(ctx)
		require.NoError(t, err)
		held, err := h.before.Hold(h.plan)
		require.NoError(t, err)
		require.NotNil(t, held.Claim)
	}
	return h
}

func (h *preparedHostBusiness) seal(t *testing.T, ctx context.Context, store *journal.Store) {
	t.Helper()
	if strings.Contains(h.mode, "reserve-first") {
		h.peer.mu.Lock()
		h.peer.loseSeal = true
		h.peer.mu.Unlock()
	}
	b, err := h.publisher.SealClosedHostRequest(ctx)
	if strings.Contains(h.mode, "reserve-first") {
		var lost *hostguard.UnconfirmedWrite
		require.ErrorAs(t, err, &lost)
		require.Empty(t, b.NodeOrigin)
		b, err = h.publisher.SealClosedHostRequest(ctx)
	}
	require.NoError(t, err)
	h.barrier = b
	if strings.Contains(h.mode, "seal-first") {
		require.Nil(t, b.Held)
		h.unblock()
		require.Error(t, <-h.result, "封存后原Reserve必须原RV冲突")
	} else if h.late != nil {
		require.NotNil(t, b.Held)
		require.Nil(t, b.Held.Claim)
		h.unblock()
		require.NoError(t, <-h.lateDone)
		ref := budget.ObjectRef{Name: h.late.Name, UID: string(h.late.UID)}
		_, err = h.allocator.BindClaim(ctx, h.before, h.plan, ref)
		require.Error(t, err)
		n, err := h.allocator.Load(ctx)
		require.NoError(t, err)
		_, err = h.allocator.BindClaim(ctx, n, h.plan, ref)
		require.ErrorIs(t, err, hostguard.ErrSealed)
	} else {
		require.NotNil(t, b.Held)
		require.NotNil(t, b.Held.Claim)
	}
	n, err := h.allocator.Load(ctx)
	require.NoError(t, err)
	_, _, err = h.allocator.Reserve(ctx, n, h.plan)
	require.ErrorIs(t, err, hostguard.ErrSealed)
	alias := h.plan
	alias.Request.Key.OperationID.Low++
	_, _, err = h.allocator.Reserve(ctx, n, alias)
	require.ErrorIs(t, err, hostguard.ErrSealed)
	s, err := store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().ServiceClosure.RetainedRecovery, "封存不代管理端保存恢复Proof")
	require.NoError(t, s.State().CheckPreparedHostBarrier(b))
}

func (h *preparedHostBusiness) checkProof(t *testing.T, ctx context.Context, s *journal.Snapshot, store *journal.Store, worker *PGWorker) {
	t.Helper()
	m := s.State()
	require.Equal(t, &h.barrier, m.ServiceClosure.RetainedRecovery.PreparedHostBarrier)
	changed := s.State()
	changed.ServiceClosure.RetainedRecovery.PreparedHostBarrier = nil
	require.Error(t, changed.Validate(), "删除屏障不能降级回旧早期恢复合同")
	changed = s.State()
	changed.ServiceClosure.RetainedRecovery.PreparedHostBarrier.Request.Key.OperationID.Low++
	require.Error(t, changed.Validate(), "同位置换ID不能借用原证明")
	// 已保存的证明不能替代当前真实来源：分别替换原Node UID、Host
	// Lease UID响应和结构合法但原摘要变化的Node屏障，读取均拒绝且不改WAL。
	for _, mode := range []string{"node-uid", "lease-uid", "barrier-raw"} {
		node, err := h.core.CoreV1().Nodes().Get(ctx, h.barrier.NodeName, metav1.GetOptions{})
		require.NoError(t, err)
		if mode == "lease-uid" {
			// 明确的协议读取故障，不声称真实Kube重建。直接改fake
			// Tracker UID会被其无UID/RV条件的续租覆盖，反而未注入故障。
			h.api.mu.Lock()
			h.api.p1HostLeaseReadUID = "source-reader-different-host-lease-uid"
			h.api.mu.Unlock()
		} else {
			replaced := node.DeepCopy()
			if mode == "node-uid" {
				replaced.UID = "same-name-recreated-node"
			} else {
				var ledger hostguard.Model
				require.NoError(t, json.Unmarshal([]byte(replaced.Annotations[hostguard.LedgerAnnotation]), &ledger))
				(*ledger.Placements)[0].Barrier.PreparedSHA256 = strings.Repeat("f", 64)
				require.NoError(t, ledger.Validate(), "反例须通过Node结构校验再命中跨域原文绑定")
				raw, err := json.Marshal(ledger)
				require.NoError(t, err)
				replaced.Annotations[hostguard.LedgerAnnotation] = string(raw)
			}
			require.NoError(t, h.core.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), replaced, ""))
		}
		require.Error(t, worker.admission.CheckServiceRetained(ctx, s), mode)
		current, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, s.State(), current.State(), "来源漂移只读拒绝不能改原Proof/Unknown")
		require.NoError(t, h.core.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), node, ""))
		h.api.mu.Lock()
		h.api.p1HostLeaseReadUID = ""
		h.api.mu.Unlock()
	}
	if h.barrier.Held != nil && h.barrier.Held.Claim != nil {
		copyModel := s.State()
		copyModel.ServiceClosure.RetainedRecovery.PreparedHostBarrier.Held.Claim.UID = "caller-copy-only"
		require.NotEqual(t, copyModel, s.State(), "Journal Clone必须独立复制屏障Claim")
	}
}

func (h *preparedHostBusiness) checkRetired(t *testing.T, ctx context.Context, m journal.Model, n *hostguard.Snapshot) {
	t.Helper()
	b, err := n.PreparedBarrier(h.plan)
	require.NoError(t, err)
	require.Equal(t, h.barrier, b)
	_, err = n.Hold(h.plan)
	require.Error(t, err, "完整恢复完成后才由可信适配退休原封存Hold")
	require.EqualValues(t, 9, n.State().ProtocolVersion)
	require.Equal(t, &h.barrier, m.RetainedCoverage.Record.ServiceCoverage.RetainedRecovery.PreparedHostBarrier)
	if h.late != nil {
		_, err = h.core.CoreV1().ConfigMaps(h.late.Namespace).Get(ctx, h.late.Name, metav1.GetOptions{})
		require.True(t, apierrors.IsNotFound(err), "完整Initial前缀退休后精确回收迟到未绑定Host")
	}
}

// 单服务边界仅验证Publisher独立封存与Agent固定来源只读消费；不进行恢复重开。
func TestPG17SP1PublisherSealAndReadOnlyConsumer(t *testing.T) {
	t.Parallel()
	t.Log("PG17-S-P1-001: 原Prepare真实Completed/确认关闭后Publisher封存，Agent只读认证无资源副作用")
	productionInitialService(t, "host-service-closure-seal")
}

// preparedScheduler使用真实Service配置/固定客户端，VM Lease GET转发同一
// owner HTTP peer；Host Lease仍来自本Core tracker，绝不镜像一个假新UID。
func preparedScheduler(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, coreClient coreclient.CoreV1Interface, vmCoord coordclient.CoordinationV1Interface, node hostguard.Model) *hostscheduler.Service {
	t.Helper()
	client.PrependReactor("get", "leases", func(action kt.Action) (bool, runtime.Object, error) {
		a := action.(kt.GetAction)
		if a.GetNamespace() != binding().Namespace || a.GetName() != journal.LeaseName(binding()) {
			return false, nil, nil
		}
		lease, err := vmCoord.Leases(binding().Namespace).Get(ctx, a.GetName(), metav1.GetOptions{})
		return true, lease, err
	})
	pod, err := client.CoreV1().Pods(binding().Namespace).Get(ctx, "runner", metav1.GetOptions{})
	require.NoError(t, err)
	pod.Spec.SchedulerName = node.Profile.SchedulerName
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
	vm, err := vmclient.NewForConfig(api.config)
	require.NoError(t, err)
	config := hostscheduler.Config{Nodes: []hostscheduler.NodeConfig{{Binding: node.Binding, Profile: *node.Profile, SystemBytes: node.OutsideBytes}}}
	scheduler, err := hostscheduler.New(coreClient, client.CoordinationV1(), vm, config, node.Profile.SchedulerName)
	require.NoError(t, err)
	return scheduler
}

func (h *preparedHostBusiness) checkSchedulerLateSettlement(t *testing.T, ctx context.Context, api *ownerAPI, store *journal.Store, session *Session) {
	t.Helper()
	require.NotNil(t, h.stopHost, "新根必须能正常停止原Host续租而非手填owner")
	s, err := store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active)
	var final budget.FinalizationCommand
	require.NoError(t, json.Unmarshal(s.State().BudgetHeld.Finalization.Command, &final))
	before, err := h.allocator.Load(ctx)
	require.NoError(t, err)
	row, err := before.State().Placement(h.plan.RunnerPodUID)
	require.NoError(t, err)
	require.Less(t, row.MemoryBytes, final.Finalized.Request.TargetVMMemoryBytes, "刻意错过Active阶段，旧placement尚未随真实新预算增长")
	scheduler := preparedScheduler(t, ctx, api, h.core, h.nodeCore, h.vmCoord, before.State())
	pod, err := h.core.CoreV1().Pods(binding().Namespace).Get(ctx, "runner", metav1.GetOptions{})
	require.NoError(t, err)
	_, _, err = scheduler.Reserve(ctx, h.barrier.NodeName, pod, 0)
	require.Error(t, err)
	// 正常停止原owner并等待原Lease自然过期，Service.Run真实选主、抬floor
	// 和完整Settle；不改holder/UID/RV，不提高生产10秒reconcile期限。
	h.stopHost()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	require.NoError(t, scheduler.Run(runCtx, zap.NewNop()))
	require.Eventually(t, func() bool {
		n, err := h.allocator.Load(ctx)
		if err != nil {
			return false
		}
		held, err := n.State().Placement(h.plan.RunnerPodUID)
		return err == nil && held.Settled != nil && held.Settled.Request == final.Finalized.Request && held.MemoryBytes == final.Finalized.Request.TargetVMMemoryBytes
	}, 90*time.Second, 100*time.Millisecond, "Node owner必须先结清再做无Key纯保留，不能被Reserve→publish顺序永久卡住")
	_, held, err := scheduler.Reserve(ctx, h.barrier.NodeName, pod, 0)
	require.NoError(t, err)
	require.Equal(t, final.Finalized.Request.TargetVMMemoryBytes, held.MemoryBytes)
	require.Equal(t, &h.barrier, held.Barrier)
	after, err := h.allocator.Load(ctx)
	require.NoError(t, err)
	require.Greater(t, after.State().LeaseFloor.Epoch, before.State().LeaseFloor.Epoch)
	current, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, s.State(), current.State(), "Node侧可达性修复不改真实新VM永久头/原审计")
}

func schedulerSealsPreparedHost(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, vmCoord coordclient.CoordinationV1Interface, allocator *hostguard.Store, plan hostguard.Plan) budget.HostReservationBarrier {
	t.Helper()
	n, err := allocator.Load(ctx)
	require.NoError(t, err)
	scheduler := preparedScheduler(t, ctx, api, client, client.CoreV1(), vmCoord, n.State())
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	require.NoError(t, scheduler.Run(runCtx, zap.NewNop()))
	var barrier budget.HostReservationBarrier
	require.Eventually(t, func() bool {
		n, err := allocator.Load(ctx)
		if err != nil {
			return false
		}
		b, err := n.PreparedBarrier(plan)
		if err != nil {
			return false
		}
		barrier = b
		return true
	}, 90*time.Second, 100*time.Millisecond, "实际Scheduler选主/恢复循环必须到达独立Seal入口")
	return barrier
}

func TestPG17SP1SchedulerCompletesMissedActivePlacement(t *testing.T) {
	t.Log("PG17-S-P1-002: 真实后继已NoActive但placement旧值，真实Node owner接管后先结清再允许纯保留")
	productionInitialService(t, "retained-safety-finalized-completed-bound-scheduler")
}
