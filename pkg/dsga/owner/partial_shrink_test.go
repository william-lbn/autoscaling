package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	kt "k8s.io/client-go/testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 多请求PG协议夹具真实保留同点容量/门禁代次；只驱动生产客户端/执行器，
// 不冒充PG内核或Provider物理释放。与仅支持同一原开放重放的旧夹具分开，
// 防止为支持close→resize→reopen而放宽旧故障测试的原命令断言。
type partialPGPeer struct {
	mu             sync.Mutex
	request        control.Request
	before         control.SharedCostResponse
	after          control.SharedCostResponse
	resized        bool
	admission      control.AdmissionRequest
	resizePosts    int
	admissionPosts int
}

func (p *partialPGPeer) sharedLocked() control.SharedCostResponse {
	s := p.before
	if p.resized {
		s = p.after
	}
	s.Observed.AdmissionOpen, s.Observed.AdmissionRevision = p.admission.Open, p.admission.Revision
	return s
}

func (p *partialPGPeer) shared() control.SharedCostResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sharedLocked()
}

func (p *partialPGPeer) configure(t *testing.T, prepared budget.Prepared) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.request, p.before, p.after, p.resized = prepared.Request, prepared.Shared, prepared.Shared, false
	s := &p.after.Observed
	s.CapacityGeneration = prepared.Request.ExpectedCapacityGeneration + 1
	s.LowBuffers, s.HighBuffers = prepared.Request.TargetBuffers, prepared.Request.TargetBuffers
	hash, err := prepared.Request.ContextSHA256()
	require.NoError(t, err)
	s.Operation = &control.OperationFact{Key: prepared.Request.Key, Kind: prepared.Request.Kind, Phase: control.Committed, Reason: control.ReasonNone, TargetBuffers: prepared.Request.TargetBuffers, ExpectedCapacityGeneration: prepared.Request.ExpectedCapacityGeneration, ContextSHA256: hash, CancelRequested: false, CancelClosed: true, Replayed: false}
	s.Arrays = append([]control.ArrayFact(nil), s.Arrays...)
	if prepared.Request.Kind == control.Shrink {
		for i := range s.Arrays {
			key := prepared.Request.Key
			s.Arrays[i].ReleaseKey, s.Arrays[i].ReleaseCapacityGeneration, s.Arrays[i].ReleaseState = &key, s.CapacityGeneration, control.ReleaseVerified
			var provider control.ProviderFact
			provider.HasResult, provider.Status = true, control.ProviderNoop
			s.Arrays[i].Provider = provider
		}
		provider := &s.Arrays[0].Provider
		provider.Status, provider.PhysicalReleaseVerified = control.ProviderOK, true
		provider.RequestedOffset, provider.AffectedOffset, provider.RequestedBytes, provider.AffectedBytes = 4096, 4096, 4096, 4096
	}
	require.NoError(t, p.after.ValidateFor(prepared.Request.Key.Instance))
}

func (p *partialPGPeer) serve(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	switch r.URL.Path {
	case "/dsg/v1/budget/shared":
		pgReply(w, p.sharedLocked())
	case "/dsg/v1/work/dsm":
		pgReply(w, vmDSM(p.sharedLocked().Observed))
	case "/dsg/v1/resize", "/dsg/v1/resize/status":
		request := p.request
		if r.URL.Path == "/dsg/v1/resize" {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			require.Equal(t, p.request, request)
			p.resizePosts++
			p.resized = true
		} else {
			// 正式status只带原Query Key，不带可改写目标的完整Request。
			var query control.Query
			require.NoError(t, json.NewDecoder(r.Body).Decode(&query))
			require.Equal(t, p.request.Key, query.Key)
		}
		require.True(t, p.resized, "本用例不允许未执行却直接读取Committed")
		s := p.sharedLocked().Observed
		fact := control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: request.Key, Disposition: control.ReceiptReplayed, HasResult: true, Result: &s}, Observed: s}
		require.NoError(t, fact.ValidateResize(request, request.Key.Instance))
		pgReply(w, fact)
	case "/dsg/v1/admission", "/dsg/v1/admission/status":
		var request control.AdmissionRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.NoError(t, request.Validate())
		require.Equal(t, p.request.Key.Instance, request.Instance)
		hash, err := request.ContextSHA256()
		require.NoError(t, err)
		query := strings.HasSuffix(r.URL.Path, "/status")
		disposition := control.AdmissionNotFound
		if !query {
			p.admissionPosts++
			require.Equal(t, p.sharedLocked().Observed.CapacityGeneration, request.ExpectedCapacityGeneration)
			require.Equal(t, p.request.Key.IssuerEpoch, request.IssuerEpoch)
			require.Equal(t, p.request.BudgetCommitmentID, request.BudgetCommitmentID)
			require.GreaterOrEqual(t, request.Revision, p.admission.Revision)
			if request.Revision == p.admission.Revision {
				require.Equal(t, p.admission, request)
			}
			p.admission = request
			disposition = control.AdmissionApplied
		} else if request == p.admission {
			disposition = control.AdmissionReplayed
		}
		pgReply(w, control.AdmissionResponse{ProtocolVersion: 1, Request: request, ContextSHA256: hash, Disposition: disposition, Observed: p.sharedLocked().Observed, VMShrinkPermit: false})
	default:
		t.Errorf("unexpected multi-request PG route %s", r.URL.Path)
		w.WriteHeader(500)
	}
}

// 一条组合：真实首次Grow/完整预算→新Shrink的真实Quiesce、PG/Guest执行器→
// 原VM未发送取消→实际预算/自己的门禁恢复→历史淘汰/Host结清→后继正常Grow。
// Prepare/PG/Root/资格仍是明确领域/协议夹具，绝不等同真实Linux物理验收。
func verifyShrinkPartialAfterGrow(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, coord coordclient.CoordinationV1Interface, store *journal.Store, session *Session, original *PGWorker, resources *ResourceWorker, root *guestRootFixture, first budget.Prepared, runner *vmguard.Runner, qmp *vmQMPPeer) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	f, err := s.State().BudgetFor(first.Request)
	require.NoError(t, err)
	var opening control.AdmissionRequest
	require.NoError(t, json.Unmarshal(budgetStep(t, store, journal.ResumeAdmission).Command, &opening))
	_, err = store.Finish(ctx, s, session.Actor(), first.Request.Key)
	require.NoError(t, err)
	node, err := client.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	var initial hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &initial))
	stopRenewal := keepUnchangedHostLease(t, ctx, client, initial.Binding)
	defer stopRenewal()
	allocator, err := hostguard.NewStore(client.CoreV1(), client.CoordinationV1(), initial.Binding, "host-fixture-holder")
	require.NoError(t, err)
	publisher, err := hostexec.New(client.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	_, err = publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	n, err := allocator.Load(ctx)
	require.NoError(t, err)
	before, err := n.State().UsedBytes()
	require.NoError(t, err)
	req := first.Request
	req.Kind, req.TargetBuffers, req.TargetVMMemoryBytes = control.Shrink, 512, 1<<30
	req.Key.Revision++
	req.Key.OperationID.Low++
	req.ExpectedCapacityGeneration = f.Shared.Observed.CapacityGeneration
	req.BudgetCommitmentID += "-partial-shrink"
	p := first
	p.Request, p.Shared, p.VMBeforeMemoryBytes = req, f.Shared, first.Request.TargetVMMemoryBytes
	p.Shared.Observed.AdmissionOpen, p.Shared.Observed.AdmissionRevision = opening.Open, opening.Revision
	p.GuestUsableMemoryBytes, p.TargetSharedUpperBytes = 768<<20, p.Shared.CurrentMainAndArraysUpperBytes
	outside := budget.OutsideReservation{ProtocolVersion: 1, VMShrinkPermit: false, Request: req, Identity: p.Identity, Incarnation: binding().Incarnation, ResidualProfileID: "explicit-qualified-platform-fixture", SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true}
	outRef := vmReservation(t, client, "outside-partial-shrink", budgetclaims.OutsideKind, outside)
	p.OutsideClaim = &outRef
	require.NoError(t, p.ValidateFor(req, p.Identity))
	var pg partialPGPeer
	pg.admission = opening
	pg.configure(t, p)
	config := pgConfig(t, req, func(w http.ResponseWriter, r *http.Request) { pg.serve(t, w, r) }, func() control.Snapshot { return pg.shared().Observed })
	s, err = store.Load(ctx)
	require.NoError(t, err)
	_, _, err = store.Begin(ctx, s, session.Actor(), req, 0)
	require.NoError(t, err)
	priorBudgetDomain(t, ctx, store, session.Actor(), req, journal.PrepareBudget, req.Key.Instance.ComputeInstanceID, p)
	_, err = publisher.Reconcile(ctx)
	require.NoError(t, err)
	original.CloseIdleConnections()
	worker, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	for _, action := range []journal.Action{journal.Quiesce, journal.ResizePG, journal.ReleasePG, journal.GuestReady, journal.ResizeVM} {
		wanted := journal.Settled
		if action == journal.ResizeVM {
			wanted = journal.Unconfirmed
		}
		advancePartialStep(t, ctx, store, worker, action, wanted)
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	_, err = store.RequestAbort(ctx, s, session.Actor(), req.Key)
	require.NoError(t, err)
	require.NoError(t, runner.Tick(ctx))
	_, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.NoEffect, budgetStep(t, store, journal.ResizeVM).Observation.Result)
	// 旧大VM资格不可被同名新UID接替；当前Root限额漂移也不能登记Cleanup。
	var rejectOld atomic.Bool
	client.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		if action.(kt.GetAction).GetName() != f.Sources.OutsideClaim.Name || !rejectOld.Load() {
			return false, nil, nil
		}
		obj, err := client.Tracker().Get(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, f.Sources.OutsideClaim.Name)
		if err != nil {
			return true, nil, err
		}
		cm := obj.(*core.ConfigMap).DeepCopy()
		cm.UID = "replacement-qualified-outside-uid"
		return true, cm, nil
	})
	rejectOld.Store(true)
	_, err = worker.Advance(ctx)
	require.Error(t, err)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active.Cleanup)
	rejectOld.Store(false)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	_, err = worker.Advance(ctx)
	require.Error(t, err)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active.Cleanup)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()
	for i := 0; i < 3; i++ {
		_, err = worker.Advance(ctx)
		require.NoError(t, err)
		s, err = store.Load(ctx)
		require.NoError(t, err)
		if s.State().Active == nil {
			break
		}
	}
	require.Nil(t, s.State().Active)
	c, err := s.State().CapacityFor(req)
	require.NoError(t, err)
	require.EqualValues(t, 2<<30, c.VMMemoryBytes)
	require.EqualValues(t, 1<<30, c.Request.TargetVMMemoryBytes)
	require.Equal(t, f.Sources.OutsideClaim, c.OutsideClaim, "VM仍大，必须保留旧大VM资格")
	require.NotEqual(t, outRef, c.OutsideClaim)
	require.EqualValues(t, 512, c.Buffers)
	require.Equal(t, req.ExpectedCapacityGeneration+1, c.CapacityGeneration)
	require.True(t, pg.shared().Observed.AdmissionOpen, "原Quiesce仍有效，应自动恢复自己关闭的服务")
	qmp.mu.Lock()
	require.Equal(t, 1, qmp.writes, "取消Shrink没有发送第二次QMP set")
	qmp.mu.Unlock()
	// 按真实annotation预算淘汰旧结果，当前实际头和旧资格仍有独立寿命。
	zero := req
	zero.Kind, zero.TargetBuffers, zero.TargetVMMemoryBytes = control.Grow, 1024, 2<<30
	zero.ExpectedCapacityGeneration = c.CapacityGeneration
	zero.Key.Revision++
	zero.Key.OperationID.Low++
	zero.BudgetCommitmentID += "-unstarted"
	s = expireBeforePartialSettlement(t, ctx, api, store, session, worker, s, zero)
	require.NoError(t, s.State().CheckClosedCapacity(req))
	_, err = publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	n, err = allocator.Load(ctx)
	require.NoError(t, err)
	after, err := n.State().UsedBytes()
	require.NoError(t, err)
	require.Equal(t, before, after, "不能按已取消的1GiB目标少记实际2GiB VM")
	row, err := n.State().Placement("runner-uid")
	require.NoError(t, err)
	require.EqualValues(t, 2<<30, row.MemoryBytes)
	require.Equal(t, zero.Key, row.Settled.ClosedThrough())
	// 下一正常Grow使用实际PG generation，原子替换实际头；VM目标保持2GiB。
	next := zero
	next.Key.Revision++
	next.Key.OperationID.Low++
	next.BudgetCommitmentID += "-normal"
	p.Request, p.Shared, p.VMBeforeMemoryBytes, p.GuestUsableMemoryBytes = next, pg.shared(), c.VMMemoryBytes, (2<<30)-(256<<20)
	p.TargetSharedUpperBytes, err = budget.GrowSharedUpper(p.Shared, next.TargetBuffers)
	require.NoError(t, err)
	outside.Request = next
	outRef = vmReservation(t, client, "outside-after-partial-normal", budgetclaims.OutsideKind, outside)
	p.OutsideClaim = &outRef
	require.NoError(t, p.ValidateFor(next, p.Identity))
	pg.configure(t, p)
	_, _, err = store.Begin(ctx, s, session.Actor(), next, 0)
	require.NoError(t, err)
	priorBudgetDomain(t, ctx, store, session.Actor(), next, journal.PrepareBudget, next.Key.Instance.ComputeInstanceID, p)
	_, err = publisher.Reconcile(ctx)
	require.NoError(t, err)
	advancePartialStep(t, ctx, store, worker, journal.ReserveHost, journal.Settled)
	advancePartialStep(t, ctx, store, worker, journal.ResizeVM, journal.Unconfirmed)
	require.NoError(t, runner.Tick(ctx))
	for _, action := range []journal.Action{journal.ResizeVM, journal.GuestReady, journal.ResizePG, journal.FinalizeBudget, journal.ResumeAdmission} {
		advancePartialStep(t, ctx, store, worker, action, journal.Settled)
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	s, err = store.Finish(ctx, s, session.Actor(), next.Key)
	require.NoError(t, err)
	require.Nil(t, s.State().CapacityHeld)
	normal, err := s.State().BudgetFor(next)
	require.NoError(t, err)
	require.Equal(t, c.CapacityGeneration+1, normal.Shared.Observed.CapacityGeneration)
	_, err = publisher.SettleCurrent(ctx)
	require.NoError(t, err)
}

func advancePartialStep(t *testing.T, ctx context.Context, store *journal.Store, worker *PGWorker, action journal.Action, wanted journal.Phase) {
	t.Helper()
	for i := 0; i < 5; i++ {
		if budgetStep(t, store, action).Phase == wanted {
			return
		}
		progress, err := worker.Advance(ctx)
		require.NoError(t, err, string(action))
		require.True(t, progress == pgexec.Recorded || progress == pgexec.Reconciled, string(action))
	}
	require.Equal(t, wanted, budgetStep(t, store, action).Phase)
}

func expireBeforePartialSettlement(t *testing.T, ctx context.Context, api *ownerAPI, store *journal.Store, session *Session, worker *PGWorker, s *journal.Snapshot, zero control.Request) *journal.Snapshot {
	t.Helper()
	annotations := s.VirtualMachine().Annotations
	room := 256*1024 - len(control.JournalAnnotation)
	for key, value := range annotations {
		if key != control.JournalAnnotation {
			room -= len(key) + len(value)
		}
	}
	paddingKey := "test-only.example/partial-history-pressure"
	padding := room - len(paddingKey) - (len(annotations[control.JournalAnnotation]) + 4095)
	require.Positive(t, padding)
	api.mu.Lock()
	api.vm.Annotations[paddingKey] = strings.Repeat("x", padding)
	api.vmVersion++
	api.vm.ResourceVersion = fmt.Sprintf("vm-rv-%d", api.vmVersion)
	api.mu.Unlock()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	s, _, err = store.Begin(ctx, s, session.Actor(), zero, 0)
	require.NoError(t, err)
	_, err = store.RequestAbort(ctx, s, session.Actor(), zero.Key)
	require.NoError(t, err)
	_, err = worker.Advance(ctx)
	require.NoError(t, err)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active)
	require.NotZero(t, s.State().ExpiredResults)
	api.mu.Lock()
	delete(api.vm.Annotations, paddingKey)
	api.vmVersion++
	api.vm.ResourceVersion = fmt.Sprintf("vm-rv-%d", api.vmVersion)
	api.mu.Unlock()
	s, err = store.Load(ctx)
	require.NoError(t, err)
	return s
}
