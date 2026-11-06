package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// 只提供显式PG协议peer；真正Session/WAL/预算/Host/Runner/Guest生产链不替换。
// 此处不是Linux内核/真实Kube或Source的验收，不能把本地peer升级为物理证明。
type pgInflightServicePeer struct {
	pg                                              *initialServicePG
	received, register, started, release, cancelled chan struct{}
	once                                            sync.Once
	mu                                              sync.Mutex
	beforeMemory                                    func()
	failReply                                       bool
	fenceCalls                                      int
}

func newPGInflightServicePeer(pg *initialServicePG) *pgInflightServicePeer {
	return &pgInflightServicePeer{pg: pg, received: make(chan struct{}), register: make(chan struct{}), started: make(chan struct{}), release: make(chan struct{}), cancelled: make(chan struct{})}
}

// 仅expiry负例返回协议真实含义：Kernel仍Preparing时Pending，已退出后才Fenced。
func (p *pgInflightServicePeer) expiryFence(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request control.FenceRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.Equal(t, p.pg.request.Key.Instance, request.Instance)
		p.mu.Lock()
		p.fenceCalls++
		p.mu.Unlock()
		observed := p.pg.shared().Observed
		disposition := control.Fenced
		if observed.Operation != nil && observed.Operation.Phase == control.Preparing {
			disposition = control.FencePending
		}
		pgReply(w, control.FenceResponse{ProtocolVersion: 1, RequestedInstance: request.Instance, RequestedEpoch: request.IssuerEpoch, Disposition: disposition, Observed: observed})
	}
}

func (p *pgInflightServicePeer) serve(t *testing.T, w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path == "/dsg/v1/memory" {
		p.mu.Lock()
		hook := p.beforeMemory
		p.beforeMemory = nil
		p.mu.Unlock()
		if hook != nil {
			hook()
		}
		pgReply(w, control.MemoryResponse{ProtocolVersion: 1, Observed: p.pg.shared().Observed})
		return true
	}
	if r.URL.Path == "/dsg/v1/resize/status" {
		var query control.Query
		require.NoError(t, json.NewDecoder(r.Body).Decode(&query))
		require.Equal(t, p.pg.request.Key, query.Key)
		p.pg.mu.Lock()
		p.pg.resizeQueries++
		observed := p.pg.current().Observed
		p.pg.mu.Unlock()
		pgReply(w, control.ControlResponse{ProtocolVersion: 1, Receipt: control.Receipt{Key: query.Key, Disposition: control.ReceiptNotFound, HasResult: false, Result: nil}, Observed: observed})
		return true
	}
	if r.URL.Path != "/dsg/v1/resize" {
		return false
	}
	var request control.Request
	require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
	require.Equal(t, p.pg.request, request)
	require.Equal(t, journal.Unconfirmed, budgetStep(t, p.pg.store, journal.ResizePG).Phase)
	p.pg.mu.Lock()
	p.pg.resizePosts++
	p.pg.mu.Unlock()
	close(p.received)
	// 真实Start及单HTTP已送达，Native尚未登记operation的启动间隙。
	select {
	case <-p.register:
	case <-r.Context().Done():
		close(p.cancelled)
		return true
	}
	p.pg.mu.Lock()
	s := clonePGInflight(t, p.pg.before)
	hash, err := request.ContextSHA256()
	require.NoError(t, err)
	s.Observed.Operation = &control.OperationFact{Key: request.Key, Kind: request.Kind, Phase: control.Preparing, Reason: control.ReasonNone, TargetBuffers: request.TargetBuffers, ExpectedCapacityGeneration: request.ExpectedCapacityGeneration, ContextSHA256: hash}
	// 同内核先记最坏upper再Unknown的窗口：只增加一数组，未假造完成或清Unknown。
	s.Observed.Arrays[0].CommittedUpperBytes += 8 << 20
	s.Observed.Arrays[0].AllocationUnknown = true
	s.CurrentMainAndArraysUpperBytes += 8 << 20
	p.pg.before = s
	p.pg.after.Observed.Operation.Replayed = false
	p.pg.mu.Unlock()
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
	case <-r.Context().Done():
		close(p.cancelled)
		return true
	}
	p.mu.Lock()
	fail := p.failReply
	p.mu.Unlock()
	if fail {
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}
	// 只有测试明确释放唯一HTTP时才提供原生终态；Owner审计期间不触发这里。
	response := pgResponse(t, request)
	p.pg.mu.Lock()
	finished := p.pg.current()
	finished.Observed.Operation.Phase = control.Committed
	finished.Observed.Operation.CancelClosed = true
	finished.Observed.CapacityGeneration = request.ExpectedCapacityGeneration + 1
	finished.Observed.LowBuffers = request.TargetBuffers
	finished.Observed.HighBuffers = request.TargetBuffers
	for i := range finished.Observed.Arrays {
		finished.Observed.Arrays[i].AllocationUnknown = false
	}
	p.pg.after = finished
	p.pg.resized = true
	observed := p.pg.current().Observed
	response.Observed = observed
	*response.Receipt.Result = observed
	p.pg.resizeReceipt = &response
	p.pg.mu.Unlock()
	pgReply(w, response)
	return true
}
func clonePGInflight[T any](t *testing.T, v T) T {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	var out T
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}
func initialPGInflightUpper(t *testing.T) (budget.Prepared, control.SharedCostResponse) {
	t.Helper()
	r := pgRequest(t, journal.Owner{Epoch: 2, HolderID: "owner"})
	id := resourceguard.Identity{VMUID: r.Key.Instance.VMUID, ComputeInstanceID: r.Key.Instance.ComputeInstanceID, GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
	p, before := preparedGuest(t, r, id)
	hash, err := r.ContextSHA256()
	require.NoError(t, err)
	current := clonePGInflight(t, before)
	current.Observed.Operation = &control.OperationFact{Key: r.Key, Kind: r.Kind, Phase: control.Preparing, Reason: control.ReasonNone, TargetBuffers: r.TargetBuffers, ExpectedCapacityGeneration: r.ExpectedCapacityGeneration, ContextSHA256: hash}
	current.Observed.Arrays[0].AllocationUnknown = true
	current.Observed.Arrays[0].CommittedUpperBytes += 8 << 20
	current.CurrentMainAndArraysUpperBytes += 8 << 20
	return p, current
}
func TestPGInflightGrowPaidUpperExactContextAndRange(t *testing.T) {
	p, current := initialPGInflightUpper(t)
	preserved := clonePGInflight(t, current)
	require.NoError(t, validatePGInflightGrowUpper(p, current, p.Request.Key.IssuerEpoch, p.Limits.MemoryMaxBytes))
	require.Equal(t, preserved, current, "Preparing/Unknown必须保留，不是完成或零成本")
	for _, tc := range []struct {
		name   string
		mutate func(*budget.Prepared, *control.SharedCostResponse, *control.Uint64)
	}{
		{"wrong-key", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			c.Observed.Operation.Key.Revision++
		}},
		{"wrong-context", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			c.Observed.Operation.ContextSHA256 = strings.Repeat("0", 64)
		}},
		{"wrong-floor", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			c.Observed.IssuerFloorEpoch++
		}},
		{"wrong-target", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			c.Observed.Operation.TargetBuffers++
		}},
		{"generation", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			c.Observed.CapacityGeneration++
		}},
		{"prefix", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) { c.Observed.LowBuffers-- }},
		{"cancel", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			c.Observed.Operation.CancelRequested = true
		}},
		{"replayed", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			c.Observed.Operation.Replayed = true
		}},
		{"no-operation", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) { c.Observed.Operation = nil }},
		{"zero-upper", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			c.Observed.Arrays[0].CommittedUpperBytes = 0
		}},
		{"array-exceeds-paid", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			c.CurrentMainAndArraysUpperBytes += 9 << 20
			c.Observed.Arrays[0].CommittedUpperBytes += 9 << 20
		}},
		{"array-lost-original-upper", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			c.CurrentMainAndArraysUpperBytes -= 9 << 20
			c.Observed.Arrays[0].CommittedUpperBytes -= 9 << 20
		}},
		{"aggregate-exceeds-paid", func(p *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			for i := range c.Observed.Arrays {
				c.Observed.Arrays[i].CommittedUpperBytes = 40 << 20
			}
			c.CurrentMainAndArraysUpperBytes = c.StartupMainUpperBytes + 160<<20
		}},
		{"main-changed", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			c.StartupMainUpperBytes++
			c.CurrentMainAndArraysUpperBytes++
			c.Observed.StartupSharedUpperBytes++
		}},
		{"baseline-unknown", func(p *budget.Prepared, _ *control.SharedCostResponse, _ *control.Uint64) {
			p.Shared.Observed.Arrays[0].AllocationUnknown = true
		}},
		{"normal-below-paid", func(p *budget.Prepared, _ *control.SharedCostResponse, n *control.Uint64) {
			*n = p.TargetSharedUpperBytes - 1
		}},
		{"release-scope-changed", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			c.Observed.Arrays[0].Provider.AllocationMayBePartial = true
		}},
		{"shrink-not-proven", func(p *budget.Prepared, _ *control.SharedCostResponse, _ *control.Uint64) {
			p.Request.Kind = control.Shrink
		}},
		{"admission-revision", func(_ *budget.Prepared, c *control.SharedCostResponse, _ *control.Uint64) {
			c.Observed.AdmissionRevision++
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := clonePGInflight(t, p)
			c := clonePGInflight(t, current)
			n := p.Limits.MemoryMaxBytes
			tc.mutate(&p, &c, &n)
			require.Error(t, validatePGInflightGrowUpper(p, c, p.Request.Key.IssuerEpoch, n))
		})
	}
	committed := clonePGInflight(t, current)
	committed.Observed.Operation.Phase = control.Committed
	committed.Observed.Operation.CancelClosed = true
	committed.Observed.CapacityGeneration++
	committed.Observed.LowBuffers = p.Request.TargetBuffers
	committed.Observed.HighBuffers = p.Request.TargetBuffers
	require.NoError(t, validatePGInflightGrowUpper(p, committed, p.Request.Key.IssuerEpoch, p.Limits.MemoryMaxBytes), "HTTP完成/Status发布交错仍须同Key已付范围，不自动落账")
}
func TestPGInflightGrowCompletePaidSourcesRenewUnknownWithoutWALMutation(t *testing.T) {
	productionInitialService(t, "pg-inflight-audit-complete")
}
func TestPGInflightGrowMissingOrChangedSourceNeverRenews(t *testing.T) {
	for _, group := range []string{"root", "sources", "native-progress", "native-cas", "deadline", "lost-reply", "late-success", "upstream-expired"} {
		t.Run(group, func(t *testing.T) { productionInitialService(t, "pg-inflight-audit-negative-"+group) })
	}
}

func verifyPGInflightServiceAudit(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, core *kubefake.Clientset, session *Session, worker *PGWorker, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, peer *pgInflightServicePeer) {
	t.Helper()
	if strings.HasPrefix(mode, "pg-inflight-audit-run-") {
		verifyPGInflightRunLifecycle(t, ctx, mode, store, worker, pg, peer)
		return
	}
	require.NotNil(t, peer)
	parent, cancel := context.WithCancel(ctx)

	defer cancel()
	stop, err := worker.executor.EnableInFlight(parent)
	require.NoError(t, err)
	defer stop()
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.AwaitingFact, progress)
	select {
	case <-peer.received:
	case <-time.After(10 * time.Second):
		t.Fatal("唯一原ResizeHTTP未送达")
	}
	gap, err := store.Load(ctx)
	require.NoError(t, err)
	require.True(t, worker.executor.MatchInFlight(gap, pg.request))
	require.Nil(t, pg.shared().Observed.Operation)
	require.Equal(t, &pg.request, worker.client.Pending())
	pg.mu.Lock()
	gapRenewals := pg.leaseRenewals
	pg.mu.Unlock()
	if strings.HasPrefix(mode, "pg-inflight-audit-negative-") {
		handled, _, err := worker.advanceServiceSafety(ctx, "")
		require.True(t, handled)
		require.Error(t, err)
	} else {
		require.Error(t, worker.inspectPGInflightService(ctx, gap))
	}
	pg.mu.Lock()
	require.Equal(t, gapRenewals, pg.leaseRenewals)
	pg.mu.Unlock()
	gapAfter, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, gap.State(), gapAfter.State())
	require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizePG).Phase)
	close(peer.register)
	select {
	case <-peer.started:
	case <-time.After(10 * time.Second):
		t.Fatal("唯一原ResizeHTTP未到达")
	}
	before, err := store.Load(ctx)
	require.NoError(t, err)
	proof, err := pgInflightGrowBoundary(before, session.Actor(), pg.request.Key.Instance)
	require.NoError(t, err)
	require.True(t, worker.executor.MatchInFlight(before, pg.request))
	require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizePG).Phase)
	require.Equal(t, &pg.request, worker.client.Pending())
	shared := clonePGInflight(t, pg.shared())
	require.True(t, shared.Observed.Arrays[0].AllocationUnknown)
	oldRenewals := func() int { pg.mu.Lock(); defer pg.mu.Unlock(); return pg.leaseRenewals }
	if strings.HasSuffix(mode, "-deadline") || strings.HasSuffix(mode, "-lost-reply") {
		old := oldRenewals()
		pg.mu.Lock()
		queriesBefore := pg.resizeQueries
		pg.mu.Unlock()
		if strings.HasSuffix(mode, "-deadline") {
			remaining := time.Until(time.UnixMilli(int64(pg.request.DeadlineUnixMillis))) + 30*time.Millisecond
			if remaining > 0 {
				time.Sleep(remaining)
			}
		} else {
			peer.mu.Lock()
			peer.failReply = true
			peer.mu.Unlock()
			close(peer.release)
		}
		require.NoError(t, parent.Err(), "原Run父ctx仍活；只允许request child deadline/HTTP错误后的原对账")
		require.Eventually(t, func() bool { return !worker.executor.MatchInFlight(before, pg.request) }, 5*time.Second, 10*time.Millisecond)
		for range 3 {
			handled, _, err := worker.advanceServiceSafety(ctx, "")
			require.True(t, handled)
			require.Error(t, err)
		}
		pg.mu.Lock()
		queries, posts := pg.resizeQueries, pg.resizePosts
		pg.mu.Unlock()
		require.Greater(t, queries, queriesBefore, "原同Key只读Status不得被续租拒门饿死")
		require.Equal(t, 1, posts)
		require.Equal(t, old, oldRenewals())
		require.Equal(t, &pg.request, worker.client.Pending())
		require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizePG).Phase)
		return
	}
	if strings.HasSuffix(mode, "-upstream-expired") {
		old := oldRenewals()
		pg.mu.Lock()
		pg.leaseExpired = true
		pg.mu.Unlock()
		peer.mu.Lock()
		fences := peer.fenceCalls
		peer.mu.Unlock()
		handled, _, err := worker.advanceServiceSafetySelected(ctx, "", true)
		require.True(t, handled)
		require.ErrorIs(t, err, ErrServiceSafety)
		require.Equal(t, old, oldRenewals())
		peer.mu.Lock()
		require.Greater(t, peer.fenceCalls, fences, "过期须进入原Fence/关闭选择，不被专用入口饿死")
		peer.mu.Unlock()
		pending, err := store.Load(ctx)
		require.NoError(t, err)
		require.Nil(t, pending.State().ServiceClosure, "原Fence仍Pending不能伪造关闭WAL或PG完成")
		// 原Native实际发布后，HTTP仍在途；原Fence现可Fenced，外层可选原关闭WAL。
		pg.mu.Lock()
		pg.before.Observed.Operation.Phase = control.Committed
		pg.before.Observed.Operation.CancelClosed = true
		pg.before.Observed.CapacityGeneration++
		pg.before.Observed.LowBuffers = pg.request.TargetBuffers
		pg.before.Observed.HighBuffers = pg.request.TargetBuffers
		pg.before.Observed.Arrays[0].AllocationUnknown = false
		pg.mu.Unlock()
		handled, _, err = worker.advanceServiceSafetySelected(ctx, "", true)
		require.True(t, handled)
		if err != nil {
			require.ErrorIs(t, err, ErrServiceSafety)
		}
		require.Equal(t, old, oldRenewals())
		closed, err := store.Load(ctx)
		require.NoError(t, err)
		require.NotNil(t, closed.State().ServiceClosure, "原Fenced后必须能选择原关闭WAL")
		require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizePG).Phase)
		require.Equal(t, &pg.request, worker.client.Pending())
		pg.mu.Lock()
		require.Equal(t, 1, pg.resizePosts)
		pg.mu.Unlock()
		return
	}

	if strings.HasSuffix(mode, "-late-success") {
		old := oldRenewals()
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
		close(peer.release)
		require.Eventually(t, func() bool { return worker.client.Pending() == nil }, 5*time.Second, 10*time.Millisecond)
		handled, progress, err := worker.advanceServiceSafety(ctx, "")
		require.True(t, handled)
		require.Equal(t, pgexec.Recorded, progress)
		require.ErrorIs(t, err, ErrServiceSafety)
		require.Equal(t, old, oldRenewals())
		step := budgetStep(t, store, journal.ResizePG)
		require.Equal(t, journal.Settled, step.Phase)
		require.Equal(t, journal.Completed, step.Observation.Result)
		require.Equal(t, journal.Planned, budgetStep(t, store, journal.FinalizeBudget).Phase)
		require.Equal(t, journal.Planned, budgetStep(t, store, journal.ResumeAdmission).Phase)
		pg.mu.Lock()
		require.Equal(t, 1, pg.resizePosts)
		pg.mu.Unlock()
		return
	}
	old := oldRenewals()
	// 真正调用原generic出口，必须因Preparing/Unknown拒绝，不能靠关闭旧门或改common门。
	require.Error(t, worker.renewCurrentServiceAudit(ctx, before, shared))
	require.Equal(t, old, oldRenewals())
	handled, progress, err := worker.advanceServiceSafety(ctx, "")
	require.NoError(t, err, "新入口必须核所有当前原来源后实际续接")
	require.False(t, handled)
	require.Equal(t, pgexec.Idle, progress)
	require.Equal(t, 1, oldRenewals()-old, "旧service_safety接线在这里真实0续租，应RED")
	after, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), after.State(), "只审计/续原期限，不补Completed或清Unknown")
	require.Equal(t, shared, pg.shared())
	t.Log("actual original generic refused Preparing/Unknown; dedicated full-source Native CAS renewal=1; WAL unchanged")
	if mode == "pg-inflight-audit-complete" {
		// Native已发布目标，但唯一HTTP仍在途；原writer尚无原回复可消费。
		// 由明确peer切到committed，不手填WAL Completed或解除SDK Pending。
		peer.mu.Lock()
		peer.beforeMemory = func() {
			pg.mu.Lock()
			pg.before.Observed.Operation.Phase = control.Committed
			pg.before.Observed.Operation.CancelClosed = true
			pg.before.Observed.CapacityGeneration++
			pg.before.Observed.LowBuffers = pg.request.TargetBuffers
			pg.before.Observed.HighBuffers = pg.request.TargetBuffers
			pg.before.Observed.Arrays[0].AllocationUnknown = false
			pg.mu.Unlock()
		}
		peer.mu.Unlock()
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		old := oldRenewals()
		require.NoError(t, worker.inspectPGInflightService(ctx, snapshot))
		require.Equal(t, old+1, oldRenewals())
		next, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, snapshot.State(), next.State())
		require.Equal(t, &pg.request, worker.client.Pending())
		require.True(t, worker.executor.MatchInFlight(next, pg.request))
		return
	}
	check := func(name string, mutate func() func()) {
		t.Run(name, func(t *testing.T) {
			snapshot, err := store.Load(ctx)
			require.NoError(t, err)
			old := oldRenewals()
			require.True(t, worker.executor.MatchInFlight(snapshot, pg.request), "负测前必须仍是原活任务，不能靠过期假绿")
			require.NoError(t, validatePGInflightGrowUpper(proof.prepared, pg.shared(), session.Actor().Epoch, proof.prepared.Limits.MemoryMaxBytes), "负例起点不能是先前污染的成本快照")
			undo := mutate()
			defer func() {
				undo()
				require.NoError(t, validatePGInflightGrowUpper(proof.prepared, pg.shared(), session.Actor().Epoch, proof.prepared.Limits.MemoryMaxBytes), "负例复原须保持完整paid快照有效")
			}()
			err = worker.inspectPGInflightService(ctx, snapshot)
			require.Error(t, err)
			require.True(t, worker.executor.MatchInFlight(snapshot, pg.request), "负测后原任务仍活，拒绝应来自所测来源")
			require.Equal(t, old, oldRenewals(), "来源缺口不能续租")
			next, loadErr := store.Load(ctx)
			require.NoError(t, loadErr)
			require.Equal(t, snapshot.State(), next.State())
		})
	}
	rootChange := func(mutate func(*resourceguard.View)) func() func() {
		return func() func() {
			root.mu.Lock()
			saved := clonePGInflight(t, root.view)
			mutate(&root.view)
			root.mu.Unlock()
			return func() { root.mu.Lock(); root.view = saved; root.mu.Unlock() }
		}
	}
	if strings.HasSuffix(mode, "-root") {
		check("normal-hard-limit", rootChange(func(v *resourceguard.View) { v.Actual.MemoryMaxBytes++ }))
		check("target-guest-floor", rootChange(func(v *resourceguard.View) { v.Actual.GuestMemTotalBytes = proof.prepared.GuestUsableMemoryBytes - 1 }))
		check("root-floor", rootChange(func(v *resourceguard.View) { v.IssuerFloorEpoch++ }))
		check("root-partial", rootChange(func(v *resourceguard.View) { v.Applied = false }))
		check("PID-hard-limit", rootChange(func(v *resourceguard.View) { v.Actual.TaskLimit.MaxTasks++ }))
		check("system-placement", rootChange(func(v *resourceguard.View) { v.SystemPlacementObserved = false }))
		check("huge-pool", rootChange(func(v *resourceguard.View) { v.Actual.HugePools[0].Pages++ }))
	}
	if strings.HasSuffix(mode, "-sources") {
		check("missing-qualification", func() func() {
			saved := worker.qualification
			worker.qualification = nil
			return func() { worker.qualification = saved }
		})
		check("missing-safety", func() func() { saved := worker.safety; worker.safety = nil; return func() { worker.safety = saved } })
		t.Run("missing-safety-selector-still-fails-closed", func(t *testing.T) {
			saved := worker.safety
			worker.safety = nil
			defer func() { worker.safety = saved }()
			old := oldRenewals()
			handled, _, err := worker.advanceServiceSafety(ctx, "")
			require.True(t, handled)
			require.Error(t, err)
			require.Equal(t, old, oldRenewals())
		})

		replaceCM := func(ref budget.ObjectRef) func() func() {
			return func() func() {
				cm, err := core.CoreV1().ConfigMaps(session.binding.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
				require.NoError(t, err)
				bad := cm.DeepCopy()
				bad.UID = types.UID(string(cm.UID) + "-replaced")
				// 仅显式Kube peer模拟server-side重建的同名坏UID；不修改真实API immutable UID/RV门。
				gvr := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
				require.NoError(t, core.Tracker().Update(gvr, bad, session.binding.Namespace))
				return func() { require.NoError(t, core.Tracker().Update(gvr, cm, session.binding.Namespace)) }
			}
		}
		check("qualification-UID", replaceCM(source.Claim))
		check("Outside-UID", replaceCM(*proof.prepared.OutsideClaim))
		check("Host-UID", replaceCM(proof.vm.HostClaim))
		check("Runner-receipt-UID", replaceCM(proof.vm.Runner.ReceiptClaim))
		check("Runner-state-UID", replaceCM(proof.vm.Runner.StateClaim))
	}
	if strings.HasSuffix(mode, "-native-progress") {
		t.Run("Memory-progress-stays-paid-exact-single-renewal", func(t *testing.T) {
			snapshot, err := store.Load(ctx)
			require.NoError(t, err)
			require.True(t, worker.executor.MatchInFlight(snapshot, pg.request))
			old := oldRenewals()
			peer.mu.Lock()
			peer.beforeMemory = func() {
				pg.mu.Lock()
				pg.before.Observed.Arrays[0].CommittedUpperBytes += 1 << 20
				pg.before.CurrentMainAndArraysUpperBytes += 1 << 20
				pg.mu.Unlock()
			}
			peer.mu.Unlock()
			require.NoError(t, worker.renewAdmissionLease(ctx))
			require.Equal(t, 1, oldRenewals()-old, "完整专用出口只能一次exact续租，不得再generic第二次")
			require.True(t, worker.executor.MatchInFlight(snapshot, pg.request))
			shared = clonePGInflight(t, pg.shared())
			require.True(t, shared.Observed.Arrays[0].AllocationUnknown)
		})
		t.Run("different-actor-zero-renew-zero-query", func(t *testing.T) {
			saved := session.actor
			old := oldRenewals()
			pg.mu.Lock()
			queries := pg.resizeQueries
			pg.mu.Unlock()
			session.actor.Epoch++
			handled, _, err := worker.advanceServiceSafety(ctx, "")
			session.actor = saved
			require.True(t, handled)
			require.ErrorIs(t, err, ErrLeadershipLost)
			require.Equal(t, old, oldRenewals())
			pg.mu.Lock()
			require.Equal(t, queries, pg.resizeQueries)
			require.Equal(t, 1, pg.resizePosts)
			pg.mu.Unlock()
		})
		check("Memory-paid-overflow", func() func() {
			saved := clonePGInflight(t, pg.shared())
			peer.mu.Lock()
			peer.beforeMemory = func() {
				pg.mu.Lock()
				pg.before.Observed.Arrays[0].CommittedUpperBytes = 33 << 20
				pg.before.CurrentMainAndArraysUpperBytes = pg.before.StartupMainUpperBytes
				for _, a := range pg.before.Observed.Arrays {
					pg.before.CurrentMainAndArraysUpperBytes += a.CommittedUpperBytes
				}
				pg.mu.Unlock()
			}
			peer.mu.Unlock()
			return func() { pg.mu.Lock(); pg.before = saved; pg.mu.Unlock() }
		})
		check("Memory-cost-regression", func() func() {
			saved := clonePGInflight(t, pg.shared())
			peer.mu.Lock()
			peer.beforeMemory = func() {
				pg.mu.Lock()
				pg.before.Observed.Arrays[0].CommittedUpperBytes--
				pg.before.CurrentMainAndArraysUpperBytes--
				pg.mu.Unlock()
			}
			peer.mu.Unlock()
			return func() { pg.mu.Lock(); pg.before = saved; pg.mu.Unlock() }
		})
	}
	if strings.HasSuffix(mode, "-native-cas") {
		check("Native-observer-mismatch", func() func() {
			pg.beforeLeaseObserve = func() {
				pg.mu.Lock()
				pg.before.Observed.Arrays[0].CommittedUpperBytes += 4096
				pg.before.CurrentMainAndArraysUpperBytes += 4096
				pg.mu.Unlock()
			}
			return func() { pg.beforeLeaseObserve = nil; pg.mu.Lock(); pg.before = shared; pg.mu.Unlock() }
		})

		t.Run("batch-progress-during-audit-rejects-then-fresh-retries", func(t *testing.T) {
			old := oldRenewals()
			pg.beforeLeaseObserve = func() {
				pg.mu.Lock()
				pg.before.Observed.Arrays[0].CommittedUpperBytes += 1 << 20
				pg.before.CurrentMainAndArraysUpperBytes += 1 << 20
				pg.mu.Unlock()
			}
			snapshot, err := store.Load(ctx)
			require.NoError(t, err)
			require.Error(t, worker.inspectPGInflightService(ctx, snapshot))
			require.Equal(t, old, oldRenewals())
			pg.beforeLeaseObserve = nil
			next, err := store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, snapshot.State(), next.State())
			require.NoError(t, worker.inspectPGInflightService(ctx, next))
			require.Equal(t, old+1, oldRenewals())
			shared = clonePGInflight(t, pg.shared())
			require.True(t, shared.Observed.Arrays[0].AllocationUnknown)
		})
		// 恰在末helper独立Check的L2变更合法VM RV，仍必须由其后独立Load拒绝。
		gets := countAuthorityScopeGETs(t, api, session)
		t.Run("last-Lease2-VM-RV", func(t *testing.T) {
			old := oldRenewals()
			var leaseReads atomic.Int32
			pg.beforeLeaseObserve = func() {
				gets.setBeforeGET(func(path string) {
					if strings.Contains(path, "/leases/") && leaseReads.Add(1) == 2 {
						api.mu.Lock()
						api.vmVersion++
						api.vm.ResourceVersion = fmt.Sprintf("inflight-rv-%d", api.vmVersion)
						api.mu.Unlock()
					}
				})
			}
			defer func() { pg.beforeLeaseObserve = nil; gets.setBeforeGET(nil) }()
			snapshot, err := store.Load(ctx)
			require.NoError(t, err)
			err = worker.inspectPGInflightService(ctx, snapshot)
			require.Error(t, err)
			require.Equal(t, old, oldRenewals())
			require.GreaterOrEqual(t, leaseReads.Load(), int32(2))
		})
	}
	t.Run("missing-live-task-cannot-fall-back-partial", func(t *testing.T) {
		old := oldRenewals()
		pg.mu.Lock()
		queriesBefore := pg.resizeQueries
		pg.mu.Unlock()
		cancel()
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		require.False(t, worker.executor.MatchInFlight(snapshot, pg.request))
		handled, _, err := worker.advanceServiceSafety(ctx, "")
		require.True(t, handled)
		require.Error(t, err)
		require.Equal(t, old, oldRenewals())
		pg.mu.Lock()
		require.Equal(t, queriesBefore, pg.resizeQueries, "父Run取消不得继续原query/record")
		pg.mu.Unlock()
		require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizePG).Phase)
	})
	pg.mu.Lock()
	posts := pg.resizePosts
	unknown := pg.before.Observed.Arrays[0].AllocationUnknown
	pg.mu.Unlock()
	require.Equal(t, 1, posts, "所有审计/负测不得重发Resize")
	require.True(t, unknown)
	require.Equal(t, &pg.request, worker.client.Pending(), "任务取消不清SDK Unknown")
}

// 历史原件只提供ABI6释放元数据；主段/normal/Outside为既有显式model预算，
// 不从Memory响应按差值或常数补实际SharedCost来源，不冒充fresh资格或Linux证明。
func TestPGInflightGrowHistoricalVerifiedReleaseMetadataSurvivesRegrow(t *testing.T) {
	var h struct {
		Historical bool                `json:"historical"`
		NotFresh   bool                `json:"not_fresh_qualification"`
		SHA        string              `json:"original_response_SHA256"`
		Scope      string              `json:"scope"`
		Instance   control.Instance    `json:"instance"`
		Floor      control.Uint64      `json:"issuer_floor_epoch"`
		Generation control.Uint64      `json:"capacity_generation"`
		High       uint32              `json:"high_buffers"`
		Arrays     []control.ArrayFact `json:"arrays"`
	}
	raw, err := os.ReadFile("testdata/pg-inflight-cap3-verified-release-historical.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &h))
	require.True(t, h.Historical && h.NotFresh)
	require.Equal(t, "8caa90623c0301027c729123c64233a4ce5195986bd8f04c53769305e9c7b3c4", h.SHA)
	require.Equal(t, control.Uint64(3), h.Generation)
	require.Equal(t, uint32(256), h.High)
	p, _ := initialPGInflightUpper(t)
	p.Request.Key.Instance = h.Instance
	p.Request.Key.IssuerEpoch = h.Floor
	p.Request.ExpectedCapacityGeneration = h.Generation
	p.Identity.VMUID = h.Instance.VMUID
	p.Identity.ComputeInstanceID = h.Instance.ComputeInstanceID
	p.Shared.Observed.Instance = h.Instance
	p.Shared.Observed.IssuerFloorEpoch = h.Floor
	p.Shared.Observed.CapacityGeneration = h.Generation
	p.Shared.Observed.LowBuffers = h.High
	p.Shared.Observed.HighBuffers = h.High
	p.Shared.Observed.Arrays = clonePGInflight(t, h.Arrays)
	p.Shared.CurrentMainAndArraysUpperBytes = p.Shared.StartupMainUpperBytes
	for _, a := range p.Shared.Observed.Arrays {
		require.Equal(t, control.ReleaseVerified, a.ReleaseState)
		require.Equal(t, control.ProviderOK, a.Provider.Status)
		p.Shared.CurrentMainAndArraysUpperBytes += a.CommittedUpperBytes
	}
	p.TargetSharedUpperBytes, err = budget.GrowSharedUpper(p.Shared, p.Request.TargetBuffers)
	require.NoError(t, err)
	current := clonePGInflight(t, p.Shared)
	hash, err := p.Request.ContextSHA256()
	require.NoError(t, err)
	current.Observed.Operation = &control.OperationFact{Key: p.Request.Key, Kind: control.Grow, Phase: control.Preparing, Reason: control.ReasonNone, TargetBuffers: p.Request.TargetBuffers, ExpectedCapacityGeneration: h.Generation, ContextSHA256: hash}
	current.Observed.Arrays[0].CommittedUpperBytes += 4096
	current.CurrentMainAndArraysUpperBytes += 4096
	current.Observed.Arrays[0].AllocationUnknown = true
	require.NoError(t, validatePGInflightGrowUpper(p, current, h.Floor, p.Limits.MemoryMaxBytes))
	for i, a := range current.Observed.Arrays {
		require.Equal(t, h.Arrays[i].Provider, a.Provider)
		require.Equal(t, h.Arrays[i].ReleaseKey, a.ReleaseKey)
		require.Equal(t, h.Arrays[i].ReleaseCapacityGeneration, a.ReleaseCapacityGeneration)
	}
	require.True(t, current.Observed.Arrays[0].AllocationUnknown)
}
