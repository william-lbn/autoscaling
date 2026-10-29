package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 仅新增实际/旧容量组合调用。原开放仍由原peer严格逐字重放；关闭peer独立
// 核原WAL，不能靠放宽旧setter接受任意revision掩盖错误。Kube/PG为显式
// 合同夹具，owner/预算来源审计/SDK/CAS执行实际代码，不称Linux物理验收。
func (p *committedAbortPG) serveCleanupClose(t *testing.T, w http.ResponseWriter, r *http.Request, store *journal.Store, snapshot control.Snapshot) {
	t.Helper()
	var request control.AdmissionRequest
	var original *control.AdmissionRequest
	if strings.Contains(r.URL.Path, "close-original") {
		var native control.AdmissionCloseRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&native))
		require.NoError(t, native.Validate())
		request, original = native.Close, &native.Original
	} else {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
	}
	s, err := store.Load(r.Context())
	require.NoError(t, err)
	c := s.State().ServiceClosure
	require.NotNil(t, c)
	require.Equal(t, c.Request, request)
	require.Equal(t, c.Original, original)
	require.False(t, request.Open)
	query := strings.HasSuffix(r.URL.Path, "/status")
	p.mu.Lock()
	defer p.mu.Unlock()
	if query {
		p.queries++
		require.Equal(t, &request, p.last)
	} else {
		require.Equal(t, journal.Unconfirmed, c.Phase)
		current := snapshot.AdmissionRevision
		if p.last != nil {
			current = p.last.Revision
		}
		require.Greater(t, request.Revision, current)
		p.posts++
		closed := request
		p.last, p.closedOverride, p.allowNext = &closed, false, false
	}
	snapshot.AdmissionOpen, snapshot.AdmissionRevision = false, request.Revision
	snapshot.IssuerFloorEpoch = request.IssuerEpoch
	hash, err := request.ContextSHA256()
	require.NoError(t, err)
	disposition := control.AdmissionApplied
	if query {
		disposition = control.AdmissionReplayed
	}
	fact := control.AdmissionResponse{ProtocolVersion: 1, Request: request, ContextSHA256: hash, Disposition: disposition, Observed: snapshot, VMShrinkPermit: false}
	if original != nil {
		pgReply(w, control.AdmissionClosure{ProtocolVersion: 1, Original: *original, ClosedBy: fact, PriorMayHaveApplied: true, VMShrinkPermit: false})
	} else {
		pgReply(w, fact)
	}
}

// 调用前资源已由原生产链明确，Cleanup由真实取消/恢复入口创建。
// 未知/完成、只读/发送均保留；旧容量必须继续引用旧完整预算而非新目标。
func verifyCapacityCleanupSourceFailure(t *testing.T, ctx context.Context, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, client *kubefake.Clientset, root *guestRootFixture, pg *committedAbortPG, shared control.SharedCostResponse, expectedBudget string, readonly, settled bool) {
	t.Helper()
	before, err := store.Load(ctx)
	require.NoError(t, err)
	boundary, err := before.State().CleanupServiceForClosure()
	require.NoError(t, err)
	require.Equal(t, expectedBudget, boundary.BudgetCommitmentID)
	require.Equal(t, readonly, boundary.Admission == nil)
	phase := journal.Unconfirmed
	if settled {
		phase = journal.Settled
	}
	require.Equal(t, phase, before.State().Active.Cleanup.Phase)
	require.Nil(t, before.State().ServiceClosure, "健康原预算不能误关合法中间容量")
	require.NoError(t, worker.inspectCleanupService(ctx, before), "原实际/旧容量来源健康时检查必须通过")
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	restore := func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes--; root.mu.Unlock() }
	defer func() { restore() }()
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	after, err := store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, after.State().ServiceClosure)
	require.False(t, after.State().ServiceClosure.Pending())
	require.Equal(t, before.State().Active, after.State().Active)
	require.Equal(t, before.State().BudgetHeld, after.State().BudgetHeld)
	require.Equal(t, before.State().CapacityHeld, after.State().CapacityHeld)
	require.Equal(t, before.State().History, after.State().History)
	require.Equal(t, expectedBudget, after.State().ServiceClosure.Request.BudgetCommitmentID)
	require.Equal(t, boundary.CapacityGeneration, after.State().ServiceClosure.Request.ExpectedCapacityGeneration)
	require.False(t, pg.shared(shared).Observed.AdmissionOpen)
	// 恢复Root、重建实际SDK后，旧Cleanup必须继续审计保留且零setter。
	restore()
	restore = func() {}
	worker.CloseIdleConnections()
	rebuilt, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
	require.NoError(t, err)
	defer rebuilt.CloseIdleConnections()
	pending, closing := rebuilt.client.PendingAdmission()
	require.Nil(t, pending)
	require.Nil(t, closing)
	pg.mu.Lock()
	posts := pg.posts
	pg.mu.Unlock()
	_, err = rebuilt.Advance(ctx)
	require.ErrorIs(t, err, ErrServiceSafety)
	_, err = rebuilt.ReplayCommittedAbortOriginal(ctx, before.State().Active.Request.Key)
	require.Error(t, err)
	_, err = rebuilt.RecoverUnchangedAdmission(ctx, before.State().Active.Request.Key)
	require.Error(t, err)
	_, err = rebuilt.RecoverCapacityAdmission(ctx, before.State().Active.Request.Key)
	require.Error(t, err)
	pg.mu.Lock()
	require.Equal(t, posts, pg.posts)
	pg.mu.Unlock()
}
