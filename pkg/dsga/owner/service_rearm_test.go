package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 仅PG原生期限为peer，正式Elector/失租/接管、完整首次预算/来源、原关闭
// 接续、管理Run、SDK和真实typed UID/RV CAS均走生产代码。真实PG另有Neon
// 独立用例；本组合不声称Linux物理或K8s端到端验收。
func TestProductionServiceRearmPreservesClosedBusinessAcrossCrashWindows(t *testing.T) {
	for _, mode := range []string{"normal", "lost-start", "setter-before", "setter-after", "lost-record", "source-unavailable", "source-after-selection"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "closure-transfer-rearm-"+mode) })
	}
}

type serviceRearmPG struct {
	epoch        control.Uint64
	original     *control.AdmissionLeaseRequest
	applied      control.Uint64
	posts        int
	queries      int
	loseBefore   bool
	loseAfter    bool
	loseRecord   bool
	api          *ownerAPI
	afterObserve func()
	afterApply   func()
}

func (p *initialServicePG) serveRearmFixture(t *testing.T, w http.ResponseWriter, request *http.Request) bool {
	t.Helper()
	p.mu.Lock()
	f := p.rearmFixture
	p.mu.Unlock()
	if f == nil || request.URL.Path != "/dsg/v1/admission/lease" && request.URL.Path != "/dsg/v1/admission/lease/rearm/status" {
		return false
	}
	var q control.AdmissionLeaseRequest
	require.NoError(t, json.NewDecoder(request.Body).Decode(&q))
	s, err := p.store.Load(request.Context())
	require.NoError(t, err)
	p.mu.Lock()
	defer p.mu.Unlock()
	query := strings.HasSuffix(request.URL.Path, "/status")
	if query {
		f.queries++
	} else if q.Action == "rearm" {
		c := s.State().ServiceClosure
		if c.Reopen != nil && c.Reopen.Rearm != nil {
			r := c.Reopen.Rearm
			require.Equal(t, q, r.Request, "再关闭恢复必须先确认完整写前WAL")
			require.Equal(t, journal.Unconfirmed, r.Phase)
		} else {
			r := c.Rearm
			require.NotNil(t, r, "真实写前WAL必须先于rearm")
			require.Equal(t, q, r.Request)
			require.Equal(t, journal.Unconfirmed, r.Phase)
		}
		f.posts++
		if f.loseBefore {
			f.loseBefore = false
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}
	}
	current := p.current().Observed
	renewed := !query && q.Action == "rearm" && p.leaseExpired && q.ExpectedDeadlineNS == p.leaseDeadline && q.IssuerEpoch > f.epoch && q.IssuerEpoch == current.IssuerFloorEpoch && q.ExpectedCapacityGeneration == current.CapacityGeneration && q.ExpectedAdmissionRevision == current.AdmissionRevision && !current.AdmissionOpen
	if renewed {
		original := q
		f.original, f.epoch = &original, q.IssuerEpoch
		p.leaseExpired = false
		p.leaseDeadline++
		f.applied = p.leaseDeadline
		if f.afterApply != nil {
			hook := f.afterApply
			f.afterApply = nil
			hook()
		}
	}
	if !query && q.Action == "rearm" && f.loseAfter {
		f.loseAfter = false
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}
	leaseRenewed := !query && q.Action == "renew" && !p.leaseExpired && q.ExpectedDeadlineNS == p.leaseDeadline && q.ExpectedCapacityGeneration == current.CapacityGeneration && q.ExpectedAdmissionRevision == current.AdmissionRevision && q.IssuerEpoch == current.IssuerFloorEpoch
	if leaseRenewed {
		p.leaseDeadline++
		p.leaseRenewals++
	}
	fact := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: q, Renewed: renewed || leaseRenewed, Armed: true, Expired: p.leaseExpired, DeadlineNS: p.leaseDeadline, LeaseEpoch: f.epoch, TimeoutMS: 30000, Observed: p.current().Observed}
	if !query && q.Action == "observe" && f.afterObserve != nil {
		hook := f.afterObserve
		f.afterObserve = nil
		hook()
	}
	if query {
		retained := f.original != nil && *f.original == q
		applied := control.Uint64(0)
		if retained {
			applied = f.applied
			if f.loseRecord {
				f.api.mu.Lock()
				f.api.loseJournalReply = true
				f.api.mu.Unlock()
				f.loseRecord = false
			}
		}
		pgReply(w, control.AdmissionRearmStatus{ProtocolVersion: 1, VMShrinkPermit: false, Request: q, Retained: retained, AppliedDeadlineNS: applied, Current: fact})
	} else {
		pgReply(w, fact)
	}
	return true
}

func verifyServiceRearm(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture) {
	t.Helper()
	before, err := store.Load(ctx)
	require.NoError(t, err)
	closure := before.State().ServiceClosure
	_, _, _, selector := closure.Selection()
	pg.mu.Lock()
	pg.leaseDeadline, pg.leaseExpired = 100, true
	pg.rearmFixture = &serviceRearmPG{epoch: session.Actor().Epoch - 1, original: nil, applied: 0, posts: 0, queries: 0, loseBefore: mode == "setter-before", loseAfter: mode == "setter-after", loseRecord: mode == "lost-record", api: api, afterObserve: nil, afterApply: nil}
	if mode == "source-after-selection" {
		pg.rearmFixture.afterObserve = func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes++; root.mu.Unlock() }
	}
	pg.mu.Unlock()
	if mode == "complete-lifecycle" {
		verifyExpiredServiceLifecycle(t, ctx, api, session, resources, worker, config, core, source, pg, root)
		return
	}
	if mode == "lost-start" {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	if mode == "source-unavailable" {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
		_, err := worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationRearmService, selector))
		require.ErrorIs(t, err, ErrServiceSafety)
		untouched, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), untouched.State())
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
	}
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationRearmService, selector))
	if mode == "normal" || mode == "source-unavailable" {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
	}
	s, err := store.Load(ctx)
	require.NoError(t, err)
	r := s.State().ServiceClosure.Rearm
	require.NotNil(t, r)
	pg.mu.Lock()
	posts := pg.rearmFixture.posts
	pg.mu.Unlock()
	if mode == "source-after-selection" {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
	}
	if mode == "lost-start" || mode == "source-after-selection" {
		require.Zero(t, posts)
	} else {
		require.Equal(t, 1, posts)
	}
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	endpoint := worker.NewControlEndpoint()
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(runCtx, PGLoopOptions{PollInterval: 10 * time.Millisecond, Control: endpoint, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: nil})
	}()
	defer func() { cancel(); require.ErrorIs(t, <-done, context.Canceled) }()
	if mode == "lost-start" || mode == "setter-before" || mode == "source-after-selection" {
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationObserveServiceRearm, r.CommandSHA256))
		require.ErrorIs(t, err, ErrServiceSafety)
		pg.mu.Lock()
		require.Equal(t, posts, pg.rearmFixture.posts)
		pg.mu.Unlock()
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceRearm, r.CommandSHA256))
		require.NoError(t, err)
		posts++
	}
	result, err := endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
	require.NoError(t, err)
	// 读状态不要求执行新setter。setter-after由普通只读Run查询恢复；lost-record
	// 已落账，无须重新写入。等待有界真实状态，不用sleep推断完成。
	require.Eventually(t, func() bool {
		result, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
		require.NoError(t, err)
		return result.State.ServiceClosure.Rearm.Phase == journal.Settled
	}, 5*time.Second, 10*time.Millisecond)
	retained := *result.State.ServiceClosure
	retained.Rearm = nil
	require.Equal(t, *closure, retained)
	require.Equal(t, before.State().InitialHeld, result.State.InitialHeld)
	require.False(t, pg.shared().Observed.AdmissionOpen)
	pg.mu.Lock()
	require.Equal(t, posts, pg.rearmFixture.posts)
	require.False(t, pg.leaseExpired)
	pg.mu.Unlock()
	require.Nil(t, worker.client.Pending())
	pending, closing := worker.client.PendingAdmission()
	require.Nil(t, pending)
	require.Nil(t, closing)
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReplayInitial, before.State().InitialHeld.Admission.CommandSHA256))
	require.ErrorIs(t, err, ErrServiceSafety, "原生恢复不能绕过业务关闭")
	pg.mu.Lock()
	pg.leaseExpired = true
	pg.mu.Unlock()
	// 后续再次过期不能自动生成第二个rearm或替换原deadline。
	result, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
	require.NoError(t, err)
	require.Equal(t, r.Request, result.State.ServiceClosure.Rearm.Request)
	pg.mu.Lock()
	require.Equal(t, posts, pg.rearmFixture.posts)
	pg.mu.Unlock()
}
