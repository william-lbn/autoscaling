package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 新组合只执行首次预算前驱。正式Session/SDK/TLS/typed UID-RV与WAL走
// 生产代码，PG/Root/Kube/Runner为显式peer；内核关闭另有真实PG回归。
func TestProductionInitialUnknownSourceFailureClosesWithoutReopening(t *testing.T) {
	for _, mode := range []string{"unreceived", "applied", "lost-start", "lost-close", "qualification", "qualification-applied", "transfer", "post-send-source-drift"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "initial-safety-"+mode) })
	}
}

func (p *initialServicePG) serveInitialClosure(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	var request control.AdmissionCloseRequest
	require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
	s, err := p.store.Load(r.Context())
	require.NoError(t, err)
	c := s.State().ServiceClosure
	require.NotNil(t, c)
	require.Equal(t, &request.Original, c.Original)
	require.Equal(t, request.Close, c.Request, "完整原开放与关闭写前CAS必须早于派发")
	query := strings.HasSuffix(r.URL.Path, "/status")
	p.mu.Lock()
	defer p.mu.Unlock()
	if query {
		p.queries++
		if p.gate == nil || *p.gate != request.Close {
			w.WriteHeader(http.StatusConflict)
			return
		}
	} else {
		require.Equal(t, journal.Unconfirmed, c.Phase)
		require.NoError(t, request.Validate())
		p.posts++
		if p.loseBefore {
			p.loseBefore = false
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		require.Greater(t, request.Close.Revision, p.current().Observed.AdmissionRevision)
		closed := request.Close
		p.gate = &closed
		if p.loseAfter {
			p.loseAfter = false
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	}
	digest, err := request.Close.ContextSHA256()
	require.NoError(t, err)
	disposition := control.AdmissionApplied
	if query {
		disposition = control.AdmissionReplayed
	}
	fact := control.AdmissionResponse{ProtocolVersion: 1, Request: request.Close, Disposition: disposition, ContextSHA256: digest, Observed: p.current().Observed, VMShrinkPermit: false}
	pgReply(w, control.AdmissionClosure{ProtocolVersion: 1, Original: request.Original, ClosedBy: fact, PriorMayHaveApplied: true, VMShrinkPermit: false})
}

func verifyInitialServiceSafety(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, client *kubefake.Clientset, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, transfer *businessOwnerTransfer) {
	t.Helper()
	mode = strings.TrimPrefix(mode, "initial-safety-")
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	if mode == "post-send-source-drift" {
		pg.afterOpen = func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes++; root.mu.Unlock() }
		defer func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes--; root.mu.Unlock() }()
		progress, err = worker.Advance(ctx)
		require.NoError(t, err, "原setter后审计失败须在同轮次转入独立关闭")
		require.Equal(t, pgexec.Recorded, progress)
		s, err := store.Load(ctx)
		require.NoError(t, err)
		require.NotNil(t, s.State().ServiceClosure)
		require.False(t, s.State().ServiceClosure.Pending())
		require.Equal(t, journal.Unconfirmed, s.State().InitialHeld.Admission.Phase)
		require.Empty(t, s.State().InitialHeld.Admission.Receipt)
		require.False(t, pg.shared().Observed.AdmissionOpen)
		posts, _, resizes := pg.counts()
		require.Equal(t, 2, posts, "只发原开放和一次关闭，不重发原开放")
		require.Zero(t, resizes)
		return
	}
	applied := mode == "applied" || mode == "lost-close" || mode == "qualification-applied"
	pg.faults(!applied, applied)
	_, err = worker.Advance(ctx)
	require.Error(t, err)
	before, err := store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, before.State().ServiceClosure, "健康来源上仅保留原首次未知")
	originalHeld := before.State().InitialHeld
	require.Equal(t, journal.Unconfirmed, originalHeld.Admission.Phase)
	require.Equal(t, applied, pg.shared().Observed.AdmissionOpen)
	var original budget.InitialAdmissionCommand
	require.NoError(t, json.Unmarshal(originalHeld.Admission.Command, &original))
	var restore func()
	if strings.HasPrefix(mode, "qualification") {
		cm, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, source.Claim.Name, metav1.GetOptions{})
		require.NoError(t, err)
		changed := cm.DeepCopy()
		stamp := metav1.Now()
		changed.DeletionTimestamp = &stamp
		require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
		restore = func() {
			require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
		}
	} else {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
		restore = func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes--; root.mu.Unlock() }
	}
	defer restore()
	if mode == "lost-start" || mode == "transfer" {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	if mode == "lost-close" {
		pg.faults(false, true)
	}
	posts, _, _ := pg.counts()
	progress, err = worker.Advance(ctx)
	switch mode {
	case "lost-start", "transfer":
		var unknown *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &unknown)
	case "lost-close":
		require.ErrorIs(t, err, ErrServiceSafety)
	default:
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
	}
	s, err := store.Load(ctx)
	require.NoError(t, err)
	c := s.State().ServiceClosure
	require.NotNil(t, c)
	require.Equal(t, originalHeld, s.State().InitialHeld)
	require.Equal(t, original.Request, *c.Original)
	require.Equal(t, original.Request.Revision+1, c.Request.Revision)
	if mode == "lost-start" || mode == "transfer" {
		after, _, _ := pg.counts()
		require.Equal(t, posts, after, "CAS丢回复零派发")
	}
	if mode == "transfer" {
		session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, client, source)
		result, err := worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationContinueServiceClosure, c.CommandSHA256))
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, result.Progress)
		require.Equal(t, original.Request, result.State.ServiceClosure.Continuation.Request.Original)
	} else {
		worker.CloseIdleConnections()
		worker, err = session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		if c.Pending() {
			pending, closing := worker.client.PendingAdmission()
			require.Equal(t, c.Original, pending)
			require.Equal(t, &c.Request, closing)
			progress, err = worker.Advance(ctx)
			if mode == "lost-start" {
				require.ErrorIs(t, err, ErrServiceSafety)
				after, _, _ := pg.counts()
				require.Equal(t, posts, after, "NotFound不重发")
				result, err := worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceClosure, c.CommandSHA256))
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, result.Progress)
			} else {
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
			}
		}
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.False(t, s.State().ServiceClosure.Pending())
	require.False(t, pg.shared().Observed.AdmissionOpen)
	require.Equal(t, originalHeld, s.State().InitialHeld)
	require.Nil(t, s.State().Active)
	require.Empty(t, s.State().History)
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	pending, closing := worker.client.PendingAdmission()
	require.Nil(t, pending)
	require.Nil(t, closing, "完整关闭后重建不得重新恢复旧开放SDK栅栏")
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, ErrServiceSafety)
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReplayInitial, originalHeld.Admission.CommandSHA256))
	require.ErrorIs(t, err, ErrServiceSafety)
	_, _, resizes := pg.counts()
	require.Zero(t, resizes)
}
