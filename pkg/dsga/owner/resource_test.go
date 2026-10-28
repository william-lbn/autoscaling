package owner

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
)

// 真实本地TLS和typed Store/Lease CAS，root和Kube API明确是协议夹具；不证明
// TokenReview、RBAC、cgroup、Linux内核或完整预算。RootFence处理器实际读取原
// Store，验证资源身份写前持久化先于任何root代次推进，不注入假Dispatch票据。
type resourceOwnerFixture struct {
	mu          sync.Mutex
	view        resourceguard.View
	discoveries int
	fences      int
	repairs     int
	dropRepair  bool
	store       *journal.Store
}

func newResourceOwnerFixture(t *testing.T, store *journal.Store) (*resourceOwnerFixture, resourceguard.ClientConfig) {
	t.Helper()
	id := resourceguard.Identity{VMUID: string(binding().VMUID), ComputeInstanceID: "compute-1", GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
	pools := []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 8}}
	limits := resourceguard.Limits{MemoryMaxBytes: 64 << 20, OutsideUpperBytes: 32 << 20, HugePools: pools}
	f := &resourceOwnerFixture{store: store, view: resourceguard.View{ProtocolVersion: 1, Identity: id, IssuerFloorEpoch: 1, Bootstrap: resourceguard.BootstrapCommand{ProtocolVersion: 1, Identity: id, IssuerEpoch: 1, BudgetCommitmentID: "bootstrap-budget", Limits: limits}, BootstrapApplied: true, Actual: resourceguard.Evidence{ProtocolVersion: 1, Identity: id, MemoryMaxBytes: 64 << 20, MemoryCurrentBytes: 4 << 20, GuestMemTotalBytes: 256 << 20, HugePools: pools, WholePoolUpperBytes: 16 << 20, HugeOvercommitDisabled: true}}}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer resource-owner-fixture" {
			t.Error("owner credential missing")
			w.WriteHeader(401)
			return
		}
		if r.URL.Path == "/dsg/v1/resource/fence" {
			var command resourceguard.FenceRequest
			if json.NewDecoder(r.Body).Decode(&command) != nil {
				w.WriteHeader(400)
				return
			}
			snapshot, err := store.Load(r.Context())
			if err != nil || snapshot.State().ResourceIdentity == nil || *snapshot.State().ResourceIdentity != command.Identity {
				t.Error("root fence before durable original Guest binding")
				w.WriteHeader(409)
				return
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if command.IssuerEpoch < f.view.IssuerFloorEpoch {
				w.WriteHeader(409)
				return
			}
			f.fences++
			f.view.IssuerFloorEpoch = command.IssuerEpoch
			pgReply(w, command.Identity)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/dsg/v1/resource/identity":
			f.discoveries++
			pgReply(w, f.view.Identity)
		case "/dsg/v1/resource/state":
			pgReply(w, f.view)
		case "/dsg/v1/resource/repair-held":
			// 这里只验证真实Owner/SDK的选择和未知回复恢复，不把此
			// HTTP夹具的赋值当内核修复。Guard文件锁/单调门槛另有实测。
			var request resourceguard.HeldRepairRequest
			digest, err := resourceguard.HeldSourceSHA256(f.view)
			if json.NewDecoder(r.Body).Decode(&request) != nil || err != nil || request.SourceSHA256 != digest || request.Identity != f.view.Identity || request.IssuerEpoch != f.view.IssuerFloorEpoch {
				w.WriteHeader(http.StatusConflict)
				return
			}
			f.repairs++
			f.view.Actual.MemoryMaxBytes = f.view.Bootstrap.Limits.MemoryMaxBytes
			if f.dropRepair {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			pgReply(w, f.view.Actual)
		default:
			t.Error("unexpected resource action", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(server.Close)
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	config := resourceguard.ClientConfig{BaseURL: server.URL, Identity: resourceguard.Identity{VMUID: id.VMUID, ComputeInstanceID: id.ComputeInstanceID}, Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}, Token: func(context.Context) (string, error) { return "resource-owner-fixture", nil }}
	return f, config
}

func TestResourceOwnerPersistsGuestBeforeFenceAndNeverRediscoversBound(t *testing.T) {
	_, store, coord := newAPI(t)
	f, config := newResourceOwnerFixture(t, store)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		worker, err := session.NewResourceWorker(ctx, config)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		require.NotNil(t, snapshot.State().ResourceIdentity)
		before := snapshot.State().Sequence
		second, err := session.NewResourceWorker(ctx, config)
		require.NoError(t, err)
		defer second.CloseIdleConnections()
		snapshot, err = store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before, snapshot.State().Sequence)
		f.mu.Lock()
		discoveries, fences, id := f.discoveries, f.fences, f.view.Identity
		f.mu.Unlock()
		require.Equal(t, 1, discoveries)
		require.Equal(t, 2, fences)
		wrong := config
		wrong.Identity = id
		wrong.Identity.CgroupInode++
		_, err = session.NewResourceWorker(ctx, wrong)
		require.Error(t, err)
		f.mu.Lock()
		f.view.IssuerFloorEpoch = session.Actor().Epoch + 1
		f.mu.Unlock()
		_, err = worker.Inspect(ctx)
		require.Error(t, err, "new resource owner floor cannot be ignored")
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestResourceOwnerRejectsOrphanEffectsBeforeBindingOrFence(t *testing.T) {
	_, store, coord := newAPI(t)
	f, config := newResourceOwnerFixture(t, store)
	c := resourceguard.Command{ProtocolVersion: 1, Identity: f.view.Identity, IssuerEpoch: 1, Revision: 1, BudgetCommitmentID: "orphan-root-budget", PostmasterPID: 123, PostmasterStartTicks: 456, MemoryMaxBytes: 64 << 20, OutsideUpperBytes: 32 << 20, HugePools: f.view.Bootstrap.Limits.HugePools}
	f.view.Original = &c
	f.view.Held = &c
	f.view.Applied = true
	require.NoError(t, f.view.Validate())
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		_, err := session.NewResourceWorker(ctx, config)
		require.Error(t, err)
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		require.Nil(t, snapshot.State().ResourceIdentity)
		f.mu.Lock()
		fences := f.fences
		f.mu.Unlock()
		require.Equal(t, 0, fences)
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestResourceOwnerPGDispatchRequiresKnownCapsButOriginalQueryRemainsReachable(t *testing.T) {
	_, store, coord := newAPI(t)
	f, resourceConfig := newResourceOwnerFixture(t, store)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceWorker(ctx, resourceConfig)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		request := pgRequest(t, session.Actor())
		var writes, observes atomic.Int32
		config := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/dsg/v1/resize":
				writes.Add(1)
			case "/dsg/v1/resize/status":
				observes.Add(1)
			default:
				t.Error("unexpected PG action", r.URL.Path)
			}
			w.WriteHeader(500)
		})
		_, err = session.NewPGWorker(ctx, config)
		require.Error(t, err, "bound resource domain must not be omitted")
		worker, err := session.NewPGWorker(ctx, config, resources)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		// 明确的前驱领域夹具，只用于抵达实际PG派发门槛，不证明完整预算/排空。
		beginPG(t, store, request)
		unknown := resourceguard.Command{ProtocolVersion: 1, Identity: f.view.Identity, IssuerEpoch: session.Actor().Epoch, Revision: 1, BudgetCommitmentID: request.BudgetCommitmentID, PostmasterPID: 123, PostmasterStartTicks: 456, MemoryMaxBytes: 64 << 20, OutsideUpperBytes: 32 << 20, HugePools: f.view.Bootstrap.Limits.HugePools}
		f.mu.Lock()
		f.view.Original = &unknown
		f.view.Applied = false
		f.mu.Unlock()
		_, err = worker.Advance(ctx)
		require.ErrorIs(t, err, resourceguard.ErrUnknown)
		require.EqualValues(t, 0, writes.Load())
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		for _, step := range snapshot.State().Active.Steps {
			if step.Action == journal.ResizePG {
				require.Equal(t, journal.Planned, step.Phase)
			}
		}
		f.mu.Lock()
		f.view.Original = nil
		f.view.Applied = false
		f.mu.Unlock()
		_, err = worker.Advance(ctx)
		require.Error(t, err, "lost/unconfirmed PG response remains an error")
		require.EqualValues(t, 1, writes.Load())
		f.mu.Lock()
		f.view.Original = &unknown
		f.mu.Unlock()
		_, err = worker.Advance(ctx)
		require.Error(t, err)
		require.EqualValues(t, 1, writes.Load())
		require.EqualValues(t, 1, observes.Load(), "resource unknown must not hide original PG query")
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestResourceOwnerLostBindingReplyDoesNotDispatchFenceOrRediscover(t *testing.T) {
	api, store, coord := newAPI(t)
	f, config := newResourceOwnerFixture(t, store)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
		_, err := session.NewResourceWorker(ctx, config)
		var unknown *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &unknown)
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		require.NotNil(t, snapshot.State().ResourceIdentity)
		f.mu.Lock()
		fences := f.fences
		f.mu.Unlock()
		require.Zero(t, fences, "lost CAS reply is not a dispatch ticket")
		restored, err := session.NewResourceWorker(ctx, config)
		require.NoError(t, err)
		defer restored.CloseIdleConnections()
		f.mu.Lock()
		discoveries, fences := f.discoveries, f.fences
		f.mu.Unlock()
		require.Equal(t, 1, discoveries, "retained original Guest must be used after reply loss")
		require.Equal(t, 1, fences)
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestResourceOwnerRestoresFullUnknownFromJournalInsteadOfClearing(t *testing.T) {
	_, store, coord := newAPI(t)
	f, config := newResourceOwnerFixture(t, store)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		worker, err := session.NewResourceWorker(ctx, config)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		request := pgRequest(t, session.Actor())
		request.Kind = control.Grow
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		snapshot, _, err = store.Begin(ctx, snapshot, session.Actor(), request, 0)
		require.NoError(t, err)
		original := resourceguard.Command{ProtocolVersion: 1, Identity: f.view.Identity, IssuerEpoch: session.Actor().Epoch, Revision: 1, BudgetCommitmentID: request.BudgetCommitmentID, PostmasterPID: 123, PostmasterStartTicks: 456, MemoryMaxBytes: 64 << 20, OutsideUpperBytes: 32 << 20, HugePools: f.view.Bootstrap.Limits.HugePools}
		for _, step := range snapshot.State().Active.Steps {
			if step.Action == journal.GuestReady {
				raw, _ := json.Marshal(original)
				_, _, err = store.Start(ctx, snapshot, session.Actor(), request.Key, step.Action, original.Identity.ComputeInstanceID, raw)
				require.NoError(t, err)
				break
			}
			// 仅为抵达本测试的原资源WAL边界，前驱是明确协议夹具，非实际VM预算。
			var dispatch *journal.Dispatch
			snapshot, dispatch, err = store.Start(ctx, snapshot, session.Actor(), request.Key, step.Action, "test-only-prior-domain", []byte(`{"fixture":true}`))
			require.NoError(t, err)
			snapshot, err = store.Record(ctx, snapshot, session.Actor(), request.Key, journal.Observation{Key: request.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256, Ordinal: dispatch.Step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: dispatch.Step.TargetID, Data: []byte(`{"fixture":true}`)})
			require.NoError(t, err)
		}
		f.mu.Lock()
		f.view.Original = &original
		f.view.Applied = false
		f.mu.Unlock()
		restored, err := session.NewResourceWorker(ctx, config)
		require.NoError(t, err)
		defer restored.CloseIdleConnections()
		require.Equal(t, &original, restored.client.Pending(), "unknown original must survive worker reconstruction")
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestResourceOwnerRepairsHeldOnlyAfterExplicitSourceSelectionAndObservesLostReply(t *testing.T) {
	_, store, coord := newAPI(t)
	f, config := newResourceOwnerFixture(t, store)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		worker, err := session.NewResourceWorker(ctx, config)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		view, err := worker.Inspect(ctx)
		require.NoError(t, err)
		selector, err := resourceguard.HeldSourceSHA256(view)
		require.NoError(t, err)
		before, err := store.Load(ctx)
		require.NoError(t, err)
		f.mu.Lock()
		f.view.Actual.MemoryMaxBytes /= 2
		f.dropRepair = true
		f.mu.Unlock()
		_, err = worker.ObserveHeldRepair(ctx, selector)
		require.ErrorIs(t, err, resourceguard.ErrUnknown)
		_, err = worker.RepairHeld(ctx, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		require.Error(t, err)
		f.mu.Lock()
		require.Zero(t, f.repairs, "普通查询/错误选择不能自动修复")
		f.mu.Unlock()
		_, err = worker.RepairHeld(ctx, selector)
		var call *resourceguard.ClientError
		require.ErrorAs(t, err, &call)
		require.True(t, call.MayHaveExecuted)
		require.NotNil(t, worker.client.PendingHeldRepair())
		_, err = worker.ObserveHeldRepair(ctx, selector)
		require.NoError(t, err)
		require.Nil(t, worker.client.PendingHeldRepair())
		after, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), after.State(), "修复不创建预算/结清或推进原操作")
		f.mu.Lock()
		require.Equal(t, 1, f.repairs)
		f.view.IssuerFloorEpoch++
		f.mu.Unlock()
		_, err = worker.RepairHeld(ctx, selector)
		require.Error(t, err)
		f.mu.Lock()
		require.Equal(t, 1, f.repairs)
		f.mu.Unlock()
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}
