package owner

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

// 完整原GuestReady/Prepared与count4是显式协议模型，不是Linux Source资格。
// Elector/Session/Store/原ProductionPGWorker/Run/HTTPS SDK均走正式实现。
func TestOwnedRootOnlyDurabilityPreservesClosedAndExpiredOriginal(t *testing.T) {
	for _, mode := range []string{"closed", "expired"} {
		t.Run(mode, func(t *testing.T) { verifyRootOnlyDurabilityAction(t, mode) })
	}
}
func TestOwnedRootOnlyDurabilityRejectsPartialCancelKeyAndOwner(t *testing.T) {
	for _, mode := range []string{"partial", "cancelled", "wrong-key", "wrong-owner"} {
		t.Run(mode, func(t *testing.T) { verifyRootOnlyDurabilityAction(t, mode) })
	}
}

func verifyRootOnlyDurabilityAction(t *testing.T, mode string) {
	t.Helper()
	api, store, coord := newAPI(t)
	var initialFixture struct {
		Initial budget.Initial                 `json:"initial"`
		Command budget.InitialAdmissionCommand `json:"command"`
		Receipt budget.InitialAdmissionReceipt `json:"receipt"`
	}
	// 原生期限过期后，Run 会先 Fence 并持久关闭；该合法前驱由
	// admission_expiry_test 单独验证。本例从已确认关闭开始计量管理动作，
	// 不把前驱的 PG 写误计为 Root-only 副作用，也不禁用生产关闭路径。
	raw, e := os.ReadFile("../journal/testdata/initial-domain.json")
	require.NoError(t, e)
	require.NoError(t, json.Unmarshal(raw, &initialFixture))
	root, rootConfig := guestRoot(t, store)
	qualifiedRootFixture(root)
	root.mu.Lock()
	root.view.NormalQuotaProtocolVersion = 1
	root.view.Bootstrap = initialFixture.Initial.PGAggregate.Bootstrap
	root.view.Birth = &initialFixture.Initial.PGAggregate.Birth
	root.view.Actual.MemoryMaxBytes = root.view.Bootstrap.Limits.MemoryMaxBytes
	root.view.Actual.HugePools = append([]resourceguard.HugePool(nil), root.view.Bootstrap.Limits.HugePools...)
	root.view.Actual.GuestMemTotalBytes = 2 << 30
	root.mu.Unlock()
	originalURL, err := url.Parse(rootConfig.BaseURL)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(originalURL)
	proxy.Transport = rootConfig.Transport
	var mu sync.Mutex
	var expected resourceguard.NormalQuotaEnvelope
	rootCalls, forbiddenRoot, pgWrites, leaseRenewals, legacyFences := 0, 0, 0, 0, 0
	rootServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dsg/v1/resource/normal-recover" {
			var v resourceguard.NormalQuotaEnvelope
			require.NoError(t, json.NewDecoder(r.Body).Decode(&v))
			mu.Lock()
			rootCalls++
			same := reflect.DeepEqual(v, expected)
			mu.Unlock()
			require.True(t, same, "只确认从原WAL派生的完整信封")
			if mode == "partial" {
				http.Error(w, "model original prefix count3 is not complete", 409)
				return
			}
			root.mu.Lock()
			root.view.Applied = true
			c := v.Command
			root.view.Held = &c
			actual := root.view.Actual
			root.mu.Unlock()
			raw, e := json.Marshal(v)
			require.NoError(t, e)
			sum := sha256.Sum256(raw)
			pgReply(w, resourceguard.NormalQuotaEvidence{ProtocolVersion: 1, EnvelopeSHA256: hex.EncodeToString(sum[:]), Command: v.Command, Actual: actual})
			return
		}
		if r.URL.Path != "/dsg/v1/resource/identity" && r.URL.Path != "/dsg/v1/resource/state" && r.URL.Path != "/dsg/v1/resource/fence" {
			mu.Lock()
			forbiddenRoot++
			mu.Unlock()
			t.Errorf("unexpected Root action %s", r.URL.Path)
			w.WriteHeader(409)
			return
		}
		if r.URL.Path == "/dsg/v1/resource/fence" {
			mu.Lock()
			legacyFences++
			mu.Unlock()
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(rootServer.Close)
	roots := x509.NewCertPool()
	roots.AddCert(rootServer.Certificate())
	rootConfig.BaseURL = rootServer.URL
	rootConfig.Transport = &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, e := session.NewResourceWorker(ctx, rootConfig)
		require.NoError(t, e)
		defer resources.CloseIdleConnections()
		q := pgRequest(t, session.Actor())
		q.Key.Instance = initialFixture.Initial.Instance
		q.TargetVMMemoryBytes = initialFixture.Initial.VMMemoryBytes * 2
		s := initialFixture.Initial.Shared.Observed
		s.IssuerFloorEpoch = session.Actor().Epoch
		s.AdmissionOpen = true
		s.AdmissionRevision = initialFixture.Command.Request.Revision
		root.mu.Lock()
		id, limits, birth := root.view.Identity, root.view.Bootstrap.Limits, *root.view.Birth
		root.mu.Unlock()
		shared := initialFixture.Initial.Shared
		shared.Observed = s
		fence := control.FenceResponse{ProtocolVersion: 1, RequestedInstance: q.Key.Instance, RequestedEpoch: q.Key.IssuerEpoch, Disposition: control.Fenced, Observed: s}
		upper, e := budget.GrowSharedUpper(shared, q.TargetBuffers)
		require.NoError(t, e)
		p := budget.Prepared{ProtocolVersion: 1, NormalQuotaProtocolVersion: 1, NormalQuotaFence: &fence, Request: q, Identity: id, RunnerPodName: "runner", VMBeforeMemoryBytes: initialFixture.Initial.VMMemoryBytes, GuestUsableMemoryBytes: q.TargetVMMemoryBytes, Shared: shared, TargetSharedUpperBytes: upper, Limits: limits, SystemOutsidePG: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 24 << 20, SourceID: "system"}, Transition: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "transition"}, Safety: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "safety"}}
		require.NoError(t, p.ValidateFor(q, id))
		c := resourceguard.Command{ProtocolVersion: 1, Identity: id, IssuerEpoch: q.Key.IssuerEpoch, Revision: 1, BudgetCommitmentID: q.BudgetCommitmentID, PostmasterPID: birth.PostmasterPID, PostmasterStartTicks: birth.PostmasterStartTicks, MemoryMaxBytes: limits.MemoryMaxBytes, PidsMaxTasks: limits.PidsMaxTasks, SystemMemoryMaxBytes: limits.SystemMemoryMaxBytes, AddressSpaceMaxBytes: limits.AddressSpaceMaxBytes, OutsideUpperBytes: limits.OutsideUpperBytes, HugePools: limits.HugePools}
		expired := mode != "closed"
		handler := func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/dsg/v1/admission/lease" {
				var request control.AdmissionLeaseRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				if request.Action != "observe" {
					mu.Lock()
					leaseRenewals++
					mu.Unlock()
					t.Errorf("closed/expired不得续租或rearm")
				}
				pgReply(w, control.AdmissionLeaseResponse{ProtocolVersion: 1, Request: request, Armed: true, Expired: expired, DeadlineNS: 1000000000, LeaseEpoch: q.Key.IssuerEpoch, TimeoutMS: 30000, Observed: s})
				return
			}
			mu.Lock()
			pgWrites++
			mu.Unlock()
			t.Errorf("new Root-only action must not call PG %s", r.URL.Path)
			w.WriteHeader(500)
		}
		config := pgConfigWithFence(t, q, handler, handler, func() control.Snapshot { return s })
		core := kubefake.NewSimpleClientset()
		worker, e := session.NewProductionPGWorker(ctx, config, resources, core.CoreV1())
		require.NoError(t, e)
		defer worker.CloseIdleConnections()
		snapshot, e := store.Load(ctx)
		require.NoError(t, e)
		{
			i, command, receipt := initialFixture.Initial, initialFixture.Command, initialFixture.Receipt
			i.IssuerEpoch = session.Actor().Epoch
			i.Shared.Observed.IssuerFloorEpoch = session.Actor().Epoch
			command.InitialSHA256, e = i.Digest()
			require.NoError(t, e)
			command.Request.IssuerEpoch = session.Actor().Epoch
			receipt.Command = command
			receipt.ResourceEpoch = session.Actor().Epoch
			receipt.Fence.RequestedEpoch = session.Actor().Epoch
			receipt.Shared.Observed.IssuerFloorEpoch = session.Actor().Epoch
			receipt.Fence.Observed = receipt.Shared.Observed
			receipt.Response.Request = command.Request
			receipt.Response.Observed.IssuerFloorEpoch = session.Actor().Epoch
			receipt.Response.ContextSHA256, e = command.Request.ContextSHA256()
			require.NoError(t, e)
			require.NoError(t, receipt.ValidateFor(i, command, session.Actor().Epoch))
			snapshot, e = store.BindInitialBudget(ctx, snapshot, session.Actor(), i)
			require.NoError(t, e)
			snapshot, _, e = store.StartInitialAdmission(ctx, snapshot, session.Actor(), command)
			require.NoError(t, e)
			snapshot, e = store.RecordInitialAdmission(ctx, snapshot, session.Actor(), receipt)
			require.NoError(t, e)
		}
		snapshot, disposition, e := store.Begin(ctx, snapshot, session.Actor(), q, 0)
		require.NoError(t, e)
		require.Equal(t, journal.Accepted, disposition)
		for _, step := range snapshot.State().Active.Steps {
			data := []byte(`{"explicit_protocol_model_predecessor":true}`)
			if step.Action == journal.PrepareBudget {
				data, e = json.Marshal(p)
				require.NoError(t, e)
			}
			if step.Action == journal.GuestReady {
				data, e = json.Marshal(c)
				require.NoError(t, e)
			}
			var dispatch *journal.Dispatch
			snapshot, dispatch, e = store.Start(ctx, snapshot, session.Actor(), q.Key, step.Action, id.ComputeInstanceID, data)
			require.NoError(t, e)
			if step.Action == journal.GuestReady {
				break
			}
			snapshot, e = store.Record(ctx, snapshot, session.Actor(), q.Key, journal.Observation{Key: q.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256, Ordinal: dispatch.Step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: dispatch.Step.TargetID, Data: data})
			require.NoError(t, e)
		}
		v := resourceguard.NormalQuotaEnvelope{ProtocolVersion: 1, Phase: "pre-grow", Command: c, PGRequest: q, PGFence: fence, PGShared: &shared}
		require.NoError(t, v.Validate())
		mu.Lock()
		expected = v
		mu.Unlock()
		root.mu.Lock()
		root.view.Original = &c
		root.view.Birth = &resourceguard.ProcessIdentity{ProtocolVersion: 1, Identity: id, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks}
		root.view.Actual.GuestMemTotalBytes = q.TargetVMMemoryBytes
		root.mu.Unlock()
		if mode == "cancelled" {
			// 关闭摘要必须固定已经取消的原操作，不能在关闭后改写 Active。
			snapshot, e = store.RequestAbort(ctx, snapshot, session.Actor(), q.Key)
			require.NoError(t, e)
		}
		{
			close := control.AdmissionRequest{ProtocolVersion: 1, Instance: q.Key.Instance, IssuerEpoch: q.Key.IssuerEpoch, Revision: 2, ExpectedCapacityGeneration: 1, Open: false, BudgetCommitmentID: q.BudgetCommitmentID}
			snapshot, _, e = store.StartActiveServiceClosure(ctx, snapshot, session.Actor(), close)
			require.NoError(t, e)
			s.AdmissionRevision = 2
			s.AdmissionOpen = false
			hash, e := close.ContextSHA256()
			require.NoError(t, e)
			fact := control.AdmissionResponse{ProtocolVersion: 1, Request: close, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: s}
			require.NoError(t, fact.ValidateFor(close, q.Key.Instance, false))
			snapshot, e = store.RecordServiceClosure(ctx, snapshot, session.Actor(), fact, false)
			require.NoError(t, e)
			require.Equal(t, journal.Settled, snapshot.State().ServiceClosure.Phase)
		}
		before := snapshot.State()
		api.mu.Lock()
		vmVersion := api.vmVersion
		api.mu.Unlock()
		root.mu.Lock()
		rootPosts, rootCancels := root.posts, root.cancellations
		root.mu.Unlock()
		mu.Lock()
		fenceBaseline := legacyFences
		mu.Unlock()
		endpoint := worker.NewControlEndpoint()
		loopCtx, stop := context.WithCancel(ctx)
		defer stop()
		done := make(chan error, 1)
		go func() {
			done <- worker.Run(loopCtx, PGLoopOptions{PollInterval: 50 * time.Millisecond, Control: endpoint})
		}()
		command := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(q.Key.Instance), ExpectedOwnerEpoch: session.Actor().Epoch, Action: control.CoordinationConfirmNormalQuotaDurability, Key: &q.Key}
		if mode == "wrong-key" {
			command.Key = new(control.Key)
			*command.Key = q.Key
			command.Key.Revision++
		}
		if mode == "wrong-owner" {
			command.ExpectedOwnerEpoch++
		}
		value, e := endpoint.Submit(ctx, command)
		switch mode {
		case "closed", "expired":
			require.NoError(t, e)
			require.Equal(t, pgexec.AwaitingFact, value.Progress)
		case "cancelled":
			require.ErrorContains(t, e, "cannot adopt cancelled")
		case "wrong-key":
			require.ErrorContains(t, e, "original owner or identity changed")
		case "wrong-owner":
			require.ErrorIs(t, e, ErrControlUnavailable)
		case "partial":
			require.Error(t, e)
		}
		// 原A仍拒；Root-only确认不等于解除闭门或期限，也不完成GuestReady。
		denied := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(q.Key.Instance), ExpectedOwnerEpoch: session.Actor().Epoch, Action: control.CoordinationRecoverNormalQuota, Key: &q.Key}
		_, e = endpoint.Submit(ctx, denied)
		require.ErrorIs(t, e, ErrServiceSafety)
		stop()
		require.ErrorIs(t, <-done, context.Canceled)
		after, e := store.Load(ctx)
		require.NoError(t, e)
		require.Equal(t, before, after.State())
		api.mu.Lock()
		require.Equal(t, vmVersion, api.vmVersion, "不写GuestReady或其它VM CAS")
		api.mu.Unlock()
		require.Empty(t, core.Actions(), "不触VM/预算来源写")
		mu.Lock()
		want := 0
		if mode == "closed" || mode == "expired" || mode == "partial" {
			want = 1
		}
		require.Equal(t, want, rootCalls)
		require.Zero(t, forbiddenRoot)
		require.Equal(t, fenceBaseline, legacyFences)
		require.Zero(t, pgWrites, "包括Fence/SharedCost/Resize/Admission在内的新PG请求均无")
		require.Zero(t, leaseRenewals)
		mu.Unlock()
		root.mu.Lock()
		require.Equal(t, rootPosts, root.posts)
		require.Equal(t, rootCancels, root.cancellations)
		if mode == "partial" {
			require.False(t, root.view.Applied)
		}
		require.Equal(t, limits.MemoryMaxBytes, root.view.Actual.MemoryMaxBytes)
		root.mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	require.True(t, expiredAdmissionControlAllowed(control.CoordinationConfirmNormalQuotaDurability))
	require.False(t, expiredAdmissionControlAllowed(control.CoordinationRecoverNormalQuota))
}
