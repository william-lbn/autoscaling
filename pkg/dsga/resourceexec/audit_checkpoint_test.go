package resourceexec

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// 协议peer只验证checkpoint派发/落账顺序；预算/Host等前驱不是物理验收来源。
// Owner完整生产者由独立组合验证，回调返回nil本身绝不是完整预算证明。
type checkpointFixture struct {
	mu                         sync.Mutex
	executor                   *Executor
	prepared                   budget.Prepared
	vm                         *vmv1.VirtualMachine
	view                       resourceguard.View
	shared                     control.SharedCostResponse
	version, applies, observes int
	loseApply, failNative      bool
}

func checkpointReply(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	require.NoError(t, json.NewEncoder(w).Encode(v))
}
func checkpointTransport(s *httptest.Server) *http.Transport {
	roots := x509.NewCertPool()
	roots.AddCert(s.Certificate())
	return &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
}
func checkpointEncode(t *testing.T, m journal.Model) string {
	t.Helper()
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	var b bytes.Buffer
	z := zlib.NewWriter(&b)
	_, err = z.Write(raw)
	require.NoError(t, err)
	require.NoError(t, z.Close())
	sum := sha256.Sum256(raw)
	out, err := json.Marshal(struct {
		Version     uint32 `json:"version"`
		Encoding    string `json:"encoding"`
		Incarnation string `json:"incarnation"`
		SHA256      string `json:"sha256"`
		Data        []byte `json:"data"`
	}{1, "zlib-v1", m.Incarnation, hex.EncodeToString(sum[:]), b.Bytes()})
	require.NoError(t, err)
	return string(out)
}
func newCheckpointFixture(t *testing.T, kind control.Kind) *checkpointFixture {
	t.Helper()
	seed := newNormalDurabilityFixture(t)
	p := seed.prepared
	current := p.Shared
	var terminal control.ControlResponse
	if kind == control.Shrink {
		p.Request.Kind = control.Shrink
		p.Request.TargetBuffers = 256
		p.VMBeforeMemoryBytes = 2 * p.Request.TargetVMMemoryBytes
		p.Limits.MemoryMaxBytes = 128 << 20
		p.TargetSharedUpperBytes = p.Shared.CurrentMainAndArraysUpperBytes
		current.Observed = cloneCheckpoint(t, p.Shared.Observed)
		current.Observed.CapacityGeneration = p.Request.ExpectedCapacityGeneration + 1
		current.Observed.LowBuffers = 256
		current.Observed.HighBuffers = 256
		current.Observed.AdmissionOpen = false
		h, err := p.Request.ContextSHA256()
		require.NoError(t, err)
		current.Observed.Operation = &control.OperationFact{Key: p.Request.Key, Kind: kind, Phase: control.Committed, Reason: control.ReasonNone, CancelClosed: true, TargetBuffers: 256, ExpectedCapacityGeneration: p.Request.ExpectedCapacityGeneration, ContextSHA256: h}
		current.CurrentMainAndArraysUpperBytes = current.StartupMainUpperBytes
		for i := range current.Observed.Arrays {
			a := &current.Observed.Arrays[i]
			k := p.Request.Key
			a.CommittedUpperBytes = 4 << 20
			a.ReleaseState = control.ReleaseVerified
			a.ReleaseKey = &k
			a.ReleaseCapacityGeneration = current.Observed.CapacityGeneration
			a.Provider = control.ProviderFact{HasResult: true, Status: control.ProviderOK, RequestedOffset: 4096, RequestedBytes: 4096, AffectedOffset: 4096, AffectedBytes: 4096, PhysicalReleaseVerified: true}
			current.CurrentMainAndArraysUpperBytes += a.CommittedUpperBytes
		}
		snap := current.Observed
		terminal = control.ControlResponse{ProtocolVersion: 1, Receipt: control.Receipt{Key: p.Request.Key, Disposition: control.ReceiptReplayed, HasResult: true, Result: &snap}, Observed: snap}
		require.NoError(t, terminal.ValidateResize(p.Request, p.Request.Key.Instance))
	}
	require.NoError(t, p.ValidateFor(p.Request, p.Identity))
	require.NoError(t, current.ValidateFor(p.Request.Key.Instance))
	m, err := journal.New(p.Request.Key.Instance.VMUID, "1234567890abcdef1234567890abcdef", "checkpoint-owner")
	require.NoError(t, err)
	m, err = m.BindResource(m.Owner, p.Identity)
	require.NoError(t, err)
	m, err = m.BindPG(m.Owner, p.Request.Key.Instance)
	require.NoError(t, err)
	m, d, err := m.Begin(m.Owner, p.Request, 0)
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, d)
	for _, step := range m.Active.Steps {
		if step.Action == journal.GuestReady {
			break
		}
		body := []byte(`{"protocol_peer_predecessor":true}`)
		data := body
		if step.Action == journal.PrepareBudget {
			data, err = json.Marshal(p)
			require.NoError(t, err)
		}
		if kind == control.Shrink && (step.Action == journal.ResizePG || step.Action == journal.ReleasePG) {
			body, err = json.Marshal(p.Request)
			require.NoError(t, err)
			data, err = json.Marshal(struct {
				ProtocolVersion uint32                  `json:"protocol_version"`
				Fence           control.FenceResponse   `json:"issuer_fence"`
				Response        control.ControlResponse `json:"response"`
			}{1, control.FenceResponse{ProtocolVersion: 1, RequestedInstance: p.Request.Key.Instance, RequestedEpoch: 1, Disposition: control.Fenced, Observed: current.Observed}, terminal})
			require.NoError(t, err)
		}
		m, err = m.Start(m.Owner, p.Request.Key, step.Action, p.Identity.ComputeInstanceID, body)
		require.NoError(t, err)
		var s journal.Step
		for _, x := range m.Active.Steps {
			if x.Action == step.Action {
				s = x
			}
		}
		m, err = m.Record(m.Owner, p.Request.Key, journal.Observation{Key: p.Request.Key, RequestSHA256: m.Active.RequestSHA256, CommandSHA256: s.CommandSHA256, Ordinal: s.Ordinal, Action: s.Action, Result: journal.Completed, SourceID: s.TargetID, Data: data})
		require.NoError(t, err)
	}
	f := &checkpointFixture{prepared: p, shared: current, version: 1}
	f.vm = &vmv1.VirtualMachine{TypeMeta: metav1.TypeMeta{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine"}, ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "checkpoint-vm", UID: types.UID(p.Identity.VMUID), ResourceVersion: "rv-1", Annotations: map[string]string{control.ModeAnnotation: "1:" + m.Incarnation, control.JournalAnnotation: checkpointEncode(t, m)}}}
	f.vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: m.Incarnation, ComputeInstanceID: p.Identity.ComputeInstanceID, VMUID: p.Identity.VMUID, ConfigSecretUID: "config", Ready: true}
	f.vm.Status.Phase = vmv1.VmRunning
	f.vm.Status.PodName = p.RunnerPodName
	f.vm.Status.PodIP = "127.0.0.1"
	n := p.Request.TargetVMMemoryBytes
	if kind == control.Shrink {
		n = p.VMBeforeMemoryBytes
	}
	f.vm.Status.MemorySize = resource.NewQuantity(int64(n), resource.BinarySI)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Method == http.MethodPatch {
			var patch jsonpatch.Patch
			raw := new(bytes.Buffer)
			_, err := raw.ReadFrom(r.Body)
			require.NoError(t, err)
			patch, err = jsonpatch.DecodePatch(raw.Bytes())
			require.NoError(t, err)
			before, err := json.Marshal(f.vm)
			require.NoError(t, err)
			after, err := patch.Apply(before)
			if err != nil {
				http.Error(w, "CAS conflict", 409)
				return
			}
			require.NoError(t, json.Unmarshal(after, &f.vm))
			f.version++
			f.vm.ResourceVersion = time.Unix(int64(f.version), 0).String()
		}
		checkpointReply(t, w, f.vm)
	}))
	t.Cleanup(api.Close)
	generated, err := vmclient.NewForConfig(&rest.Config{Host: api.URL})
	require.NoError(t, err)
	store, err := journal.NewStore(generated.NeonvmV1().VirtualMachines("test"), journal.Binding{Namespace: "test", Name: f.vm.Name, VMUID: f.vm.UID, Incarnation: m.Incarnation})
	require.NoError(t, err)
	beforeLimits := p.Limits
	if kind == control.Shrink {
		beforeLimits.MemoryMaxBytes = 256 << 20
	}
	f.view = resourceguard.View{ProtocolVersion: 1, NormalQuotaProtocolVersion: 1, Identity: p.Identity, IssuerFloorEpoch: 1, Bootstrap: resourceguard.BootstrapCommand{ProtocolVersion: 1, Identity: p.Identity, IssuerEpoch: 1, BudgetCommitmentID: "bootstrap", Limits: beforeLimits}, BootstrapApplied: true, Birth: &resourceguard.ProcessIdentity{ProtocolVersion: 1, Identity: p.Identity, PostmasterPID: 123, PostmasterStartTicks: 10}, Actual: resourceguard.Evidence{ProtocolVersion: 1, Identity: p.Identity, MemoryMaxBytes: beforeLimits.MemoryMaxBytes, MemoryCurrentBytes: 4 << 20, GuestMemTotalBytes: n, HugePools: beforeLimits.HugePools, WholePoolUpperBytes: 16 << 20, HugeOvercommitDisabled: true, CgroupPopulated: true}}
	require.NoError(t, f.view.Validate())
	_, err = f.view.ConfirmedLimits()
	require.NoError(t, err)
	rootServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		if r.URL.Path == "/dsg/v1/resource/state" {
			v := cloneCheckpoint(t, f.view)
			f.mu.Unlock()
			checkpointReply(t, w, v)
			return
		}
		if r.URL.Path == "/dsg/v1/resource/process" {
			v := *f.view.Birth
			f.mu.Unlock()
			checkpointReply(t, w, v)
			return
		}
		f.mu.Unlock()
		require.Contains(t, []string{"/dsg/v1/resource/normal-apply", "/dsg/v1/resource/normal-observe"}, r.URL.Path)
		var e resourceguard.NormalQuotaEnvelope
		require.NoError(t, json.NewDecoder(r.Body).Decode(&e))
		require.Equal(t, p.Request, e.PGRequest)
		require.Equal(t, p.Limits.MemoryMaxBytes, e.Command.MemoryMaxBytes)
		f.mu.Lock()
		lose := false
		if r.URL.Path == "/dsg/v1/resource/normal-apply" {
			f.applies++
			c := e.Command
			f.view.Original = &c
			f.view.Held = &c
			f.view.Applied = true
			f.view.Actual.MemoryMaxBytes = c.MemoryMaxBytes
			lose = f.loseApply
		} else {
			f.observes++
		}
		actual := f.view.Actual
		f.mu.Unlock()
		if lose {
			http.Error(w, "original response lost after apply", 503)
			return
		}
		raw, err := json.Marshal(e)
		require.NoError(t, err)
		sum := sha256.Sum256(raw)
		checkpointReply(t, w, resourceguard.NormalQuotaEvidence{ProtocolVersion: 1, EnvelopeSHA256: hex.EncodeToString(sum[:]), Command: e.Command, Actual: actual})
	}))
	t.Cleanup(rootServer.Close)
	root, err := resourceguard.NewClient(resourceguard.ClientConfig{Context: context.Background(), BaseURL: rootServer.URL, Identity: p.Identity, IssuerEpoch: 1, Token: func(context.Context) (string, error) { return "fixture", nil }, Timeout: 3 * time.Second, Transport: checkpointTransport(rootServer)})
	require.NoError(t, err)
	t.Cleanup(root.CloseIdleConnections)
	pgServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dsg/v1/capabilities" {
			h := uint32(16)
			provider := true
			boot := p.Request.Key.Instance.PGBootID
			checkpointReply(t, w, control.Capabilities{ProtocolVersion: 1, Supported: true, BufferAPIVersion: 6, Enabled: true, InstanceBound: true, HistoryCapacity: &h, ProviderCanRelease: &provider, Instance: &control.DiscoveryIdentity{VMUID: p.Identity.VMUID, ComputeInstanceID: p.Identity.ComputeInstanceID, PGBootID: &boot}})
			return
		}
		f.mu.Lock()
		bad := f.failNative
		shared := cloneCheckpoint(t, f.shared)
		f.mu.Unlock()
		if bad {
			http.Error(w, "native unavailable", 503)
			return
		}
		switch r.URL.Path {
		case "/dsg/v1/issuer/fence":
			checkpointReply(t, w, control.FenceResponse{ProtocolVersion: 1, RequestedInstance: p.Request.Key.Instance, RequestedEpoch: 1, Disposition: control.Fenced, Observed: shared.Observed})
		case "/dsg/v1/budget/shared":
			checkpointReply(t, w, shared)
		default:
			t.Errorf("unexpected PG operation %s", r.URL.Path)
			http.Error(w, "forbidden", 409)
		}
	}))
	t.Cleanup(pgServer.Close)
	pg, err := compute.New(context.Background(), compute.Config{BaseURL: pgServer.URL, Instance: p.Request.Key.Instance, IssuerEpoch: 1, Token: func(context.Context) (string, error) { return "fixture", nil }, Timeout: 3 * time.Second, Transport: checkpointTransport(pgServer)})
	require.NoError(t, err)
	t.Cleanup(pg.CloseIdleConnections)
	f.executor, err = New(store, pg, root, m.Owner)
	require.NoError(t, err)
	return f
}
func cloneCheckpoint[T any](t *testing.T, v T) T {
	t.Helper()
	b, e := json.Marshal(v)
	require.NoError(t, e)
	var x T
	require.NoError(t, json.Unmarshal(b, &x))
	return x
}
func (f *checkpointFixture) snapshot(t *testing.T) *journal.Snapshot {
	t.Helper()
	s, e := f.executor.store.Load(context.Background())
	require.NoError(t, e)
	return s
}
func (f *checkpointFixture) guest(t *testing.T) journal.Step {
	t.Helper()
	for _, s := range f.snapshot(t).State().Active.Steps {
		if s.Action == journal.GuestReady {
			return s
		}
	}
	t.Fatal("missing GuestReady")
	return journal.Step{}
}
func (f *checkpointFixture) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applies, f.observes
}
func TestResourceAuditCheckpointBeforeStartStopsOriginalDispatch(t *testing.T) {
	f := newCheckpointFixture(t, control.Grow)
	sentinel := errors.New("complete source producer rejected")
	calls := 0
	p, e := f.executor.AdvanceWithAudit(context.Background(), func(context.Context, *journal.Snapshot, control.SharedCostResponse) error { calls++; return sentinel })
	if !errors.Is(e, sentinel) {
		a, _ := f.counts()
		t.Fatalf("source audit rejection did not stop original GuestReady before Start/Root Apply: applies=%d phase=%s", a, f.guest(t).Phase)
	}
	require.Equal(t, pgexec.AwaitingFact, p)
	require.Equal(t, 1, calls)
	a, o := f.counts()
	require.Zero(t, a)
	require.Zero(t, o)
	require.Equal(t, journal.Planned, f.guest(t).Phase)
}
func TestResourceAuditCheckpointsBracketApplyAndRecord(t *testing.T) {
	for _, kind := range []control.Kind{control.Grow, control.Shrink} {
		t.Run(string(kind), func(t *testing.T) {
			f := newCheckpointFixture(t, kind)
			phases := []journal.Phase{}
			p, e := f.executor.AdvanceWithAudit(context.Background(), func(ctx context.Context, s *journal.Snapshot, shared control.SharedCostResponse) error {
				require.NoError(t, ctx.Err())
				require.NoError(t, shared.ValidateFor(f.prepared.Request.Key.Instance))
				step, _ := guestStep(s.State().Active)
				require.NotNil(t, step)
				phases = append(phases, step.Phase)
				a, _ := f.counts()
				if len(phases) == 1 {
					require.Zero(t, a)
				} else {
					require.Equal(t, 1, a)
					require.Nil(t, f.executor.resources.Pending())
				}
				return nil
			})
			require.NoError(t, e)
			require.Equal(t, pgexec.Recorded, p)
			require.Equal(t, []journal.Phase{journal.Planned, journal.Unconfirmed}, phases)
			require.Equal(t, journal.Settled, f.guest(t).Phase)
			a, _ := f.counts()
			require.Equal(t, 1, a)
		})
	}
}
func TestResourceAuditPostFailureKeepsOriginalAndOnlyObserves(t *testing.T) {
	f := newCheckpointFixture(t, control.Shrink)
	sentinel := errors.New("post source rejected")
	calls := 0
	p, e := f.executor.AdvanceWithAudit(context.Background(), func(context.Context, *journal.Snapshot, control.SharedCostResponse) error {
		calls++
		if calls == 2 {
			return sentinel
		}
		return nil
	})
	require.ErrorIs(t, e, sentinel)
	require.Equal(t, pgexec.AwaitingFact, p)
	require.Equal(t, journal.Unconfirmed, f.guest(t).Phase)
	require.Equal(t, 2, calls)
	a, o := f.counts()
	require.Equal(t, 1, a)
	require.Zero(t, o)
	calls = 0
	p, e = f.executor.AdvanceWithAudit(context.Background(), func(context.Context, *journal.Snapshot, control.SharedCostResponse) error { calls++; return nil })
	require.NoError(t, e)
	require.Equal(t, pgexec.Recorded, p)
	require.Equal(t, 1, calls)
	a, o = f.counts()
	require.Equal(t, 1, a)
	require.Equal(t, 1, o)
}
func TestResourceAuditApplyUnknownNeverCallsPostOrReplays(t *testing.T) {
	f := newCheckpointFixture(t, control.Grow)
	f.loseApply = true
	calls := 0
	_, e := f.executor.AdvanceWithAudit(context.Background(), func(context.Context, *journal.Snapshot, control.SharedCostResponse) error { calls++; return nil })
	require.Error(t, e)
	require.Equal(t, 1, calls)
	require.Equal(t, journal.Unconfirmed, f.guest(t).Phase)
	require.NotNil(t, f.executor.resources.Pending())
	f.mu.Lock()
	f.loseApply = false
	f.mu.Unlock()
	p, e := f.executor.AdvanceWithAudit(context.Background(), func(context.Context, *journal.Snapshot, control.SharedCostResponse) error { calls++; return nil })
	require.NoError(t, e)
	require.Equal(t, pgexec.Recorded, p)
	require.Equal(t, 2, calls)
	a, o := f.counts()
	require.Equal(t, 1, a)
	require.Equal(t, 1, o)
}
func TestResourceAuditCheckpointRetainsCASAndCancellation(t *testing.T) {
	for _, phase := range []int{1, 2} {
		for _, change := range []string{"owner", "rv", "cancel"} {
			t.Run(time.Duration(phase).String()+change, func(t *testing.T) {
				f := newCheckpointFixture(t, control.Grow)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				_, e := f.executor.AdvanceWithAudit(ctx, func(_ context.Context, s *journal.Snapshot, _ control.SharedCostResponse) error {
					calls++
					if calls != phase {
						return nil
					}
					switch change {
					case "owner":
						_, e := f.executor.store.Takeover(ctx, s, s.State().Owner, "successor")
						require.NoError(t, e)
					case "rv":
						f.mu.Lock()
						f.vm.ResourceVersion = "changed-audit-rv"
						f.mu.Unlock()
					case "cancel":
						cancel()
					}
					return nil
				})
				require.Error(t, e)
				a, _ := f.counts()
				if phase == 1 {
					require.Zero(t, a)
					require.Equal(t, journal.Planned, f.guest(t).Phase)
				} else {
					require.Equal(t, 1, a)
					require.Equal(t, journal.Unconfirmed, f.guest(t).Phase)
				}
			})
		}
	}
}
func TestResourceAuditDefaultAndPrecheckFailureKeepOriginalPaths(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		f := newCheckpointFixture(t, control.Grow)
		p, e := f.executor.Advance(context.Background())
		require.NoError(t, e)
		require.Equal(t, pgexec.Recorded, p)
		a, _ := f.counts()
		require.Equal(t, 1, a)
	})
	t.Run("nil", func(t *testing.T) {
		f := newCheckpointFixture(t, control.Grow)
		_, e := f.executor.AdvanceWithAudit(context.Background(), nil)
		require.Error(t, e)
		require.Equal(t, journal.Planned, f.guest(t).Phase)
		a, _ := f.counts()
		require.Zero(t, a)
	})
	t.Run("native-failure", func(t *testing.T) {
		f := newCheckpointFixture(t, control.Grow)
		f.failNative = true
		calls := 0
		_, e := f.executor.AdvanceWithAudit(context.Background(), func(context.Context, *journal.Snapshot, control.SharedCostResponse) error { calls++; return nil })
		require.Error(t, e)
		require.Zero(t, calls)
		require.Equal(t, journal.Planned, f.guest(t).Phase)
		a, _ := f.counts()
		require.Zero(t, a)
	})
}
